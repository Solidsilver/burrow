//! Control socket server: accepts unix-socket connections from the CLI and
//! dispatches framed `CtrlRequest`s.

use std::sync::Arc;

use burrow_proto::ctrl::{read_frame, write_frame, CtrlError, CtrlOk, CtrlRequest, CtrlResult};
use tokio::net::{UnixListener, UnixStream};

use crate::daemon::AppState;

pub async fn serve(state: Arc<AppState>, listener: UnixListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(state, stream).await {
                        tracing::debug!("control connection ended: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::warn!("control socket accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_conn(state: Arc<AppState>, mut stream: UnixStream) -> std::io::Result<()> {
    loop {
        let req: CtrlRequest = match read_frame(&mut stream).await {
            Ok(req) => req,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        let result: CtrlResult = dispatch(&state, req)
            .await
            .map_err(|e| CtrlError(format!("{e:#}")));
        write_frame(&mut stream, &result).await?;
    }
}

async fn dispatch(state: &Arc<AppState>, req: CtrlRequest) -> anyhow::Result<CtrlOk> {
    match req {
        CtrlRequest::Ping => Ok(CtrlOk::Pong),
        CtrlRequest::Status => Ok(CtrlOk::Status(crate::ops::status(state).await?)),
        CtrlRequest::BackupRun { backup_id } => Ok(CtrlOk::BackupDone(
            crate::ops::backup_run(state, &backup_id).await?,
        )),
        CtrlRequest::SnapshotList { backup_id } => Ok(CtrlOk::Snapshots(
            crate::ops::snapshot_list(state, backup_id).await?,
        )),
        CtrlRequest::Restore {
            backup_id,
            snapshot,
            target,
        } => {
            let (files, bytes, target) =
                crate::ops::restore(state, &backup_id, snapshot, target).await?;
            Ok(CtrlOk::RestoreDone {
                files,
                bytes,
                target,
            })
        }
        CtrlRequest::PeerInvite => Ok(CtrlOk::Ticket(crate::peers::invite(state).await?)),
        CtrlRequest::PeerAdd { ticket, name } => Ok(CtrlOk::Done(
            crate::peers::add(state, &ticket, &name).await?,
        )),
        CtrlRequest::PeerList => Ok(CtrlOk::Peers(crate::peers::list(state).await?)),
        CtrlRequest::PeerRemove { name } => {
            Ok(CtrlOk::Done(crate::peers::remove(state, &name).await?))
        }
        CtrlRequest::PendingList => {
            let (peers, space_requests) = crate::peers::pending(state).await?;
            Ok(CtrlOk::Pending {
                peers,
                space_requests,
            })
        }
        CtrlRequest::Approve { name } => {
            Ok(CtrlOk::Done(crate::peers::approve(state, &name).await?))
        }
        CtrlRequest::Deny { name } => Ok(CtrlOk::Done(crate::peers::deny(state, &name).await?)),
        CtrlRequest::Grant { name, bytes } => Ok(CtrlOk::Done(
            crate::peers::grant(state, &name, bytes).await?,
        )),
        CtrlRequest::RequestSpace { name, bytes } => Ok(CtrlOk::Done(
            crate::peers::request_space(state, &name, bytes).await?,
        )),
        CtrlRequest::Resync => Ok(CtrlOk::Done(crate::ops::resync(state).await?)),
        CtrlRequest::Pause { seconds } => {
            let until = match seconds {
                Some(s) => {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                        + s
                }
                None => u64::MAX,
            };
            *state.paused_until.lock().expect("pause lock poisoned") = Some(until);
            // Persist so a daemon restart doesn't silently resume.
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "INSERT INTO kv (key, value) VALUES ('paused_until', ?1)
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        [until.to_string()],
                    )?;
                    Ok(())
                })
                .await?;
            Ok(CtrlOk::Done(match seconds {
                Some(s) => format!("paused scheduled backups and replication for {s}s"),
                None => "paused until `burrow resume`".to_string(),
            }))
        }
        CtrlRequest::Resume => {
            *state.paused_until.lock().expect("pause lock poisoned") = None;
            state
                .db
                .call(|conn| {
                    conn.execute("DELETE FROM kv WHERE key = 'paused_until'", [])?;
                    Ok(())
                })
                .await?;
            Ok(CtrlOk::Done("resumed".to_string()))
        }
        CtrlRequest::DeviceJoin { ticket } => {
            let (reply, _) = crate::peers::hello_via_ticket(state, &ticket).await?;
            if reply.identity.owner_pk != state.owner_pk {
                anyhow::bail!(
                    "that ticket belongs to {:?}, not to you — use `burrow peer add` for friends",
                    reply.identity.owner_name
                );
            }
            Ok(CtrlOk::Done(format!(
                "linked with your device {:?} — it now recognizes this machine automatically\n\
                 note: device names must be unique among your machines. This device is\n\
                 {:?}; a second machine joining under the same name would derive the SAME\n\
                 identity and the two would be indistinguishable to your peers. If another\n\
                 machine already uses this name, re-join with --device <unique-name>.",
                reply.identity.device_name, state.device_name
            )))
        }
        CtrlRequest::RepairNow => {
            let (ok, lost) = crate::verify::verify_round(state).await?;
            let placed = crate::replicate::tick(state).await?;
            Ok(CtrlOk::Done(format!(
                "verified {ok} replicas ({lost} lost), placed {placed} new replicas"
            )))
        }
    }
}
