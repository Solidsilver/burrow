//! Per-peer authorization for the iroh-blobs data plane.
//!
//! Connections are accepted only from devices of approved owners (or our
//! own). Blob GETs are served when the requesting owner owns the blob (rows
//! in `held`), when we placed it on that device (`placements`), or — for our
//! own devices — always (cross-device restore).

use std::collections::HashMap;
use std::sync::Arc;

use iroh::EndpointId;
use iroh_blobs::provider::events::{
    AbortReason, ConnectMode, EventMask, ProviderMessage, RequestMode,
};

use crate::daemon::AppState;

pub fn event_mask() -> EventMask {
    EventMask {
        connected: ConnectMode::Intercept,
        get: RequestMode::Intercept,
        ..EventMask::DEFAULT
    }
}

struct ConnInfo {
    device: EndpointId,
    owner_pk: [u8; 32],
    is_self: bool,
}

pub fn spawn_auth_loop(
    state: std::sync::Weak<AppState>,
    mut rx: tokio::sync::mpsc::Receiver<ProviderMessage>,
) {
    tokio::spawn(async move {
        // connection id -> authenticated caller, for request-level checks.
        let mut conns: HashMap<u64, ConnInfo> = HashMap::new();
        while let Some(msg) = rx.recv().await {
            let Some(state) = state.upgrade() else { break };
            match msg {
                ProviderMessage::ClientConnected(m) => {
                    let allowed = match m.endpoint_id {
                        Some(id) => match lookup_device(&state, id).await {
                            Some((owner_pk, is_self)) => {
                                conns.insert(
                                    m.connection_id,
                                    ConnInfo { device: id, owner_pk, is_self },
                                );
                                true
                            }
                            None => false,
                        },
                        None => false,
                    };
                    tracing::debug!(
                        device = ?m.endpoint_id.map(|id| id.fmt_short().to_string()),
                        allowed,
                        "blobs connection"
                    );
                    let res = if allowed { Ok(()) } else { Err(AbortReason::Permission) };
                    m.tx.send(res).await.ok();
                }
                ProviderMessage::ConnectionClosed(m) => {
                    conns.remove(&m.connection_id);
                }
                ProviderMessage::GetRequestReceived(m) => {
                    let res = match conns.get(&m.connection_id) {
                        Some(info) => {
                            let hash = m.request.hash.as_bytes();
                            // Own devices may fetch anything (restore/sync);
                            // friends fetch what they own or what we placed
                            // on that specific device.
                            if info.is_self
                                || owner_owns_blob(&state, &info.owner_pk, hash).await
                                || is_placed_on(&state, info.device, hash).await
                            {
                                Ok(())
                            } else {
                                Err(AbortReason::Permission)
                            }
                        }
                        None => Err(AbortReason::Permission),
                    };
                    m.tx.send(res).await.ok();
                }
                _ => {}
            }
        }
    });
}

/// Device of an approved owner (or self)? Returns (owner_pk, is_self).
async fn lookup_device(state: &Arc<AppState>, id: EndpointId) -> Option<([u8; 32], bool)> {
    let bytes = id.as_bytes().to_vec();
    state
        .db
        .call(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT d.owner_pk, o.state FROM devices d
                     JOIN owners o ON o.owner_pk = d.owner_pk
                     WHERE d.endpoint_id = ?1 AND o.state IN ('active', 'self')",
                    [&bytes],
                    |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
                )
                .ok())
        })
        .await
        .ok()
        .flatten()
        .and_then(|(pk, st)| {
            let owner_pk: [u8; 32] = pk.try_into().ok()?;
            Some((owner_pk, st == "self"))
        })
}

async fn owner_owns_blob(state: &Arc<AppState>, owner_pk: &[u8; 32], hash: &[u8; 32]) -> bool {
    let pk = owner_pk.to_vec();
    let hash_bytes = hash.to_vec();
    state
        .db
        .call(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM held WHERE owner_pk = ?1 AND blob_hash = ?2",
                rusqlite::params![pk, hash_bytes],
                |r| r.get::<_, i64>(0),
            )? > 0)
        })
        .await
        .unwrap_or(false)
}

async fn is_placed_on(state: &Arc<AppState>, device: EndpointId, hash: &[u8; 32]) -> bool {
    let device_bytes = device.as_bytes().to_vec();
    let hash_bytes = hash.to_vec();
    state
        .db
        .call(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM placements
                 WHERE device = ?1 AND blob_hash = ?2 AND state != 'lost'",
                rusqlite::params![device_bytes, hash_bytes],
                |r| r.get::<_, i64>(0),
            )? > 0)
        })
        .await
        .unwrap_or(false)
}
