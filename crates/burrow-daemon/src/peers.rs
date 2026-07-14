//! Peer lifecycle and storage contracts: pairing, approval, grants, space
//! requests — both the ctrl-side operations and the PeerRequest handler.

use std::sync::Arc;

use anyhow::{bail, Context};
use burrow_proto::ctrl::{PeerInfo, SpaceRequestInfo};
use burrow_proto::peer::{HelloReply, PeerReply, PeerRequest, QuotaReply};
use burrow_proto::PROTO_VERSION;
use iroh::{EndpointAddr, EndpointId};
use iroh_tickets::endpoint::EndpointTicket;

use crate::daemon::AppState;
use crate::net::peer_call;

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

// ---------- ctrl-side operations ----------

pub async fn invite(state: &Arc<AppState>) -> anyhow::Result<String> {
    // Make sure the ticket carries reachable addresses (relay and/or direct).
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        state.endpoint.online(),
    )
    .await;
    let addr = state.endpoint.addr();
    let ticket = EndpointTicket::from(addr);
    Ok(ticket.to_string())
}

pub async fn add(state: &Arc<AppState>, ticket_str: &str, name: &str) -> anyhow::Result<String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        bail!("peer name must be non-empty [a-zA-Z0-9_-]");
    }
    let ticket: EndpointTicket = ticket_str.parse().context("parsing ticket")?;
    let addr: EndpointAddr = ticket.into();
    let peer_id = addr.id;
    if peer_id == state.endpoint.id() {
        bail!("that ticket is this node's own ticket");
    }

    let id_bytes = peer_id.as_bytes().to_vec();
    let name_owned = name.to_string();
    let ticket_owned = ticket_str.to_string();
    let now = now_unix();
    state
        .db
        .call(move |conn| {
            // We initiated: we implicitly approve them (state 'active').
            let n = conn.execute(
                "INSERT INTO peers (endpoint_id, name, state, ticket, added_at)
                 VALUES (?1, ?2, 'active', ?3, ?4)
                 ON CONFLICT(endpoint_id) DO UPDATE SET
                   name = excluded.name, state = 'active', ticket = excluded.ticket",
                rusqlite::params![id_bytes, name_owned, ticket_owned, now],
            );
            match n {
                Ok(_) => Ok(()),
                Err(rusqlite::Error::SqliteFailure(e, Some(msg)))
                    if msg.contains("peers.name") =>
                {
                    let _ = e;
                    Err(anyhow::anyhow!("a different peer is already named {name_owned:?}"))
                }
                Err(e) => Err(e.into()),
            }
        })
        .await?;

    // Say hello using the ticket's addresses (works before discovery syncs).
    match say_hello(state, addr).await {
        Ok(reply) => {
            record_hello_result(state, peer_id, &reply).await?;
            if reply.approved {
                Ok(format!("peer {name:?} added — they already approved us"))
            } else {
                Ok(format!(
                    "peer {name:?} added ({}). They now need to run `burrow approve` on their side.",
                    reply.name
                ))
            }
        }
        Err(e) => Ok(format!(
            "peer {name:?} added, but couldn't reach them yet ({e:#}). \
             Will connect when both sides are online."
        )),
    }
}

async fn say_hello(
    state: &Arc<AppState>,
    addr: impl Into<EndpointAddr>,
) -> anyhow::Result<HelloReply> {
    let req = PeerRequest::Hello {
        name: state.config.node_name(),
        proto_version: PROTO_VERSION,
    };
    match peer_call(&state.endpoint, addr, &req).await? {
        PeerReply::Hello(h) => Ok(h),
        PeerReply::Error(e) => bail!("peer refused: {e}"),
        other => bail!("unexpected reply: {other:?}"),
    }
}

async fn record_hello_result(
    state: &Arc<AppState>,
    peer: EndpointId,
    reply: &HelloReply,
) -> anyhow::Result<()> {
    let id = peer.as_bytes().to_vec();
    let hello_name = reply.name.clone();
    let approved = reply.approved;
    let now = now_unix();
    state
        .db
        .call(move |conn| {
            conn.execute(
                "UPDATE peers SET hello_name = ?2, approved_by_them = ?3, last_seen = ?4
                 WHERE endpoint_id = ?1",
                rusqlite::params![id, hello_name, approved, now],
            )?;
            Ok(())
        })
        .await
}

/// Row-level peer record used by several ops.
pub struct PeerRow {
    pub endpoint_id: EndpointId,
    pub name: String,
    pub state: String,
    pub ticket: Option<String>,
}

pub async fn peer_by_name(state: &Arc<AppState>, name: &str) -> anyhow::Result<PeerRow> {
    let name_owned = name.to_string();
    let row = state
        .db
        .call(move |conn| {
            conn.query_row(
                "SELECT endpoint_id, name, state, ticket FROM peers WHERE name = ?1",
                [&name_owned],
                |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    anyhow::anyhow!("no peer named {name_owned:?}")
                }
                e => e.into(),
            })
        })
        .await?;
    let id_bytes: [u8; 32] =
        row.0.try_into().map_err(|_| anyhow::anyhow!("corrupt endpoint id in db"))?;
    Ok(PeerRow {
        endpoint_id: EndpointId::from_bytes(&id_bytes)?,
        name: row.1,
        state: row.2,
        ticket: row.3,
    })
}

/// Best address for dialing a peer: full ticket addr if we have one, else id.
fn dial_addr(row: &PeerRow) -> EndpointAddr {
    row.ticket
        .as_deref()
        .and_then(|t| t.parse::<EndpointTicket>().ok())
        .map(EndpointAddr::from)
        .unwrap_or_else(|| row.endpoint_id.into())
}

pub async fn list(state: &Arc<AppState>) -> anyhow::Result<Vec<PeerInfo>> {
    let mut peers = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT p.endpoint_id, p.name, p.state, p.hello_name, p.approved_by_them, p.last_seen,
                        COALESCE(g.granted_bytes, 0), COALESCE(g.used_bytes, 0),
                        COALESCE(r.granted_bytes, 0), COALESCE(r.used_bytes, 0)
                 FROM peers p
                 LEFT JOIN grants g ON g.peer = p.endpoint_id AND g.direction = 'given'
                 LEFT JOIN grants r ON r.peer = p.endpoint_id AND r.direction = 'received'
                 ORDER BY p.name",
            )?;
            let rows = stmt.query_map([], |r| {
                let id: Vec<u8> = r.get(0)?;
                Ok(PeerInfo {
                    endpoint_id: id.try_into().unwrap_or([0; 32]),
                    name: r.get(1)?,
                    state: r.get(2)?,
                    hello_name: r.get(3)?,
                    approved_by_them: r.get::<_, Option<bool>>(4)?,
                    last_seen: r.get(5)?,
                    given_bytes: r.get(6)?,
                    given_used: r.get(7)?,
                    received_bytes: r.get(8)?,
                    received_used: r.get(9)?,
                    online: None,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await?;

    // Live refresh: query active peers in parallel with a short timeout.
    let mut handles = Vec::new();
    for (i, p) in peers.iter().enumerate() {
        if p.state != "active" {
            continue;
        }
        let state = state.clone();
        let id = EndpointId::from_bytes(&p.endpoint_id)?;
        handles.push((i, tokio::spawn(async move { refresh_peer(&state, id).await })));
    }
    for (i, h) in handles {
        match h.await {
            Ok(Ok(quota)) => {
                peers[i].online = Some(true);
                peers[i].received_bytes = quota.granted_to_you;
                peers[i].received_used = quota.used_by_you;
                peers[i].approved_by_them = Some(quota.approved);
                peers[i].last_seen = Some(now_unix());
            }
            _ => peers[i].online = Some(false),
        }
    }
    Ok(peers)
}

/// Ask one peer for our quota standing and persist what they said.
async fn refresh_peer(state: &Arc<AppState>, peer: EndpointId) -> anyhow::Result<QuotaReply> {
    let reply = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        peer_call(&state.endpoint, peer, &PeerRequest::QuotaStatus),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timeout"))??;
    let quota = match reply {
        PeerReply::QuotaStatus(q) => q,
        PeerReply::Error(e) => bail!("{e}"),
        other => bail!("unexpected reply: {other:?}"),
    };
    let id = peer.as_bytes().to_vec();
    let (granted, used, approved) = (quota.granted_to_you, quota.used_by_you, quota.approved);
    let now = now_unix();
    state
        .db
        .call(move |conn| {
            conn.execute(
                "UPDATE peers SET approved_by_them = ?2, last_seen = ?3 WHERE endpoint_id = ?1",
                rusqlite::params![id, approved, now],
            )?;
            conn.execute(
                "INSERT INTO grants (peer, direction, granted_bytes, used_bytes, updated_at)
                 VALUES (?1, 'received', ?2, ?3, ?4)
                 ON CONFLICT(peer, direction) DO UPDATE SET
                   granted_bytes = excluded.granted_bytes,
                   used_bytes = excluded.used_bytes,
                   updated_at = excluded.updated_at",
                rusqlite::params![id, granted, used, now],
            )?;
            Ok(())
        })
        .await?;
    Ok(quota)
}

pub async fn remove(state: &Arc<AppState>, name: &str) -> anyhow::Result<String> {
    let row = peer_by_name(state, name).await?;
    let id = row.endpoint_id.as_bytes().to_vec();
    state
        .db
        .call(move |conn| {
            conn.execute("DELETE FROM peers WHERE endpoint_id = ?1", [&id])?;
            Ok(())
        })
        .await?;
    Ok(format!(
        "peer {name:?} removed. Data they stored here is no longer served; \
         data of yours they hold will be re-replicated elsewhere once repair lands (M5)."
    ))
}

pub async fn pending(
    state: &Arc<AppState>,
) -> anyhow::Result<(Vec<PeerInfo>, Vec<SpaceRequestInfo>)> {
    let peers = list(state).await?;
    let pending_peers: Vec<PeerInfo> =
        peers.into_iter().filter(|p| p.state == "pending_in").collect();
    let requests = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT p.name, s.bytes, s.given_total, s.received_total, s.requested_at
                 FROM space_requests s JOIN peers p ON p.endpoint_id = s.peer
                 ORDER BY s.requested_at",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok(SpaceRequestInfo {
                    peer_name: r.get(0)?,
                    bytes: r.get(1)?,
                    given_total: r.get(2)?,
                    received_total: r.get(3)?,
                    requested_at: r.get(4)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await?;
    Ok((pending_peers, requests))
}

pub async fn approve(state: &Arc<AppState>, name: &str) -> anyhow::Result<String> {
    let row = peer_by_name(state, name).await?;
    if row.state == "active" {
        return Ok(format!("peer {name:?} is already approved"));
    }
    let id = row.endpoint_id.as_bytes().to_vec();
    state
        .db
        .call(move |conn| {
            conn.execute("UPDATE peers SET state = 'active' WHERE endpoint_id = ?1", [&id])?;
            Ok(())
        })
        .await?;
    Ok(format!("peer {name:?} approved — you can now grant them space or request some"))
}

pub async fn deny(state: &Arc<AppState>, name: &str) -> anyhow::Result<String> {
    let row = peer_by_name(state, name).await?;
    let id = row.endpoint_id.as_bytes().to_vec();
    if row.state == "pending_in" {
        state
            .db
            .call(move |conn| {
                conn.execute("DELETE FROM peers WHERE endpoint_id = ?1", [&id])?;
                Ok(())
            })
            .await?;
        Ok(format!("pending peer {name:?} denied and removed"))
    } else {
        let cleared = state
            .db
            .call(move |conn| {
                Ok(conn.execute("DELETE FROM space_requests WHERE peer = ?1", [&id])?)
            })
            .await?;
        if cleared > 0 {
            Ok(format!("space request from {name:?} denied"))
        } else {
            Ok(format!("nothing pending for {name:?}"))
        }
    }
}

pub async fn grant(state: &Arc<AppState>, name: &str, bytes: u64) -> anyhow::Result<String> {
    let row = peer_by_name(state, name).await?;
    if row.state != "active" {
        bail!("peer {name:?} is not approved yet (run `burrow approve {name}`)");
    }
    // Respect the configured global ceiling if present.
    if let Some(max) = &state.config.storage.offer_max {
        let ceiling = crate::config::parse_size(max)?;
        let id = row.endpoint_id.as_bytes().to_vec();
        let others: u64 = state
            .db
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT COALESCE(SUM(granted_bytes), 0) FROM grants
                     WHERE direction = 'given' AND peer != ?1",
                    [&id],
                    |r| r.get(0),
                )?)
            })
            .await?;
        if others + bytes > ceiling {
            bail!(
                "granting {} would exceed storage.offer_max ({}); {} already granted to others",
                bytes,
                ceiling,
                others
            );
        }
    }

    let id = row.endpoint_id.as_bytes().to_vec();
    let now = now_unix();
    state
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO grants (peer, direction, granted_bytes, updated_at)
                 VALUES (?1, 'given', ?2, ?3)
                 ON CONFLICT(peer, direction) DO UPDATE SET
                   granted_bytes = excluded.granted_bytes, updated_at = excluded.updated_at",
                rusqlite::params![id, bytes, now],
            )?;
            // A grant answers any open space request.
            conn.execute("DELETE FROM space_requests WHERE peer = ?1", rusqlite::params![id])?;
            Ok(())
        })
        .await?;

    // Best-effort notification; they'll also see it on their next refresh.
    let notified = peer_call(
        &state.endpoint,
        dial_addr(&row),
        &PeerRequest::GrantChanged { granted_bytes: bytes },
    )
    .await
    .is_ok();
    Ok(format!(
        "granted {} to {name:?}{}",
        crate::config::fmt_size(bytes),
        if notified { "" } else { " (they're offline; they'll learn of it when back)" }
    ))
}

pub async fn request_space(state: &Arc<AppState>, name: &str, bytes: u64) -> anyhow::Result<String> {
    let row = peer_by_name(state, name).await?;
    if row.state != "active" {
        bail!("peer {name:?} is not approved yet");
    }
    let (given_total, received_total) = totals(state).await?;
    let reply = peer_call(
        &state.endpoint,
        dial_addr(&row),
        &PeerRequest::RequestSpace { bytes, given_total, received_total },
    )
    .await?;
    match reply {
        PeerReply::RequestSpaceRecorded => Ok(format!(
            "asked {name:?} for {} — they'll see it in `burrow requests`",
            crate::config::fmt_size(bytes)
        )),
        PeerReply::Error(e) => bail!("peer refused: {e}"),
        other => bail!("unexpected reply: {other:?}"),
    }
}

/// (bytes I grant to others, bytes others grant to me) — the advisory ratio.
pub async fn totals(state: &Arc<AppState>) -> anyhow::Result<(u64, u64)> {
    state
        .db
        .call(|conn| {
            let given: u64 = conn.query_row(
                "SELECT COALESCE(SUM(granted_bytes), 0) FROM grants WHERE direction = 'given'",
                [],
                |r| r.get(0),
            )?;
            let received: u64 = conn.query_row(
                "SELECT COALESCE(SUM(granted_bytes), 0) FROM grants WHERE direction = 'received'",
                [],
                |r| r.get(0),
            )?;
            Ok((given, received))
        })
        .await
}

// ---------- PeerRequest handler (called from net.rs with authenticated id) ----------

pub async fn handle_peer_request(
    state: &Arc<AppState>,
    remote: EndpointId,
    req: PeerRequest,
) -> PeerReply {
    match handle_inner(state, remote, req).await {
        Ok(reply) => reply,
        Err(e) => PeerReply::Error(format!("{e:#}")),
    }
}

async fn handle_inner(
    state: &Arc<AppState>,
    remote: EndpointId,
    req: PeerRequest,
) -> anyhow::Result<PeerReply> {
    let id = remote.as_bytes().to_vec();
    let peer_state: Option<String> = {
        let id = id.clone();
        state
            .db
            .call(move |conn| {
                Ok(conn
                    .query_row("SELECT state FROM peers WHERE endpoint_id = ?1", [&id], |r| {
                        r.get(0)
                    })
                    .ok())
            })
            .await?
    };
    let is_active = peer_state.as_deref() == Some("active");

    match req {
        PeerRequest::Hello { name, proto_version } => {
            if proto_version != PROTO_VERSION {
                return Ok(PeerReply::Error(format!(
                    "protocol version mismatch: you {proto_version}, me {PROTO_VERSION}"
                )));
            }
            let now = now_unix();
            if peer_state.is_none() {
                // First contact: record as pending until a human approves.
                let hello = name.clone();
                let id = id.clone();
                state
                    .db
                    .call(move |conn| {
                        // Nickname defaults to their name; disambiguate on clash.
                        let nick = hello.chars()
                            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                            .collect::<String>();
                        let nick = if nick.is_empty() { "peer".to_string() } else { nick };
                        let mut candidate = nick.clone();
                        let mut i = 2;
                        loop {
                            let taken: i64 = conn.query_row(
                                "SELECT COUNT(*) FROM peers WHERE name = ?1",
                                [&candidate],
                                |r| r.get(0),
                            )?;
                            if taken == 0 {
                                break;
                            }
                            candidate = format!("{nick}-{i}");
                            i += 1;
                        }
                        conn.execute(
                            "INSERT INTO peers (endpoint_id, name, state, hello_name, added_at, last_seen)
                             VALUES (?1, ?2, 'pending_in', ?3, ?4, ?4)",
                            rusqlite::params![id, candidate, hello, now],
                        )?;
                        Ok(())
                    })
                    .await?;
                tracing::info!(peer = %remote.fmt_short(), name, "new peering request");
            } else {
                let hello = name.clone();
                let id = id.clone();
                state
                    .db
                    .call(move |conn| {
                        conn.execute(
                            "UPDATE peers SET hello_name = ?2, last_seen = ?3 WHERE endpoint_id = ?1",
                            rusqlite::params![id, hello, now],
                        )?;
                        Ok(())
                    })
                    .await?;
            }
            Ok(PeerReply::Hello(HelloReply {
                name: state.config.node_name(),
                proto_version: PROTO_VERSION,
                approved: is_active,
            }))
        }
        PeerRequest::RequestSpace { bytes, given_total, received_total } => {
            if !is_active {
                return Ok(PeerReply::Error("peering not approved yet".into()));
            }
            let now = now_unix();
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "INSERT INTO space_requests (peer, bytes, given_total, received_total, requested_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(peer) DO UPDATE SET
                           bytes = excluded.bytes, given_total = excluded.given_total,
                           received_total = excluded.received_total, requested_at = excluded.requested_at",
                        rusqlite::params![id, bytes, given_total, received_total, now],
                    )?;
                    Ok(())
                })
                .await?;
            tracing::info!(peer = %remote.fmt_short(), bytes, "space requested");
            Ok(PeerReply::RequestSpaceRecorded)
        }
        PeerRequest::GrantChanged { granted_bytes } => {
            if !is_active {
                return Ok(PeerReply::Error("peering not approved yet".into()));
            }
            let now = now_unix();
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "INSERT INTO grants (peer, direction, granted_bytes, updated_at)
                         VALUES (?1, 'received', ?2, ?3)
                         ON CONFLICT(peer, direction) DO UPDATE SET
                           granted_bytes = excluded.granted_bytes, updated_at = excluded.updated_at",
                        rusqlite::params![id, granted_bytes, now],
                    )?;
                    Ok(())
                })
                .await?;
            tracing::info!(peer = %remote.fmt_short(), granted_bytes, "grant changed");
            Ok(PeerReply::GrantChangedAck)
        }
        PeerRequest::QuotaStatus => {
            let (granted, used) = state
                .db
                .call(move |conn| {
                    let granted: u64 = conn
                        .query_row(
                            "SELECT granted_bytes FROM grants
                             WHERE peer = ?1 AND direction = 'given'",
                            [&id],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    let used: u64 = conn
                        .query_row(
                            "SELECT COALESCE(SUM(size), 0) FROM held WHERE owner = ?1",
                            [&id],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    Ok((granted, used))
                })
                .await?;
            Ok(PeerReply::QuotaStatus(QuotaReply {
                name: state.config.node_name(),
                approved: is_active,
                granted_to_you: granted,
                used_by_you: used,
            }))
        }
    }
}
