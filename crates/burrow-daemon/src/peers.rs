//! Owner-level peering and storage contracts.
//!
//! People (owners) are the unit of trust: approval, grants, quotas, and held
//! data are all keyed by owner. Devices are the unit of placement: they carry
//! an owner-signed certificate in Hello, so a known owner's new device is
//! recognized automatically — including our own devices, which pair with no
//! ceremony at all.

use std::sync::Arc;

use anyhow::{bail, Context};
use burrow_proto::ctrl::{DeviceInfo, PeerInfo, SpaceRequestInfo};
use burrow_proto::peer::{DeviceIdentity, HelloReply, PeerReply, PeerRequest, QuotaReply};
use burrow_proto::PROTO_VERSION;
use iroh::{EndpointAddr, EndpointId};
use iroh_tickets::endpoint::EndpointTicket;

use crate::config::DeviceMode;
use crate::daemon::AppState;
use crate::net::peer_call;

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

/// Resolved caller identity for a peer request.
pub struct Caller {
    pub owner_pk: [u8; 32],
    pub is_self: bool,
    pub is_active: bool,
}

async fn resolve_caller(state: &Arc<AppState>, remote: EndpointId) -> anyhow::Result<Option<Caller>> {
    let id = remote.as_bytes().to_vec();
    let row: Option<(Vec<u8>, String)> = state
        .db
        .call(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT d.owner_pk, o.state FROM devices d
                     JOIN owners o ON o.owner_pk = d.owner_pk
                     WHERE d.endpoint_id = ?1",
                    [&id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok())
        })
        .await?;
    Ok(row.and_then(|(pk, st)| {
        let owner_pk: [u8; 32] = pk.try_into().ok()?;
        Some(Caller {
            owner_pk,
            is_self: st == "self",
            is_active: st == "active" || st == "self",
        })
    }))
}

// ---------- ctrl-side operations ----------

pub async fn invite(state: &Arc<AppState>) -> anyhow::Result<String> {
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        state.endpoint.online(),
    )
    .await;
    let addr = state.endpoint.addr();
    Ok(EndpointTicket::from(addr).to_string())
}

/// Add a FRIEND from their ticket. (Own devices use `device join`, which is
/// automatic — see `hello_via_ticket` + the Hello handler's self branch.)
pub async fn add(state: &Arc<AppState>, ticket_str: &str, name: &str) -> anyhow::Result<String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        bail!("peer name must be non-empty [a-zA-Z0-9_-]");
    }
    let (reply, remote_id) = hello_via_ticket(state, ticket_str).await?;
    let identity = reply.identity;
    if identity.owner_pk == state.owner_pk {
        // It's one of our own devices: it was recorded by the Hello exchange.
        return Ok(format!(
            "that's your own device {:?} — linked automatically, no approval needed",
            identity.device_name
        ));
    }

    let now = now_unix();
    let owner_pk = identity.owner_pk.to_vec();
    let name_owned = name.to_string();
    let device_id = remote_id.as_bytes().to_vec();
    let (device_name, mode) = (identity.device_name.clone(), identity.mode.clone());
    let ticket_owned = ticket_str.to_string();
    state
        .db
        .call(move |conn| {
            let existing_name: Option<String> = conn
                .query_row("SELECT name FROM owners WHERE owner_pk = ?1", [&owner_pk], |r| r.get(0))
                .ok();
            match existing_name {
                Some(n) if n != name_owned => {
                    // Keep the established nickname; adding a device is fine.
                }
                _ => {
                    conn.execute(
                        "INSERT INTO owners (owner_pk, name, state, added_at, last_seen)
                         VALUES (?1, ?2, 'active', ?3, ?3)
                         ON CONFLICT(owner_pk) DO UPDATE SET state = 'active', last_seen = ?3",
                        rusqlite::params![owner_pk, name_owned, now],
                    )
                    .map_err(|e| match e {
                        rusqlite::Error::SqliteFailure(_, Some(msg)) if msg.contains("owners.name") => {
                            anyhow::anyhow!("a different person is already named {name_owned:?}")
                        }
                        e => e.into(),
                    })?;
                }
            }
            conn.execute(
                "INSERT INTO devices (endpoint_id, owner_pk, device_name, mode, ticket, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(endpoint_id) DO UPDATE SET
                   device_name = excluded.device_name, mode = excluded.mode,
                   ticket = excluded.ticket, last_seen = excluded.last_seen",
                rusqlite::params![device_id, owner_pk, device_name, mode, ticket_owned, now],
            )?;
            Ok(())
        })
        .await?;

    if reply.approved {
        Ok(format!("peer {name:?} added — they already approved us"))
    } else {
        Ok(format!(
            "peer {name:?} added ({} on {}). They now need to run `burrow approve` on their side.",
            identity.owner_name, identity.device_name
        ))
    }
}

/// Dial a ticket, exchange Hellos, verify the responder's certificate.
pub async fn hello_via_ticket(
    state: &Arc<AppState>,
    ticket_str: &str,
) -> anyhow::Result<(HelloReply, EndpointId)> {
    let ticket: EndpointTicket = ticket_str.parse().context("parsing ticket")?;
    let addr: EndpointAddr = ticket.into();
    let remote_id = addr.id;
    if remote_id == state.endpoint.id() {
        bail!("that ticket is this device's own ticket");
    }
    let req = PeerRequest::Hello {
        identity: state.identity.clone(),
        proto_version: PROTO_VERSION,
    };
    let reply = match peer_call(&state.endpoint, addr, &req).await? {
        PeerReply::Hello(h) => h,
        PeerReply::Error(e) => bail!("peer refused: {e}"),
        other => bail!("unexpected reply: {other:?}"),
    };
    if !crate::net::verify_device_cert(&reply.identity.owner_pk, remote_id, &reply.identity.cert) {
        bail!("remote device failed certificate verification — refusing to trust it");
    }
    // Record our own devices immediately (self branch of what `add` does for
    // friends); friend recording happens in `add` where the nickname is known.
    if reply.identity.owner_pk == state.owner_pk {
        record_self_device(state, remote_id, &reply.identity, Some(ticket_str)).await?;
    }
    Ok((reply, remote_id))
}

async fn record_self_device(
    state: &Arc<AppState>,
    device: EndpointId,
    identity: &DeviceIdentity,
    ticket: Option<&str>,
) -> anyhow::Result<()> {
    let now = now_unix();
    let owner_pk = state.owner_pk.to_vec();
    let device_id = device.as_bytes().to_vec();
    let (device_name, mode) = (identity.device_name.clone(), identity.mode.clone());
    let ticket = ticket.map(str::to_string);
    state
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO devices (endpoint_id, owner_pk, device_name, mode, ticket, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(endpoint_id) DO UPDATE SET
                   device_name = excluded.device_name, mode = excluded.mode,
                   ticket = COALESCE(excluded.ticket, devices.ticket),
                   last_seen = excluded.last_seen",
                rusqlite::params![device_id, owner_pk, device_name, mode, ticket, now],
            )?;
            Ok(())
        })
        .await
}

pub struct OwnerRow {
    pub owner_pk: [u8; 32],
    pub name: String,
    pub state: String,
}

pub async fn owner_by_name(state: &Arc<AppState>, name: &str) -> anyhow::Result<OwnerRow> {
    let name_owned = name.to_string();
    let row = state
        .db
        .call(move |conn| {
            conn.query_row(
                "SELECT owner_pk, name, state FROM owners WHERE name = ?1",
                [&name_owned],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    anyhow::anyhow!("no peer named {name_owned:?}")
                }
                e => e.into(),
            })
        })
        .await?;
    Ok(OwnerRow {
        owner_pk: row.0.try_into().map_err(|_| anyhow::anyhow!("corrupt owner pk in db"))?,
        name: row.1,
        state: row.2,
    })
}

/// Devices of an owner, with dial info: (endpoint_id, ticket).
async fn devices_of(
    state: &Arc<AppState>,
    owner_pk: [u8; 32],
) -> anyhow::Result<Vec<([u8; 32], Option<String>)>> {
    let pk = owner_pk.to_vec();
    state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT endpoint_id, ticket FROM devices WHERE owner_pk = ?1",
            )?;
            let rows = stmt.query_map([&pk], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<String>>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (id, ticket) = row?;
                if let Ok(id) = <[u8; 32]>::try_from(id) {
                    out.push((id, ticket));
                }
            }
            Ok(out)
        })
        .await
}

fn dial_addr(device: [u8; 32], ticket: &Option<String>) -> anyhow::Result<EndpointAddr> {
    if let Some(t) = ticket {
        if let Ok(parsed) = t.parse::<EndpointTicket>() {
            return Ok(parsed.into());
        }
    }
    Ok(EndpointId::from_bytes(&device)?.into())
}

pub async fn list(state: &Arc<AppState>) -> anyhow::Result<Vec<PeerInfo>> {
    let mut owners = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT o.owner_pk, o.name, o.state,
                        COALESCE(g.granted_bytes, 0),
                        COALESCE((SELECT SUM(h.size) FROM held h WHERE h.owner_pk = o.owner_pk), 0),
                        COALESCE((SELECT SUM(r.granted_bytes) FROM grants_received r
                                  JOIN devices d ON d.endpoint_id = r.device
                                  WHERE d.owner_pk = o.owner_pk), 0),
                        COALESCE((SELECT SUM(r.used_bytes) FROM grants_received r
                                  JOIN devices d ON d.endpoint_id = r.device
                                  WHERE d.owner_pk = o.owner_pk), 0)
                 FROM owners o
                 LEFT JOIN grants_given g ON g.owner_pk = o.owner_pk
                 ORDER BY o.state = 'self' DESC, o.name",
            )?;
            let rows = stmt.query_map([], |r| {
                let pk: Vec<u8> = r.get(0)?;
                Ok(PeerInfo {
                    owner_pk: pk.try_into().unwrap_or([0; 32]),
                    name: r.get(1)?,
                    state: r.get(2)?,
                    given_bytes: r.get(3)?,
                    given_used: r.get(4)?,
                    received_bytes: r.get(5)?,
                    received_used: r.get(6)?,
                    approved_by_them: None,
                    devices: Vec::new(),
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await?;

    // Attach devices.
    for owner in owners.iter_mut() {
        let pk = owner.owner_pk.to_vec();
        owner.devices = state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT device_name, endpoint_id, mode, last_seen FROM devices
                     WHERE owner_pk = ?1 ORDER BY device_name",
                )?;
                let rows = stmt.query_map([&pk], |r| {
                    let id: Vec<u8> = r.get(1)?;
                    Ok(DeviceInfo {
                        device_name: r.get(0)?,
                        endpoint_id: id.try_into().unwrap_or([0; 32]),
                        mode: r.get(2)?,
                        last_seen: r.get(3)?,
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
    }

    // Live refresh: probe each known device (except this one) in parallel.
    let my_id = *state.endpoint.id().as_bytes();
    let mut handles = Vec::new();
    for (oi, owner) in owners.iter().enumerate() {
        if owner.state == "pending_in" {
            continue;
        }
        for (di, dev) in owner.devices.iter().enumerate() {
            if dev.endpoint_id == my_id {
                continue;
            }
            let state = state.clone();
            let device = dev.endpoint_id;
            handles.push((oi, di, tokio::spawn(async move { refresh_device(&state, device).await })));
        }
    }
    for (oi, di, h) in handles {
        match h.await {
            Ok(Ok(quota)) => {
                owners[oi].devices[di].online = Some(true);
                owners[oi].devices[di].last_seen = Some(now_unix());
                if owners[oi].state != "self" {
                    owners[oi].approved_by_them = Some(quota.approved);
                }
            }
            _ => owners[oi].devices[di].online = Some(false),
        }
    }
    // Re-aggregate received grants after refresh.
    for owner in owners.iter_mut() {
        let pk = owner.owner_pk.to_vec();
        let (g, u) = state
            .db
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT COALESCE(SUM(r.granted_bytes), 0), COALESCE(SUM(r.used_bytes), 0)
                     FROM grants_received r JOIN devices d ON d.endpoint_id = r.device
                     WHERE d.owner_pk = ?1",
                    [&pk],
                    |r| Ok((r.get::<_, u64>(0)?, r.get::<_, u64>(1)?)),
                )?)
            })
            .await?;
        owner.received_bytes = g;
        owner.received_used = u;
    }
    Ok(owners)
}

/// Sync friend/device knowledge from our own other devices, so a friend
/// added on the NAS is known to the laptop too. Merge is additive and never
/// downgrades an owner we've already approved.
pub async fn sync_from_own_devices(state: &Arc<AppState>) -> anyhow::Result<()> {
    let my_device = *state.endpoint.id().as_bytes();
    let own_devices: Vec<([u8; 32], Option<String>)> = devices_of(state, state.owner_pk)
        .await?
        .into_iter()
        .filter(|(id, _)| *id != my_device)
        .collect();
    for (device, ticket) in own_devices {
        let Ok(addr) = dial_addr(device, &ticket) else { continue };
        let reply = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer_call(&state.endpoint, addr, &PeerRequest::SyncPeers),
        )
        .await
        {
            Ok(Ok(PeerReply::PeersSnapshot { owners, devices })) => (owners, devices),
            _ => continue,
        };
        let (owners, devices) = reply;
        let now = now_unix();
        state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                for o in &owners {
                    // Insert unknown owners; upgrade pending->active if any of
                    // our devices approved them; never touch self.
                    tx.execute(
                        "INSERT INTO owners (owner_pk, name, state, added_at)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(owner_pk) DO UPDATE SET
                           state = CASE WHEN owners.state = 'pending_in' AND excluded.state = 'active'
                                        THEN 'active' ELSE owners.state END
                         WHERE owners.state != 'self'",
                        rusqlite::params![o.owner_pk.as_slice(), o.name, o.state, now],
                    )
                    .ok(); // name collisions: keep local nickname
                }
                for d in &devices {
                    tx.execute(
                        "INSERT INTO devices (endpoint_id, owner_pk, device_name, mode, ticket)
                         SELECT ?1, ?2, ?3, ?4, ?5
                         WHERE EXISTS (SELECT 1 FROM owners WHERE owner_pk = ?2)
                         ON CONFLICT(endpoint_id) DO UPDATE SET
                           ticket = COALESCE(devices.ticket, excluded.ticket)",
                        rusqlite::params![
                            d.endpoint_id.as_slice(),
                            d.owner_pk.as_slice(),
                            d.device_name,
                            d.mode,
                            d.ticket
                        ],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await?;
    }
    Ok(())
}

/// Probe one device: QuotaStatus, persisting its grant to us and liveness.
/// If the device doesn't know us yet (learned via sync, never met), a Hello
/// introduces us first — our certificate makes recognition automatic.
pub async fn refresh_device(state: &Arc<AppState>, device: [u8; 32]) -> anyhow::Result<QuotaReply> {
    let ticket: Option<String> = {
        let id = device.to_vec();
        state
            .db
            .call(move |conn| {
                Ok(conn
                    .query_row("SELECT ticket FROM devices WHERE endpoint_id = ?1", [&id], |r| {
                        r.get(0)
                    })
                    .ok()
                    .flatten())
            })
            .await?
    };
    let addr = dial_addr(device, &ticket)?;
    let mut reply = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        peer_call(&state.endpoint, addr.clone(), &PeerRequest::QuotaStatus),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timeout"))??;
    if matches!(&reply, PeerReply::Error(e) if e.contains("say Hello first")) {
        let hello = PeerRequest::Hello {
            identity: state.identity.clone(),
            proto_version: PROTO_VERSION,
        };
        let _ = peer_call(&state.endpoint, addr.clone(), &hello).await?;
        reply = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer_call(&state.endpoint, addr, &PeerRequest::QuotaStatus),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timeout"))??;
    }
    let quota = match reply {
        PeerReply::QuotaStatus(q) => q,
        PeerReply::Error(e) => bail!("{e}"),
        other => bail!("unexpected reply: {other:?}"),
    };
    let now = now_unix();
    let id = device.to_vec();
    let (granted, used) = (quota.granted_to_you, quota.used_by_you);
    state
        .db
        .call(move |conn| {
            conn.execute(
                "UPDATE devices SET last_seen = ?2 WHERE endpoint_id = ?1",
                rusqlite::params![id, now],
            )?;
            conn.execute(
                "INSERT INTO grants_received (device, owner_pk, granted_bytes, used_bytes, updated_at)
                 SELECT endpoint_id, owner_pk, ?2, ?3, ?4 FROM devices WHERE endpoint_id = ?1
                 ON CONFLICT(device) DO UPDATE SET
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
    let row = owner_by_name(state, name).await?;
    if row.state == "self" {
        bail!("that's you — remove individual devices with `burrow device remove` (or just retire the machine)");
    }
    let pk = row.owner_pk.to_vec();
    state
        .db
        .call(move |conn| {
            conn.execute("DELETE FROM owners WHERE owner_pk = ?1", [&pk])?;
            Ok(())
        })
        .await?;
    Ok(format!(
        "peer {name:?} removed. Their data here is no longer served; your data on \
         their machines will be re-replicated elsewhere by repair."
    ))
}

pub async fn pending(
    state: &Arc<AppState>,
) -> anyhow::Result<(Vec<PeerInfo>, Vec<SpaceRequestInfo>)> {
    let owners = list(state).await?;
    let pending: Vec<PeerInfo> = owners.into_iter().filter(|o| o.state == "pending_in").collect();
    let requests = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT o.name, s.bytes, s.given_total, s.received_total, s.requested_at
                 FROM space_requests s JOIN owners o ON o.owner_pk = s.owner_pk
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
    Ok((pending, requests))
}

pub async fn approve(state: &Arc<AppState>, name: &str) -> anyhow::Result<String> {
    let row = owner_by_name(state, name).await?;
    if row.state != "pending_in" {
        return Ok(format!("peer {name:?} is already approved"));
    }
    let pk = row.owner_pk.to_vec();
    state
        .db
        .call(move |conn| {
            conn.execute("UPDATE owners SET state = 'active' WHERE owner_pk = ?1", [&pk])?;
            Ok(())
        })
        .await?;
    Ok(format!("peer {name:?} approved — all their devices are now trusted"))
}

pub async fn deny(state: &Arc<AppState>, name: &str) -> anyhow::Result<String> {
    let row = owner_by_name(state, name).await?;
    let pk = row.owner_pk.to_vec();
    if row.state == "pending_in" {
        state
            .db
            .call(move |conn| {
                conn.execute("DELETE FROM owners WHERE owner_pk = ?1", [&pk])?;
                Ok(())
            })
            .await?;
        Ok(format!("pending peer {name:?} denied and removed"))
    } else {
        let cleared = state
            .db
            .call(move |conn| {
                Ok(conn.execute("DELETE FROM space_requests WHERE owner_pk = ?1", [&pk])?)
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
    if state.config.device.mode == DeviceMode::Client {
        bail!("this device is in client mode — it doesn't host data. Grant from a host device.");
    }
    let row = owner_by_name(state, name).await?;
    if row.state == "self" {
        bail!("no grant needed for your own devices — host devices serve you automatically");
    }
    if row.state != "active" {
        bail!("peer {name:?} is not approved yet (run `burrow approve {name}`)");
    }
    if let Some(max) = &state.config.storage.offer_max {
        let ceiling = crate::config::parse_size(max)?;
        let pk = row.owner_pk.to_vec();
        let others: u64 = state
            .db
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT COALESCE(SUM(granted_bytes), 0) FROM grants_given WHERE owner_pk != ?1",
                    [&pk],
                    |r| r.get(0),
                )?)
            })
            .await?;
        if others + bytes > ceiling {
            bail!(
                "granting {} would exceed storage.offer_max ({}); {} already granted to others",
                bytes, ceiling, others
            );
        }
    }

    let pk = row.owner_pk.to_vec();
    let now = now_unix();
    let evac_window = state.config.repair.evac_window_secs();
    let shrunk_below_usage = state
        .db
        .call(move |conn| {
            let used: u64 = conn
                .query_row("SELECT COALESCE(SUM(size), 0) FROM held WHERE owner_pk = ?1", [&pk], |r| {
                    r.get(0)
                })
                .unwrap_or(0);
            let deadline: Option<u64> = if bytes < used { Some(now + evac_window) } else { None };
            conn.execute(
                "INSERT INTO grants_given (owner_pk, granted_bytes, updated_at, shrink_deadline)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(owner_pk) DO UPDATE SET
                   granted_bytes = excluded.granted_bytes,
                   updated_at = excluded.updated_at,
                   shrink_deadline = excluded.shrink_deadline",
                rusqlite::params![pk, bytes, now, deadline],
            )?;
            conn.execute("DELETE FROM space_requests WHERE owner_pk = ?1", rusqlite::params![pk])?;
            Ok(bytes < used)
        })
        .await?;

    // Best-effort notify each of their devices (each records our grant).
    let mut notified = false;
    for (device, ticket) in devices_of(state, row.owner_pk).await? {
        if let Ok(addr) = dial_addr(device, &ticket) {
            if peer_call(&state.endpoint, addr, &PeerRequest::GrantChanged { granted_bytes: bytes })
                .await
                .is_ok()
            {
                notified = true;
            }
        }
    }
    let mut msg = format!(
        "granted {} of this device's space to {name:?}{}",
        crate::config::fmt_size(bytes),
        if notified { "" } else { " (unreachable right now; they'll learn of it when back)" }
    );
    if shrunk_below_usage {
        msg.push_str(&format!(
            "\nnote: they currently use more than that — they have {} to move data elsewhere before eviction",
            state.config.repair.evac_window
        ));
    }
    Ok(msg)
}

pub async fn request_space(state: &Arc<AppState>, name: &str, bytes: u64) -> anyhow::Result<String> {
    let row = owner_by_name(state, name).await?;
    if row.state == "self" {
        bail!("your own host devices serve you automatically — no request needed");
    }
    if row.state != "active" {
        bail!("peer {name:?} is not approved yet");
    }
    let (given_total, received_total) = totals(state).await?;
    let mut last_err = None;
    for (device, ticket) in devices_of(state, row.owner_pk).await? {
        let Ok(addr) = dial_addr(device, &ticket) else { continue };
        match peer_call(
            &state.endpoint,
            addr,
            &PeerRequest::RequestSpace { bytes, given_total, received_total },
        )
        .await
        {
            Ok(PeerReply::RequestSpaceRecorded) => {
                return Ok(format!(
                    "asked {name:?} for {} — they'll see it in `burrow requests`",
                    crate::config::fmt_size(bytes)
                ))
            }
            Ok(PeerReply::Error(e)) => last_err = Some(anyhow::anyhow!("peer refused: {e}")),
            Ok(other) => last_err = Some(anyhow::anyhow!("unexpected reply: {other:?}")),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no reachable device for {name:?}")))
}

/// (bytes this device grants friends, bytes friends' devices grant my owner).
/// Own devices are excluded from the advisory ratio.
pub async fn totals(state: &Arc<AppState>) -> anyhow::Result<(u64, u64)> {
    let self_pk = state.owner_pk.to_vec();
    state
        .db
        .call(move |conn| {
            let given: u64 = conn.query_row(
                "SELECT COALESCE(SUM(granted_bytes), 0) FROM grants_given WHERE owner_pk != ?1",
                [&self_pk],
                |r| r.get(0),
            )?;
            let received: u64 = conn.query_row(
                "SELECT COALESCE(SUM(r.granted_bytes), 0) FROM grants_received r
                 JOIN devices d ON d.endpoint_id = r.device
                 WHERE d.owner_pk != ?1",
                [&self_pk],
                |r| r.get(0),
            )?;
            Ok((given, received))
        })
        .await
}

/// This device's hosting capacity available to `owner` (self gets everything
/// not promised to friends; friends get their grant).
async fn capacity_for(state: &Arc<AppState>, caller: &Caller) -> anyhow::Result<(u64, u64)> {
    let pk = caller.owner_pk.to_vec();
    let is_self = caller.is_self;
    let offer_max = state
        .config
        .storage
        .offer_max
        .as_deref()
        .map(crate::config::parse_size)
        .transpose()?;
    let disk_free = crate::sys::available_disk_bytes(&crate::paths::data_dir());
    state
        .db
        .call(move |conn| {
            let used_by_owner: u64 = conn
                .query_row("SELECT COALESCE(SUM(size), 0) FROM held WHERE owner_pk = ?1", [&pk], |r| {
                    r.get(0)
                })
                .unwrap_or(0);
            let held_total: u64 = conn
                .query_row("SELECT COALESCE(SUM(size), 0) FROM held", [], |r| r.get(0))
                .unwrap_or(0);
            let granted = if is_self {
                let friends_held = held_total - used_by_owner;
                let physical = disk_free.map(|f| f + held_total).unwrap_or(u64::MAX);
                offer_max.unwrap_or(physical).min(physical).saturating_sub(friends_held)
            } else {
                conn.query_row(
                    "SELECT granted_bytes FROM grants_given WHERE owner_pk = ?1",
                    [&pk],
                    |r| r.get(0),
                )
                .unwrap_or(0)
            };
            Ok((granted, used_by_owner))
        })
        .await
}

// ---------- PeerRequest handler ----------

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
    // Hello establishes identity; everything else requires a known caller.
    if let PeerRequest::Hello { identity, proto_version } = req {
        return handle_hello(state, remote, identity, proto_version).await;
    }
    let Some(caller) = resolve_caller(state, remote).await? else {
        return Ok(PeerReply::Error("unknown device — say Hello first".into()));
    };
    if !caller.is_active {
        return Ok(PeerReply::Error("peering not approved yet".into()));
    }
    let pk = caller.owner_pk.to_vec();

    match req {
        PeerRequest::Hello { .. } => unreachable!("handled above"),
        PeerRequest::RequestSpace { bytes, given_total, received_total } => {
            if caller.is_self {
                return Ok(PeerReply::Error("own devices don't need space requests".into()));
            }
            let now = now_unix();
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "INSERT INTO space_requests (owner_pk, bytes, given_total, received_total, requested_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(owner_pk) DO UPDATE SET
                           bytes = excluded.bytes, given_total = excluded.given_total,
                           received_total = excluded.received_total, requested_at = excluded.requested_at",
                        rusqlite::params![pk, bytes, given_total, received_total, now],
                    )?;
                    Ok(())
                })
                .await?;
            tracing::info!(peer = %remote.fmt_short(), bytes, "space requested");
            Ok(PeerReply::RequestSpaceRecorded)
        }
        PeerRequest::GrantChanged { granted_bytes } => {
            let now = now_unix();
            let device_id = remote.as_bytes().to_vec();
            let over_quota = state
                .db
                .call(move |conn| {
                    conn.execute(
                        "INSERT INTO grants_received (device, owner_pk, granted_bytes, updated_at)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(device) DO UPDATE SET
                           granted_bytes = excluded.granted_bytes, updated_at = excluded.updated_at",
                        rusqlite::params![device_id, pk, granted_bytes, now],
                    )?;
                    let placed: u64 = conn
                        .query_row(
                            "SELECT COALESCE(SUM(size), 0) FROM placements
                             WHERE device = ?1 AND state != 'lost'",
                            [&device_id],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    Ok(placed > granted_bytes)
                })
                .await?;
            tracing::info!(peer = %remote.fmt_short(), granted_bytes, "grant changed");
            if over_quota {
                let state = state.clone();
                tokio::spawn(async move {
                    let _ = crate::replicate::tick(&state).await;
                });
            }
            Ok(PeerReply::GrantChangedAck)
        }
        PeerRequest::RequestStore { hash, size, is_manifest } => {
            if state.config.device.mode == DeviceMode::Client {
                return Ok(PeerReply::Error("client-mode device: does not host data".into()));
            }
            let (granted, used) = capacity_for(state, &caller).await?;
            let already_held = {
                let pk = pk.clone();
                let h = hash.to_vec();
                state
                    .db
                    .call(move |conn| {
                        Ok(conn.query_row(
                            "SELECT COUNT(*) FROM held WHERE owner_pk = ?1 AND blob_hash = ?2",
                            rusqlite::params![pk, h],
                            |r| r.get::<_, i64>(0),
                        )? > 0)
                    })
                    .await?
            };
            // Fast refusal on the claimed size; authoritative check below.
            if !already_held && used + size > granted {
                return Ok(PeerReply::Error(format!(
                    "quota exceeded: {used} + {size} > {granted} available"
                )));
            }
            let iroh_hash = iroh_blobs::Hash::from_bytes(hash);
            // Pin the incoming blob: it isn't in `held` yet, so a GC pass
            // between fetch and the row commit would otherwise delete it
            // while we report StoreDone.
            let _gc_guard = state
                .blobs
                .tags()
                .temp_tag(iroh_blobs::HashAndFormat::raw(iroh_hash))
                .await
                .map_err(|e| anyhow::anyhow!("pinning incoming blob: {e}"))?;
            crate::net::fetch_blob(state, remote, iroh_hash)
                .await
                .map_err(|e| anyhow::anyhow!("fetching blob from you failed: {e:#}"))?;
            // Quota accounting uses the size we actually stored, not the
            // caller's claim.
            let actual_size = match state.blobs.blobs().status(iroh_hash).await? {
                iroh_blobs::api::proto::BlobStatus::Complete { size } => size,
                other => anyhow::bail!("blob incomplete after fetch: {other:?}"),
            };
            let now = now_unix();
            let h = hash.to_vec();
            // Re-check the quota inside the same transaction as the insert:
            // concurrent stores each see the other's committed usage, so a
            // pair of in-flight requests can't both squeeze under the limit.
            let accepted = state
                .db
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    let already: bool = tx.query_row(
                        "SELECT COUNT(*) FROM held WHERE owner_pk = ?1 AND blob_hash = ?2",
                        rusqlite::params![pk, h],
                        |r| r.get::<_, i64>(0),
                    )? > 0;
                    let used: u64 = tx.query_row(
                        "SELECT COALESCE(SUM(size), 0) FROM held WHERE owner_pk = ?1",
                        [&pk],
                        |r| r.get(0),
                    )?;
                    if !already && used + actual_size > granted {
                        return Ok(false);
                    }
                    tx.execute(
                        "INSERT INTO held (owner_pk, blob_hash, size, is_manifest, stored_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(owner_pk, blob_hash) DO UPDATE SET
                           size = excluded.size, is_manifest = excluded.is_manifest",
                        rusqlite::params![pk, h, actual_size, is_manifest, now],
                    )?;
                    tx.execute(
                        "UPDATE grants_given SET used_bytes =
                           (SELECT COALESCE(SUM(size), 0) FROM held WHERE owner_pk = ?1),
                           updated_at = ?2
                         WHERE owner_pk = ?1",
                        rusqlite::params![pk, now],
                    )?;
                    tx.commit()?;
                    Ok(true)
                })
                .await?;
            if !accepted {
                // Not recorded in `held`; the pin drops here and GC cleans up.
                return Ok(PeerReply::Error("quota exceeded".into()));
            }
            tracing::debug!(peer = %remote.fmt_short(), size = actual_size, "stored blob");
            Ok(PeerReply::StoreDone)
        }
        PeerRequest::Release { hashes } => {
            let now = now_unix();
            let dropped = state
                .db
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    let mut dropped = 0u32;
                    for h in &hashes {
                        dropped += tx.execute(
                            "DELETE FROM held WHERE owner_pk = ?1 AND blob_hash = ?2",
                            rusqlite::params![pk, h.as_slice()],
                        )? as u32;
                    }
                    tx.execute(
                        "UPDATE grants_given SET used_bytes =
                           (SELECT COALESCE(SUM(size), 0) FROM held WHERE owner_pk = ?1),
                           updated_at = ?2
                         WHERE owner_pk = ?1",
                        rusqlite::params![pk, now],
                    )?;
                    tx.commit()?;
                    Ok(dropped)
                })
                .await?;
            Ok(PeerReply::ReleaseAck { dropped })
        }
        PeerRequest::ListHeld { offset } => {
            let (entries, more) = state
                .db
                .call(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT blob_hash, size, is_manifest FROM held WHERE owner_pk = ?1
                         ORDER BY blob_hash LIMIT ?2 OFFSET ?3",
                    )?;
                    let page = burrow_proto::peer::HELD_PAGE;
                    let rows = stmt.query_map(rusqlite::params![pk, page + 1, offset], |r| {
                        Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u64>(1)?, r.get::<_, bool>(2)?))
                    })?;
                    let mut entries = Vec::new();
                    for row in rows {
                        let (h, size, is_manifest) = row?;
                        if let Ok(hash) = <[u8; 32]>::try_from(h) {
                            entries.push(burrow_proto::peer::HeldEntry { hash, size, is_manifest });
                        }
                    }
                    let more = entries.len() as u64 > page;
                    entries.truncate(page as usize);
                    Ok((entries, more))
                })
                .await?;
            Ok(PeerReply::HeldPage { entries, more })
        }
        PeerRequest::SyncPeers => {
            if !caller.is_self {
                return Ok(PeerReply::Error("peer sync is between your own devices only".into()));
            }
            let my_device = state.endpoint.id().as_bytes().to_vec();
            let (owners, devices) = state
                .db
                .call(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT owner_pk, name, state FROM owners WHERE state != 'self'",
                    )?;
                    let mut owners = Vec::new();
                    for row in stmt.query_map([], |r| {
                        Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
                    })? {
                        let (pk, name, st) = row?;
                        if let Ok(owner_pk) = <[u8; 32]>::try_from(pk) {
                            owners.push(burrow_proto::peer::OwnerEntry { owner_pk, name, state: st });
                        }
                    }
                    let mut stmt = conn.prepare(
                        "SELECT endpoint_id, owner_pk, device_name, mode, ticket FROM devices
                         WHERE endpoint_id != ?1",
                    )?;
                    let mut devices = Vec::new();
                    for row in stmt.query_map([&my_device], |r| {
                        Ok((
                            r.get::<_, Vec<u8>>(0)?,
                            r.get::<_, Vec<u8>>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, Option<String>>(4)?,
                        ))
                    })? {
                        let (id, pk, name, mode, ticket) = row?;
                        if let (Ok(endpoint_id), Ok(owner_pk)) =
                            (<[u8; 32]>::try_from(id), <[u8; 32]>::try_from(pk))
                        {
                            devices.push(burrow_proto::peer::DeviceEntry {
                                endpoint_id,
                                owner_pk,
                                device_name: name,
                                mode,
                                ticket,
                            });
                        }
                    }
                    Ok((owners, devices))
                })
                .await?;
            Ok(PeerReply::PeersSnapshot { owners, devices })
        }
        PeerRequest::QuotaStatus => {
            let (granted, used) = if state.config.device.mode == DeviceMode::Client {
                (0, 0)
            } else {
                capacity_for(state, &caller).await?
            };
            Ok(PeerReply::QuotaStatus(QuotaReply {
                name: state.config.node_name(),
                approved: caller.is_active,
                granted_to_you: granted,
                used_by_you: used,
            }))
        }
    }
}

async fn handle_hello(
    state: &Arc<AppState>,
    remote: EndpointId,
    identity: DeviceIdentity,
    proto_version: u32,
) -> anyhow::Result<PeerReply> {
    if proto_version != PROTO_VERSION {
        return Ok(PeerReply::Error(format!(
            "protocol version mismatch: you {proto_version}, me {PROTO_VERSION}"
        )));
    }
    if !crate::net::verify_device_cert(&identity.owner_pk, remote, &identity.cert) {
        tracing::warn!(device = %remote.fmt_short(), "Hello with INVALID device certificate");
        return Ok(PeerReply::Error("device certificate verification failed".into()));
    }

    let now = now_unix();
    let approved = if identity.owner_pk == state.owner_pk {
        // One of our own devices: recognized with no ceremony.
        record_self_device(state, remote, &identity, None).await?;
        tracing::info!(device = %identity.device_name, "own device linked");
        true
    } else {
        let pk = identity.owner_pk.to_vec();
        let device_id = remote.as_bytes().to_vec();
        let (device_name, mode, owner_name) =
            (identity.device_name.clone(), identity.mode.clone(), identity.owner_name.clone());
        let state_str: String = state
            .db
            .call(move |conn| {
                let existing: Option<String> = conn
                    .query_row("SELECT state FROM owners WHERE owner_pk = ?1", [&pk], |r| r.get(0))
                    .ok();
                let owner_state = match existing {
                    Some(s) => {
                        conn.execute(
                            "UPDATE owners SET last_seen = ?2 WHERE owner_pk = ?1",
                            rusqlite::params![pk, now],
                        )?;
                        s
                    }
                    None => {
                        // First contact: pending until a human approves the OWNER.
                        let nick: String = owner_name
                            .chars()
                            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                            .collect();
                        let nick = if nick.is_empty() { "peer".to_string() } else { nick };
                        let mut candidate = nick.clone();
                        let mut i = 2;
                        loop {
                            let taken: i64 = conn.query_row(
                                "SELECT COUNT(*) FROM owners WHERE name = ?1",
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
                            "INSERT INTO owners (owner_pk, name, state, added_at, last_seen)
                             VALUES (?1, ?2, 'pending_in', ?3, ?3)",
                            rusqlite::params![pk, candidate, now],
                        )?;
                        "pending_in".to_string()
                    }
                };
                // Device rides on the owner's standing (cert already verified).
                conn.execute(
                    "INSERT INTO devices (endpoint_id, owner_pk, device_name, mode, last_seen)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(endpoint_id) DO UPDATE SET
                       device_name = excluded.device_name, mode = excluded.mode,
                       last_seen = excluded.last_seen",
                    rusqlite::params![device_id, pk, device_name, mode, now],
                )?;
                Ok(owner_state)
            })
            .await?;
        if state_str == "pending_in" {
            tracing::info!(owner = %identity.owner_name, device = %identity.device_name, "new peering request");
        }
        state_str == "active"
    };

    Ok(PeerReply::Hello(HelloReply {
        identity: state.identity.clone(),
        proto_version: PROTO_VERSION,
        approved,
    }))
}
