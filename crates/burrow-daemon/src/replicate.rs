//! Replication driver: turns the core planner's decisions into RequestStore
//! calls. Runs after every backup and on a periodic tick.
//!
//! Transfers are pull-based: we ask a device to hold a blob; it fetches the
//! blob from us over iroh-blobs (its quota check, its bandwidth, iroh's
//! resumable verified streaming) and replies once the replica exists.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use burrow_core::planner::{plan, BlobNeed, DeviceId, OwnerId, PeerSpace, Placement};
use burrow_proto::peer::{PeerReply, PeerRequest};
use iroh::EndpointId;

use crate::daemon::AppState;
use crate::net::peer_call;

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

/// How long a 'pending' placement may sit before the planner retries it
/// elsewhere (device accepted but the transfer never completed).
const PENDING_RETRY_SECS: u64 = 15 * 60;

pub fn spawn_replication_loop(state: std::sync::Weak<AppState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            let Some(state) = state.upgrade() else { break };
            if state.is_paused() {
                continue;
            }
            if crate::sys::on_battery() && !state.config.device.run_on_battery {
                tracing::debug!("on battery — skipping replication tick");
                continue;
            }
            if let Err(e) = tick(&state).await {
                tracing::warn!("replication tick failed: {e:#}");
            }
        }
    });
}

/// Per-backup replica knobs, deduped per blob across backups.
fn blob_targets_for(
    state: &Arc<AppState>,
    backups_csv: &str,
) -> Option<(u32, u32)> {
    let mut target = None;
    let mut offsite = 0u32;
    for id in backups_csv.split(',') {
        if let Some(b) = state.config.backup(id) {
            target = Some(target.map_or(b.replicas, |t: u32| t.max(b.replicas)));
            offsite = offsite.max(b.min_offsite);
        }
    }
    target.map(|t| (t, offsite))
}

/// One replication pass: plan, then execute placements concurrently.
pub async fn tick(state: &Arc<AppState>) -> anyhow::Result<usize> {
    let _guard = match state.replicate_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return Ok(0), // a pass is already running
    };

    // Learn friends/devices any of our own devices know about.
    if let Err(e) = crate::peers::sync_from_own_devices(state).await {
        tracing::debug!("own-device sync failed: {e:#}");
    }

    let (blobs, peers) = gather(state).await?;
    let owner_of: HashMap<DeviceId, OwnerId> = peers.iter().map(|p| (p.id, p.owner)).collect();
    let placements = plan(&blobs, &peers, &state.owner_pk);
    if placements.is_empty() {
        if let Err(e) = rebalance(state).await {
            tracing::warn!("rebalance failed: {e:#}");
        }
        if let Err(e) = evict_overdue(state).await {
            tracing::warn!("eviction check failed: {e:#}");
        }
        return Ok(0);
    }
    tracing::info!(count = placements.len(), "replicating blobs to devices");

    // Record intent, then execute with bounded concurrency per tick.
    let now = now_unix();
    let sizes: HashMap<[u8; 32], u64> = blobs.iter().map(|b| (b.hash, b.size)).collect();
    let manifests: HashSet<[u8; 32]> = manifest_hashes(state).await?;
    {
        let rows: Vec<([u8; 32], DeviceId, OwnerId, u64)> = placements
            .iter()
            .filter_map(|p| {
                Some((p.hash, p.device, *owner_of.get(&p.device)?, *sizes.get(&p.hash)?))
            })
            .collect();
        state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                for (hash, device, owner, size) in rows {
                    tx.execute(
                        "INSERT INTO placements (blob_hash, device, owner_pk, size, state, updated_at)
                         VALUES (?1, ?2, ?3, ?4, 'pending', ?5)
                         ON CONFLICT(blob_hash, device) DO UPDATE SET
                           state = 'pending', updated_at = excluded.updated_at",
                        rusqlite::params![
                            hash.as_slice(),
                            device.as_slice(),
                            owner.as_slice(),
                            size,
                            now
                        ],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await?;
    }

    let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
    let mut handles = Vec::new();
    for p in placements.iter().cloned() {
        let state = state.clone();
        let sem = semaphore.clone();
        let size = *sizes.get(&p.hash).unwrap_or(&0);
        let is_manifest = manifests.contains(&p.hash);
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.ok()?;
            Some((p.clone(), execute_placement(&state, &p, size, is_manifest).await))
        }));
    }
    let mut ok = 0;
    for h in handles {
        if let Ok(Some((p, result))) = h.await {
            match result {
                Ok(()) => ok += 1,
                Err(e) => {
                    tracing::warn!(
                        device = %EndpointId::from_bytes(&p.device).map(|i| i.fmt_short().to_string()).unwrap_or_default(),
                        "placement failed: {e:#}"
                    );
                }
            }
        }
    }
    tracing::info!(succeeded = ok, attempted = placements.len(), "replication pass done");

    if let Err(e) = rebalance(state).await {
        tracing::warn!("rebalance failed: {e:#}");
    }
    if let Err(e) = evict_overdue(state).await {
        tracing::warn!("eviction check failed: {e:#}");
    }
    Ok(ok)
}

async fn dial_ticket(state: &Arc<AppState>, device: DeviceId) -> Option<String> {
    let id = device.to_vec();
    state
        .db
        .call(move |conn| {
            Ok(conn
                .query_row("SELECT ticket FROM devices WHERE endpoint_id = ?1", [&id], |r| r.get(0))
                .ok()
                .flatten())
        })
        .await
        .ok()
        .flatten()
}

async fn dial(state: &Arc<AppState>, device: DeviceId) -> anyhow::Result<iroh::EndpointAddr> {
    if let Some(t) = dial_ticket(state, device).await {
        if let Ok(parsed) = t.parse::<iroh_tickets::endpoint::EndpointTicket>() {
            return Ok(parsed.into());
        }
    }
    Ok(EndpointId::from_bytes(&device)?.into())
}

async fn execute_placement(
    state: &Arc<AppState>,
    p: &Placement,
    size: u64,
    is_manifest: bool,
) -> anyhow::Result<()> {
    let addr = dial(state, p.device).await?;
    let reply = peer_call(
        &state.endpoint,
        addr,
        &PeerRequest::RequestStore { hash: p.hash, size, is_manifest },
    )
    .await?;
    let now = now_unix();
    let (hash, device) = (p.hash, p.device);
    match reply {
        PeerReply::StoreDone => {
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "UPDATE placements SET state = 'stored', updated_at = ?3
                         WHERE blob_hash = ?1 AND device = ?2",
                        rusqlite::params![hash.as_slice(), device.as_slice(), now],
                    )?;
                    Ok(())
                })
                .await?;
            Ok(())
        }
        PeerReply::Error(e) => {
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "DELETE FROM placements
                         WHERE blob_hash = ?1 AND device = ?2 AND state = 'pending'",
                        rusqlite::params![hash.as_slice(), device.as_slice()],
                    )?;
                    Ok(())
                })
                .await?;
            anyhow::bail!("device refused: {e}")
        }
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

/// Work list + capacity view for the planner.
async fn gather(state: &Arc<AppState>) -> anyhow::Result<(Vec<BlobNeed>, Vec<PeerSpace>)> {
    let retry_before = now_unix().saturating_sub(PENDING_RETRY_SECS);
    let grace_cutoff = now_unix().saturating_sub(state.config.repair.grace_period_secs());
    let my_device = *state.endpoint.id().as_bytes();

    #[allow(clippy::type_complexity)]
    let (needs, mut holder_rows): (
        Vec<([u8; 32], u64, String)>,
        Vec<([u8; 32], DeviceId, OwnerId, u64, u64)>, // blob, device, owner, last_verified, size
    ) = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT blob_hash, MAX(size), GROUP_CONCAT(DISTINCT backup_id) FROM chunk_refs
                 GROUP BY blob_hash",
            )?;
            let mut needs = Vec::new();
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u64>(1)?, r.get::<_, String>(2)?))
            })?;
            for row in rows {
                let (hash, size, ids) = row?;
                if let Ok(h) = <[u8; 32]>::try_from(hash) {
                    needs.push((h, size, ids));
                }
            }
            // Holders: not lost, not stale-pending, device seen within grace.
            let mut holder_rows = Vec::new();
            let mut stmt = conn.prepare(
                "SELECT pl.blob_hash, pl.device, pl.owner_pk, COALESCE(pl.last_verified, 0), pl.size
                 FROM placements pl
                 JOIN devices d ON d.endpoint_id = pl.device
                 WHERE COALESCE(d.last_seen, 0) > ?2
                   AND (pl.state IN ('stored', 'verified')
                        OR (pl.state = 'pending' AND pl.updated_at > ?1))",
            )?;
            let rows = stmt.query_map([retry_before, grace_cutoff], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, u64>(3)?,
                    r.get::<_, u64>(4)?,
                ))
            })?;
            for row in rows {
                let (h, d, o, v, s) = row?;
                if let (Ok(h), Ok(d), Ok(o)) = (
                    <[u8; 32]>::try_from(h),
                    <[u8; 32]>::try_from(d),
                    <[u8; 32]>::try_from(o),
                ) {
                    holder_rows.push((h, d, o, v, s));
                }
            }
            Ok((needs, holder_rows))
        })
        .await?;

    // Placement candidates: host devices of active owners or self (never this
    // device itself), with the space they grant us.
    let device_rows: Vec<(Vec<u8>, Vec<u8>, u64, u64)> = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT d.endpoint_id, d.owner_pk,
                        COALESCE(r.granted_bytes, 0),
                        COALESCE((SELECT SUM(pl.size) FROM placements pl
                                  WHERE pl.device = d.endpoint_id AND pl.state != 'lost'), 0)
                 FROM devices d
                 JOIN owners o ON o.owner_pk = d.owner_pk
                 LEFT JOIN grants_received r ON r.device = d.endpoint_id
                 WHERE o.state IN ('active', 'self')
                   AND d.mode = 'host'
                   AND d.endpoint_id != ?1",
            )?;
            let rows = stmt.query_map([my_device.as_slice()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await?;

    // Over-quota devices: excess blobs (least recently verified first) stop
    // counting as holders so the planner re-places them; rebalance releases
    // them once covered elsewhere.
    for (id_bytes, _, granted, used) in &device_rows {
        let Ok(device) = <[u8; 32]>::try_from(id_bytes.as_slice()) else { continue };
        if used <= granted {
            continue;
        }
        let mut excess = used - granted;
        let mut mine: Vec<usize> = holder_rows
            .iter()
            .enumerate()
            .filter(|(_, (_, d, _, _, _))| *d == device)
            .map(|(i, _)| i)
            .collect();
        mine.sort_by_key(|&i| holder_rows[i].3);
        let mut evacuating = Vec::new();
        for i in mine {
            if excess == 0 {
                break;
            }
            excess = excess.saturating_sub(holder_rows[i].4);
            evacuating.push(i);
        }
        evacuating.sort_unstable_by(|a, b| b.cmp(a));
        for i in evacuating {
            holder_rows.swap_remove(i);
        }
    }

    let mut holders: HashMap<[u8; 32], Vec<(DeviceId, OwnerId)>> = HashMap::new();
    for (h, d, o, _, _) in &holder_rows {
        holders.entry(*h).or_default().push((*d, *o));
    }
    let blobs: Vec<BlobNeed> = needs
        .into_iter()
        .filter_map(|(hash, size, backup_ids)| {
            let (target, min_offsite) = blob_targets_for(state, &backup_ids)?;
            Some(BlobNeed {
                hash,
                size,
                target,
                min_offsite,
                holders: holders.get(&hash).cloned().unwrap_or_default(),
            })
        })
        .collect();

    // Liveness probe (also refreshes grants_received, incl. self capacity).
    let mut peers = Vec::new();
    let mut probes = Vec::new();
    for (id_bytes, owner_bytes, _, _) in device_rows {
        let (Ok(device), Ok(owner)) = (
            <[u8; 32]>::try_from(id_bytes),
            <[u8; 32]>::try_from(owner_bytes),
        ) else {
            continue;
        };
        let state = state.clone();
        probes.push(tokio::spawn(async move {
            match crate::peers::refresh_device(&state, device).await {
                Ok(q) => Some(PeerSpace {
                    id: device,
                    owner,
                    free: q.granted_to_you.saturating_sub(q.used_by_you),
                    online: true,
                }),
                Err(_) => Some(PeerSpace { id: device, owner, free: 0, online: false }),
            }
        }));
    }
    for probe in probes {
        if let Ok(Some(p)) = probe.await {
            peers.push(p);
        }
    }
    Ok((blobs, peers))
}

/// Release surplus replicas and evacuate over-granted devices, never dropping
/// below replica targets or the off-site guarantee.
async fn rebalance(state: &Arc<AppState>) -> anyhow::Result<()> {
    let grace_cutoff = now_unix().saturating_sub(state.config.repair.grace_period_secs());
    let self_owner = state.owner_pk;

    #[allow(clippy::type_complexity)]
    let (target_rows, holder_rows, grants): (
        Vec<(Vec<u8>, String)>,
        Vec<([u8; 32], DeviceId, OwnerId, u64, u64)>,
        HashMap<DeviceId, u64>,
    ) = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT blob_hash, GROUP_CONCAT(DISTINCT backup_id) FROM chunk_refs GROUP BY blob_hash",
            )?;
            let target_rows: Vec<(Vec<u8>, String)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<_, _>>()?;
            let mut stmt = conn.prepare(
                "SELECT pl.blob_hash, pl.device, pl.owner_pk, COALESCE(pl.last_verified, 0), pl.size
                 FROM placements pl
                 JOIN devices d ON d.endpoint_id = pl.device
                 WHERE pl.state IN ('stored', 'verified')
                   AND COALESCE(d.last_seen, 0) > ?1",
            )?;
            let mut holder_rows = Vec::new();
            let rows = stmt.query_map([grace_cutoff], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, u64>(3)?,
                    r.get::<_, u64>(4)?,
                ))
            })?;
            for row in rows {
                let (h, d, o, v, s) = row?;
                if let (Ok(h), Ok(d), Ok(o)) = (
                    <[u8; 32]>::try_from(h),
                    <[u8; 32]>::try_from(d),
                    <[u8; 32]>::try_from(o),
                ) {
                    holder_rows.push((h, d, o, v, s));
                }
            }
            let mut grants = HashMap::new();
            let mut stmt = conn.prepare("SELECT device, granted_bytes FROM grants_received")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u64>(1)?)))?;
            for row in rows {
                let (d, g) = row?;
                if let Ok(d) = <[u8; 32]>::try_from(d) {
                    grants.insert(d, g);
                }
            }
            Ok((target_rows, holder_rows, grants))
        })
        .await?;

    let mut blob_targets: HashMap<[u8; 32], (u32, u32)> = HashMap::new();
    for (h, ids) in target_rows {
        if let Ok(h) = <[u8; 32]>::try_from(h) {
            if let Some(t) = blob_targets_for(state, &ids) {
                blob_targets.insert(h, t);
            }
        }
    }

    let mut per_blob: HashMap<[u8; 32], Vec<(DeviceId, OwnerId, u64, u64)>> = HashMap::new();
    let mut usage: HashMap<DeviceId, u64> = HashMap::new();
    for (h, d, o, v, s) in &holder_rows {
        per_blob.entry(*h).or_default().push((*d, *o, *v, *s));
        *usage.entry(*d).or_default() += *s;
    }
    let over_quota: HashSet<DeviceId> = usage
        .iter()
        .filter(|(d, used)| **used > grants.get(*d).copied().unwrap_or(0))
        .map(|(d, _)| *d)
        .collect();

    let mut to_release: HashMap<DeviceId, Vec<[u8; 32]>> = HashMap::new();

    for (hash, holders) in &per_blob {
        let Some(&(target, min_offsite)) = blob_targets.get(hash) else {
            // Blob no longer referenced by any backup: release everywhere.
            for (d, _, _, s) in holders {
                to_release.entry(*d).or_default().push(*hash);
                *usage.get_mut(d).unwrap() -= s;
            }
            continue;
        };
        if holders.len() as u32 <= target {
            continue;
        }
        // Keep-set: within-quota preferred, then best-verified. Must include
        // at least min_offsite non-self holders when available.
        let mut sorted = holders.clone();
        sorted.sort_by(|a, b| {
            over_quota
                .contains(&a.0)
                .cmp(&over_quota.contains(&b.0))
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        let mut keep: Vec<DeviceId> = Vec::new();
        // Offsite picks first.
        for (d, o, _, _) in sorted.iter().filter(|(_, o, _, _)| *o != self_owner) {
            if (keep.len() as u32) >= min_offsite {
                break;
            }
            let _ = o;
            keep.push(*d);
        }
        for (d, _, _, _) in sorted.iter() {
            if keep.len() as u32 >= target.max(min_offsite) {
                break;
            }
            if !keep.contains(d) {
                keep.push(*d);
            }
        }
        for (d, _, _, s) in holders {
            if !keep.contains(d) {
                to_release.entry(*d).or_default().push(*hash);
                *usage.get_mut(d).unwrap() -= s;
            }
        }
    }

    // Over-quota evacuation: shed least-recently-verified blobs that are
    // safe elsewhere (target AND offsite still satisfied without this copy).
    for (device, used) in usage.clone() {
        let granted = grants.get(&device).copied().unwrap_or(0);
        if used <= granted {
            continue;
        }
        let mut excess = used - granted;
        let mut mine: Vec<([u8; 32], u64, u64, OwnerId)> = per_blob
            .iter()
            .filter_map(|(h, hs)| {
                hs.iter().find(|(d, _, _, _)| *d == device).map(|(_, o, v, s)| (*h, *v, *s, *o))
            })
            .collect();
        mine.sort_by_key(|(_, v, _, _)| *v);
        for (hash, _, size, _) in mine {
            if excess == 0 {
                break;
            }
            if to_release.get(&device).is_some_and(|v| v.contains(&hash)) {
                excess = excess.saturating_sub(size);
                continue;
            }
            let (target, min_offsite) = blob_targets.get(&hash).copied().unwrap_or((0, 0));
            let remaining: Vec<&(DeviceId, OwnerId, u64, u64)> = per_blob
                .get(&hash)
                .map(|hs| {
                    hs.iter()
                        .filter(|(d, _, _, _)| {
                            *d != device
                                && !to_release.get(d).is_some_and(|v| v.contains(&hash))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let remaining_offsite =
                remaining.iter().filter(|(_, o, _, _)| *o != self_owner).count() as u32;
            if remaining.len() as u32 >= target && remaining_offsite >= min_offsite {
                to_release.entry(device).or_default().push(hash);
                excess = excess.saturating_sub(size);
            }
        }
    }

    for (device, hashes) in to_release {
        if hashes.is_empty() {
            continue;
        }
        if let Err(e) = release_from_device(state, device, &hashes).await {
            tracing::warn!("release failed: {e:#}");
        }
    }
    Ok(())
}

pub async fn release_from_device(
    state: &Arc<AppState>,
    device: DeviceId,
    hashes: &[[u8; 32]],
) -> anyhow::Result<()> {
    let addr = dial(state, device).await?;
    let reply =
        peer_call(&state.endpoint, addr, &PeerRequest::Release { hashes: hashes.to_vec() }).await?;
    match reply {
        PeerReply::ReleaseAck { dropped } => {
            let short = EndpointId::from_bytes(&device)
                .map(|i| i.fmt_short().to_string())
                .unwrap_or_default();
            tracing::info!(device = %short, released = hashes.len(), dropped, "released replicas");
            let rows: Vec<[u8; 32]> = hashes.to_vec();
            state
                .db
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    for h in &rows {
                        tx.execute(
                            "DELETE FROM placements WHERE blob_hash = ?1 AND device = ?2",
                            rusqlite::params![h.as_slice(), device.as_slice()],
                        )?;
                    }
                    tx.commit()?;
                    Ok(())
                })
                .await?;
            Ok(())
        }
        PeerReply::Error(e) => anyhow::bail!("device refused release: {e}"),
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

/// Holder side: enforce shrink deadlines. After the evacuation window closes,
/// evict oldest-stored blobs down to the granted size. The owner's verifier
/// discovers the loss and repairs elsewhere.
async fn evict_overdue(state: &Arc<AppState>) -> anyhow::Result<()> {
    let now = now_unix();
    let victims: Vec<(Vec<u8>, u64)> = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT owner_pk, granted_bytes FROM grants_given
                 WHERE used_bytes > granted_bytes
                   AND shrink_deadline IS NOT NULL AND shrink_deadline < ?1",
            )?;
            let rows = stmt.query_map([now], |r| Ok((r.get(0)?, r.get(1)?)))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await?;

    for (owner, granted) in victims {
        let evicted = state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                let mut used: u64 = tx.query_row(
                    "SELECT COALESCE(SUM(size), 0) FROM held WHERE owner_pk = ?1",
                    [&owner],
                    |r| r.get(0),
                )?;
                let mut evicted = 0u32;
                let rows: Vec<(Vec<u8>, u64)> = tx
                    .prepare(
                        "SELECT blob_hash, size FROM held WHERE owner_pk = ?1
                         ORDER BY is_manifest ASC, stored_at ASC",
                    )?
                    .query_map([&owner], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                for (hash, size) in rows {
                    if used <= granted {
                        break;
                    }
                    tx.execute(
                        "DELETE FROM held WHERE owner_pk = ?1 AND blob_hash = ?2",
                        rusqlite::params![owner, hash],
                    )?;
                    used = used.saturating_sub(size);
                    evicted += 1;
                }
                tx.execute(
                    "UPDATE grants_given SET used_bytes = ?2,
                            shrink_deadline = CASE WHEN ?2 <= granted_bytes THEN NULL
                                                   ELSE shrink_deadline END
                     WHERE owner_pk = ?1",
                    rusqlite::params![owner, used],
                )?;
                tx.commit()?;
                Ok(evicted)
            })
            .await?;
        if evicted > 0 {
            tracing::warn!(evicted, "shrink deadline passed — evicted data down to grant");
        }
    }
    Ok(())
}

async fn manifest_hashes(state: &Arc<AppState>) -> anyhow::Result<HashSet<[u8; 32]>> {
    state
        .db
        .call(|conn| {
            let mut stmt =
                conn.prepare("SELECT DISTINCT blob_hash FROM chunk_refs WHERE is_manifest = 1")?;
            let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
            let mut out = HashSet::new();
            for row in rows {
                if let Ok(h) = <[u8; 32]>::try_from(row?) {
                    out.insert(h);
                }
            }
            Ok(out)
        })
        .await
}
