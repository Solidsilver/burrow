//! Replication driver: turns the core planner's decisions into RequestStore
//! calls. Runs after every backup and on a periodic tick.
//!
//! Transfers are pull-based: we ask a peer to hold a blob; the peer fetches it
//! from us over iroh-blobs (their quota check, their bandwidth pacing, iroh's
//! resumable verified streaming) and replies once the replica exists.

use std::collections::HashMap;
use std::sync::Arc;

use burrow_core::planner::{plan, BlobNeed, PeerSpace, Placement};
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
/// elsewhere (peer accepted but the transfer never completed).
const PENDING_RETRY_SECS: u64 = 15 * 60;

pub fn spawn_replication_loop(state: std::sync::Weak<AppState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            let Some(state) = state.upgrade() else { break };
            if let Err(e) = tick(&state).await {
                tracing::warn!("replication tick failed: {e:#}");
            }
        }
    });
}

/// One replication pass: plan, then execute placements concurrently.
pub async fn tick(state: &Arc<AppState>) -> anyhow::Result<usize> {
    let _guard = match state.replicate_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return Ok(0), // a pass is already running
    };

    let (blobs, peers) = gather(state).await?;
    let placements = plan(&blobs, &peers);
    if placements.is_empty() {
        return Ok(0);
    }
    tracing::info!(count = placements.len(), "replicating blobs to peers");

    // Record intent, then execute with bounded concurrency per tick.
    let now = now_unix();
    let sizes: HashMap<[u8; 32], u64> = blobs.iter().map(|b| (b.hash, b.size)).collect();
    let manifests: std::collections::HashSet<[u8; 32]> = manifest_hashes(state).await?;
    {
        let rows: Vec<([u8; 32], [u8; 32], u64)> = placements
            .iter()
            .map(|p| (p.hash, p.peer, *sizes.get(&p.hash).unwrap_or(&0)))
            .collect();
        state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                for (hash, peer, size) in rows {
                    tx.execute(
                        "INSERT INTO placements (blob_hash, peer, size, state, updated_at)
                         VALUES (?1, ?2, ?3, 'pending', ?4)
                         ON CONFLICT(blob_hash, peer) DO UPDATE SET
                           state = 'pending', updated_at = excluded.updated_at",
                        rusqlite::params![hash.as_slice(), peer.as_slice(), size, now],
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
                        peer = %EndpointId::from_bytes(&p.peer).map(|i| i.fmt_short().to_string()).unwrap_or_default(),
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

/// Release surplus replicas (above target) and evacuate peers whose grant to
/// us has shrunk below what we've placed there. Only releases a replica when
/// the blob still meets its target on other live holders.
async fn rebalance(state: &Arc<AppState>) -> anyhow::Result<()> {
    let targets: HashMap<String, u32> =
        state.config.backups.iter().map(|b| (b.id.clone(), b.replicas)).collect();
    let grace_cutoff = now_unix().saturating_sub(state.config.repair.grace_period_secs());

    // (blob, target) + live holder rows with verification recency + per-peer
    // received grants and usage.
    #[allow(clippy::type_complexity)]
    let (blob_targets, holder_rows, grants): (
        HashMap<[u8; 32], u32>,
        Vec<([u8; 32], [u8; 32], u64, u64)>, // blob, peer, last_verified, size
        HashMap<[u8; 32], u64>,              // peer -> granted to me
    ) = {
        let targets = targets.clone();
        state
            .db
            .call(move |conn| {
                let mut blob_targets = HashMap::new();
                let mut stmt = conn.prepare(
                    "SELECT blob_hash, GROUP_CONCAT(DISTINCT backup_id) FROM chunk_refs
                     GROUP BY blob_hash",
                )?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)))?;
                for row in rows {
                    let (h, ids) = row?;
                    if let Ok(h) = <[u8; 32]>::try_from(h) {
                        if let Some(t) =
                            ids.split(',').filter_map(|id| targets.get(id)).max().copied()
                        {
                            blob_targets.insert(h, t);
                        }
                    }
                }
                let mut stmt = conn.prepare(
                    "SELECT pl.blob_hash, pl.peer, COALESCE(pl.last_verified, 0), pl.size
                     FROM placements pl
                     JOIN peers pe ON pe.endpoint_id = pl.peer
                     WHERE pl.state IN ('stored', 'verified')
                       AND COALESCE(pe.last_seen, 0) > ?1",
                )?;
                let mut holder_rows = Vec::new();
                let rows = stmt.query_map([grace_cutoff], |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, u64>(2)?,
                        r.get::<_, u64>(3)?,
                    ))
                })?;
                for row in rows {
                    let (h, p, v, s) = row?;
                    if let (Ok(h), Ok(p)) = (<[u8; 32]>::try_from(h), <[u8; 32]>::try_from(p)) {
                        holder_rows.push((h, p, v, s));
                    }
                }
                let mut grants = HashMap::new();
                let mut stmt = conn.prepare(
                    "SELECT peer, granted_bytes FROM grants WHERE direction = 'received'",
                )?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u64>(1)?)))?;
                for row in rows {
                    let (p, g) = row?;
                    if let Ok(p) = <[u8; 32]>::try_from(p) {
                        grants.insert(p, g);
                    }
                }
                Ok((blob_targets, holder_rows, grants))
            })
            .await?
    };

    // Index holders per blob and usage per peer.
    let mut per_blob: HashMap<[u8; 32], Vec<([u8; 32], u64, u64)>> = HashMap::new();
    let mut usage: HashMap<[u8; 32], u64> = HashMap::new();
    for (h, p, v, s) in &holder_rows {
        per_blob.entry(*h).or_default().push((*p, *v, *s));
        *usage.entry(*p).or_default() += *s;
    }

    let mut to_release: HashMap<[u8; 32], Vec<[u8; 32]>> = HashMap::new(); // peer -> hashes

    // Peers whose grant to us is smaller than what we've placed there: their
    // replicas rank last when choosing which copies to keep, otherwise
    // evacuation and surplus-trim fight each other forever.
    let over_quota: std::collections::HashSet<[u8; 32]> = usage
        .iter()
        .filter(|(p, used)| **used > grants.get(*p).copied().unwrap_or(0))
        .map(|(p, _)| *p)
        .collect();

    // 1) Surplus: keep the `target` best holders (within-quota peers first,
    //    then most recently verified), release the rest.
    for (hash, holders) in &per_blob {
        let Some(&target) = blob_targets.get(hash) else {
            // Blob no longer referenced by any backup: release everywhere.
            for (p, _, s) in holders {
                to_release.entry(*p).or_default().push(*hash);
                *usage.get_mut(p).unwrap() -= s;
            }
            continue;
        };
        if holders.len() as u32 > target {
            let mut sorted = holders.clone();
            sorted.sort_by(|a, b| {
                over_quota
                    .contains(&a.0)
                    .cmp(&over_quota.contains(&b.0))
                    .then_with(|| b.1.cmp(&a.1))
                    .then_with(|| a.0.cmp(&b.0))
            });
            for (p, _, s) in sorted.iter().skip(target as usize) {
                to_release.entry(*p).or_default().push(*hash);
                *usage.get_mut(p).unwrap() -= s;
            }
        }
    }

    // 2) Over-quota evacuation: shed least-recently-verified blobs from peers
    //    we overfill, but only replicas that are still safe elsewhere.
    for (peer, used) in usage.clone() {
        let granted = grants.get(&peer).copied().unwrap_or(0);
        if used <= granted {
            continue;
        }
        let mut excess = used - granted;
        let mut mine: Vec<([u8; 32], u64, u64)> = per_blob
            .iter()
            .filter_map(|(h, hs)| {
                hs.iter().find(|(p, _, _)| *p == peer).map(|(_, v, s)| (*h, *v, *s))
            })
            .collect();
        mine.sort_by_key(|(_, v, _)| *v);
        for (hash, _, size) in mine {
            if excess == 0 {
                break;
            }
            let already = to_release.get(&peer).is_some_and(|v| v.contains(&hash));
            if already {
                excess = excess.saturating_sub(size);
                continue;
            }
            let target = blob_targets.get(&hash).copied().unwrap_or(0);
            let live_elsewhere = per_blob
                .get(&hash)
                .map(|hs| {
                    hs.iter()
                        .filter(|(p, _, _)| {
                            *p != peer
                                && !to_release.get(p).is_some_and(|v| v.contains(&hash))
                        })
                        .count() as u32
                })
                .unwrap_or(0);
            if live_elsewhere >= target {
                to_release.entry(peer).or_default().push(hash);
                excess = excess.saturating_sub(size);
            }
            // else: planner will place it elsewhere first; evacuate next tick.
        }
    }

    for (peer, hashes) in to_release {
        if hashes.is_empty() {
            continue;
        }
        if let Err(e) = release_from_peer(state, peer, &hashes).await {
            tracing::warn!("release failed: {e:#}");
        }
    }
    Ok(())
}

pub async fn release_from_peer(
    state: &Arc<AppState>,
    peer: [u8; 32],
    hashes: &[[u8; 32]],
) -> anyhow::Result<()> {
    let id = EndpointId::from_bytes(&peer)?;
    let reply = peer_call(
        &state.endpoint,
        id,
        &PeerRequest::Release { hashes: hashes.to_vec() },
    )
    .await?;
    match reply {
        PeerReply::ReleaseAck { dropped } => {
            tracing::info!(peer = %id.fmt_short(), released = hashes.len(), dropped, "released replicas");
            let rows: Vec<[u8; 32]> = hashes.to_vec();
            state
                .db
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    for h in &rows {
                        tx.execute(
                            "DELETE FROM placements WHERE blob_hash = ?1 AND peer = ?2",
                            rusqlite::params![h.as_slice(), peer.as_slice()],
                        )?;
                    }
                    tx.commit()?;
                    Ok(())
                })
                .await?;
            Ok(())
        }
        PeerReply::Error(e) => anyhow::bail!("peer refused release: {e}"),
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
                "SELECT peer, granted_bytes FROM grants
                 WHERE direction = 'given' AND used_bytes > granted_bytes
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

    for (peer, granted) in victims {
        let peer_disp = <[u8; 32]>::try_from(peer.as_slice())
            .ok()
            .and_then(|a| EndpointId::from_bytes(&a).ok())
            .map(|i| i.fmt_short().to_string())
            .unwrap_or_default();
        let evicted = state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                let mut used: u64 = tx.query_row(
                    "SELECT COALESCE(SUM(size), 0) FROM held WHERE owner = ?1",
                    [&peer],
                    |r| r.get(0),
                )?;
                let mut evicted = 0u32;
                // Oldest first; manifests last (they're the recovery entry point).
                let rows: Vec<(Vec<u8>, u64)> = tx
                    .prepare(
                        "SELECT blob_hash, size FROM held WHERE owner = ?1
                         ORDER BY is_manifest ASC, stored_at ASC",
                    )?
                    .query_map([&peer], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                for (hash, size) in rows {
                    if used <= granted {
                        break;
                    }
                    tx.execute(
                        "DELETE FROM held WHERE owner = ?1 AND blob_hash = ?2",
                        rusqlite::params![peer, hash],
                    )?;
                    used = used.saturating_sub(size);
                    evicted += 1;
                }
                tx.execute(
                    "UPDATE grants SET used_bytes = ?2,
                            shrink_deadline = CASE WHEN ?2 <= granted_bytes THEN NULL
                                                   ELSE shrink_deadline END
                     WHERE peer = ?1 AND direction = 'given'",
                    rusqlite::params![peer, used],
                )?;
                tx.commit()?;
                Ok(evicted)
            })
            .await?;
        if evicted > 0 {
            tracing::warn!(
                peer = %peer_disp,
                evicted,
                "shrink deadline passed — evicted peer data down to grant"
            );
        }
    }
    Ok(())
}

async fn execute_placement(
    state: &Arc<AppState>,
    p: &Placement,
    size: u64,
    is_manifest: bool,
) -> anyhow::Result<()> {
    let peer = EndpointId::from_bytes(&p.peer)?;
    let reply = peer_call(
        &state.endpoint,
        peer,
        &PeerRequest::RequestStore { hash: p.hash, size, is_manifest },
    )
    .await?;
    let now = now_unix();
    let (hash, peer_bytes) = (p.hash, p.peer);
    match reply {
        PeerReply::StoreDone => {
            state
                .db
                .call(move |conn| {
                    conn.execute(
                        "UPDATE placements SET state = 'stored', updated_at = ?3
                         WHERE blob_hash = ?1 AND peer = ?2",
                        rusqlite::params![hash.as_slice(), peer_bytes.as_slice(), now],
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
                         WHERE blob_hash = ?1 AND peer = ?2 AND state = 'pending'",
                        rusqlite::params![hash.as_slice(), peer_bytes.as_slice()],
                    )?;
                    Ok(())
                })
                .await?;
            anyhow::bail!("peer refused: {e}")
        }
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

/// Work list + capacity view for the planner.
async fn gather(state: &Arc<AppState>) -> anyhow::Result<(Vec<BlobNeed>, Vec<PeerSpace>)> {
    // replica target per backup id, from config.
    let targets: HashMap<String, u32> =
        state.config.backups.iter().map(|b| (b.id.clone(), b.replicas)).collect();
    let retry_before = now_unix().saturating_sub(PENDING_RETRY_SECS);
    let grace_cutoff = now_unix().saturating_sub(state.config.repair.grace_period_secs());

    let blobs = state
        .db
        .call(move |conn| {
            // Deduplicate blobs across backups, taking the max target.
            let mut stmt = conn.prepare(
                "SELECT blob_hash, MAX(size), GROUP_CONCAT(DISTINCT backup_id) FROM chunk_refs
                 GROUP BY blob_hash",
            )?;
            let mut needs: Vec<([u8; 32], u64, Vec<String>)> = Vec::new();
            let rows = stmt.query_map([], |r| {
                let hash: Vec<u8> = r.get(0)?;
                let size: u64 = r.get(1)?;
                let ids: String = r.get(2)?;
                Ok((hash, size, ids))
            })?;
            for row in rows {
                let (hash, size, ids) = row?;
                if let Ok(h) = <[u8; 32]>::try_from(hash) {
                    needs.push((h, size, ids.split(',').map(str::to_string).collect()));
                }
            }
            // Current holders: not lost, not stale-pending, and the peer has
            // been seen within the grace period (a vanished peer's replicas
            // stop counting, which is what triggers repair).
            let mut holder_rows: Vec<([u8; 32], [u8; 32], u64, u64)> = Vec::new();
            let mut stmt = conn.prepare(
                "SELECT pl.blob_hash, pl.peer, COALESCE(pl.last_verified, 0), pl.size
                 FROM placements pl
                 JOIN peers pe ON pe.endpoint_id = pl.peer
                 WHERE COALESCE(pe.last_seen, 0) > ?2
                   AND (pl.state IN ('stored', 'verified')
                        OR (pl.state = 'pending' AND pl.updated_at > ?1))",
            )?;
            let rows = stmt.query_map([retry_before, grace_cutoff], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, u64>(2)?,
                    r.get::<_, u64>(3)?,
                ))
            })?;
            for row in rows {
                let (h, p, v, s) = row?;
                if let (Ok(h), Ok(p)) = (<[u8; 32]>::try_from(h), <[u8; 32]>::try_from(p)) {
                    holder_rows.push((h, p, v, s));
                }
            }
            Ok((needs, holder_rows))
        })
        .await?;
    let (needs, mut holder_rows) = blobs;

    // Peers that grant us space (any amount, including revoked-to-zero), with
    // how much of it we've already placed.
    let peer_rows: Vec<(Vec<u8>, u64, u64)> = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT p.endpoint_id, g.granted_bytes,
                        COALESCE((SELECT SUM(pl.size) FROM placements pl
                                  WHERE pl.peer = p.endpoint_id AND pl.state != 'lost'), 0)
                 FROM peers p
                 JOIN grants g ON g.peer = p.endpoint_id AND g.direction = 'received'
                 WHERE p.state = 'active'",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u64>(1)?, r.get::<_, u64>(2)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await?;

    // Over-quota peers (their grant shrank below what we placed): their
    // excess blobs — least recently verified first — stop counting as
    // holders, so the planner places those replicas somewhere safe. The
    // rebalance pass then releases them once they're covered elsewhere.
    for (id_bytes, granted, used) in &peer_rows {
        let Ok(peer) = <[u8; 32]>::try_from(id_bytes.as_slice()) else { continue };
        if used <= granted {
            continue;
        }
        let mut excess = used - granted;
        let mut mine: Vec<usize> = holder_rows
            .iter()
            .enumerate()
            .filter(|(_, (_, p, _, _))| *p == peer)
            .map(|(i, _)| i)
            .collect();
        mine.sort_by_key(|&i| holder_rows[i].2); // last_verified asc
        let mut evacuating = Vec::new();
        for i in mine {
            if excess == 0 {
                break;
            }
            excess = excess.saturating_sub(holder_rows[i].3);
            evacuating.push(i);
        }
        // Remove in reverse index order to keep indices valid.
        evacuating.sort_unstable_by(|a, b| b.cmp(a));
        for i in evacuating {
            holder_rows.swap_remove(i);
        }
    }

    let mut holders: HashMap<[u8; 32], Vec<[u8; 32]>> = HashMap::new();
    for (h, p, _, _) in &holder_rows {
        holders.entry(*h).or_default().push(*p);
    }
    let blobs: Vec<BlobNeed> = needs
        .into_iter()
        .filter_map(|(hash, size, backup_ids)| {
            let target = backup_ids.iter().filter_map(|id| targets.get(id)).max().copied()?;
            Some(BlobNeed {
                hash,
                size,
                target,
                holders: holders.get(&hash).cloned().unwrap_or_default(),
            })
        })
        .collect();

    // Liveness probe: a peer is placeable if we can reach it right now.
    let mut peers = Vec::new();
    let mut probes = Vec::new();
    for (id_bytes, granted, used) in peer_rows {
        let Ok(id_arr) = <[u8; 32]>::try_from(id_bytes) else { continue };
        let free = granted.saturating_sub(used);
        let state = state.clone();
        probes.push(tokio::spawn(async move {
            let Ok(id) = EndpointId::from_bytes(&id_arr) else {
                return None;
            };
            let online = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                peer_call(&state.endpoint, id, &PeerRequest::QuotaStatus),
            )
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
            if online {
                // Probes are the liveness source for the grace-period rule.
                let now = now_unix();
                let _ = state
                    .db
                    .call(move |conn| {
                        conn.execute(
                            "UPDATE peers SET last_seen = ?2 WHERE endpoint_id = ?1",
                            rusqlite::params![id_arr.as_slice(), now],
                        )?;
                        Ok(())
                    })
                    .await;
            }
            Some(PeerSpace { id: id_arr, free, online })
        }));
    }
    for probe in probes {
        if let Ok(Some(p)) = probe.await {
            peers.push(p);
        }
    }
    Ok((blobs, peers))
}

async fn manifest_hashes(
    state: &Arc<AppState>,
) -> anyhow::Result<std::collections::HashSet<[u8; 32]>> {
    state
        .db
        .call(|conn| {
            let mut stmt =
                conn.prepare("SELECT DISTINCT blob_hash FROM chunk_refs WHERE is_manifest = 1")?;
            let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
            let mut out = std::collections::HashSet::new();
            for row in rows {
                if let Ok(h) = <[u8; 32]>::try_from(row?) {
                    out.insert(h);
                }
            }
            Ok(out)
        })
        .await
}
