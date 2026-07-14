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
    Ok(ok)
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
            // Current holders (anything not lost; stale pendings don't count).
            let mut holders: HashMap<[u8; 32], Vec<[u8; 32]>> = HashMap::new();
            let mut stmt = conn.prepare(
                "SELECT blob_hash, peer FROM placements
                 WHERE state IN ('stored', 'verified')
                    OR (state = 'pending' AND updated_at > ?1)",
            )?;
            let rows = stmt.query_map([retry_before], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (h, p) = row?;
                if let (Ok(h), Ok(p)) = (<[u8; 32]>::try_from(h), <[u8; 32]>::try_from(p)) {
                    holders.entry(h).or_default().push(p);
                }
            }
            Ok((needs, holders))
        })
        .await
        .map(|(needs, holders)| {
            needs
                .into_iter()
                .filter_map(|(hash, size, backup_ids)| {
                    let target =
                        backup_ids.iter().filter_map(|id| targets.get(id)).max().copied()?;
                    Some(BlobNeed {
                        hash,
                        size,
                        target,
                        holders: holders.get(&hash).cloned().unwrap_or_default(),
                    })
                })
                .collect::<Vec<_>>()
        })?;

    // Peers that grant us space, with how much of it we've already used.
    let peer_rows = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT p.endpoint_id, g.granted_bytes,
                        COALESCE((SELECT SUM(pl.size) FROM placements pl
                                  WHERE pl.peer = p.endpoint_id AND pl.state != 'lost'), 0)
                 FROM peers p
                 JOIN grants g ON g.peer = p.endpoint_id AND g.direction = 'received'
                 WHERE p.state = 'active' AND g.granted_bytes > 0",
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
