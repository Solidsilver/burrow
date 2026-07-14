//! Spot-check verifier: prove peers still hold our blobs.
//!
//! iroh-blobs transfers are BLAKE3-verified streams, so fetching one random
//! chunk of a blob from a peer *into a throwaway store* is a cryptographic
//! proof of possession for that chunk — no extra protocol needed. Verified
//! placements update `last_verified`; failures against a reachable peer mark
//! the placement `lost`, which the planner then repairs.

use std::sync::Arc;

use iroh::EndpointId;
use iroh_blobs::protocol::{ChunkRanges, ChunkRangesExt, GetRequest};
use iroh_blobs::store::mem::MemStore;

use crate::daemon::AppState;

/// Blobs spot-checked per peer per verification round.
const CHECKS_PER_PEER: usize = 8;

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

pub fn spawn_verify_loop(state: std::sync::Weak<AppState>) {
    tokio::spawn(async move {
        loop {
            {
                let Some(state) = state.upgrade() else { break };
                let interval = state.config.repair.verify_interval_secs();
                drop(state);
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            }
            let Some(state) = state.upgrade() else { break };
            if let Err(e) = verify_round(&state).await {
                tracing::warn!("verification round failed: {e:#}");
            }
        }
    });
}

pub async fn verify_round(state: &Arc<AppState>) -> anyhow::Result<(u32, u32)> {
    // Oldest-verified placements first, grouped per device.
    let work: Vec<(Vec<u8>, Vec<(Vec<u8>, u64)>)> = state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT device FROM placements WHERE state IN ('stored', 'verified')
                 GROUP BY device",
            )?;
            let devices: Vec<Vec<u8>> = stmt
                .query_map([], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            let mut out = Vec::new();
            for device in devices {
                let mut stmt = conn.prepare(
                    "SELECT blob_hash, size FROM placements
                     WHERE device = ?1 AND state IN ('stored', 'verified')
                     ORDER BY COALESCE(last_verified, 0) ASC
                     LIMIT ?2",
                )?;
                let blobs: Vec<(Vec<u8>, u64)> = stmt
                    .query_map(rusqlite::params![device, CHECKS_PER_PEER as i64], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?
                    .collect::<Result<_, _>>()?;
                out.push((device, blobs));
            }
            Ok(out)
        })
        .await?;

    let (mut ok, mut lost) = (0u32, 0u32);
    for (peer_bytes, blobs) in work {
        let Ok(id_arr) = <[u8; 32]>::try_from(peer_bytes) else { continue };
        let Ok(peer) = EndpointId::from_bytes(&id_arr) else { continue };
        let conn = match state.endpoint.connect(peer, iroh_blobs::protocol::ALPN).await {
            Ok(c) => c,
            Err(_) => continue, // unreachable: liveness handles this, not us
        };
        // Throwaway store so verification bypasses our local blob store.
        let scratch = MemStore::new();
        for (hash_bytes, size) in blobs {
            let Ok(hash_arr) = <[u8; 32]>::try_from(hash_bytes) else { continue };
            let hash = iroh_blobs::Hash::from_bytes(hash_arr);
            // Random 1 KiB bao chunk within the blob.
            let chunk_count = size.div_ceil(1024).max(1);
            let mut rand = [0u8; 8];
            getrandom::fill(&mut rand).expect("OS RNG unavailable");
            let idx = u64::from_le_bytes(rand) % chunk_count;
            let request =
                GetRequest::builder().root(ChunkRanges::chunk(idx)).build(hash);
            let verified = scratch
                .remote()
                .execute_get(conn.clone(), request)
                .await
                .is_ok();
            let now = now_unix();
            let (h, p) = (hash_arr, id_arr);
            if verified {
                ok += 1;
                state
                    .db
                    .call(move |conn| {
                        conn.execute(
                            "UPDATE placements SET state = 'verified', last_verified = ?3,
                                    updated_at = ?3
                             WHERE blob_hash = ?1 AND device = ?2",
                            rusqlite::params![h.as_slice(), p.as_slice(), now],
                        )?;
                        Ok(())
                    })
                    .await?;
            } else {
                lost += 1;
                tracing::warn!(
                    peer = %peer.fmt_short(),
                    blob = %hash,
                    "spot check FAILED — marking placement lost"
                );
                state
                    .db
                    .call(move |conn| {
                        conn.execute(
                            "UPDATE placements SET state = 'lost', updated_at = ?3
                             WHERE blob_hash = ?1 AND device = ?2",
                            rusqlite::params![h.as_slice(), p.as_slice(), now],
                        )?;
                        Ok(())
                    })
                    .await?;
            }
        }
        // A completed round trip is also a liveness signal.
        let now = now_unix();
        state
            .db
            .call(move |conn| {
                conn.execute(
                    "UPDATE devices SET last_seen = ?2 WHERE endpoint_id = ?1",
                    rusqlite::params![id_arr.as_slice(), now],
                )?;
                Ok(())
            })
            .await?;
    }
    if lost > 0 {
        // Repair immediately rather than waiting for the next tick.
        let state = state.clone();
        tokio::spawn(async move {
            let _ = crate::replicate::tick(&state).await;
        });
    }
    tracing::debug!(ok, lost, "verification round done");
    Ok((ok, lost))
}
