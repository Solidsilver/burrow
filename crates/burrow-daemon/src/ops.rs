//! Daemon-side operations behind the control protocol: run a backup, list
//! snapshots, restore.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context};
use burrow_core::manifest::EntryKind;
use burrow_core::snapshot::{create_snapshot, restore_snapshot, SnapshotOptions};
use burrow_proto::ctrl::{BackupStatus, SnapshotInfo, StatusInfo};

use crate::blobstore::{to_iroh_hash, IrohBlobStore};
use crate::daemon::AppState;

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

pub async fn status(state: &Arc<AppState>) -> anyhow::Result<StatusInfo> {
    let mut backups = Vec::new();
    for b in &state.config.backups {
        let backup_id = b.id.clone();
        let (count, last) = state
            .db
            .call(move |conn| {
                let count: u64 = conn.query_row(
                    "SELECT COUNT(*) FROM snapshots WHERE backup_id = ?1",
                    [&backup_id],
                    |r| r.get(0),
                )?;
                let last = conn
                    .query_row(
                        "SELECT * FROM snapshots WHERE backup_id = ?1
                         ORDER BY created_at DESC LIMIT 1",
                        [&backup_id],
                        crate::db::rows::snapshot_info,
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })?;
                Ok((count, last))
            })
            .await?;
        backups.push(BackupStatus {
            backup_id: b.id.clone(),
            paths: b.paths.clone(),
            replicas: b.replicas,
            snapshot_count: count,
            last_snapshot: last,
            health: replication_health(state, &b.id, b.replicas).await?,
        });
    }
    Ok(StatusInfo {
        node_name: state.config.node_name(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        data_dir: crate::paths::data_dir(),
        endpoint_id: *state.endpoint.id().as_bytes(),
        backups,
    })
}

async fn replication_health(
    state: &Arc<AppState>,
    backup_id: &str,
    target: u32,
) -> anyhow::Result<burrow_proto::ctrl::ReplicationHealth> {
    let id = backup_id.to_string();
    state
        .db
        .call(move |conn| {
            let (total, satisfied, degraded, critical) = conn.query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(CASE WHEN cnt >= ?2 THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN cnt > 0 AND cnt < ?2 THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN cnt = 0 THEN 1 ELSE 0 END), 0)
                 FROM (SELECT (SELECT COUNT(*) FROM placements p
                               WHERE p.blob_hash = cr.blob_hash
                                 AND p.state IN ('stored', 'verified')) AS cnt
                       FROM chunk_refs cr WHERE cr.backup_id = ?1)",
                rusqlite::params![id, target],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            Ok(burrow_proto::ctrl::ReplicationHealth { total_blobs: total, satisfied, degraded, critical })
        })
        .await
}

pub async fn backup_run(state: &Arc<AppState>, backup_id: &str) -> anyhow::Result<SnapshotInfo> {
    let Some(cfg) = state.config.backup(backup_id) else {
        bail!("no backup {backup_id:?} in config");
    };
    let _guard = state
        .backup_lock
        .try_lock()
        .map_err(|_| anyhow::anyhow!("another backup is already running"))?;

    let created_at = now_unix();
    let opts = SnapshotOptions {
        backup_id: cfg.id.clone(),
        node_name: state.config.node_name(),
        created_at,
        exclude: cfg.exclude.clone(),
    };
    let roots = cfg.paths.clone();
    for root in &roots {
        if !root.exists() {
            bail!("backup path {} does not exist", root.display());
        }
    }

    let store = state.blobs.clone();
    let repo_key = state.repo_key.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut adapter = IrohBlobStore::new(store);
        create_snapshot(&mut adapter, &repo_key, &roots, &opts)
    })
    .await
    .context("backup task panicked")??;

    // Pin the manifest so GC can never collect a snapshot's entry point.
    let tag_name = format!("snapshot/{}/{}", cfg.id, created_at);
    state
        .blobs
        .tags()
        .set(tag_name, to_iroh_hash(&result.manifest_hash))
        .await
        .context("tagging snapshot manifest")?;

    let file_count = result
        .manifest
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::File { .. }))
        .count() as u64;
    let info = SnapshotInfo {
        backup_id: cfg.id.clone(),
        created_at,
        manifest_hash: result.manifest_hash.0,
        file_count,
        bytes_scanned: result.bytes_scanned,
        bytes_new: result.bytes_new,
        chunk_count: result.manifest.referenced_blobs().len() as u64,
    };

    // A deterministic pipeline means an unchanged tree snapshotted at the
    // same second yields a byte-identical manifest; that's the same snapshot,
    // not an error.
    let row = info.clone();
    let inserted = state
        .db
        .call(move |conn| {
            let n = conn.execute(
                "INSERT INTO snapshots
                 (backup_id, created_at, manifest_hash, file_count, bytes_scanned, bytes_new, chunk_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(manifest_hash) DO NOTHING",
                rusqlite::params![
                    row.backup_id,
                    row.created_at,
                    row.manifest_hash.as_slice(),
                    row.file_count,
                    row.bytes_scanned,
                    row.bytes_new,
                    row.chunk_count,
                ],
            )?;
            Ok(n > 0)
        })
        .await?;
    if !inserted {
        tracing::info!(backup = %cfg.id, "snapshot identical to an existing one; not re-recorded");
    }

    // Register every blob this snapshot depends on as replication work.
    {
        let backup = cfg.id.clone();
        let mut refs: Vec<([u8; 32], u64, bool)> = result
            .manifest
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                EntryKind::File { chunks, .. } => Some(chunks.iter().map(|c| {
                    (c.blob_hash.0, c.size as u64 + burrow_core::crypto::BLOB_OVERHEAD as u64, false)
                })),
                _ => None,
            })
            .flatten()
            .collect();
        refs.push((result.manifest_hash.0, result.manifest_size, true));
        state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                for (hash, size, is_manifest) in refs {
                    tx.execute(
                        "INSERT INTO chunk_refs (backup_id, blob_hash, size, is_manifest)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(backup_id, blob_hash) DO NOTHING",
                        rusqlite::params![backup, hash.as_slice(), size, is_manifest],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await?;
    }

    if let Err(e) = prune(state, cfg).await {
        tracing::warn!(backup = %cfg.id, "pruning failed: {e:#}");
    }

    // Kick replication in the background; failures surface in `status`.
    {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::replicate::tick(&state).await {
                tracing::warn!("post-backup replication failed: {e:#}");
            }
        });
    }

    tracing::info!(
        backup = %cfg.id,
        files = file_count,
        new_bytes = result.bytes_new,
        "snapshot complete"
    );
    Ok(info)
}

/// Enforce `keep_last`: drop old snapshots, rebuild this backup's chunk_refs
/// from the surviving manifests, unpin pruned manifest tags, and release
/// now-orphaned blobs from peers. Local orphans then fall to GC.
async fn prune(state: &Arc<AppState>, cfg: &crate::config::BackupConfig) -> anyhow::Result<()> {
    let Some(keep) = cfg.keep_last else { return Ok(()) };
    let backup_id = cfg.id.clone();

    let (victims, survivors): (Vec<(u64, [u8; 32])>, Vec<[u8; 32]>) = {
        let id = backup_id.clone();
        state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT created_at, manifest_hash FROM snapshots
                     WHERE backup_id = ?1 ORDER BY created_at DESC",
                )?;
                let rows: Vec<(u64, Vec<u8>)> = stmt
                    .query_map([&id], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                let mut victims = Vec::new();
                let mut survivors = Vec::new();
                for (i, (ts, hash)) in rows.into_iter().enumerate() {
                    let Ok(h) = <[u8; 32]>::try_from(hash) else { continue };
                    if i < keep as usize {
                        survivors.push(h);
                    } else {
                        victims.push((ts, h));
                    }
                }
                Ok((victims, survivors))
            })
            .await?
    };
    if victims.is_empty() {
        return Ok(());
    }

    // Rebuild chunk_refs for this backup from the surviving manifests.
    let mut fresh_refs: Vec<([u8; 32], u64, bool)> = Vec::new();
    for mh in &survivors {
        let bytes = state
            .blobs
            .blobs()
            .get_bytes(iroh_blobs::Hash::from_bytes(*mh))
            .await
            .context("reading surviving manifest")?;
        let manifest = burrow_core::manifest::Manifest::open(&state.repo_key, &bytes)?;
        for e in &manifest.entries {
            if let burrow_core::manifest::EntryKind::File { chunks, .. } = &e.kind {
                for c in chunks {
                    fresh_refs.push((
                        c.blob_hash.0,
                        c.size as u64 + burrow_core::crypto::BLOB_OVERHEAD as u64,
                        false,
                    ));
                }
            }
        }
        fresh_refs.push((*mh, bytes.len() as u64, true));
    }

    // Swap in the new refs and drop pruned snapshot rows.
    let orphans: Vec<(Vec<u8>, Vec<u8>)> = {
        let id = backup_id.clone();
        let victims = victims.clone();
        state
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                for (ts, _) in &victims {
                    tx.execute(
                        "DELETE FROM snapshots WHERE backup_id = ?1 AND created_at = ?2",
                        rusqlite::params![id, ts],
                    )?;
                }
                tx.execute("DELETE FROM chunk_refs WHERE backup_id = ?1", [&id])?;
                for (hash, size, is_manifest) in &fresh_refs {
                    tx.execute(
                        "INSERT INTO chunk_refs (backup_id, blob_hash, size, is_manifest)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(backup_id, blob_hash) DO NOTHING",
                        rusqlite::params![id, hash.as_slice(), size, is_manifest],
                    )?;
                }
                // Placements for blobs no backup references anymore.
                let mut stmt = tx.prepare(
                    "SELECT peer, blob_hash FROM placements
                     WHERE blob_hash NOT IN (SELECT blob_hash FROM chunk_refs)",
                )?;
                let orphans: Vec<(Vec<u8>, Vec<u8>)> = stmt
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                drop(stmt);
                tx.commit()?;
                Ok(orphans)
            })
            .await?
    };

    // Unpin pruned manifests so local GC can collect the old snapshot data.
    for (ts, _) in &victims {
        let tag = format!("snapshot/{}/{}", backup_id, ts);
        if let Err(e) = state.blobs.tags().delete(tag).await {
            tracing::debug!("tag delete failed (may not exist): {e}");
        }
    }

    // Ask peers to drop orphaned replicas.
    let mut per_peer: std::collections::HashMap<[u8; 32], Vec<[u8; 32]>> = Default::default();
    for (peer, hash) in orphans {
        if let (Ok(p), Ok(h)) =
            (<[u8; 32]>::try_from(peer), <[u8; 32]>::try_from(hash))
        {
            per_peer.entry(p).or_default().push(h);
        }
    }
    for (peer, hashes) in per_peer {
        if let Err(e) = crate::replicate::release_from_peer(state, peer, &hashes).await {
            tracing::warn!("releasing pruned blobs failed (will retry next tick): {e:#}");
        }
    }

    tracing::info!(backup = %backup_id, pruned = victims.len(), kept = survivors.len(), "snapshots pruned");
    Ok(())
}

/// Fetch any of `hashes` that aren't local from peers that hold replicas.
async fn fetch_missing(
    state: &Arc<AppState>,
    hashes: &[burrow_core::BlobHash],
) -> anyhow::Result<()> {
    let mut missing = Vec::new();
    for h in hashes {
        if !state.blobs.blobs().has(to_iroh_hash(h)).await.unwrap_or(false) {
            missing.push(*h);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    tracing::info!(count = missing.len(), "fetching missing blobs from peers");

    // Holders per missing blob, most-recently-verified first.
    for h in &missing {
        let hash_bytes = h.0.to_vec();
        let holders: Vec<Vec<u8>> = state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT peer FROM placements
                     WHERE blob_hash = ?1 AND state IN ('stored', 'verified')
                     ORDER BY COALESCE(last_verified, 0) DESC",
                )?;
                let rows = stmt.query_map([&hash_bytes], |r| r.get::<_, Vec<u8>>(0))?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            })
            .await?;
        if holders.is_empty() {
            anyhow::bail!("blob {h} is not local and no peer holds a replica");
        }
        let mut fetched = false;
        let mut last_err = None;
        for holder in holders {
            let Ok(id_arr) = <[u8; 32]>::try_from(holder) else { continue };
            let Ok(peer) = iroh::EndpointId::from_bytes(&id_arr) else { continue };
            match crate::net::fetch_blob(state, peer, to_iroh_hash(h)).await {
                Ok(()) => {
                    fetched = true;
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }
        if !fetched {
            anyhow::bail!(
                "could not fetch blob {h} from any holder: {}",
                last_err.map(|e| format!("{e:#}")).unwrap_or_default()
            );
        }
    }
    Ok(())
}

pub async fn snapshot_list(
    state: &Arc<AppState>,
    backup_id: Option<String>,
) -> anyhow::Result<Vec<SnapshotInfo>> {
    state
        .db
        .call(move |conn| {
            let mut out = Vec::new();
            match backup_id {
                Some(id) => {
                    let mut stmt = conn.prepare(
                        "SELECT * FROM snapshots WHERE backup_id = ?1 ORDER BY created_at",
                    )?;
                    let rows = stmt.query_map([&id], crate::db::rows::snapshot_info)?;
                    for r in rows {
                        out.push(r?);
                    }
                }
                None => {
                    let mut stmt =
                        conn.prepare("SELECT * FROM snapshots ORDER BY backup_id, created_at")?;
                    let rows = stmt.query_map([], crate::db::rows::snapshot_info)?;
                    for r in rows {
                        out.push(r?);
                    }
                }
            }
            Ok(out)
        })
        .await
}

pub async fn restore(
    state: &Arc<AppState>,
    backup_id: &str,
    snapshot: Option<u64>,
    target: PathBuf,
) -> anyhow::Result<(u64, u64, PathBuf)> {
    let id = backup_id.to_string();
    let info = state
        .db
        .call(move |conn| {
            let result = match snapshot {
                Some(ts) => conn.query_row(
                    "SELECT * FROM snapshots WHERE backup_id = ?1 AND created_at = ?2",
                    rusqlite::params![id, ts],
                    crate::db::rows::snapshot_info,
                ),
                None => conn.query_row(
                    "SELECT * FROM snapshots WHERE backup_id = ?1
                     ORDER BY created_at DESC LIMIT 1",
                    [&id],
                    crate::db::rows::snapshot_info,
                ),
            };
            result.map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    anyhow::anyhow!("no matching snapshot for backup {id:?}")
                }
                e => e.into(),
            })
        })
        .await?;

    // Make sure every needed blob is local, pulling from replica holders for
    // anything missing (e.g. this machine lost its blob store).
    let manifest_hash = burrow_core::BlobHash(info.manifest_hash);
    fetch_missing(state, &[manifest_hash]).await?;
    let manifest_bytes = state
        .blobs
        .blobs()
        .get_bytes(to_iroh_hash(&manifest_hash))
        .await
        .context("reading manifest blob")?;
    let parsed = burrow_core::manifest::Manifest::open(&state.repo_key, &manifest_bytes)?;
    fetch_missing(state, &parsed.referenced_blobs()).await?;

    let store = state.blobs.clone();
    let repo_key = state.repo_key.clone();
    let target_clone = target.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        let adapter = IrohBlobStore::new(store);
        restore_snapshot(&adapter, &repo_key, &manifest_hash, &target_clone)
    })
    .await
    .context("restore task panicked")??;

    let files = manifest
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::File { .. }))
        .count() as u64;
    let bytes = manifest.total_bytes();
    tracing::info!(backup = backup_id, files, bytes, target = %target.display(), "restore complete");
    Ok((files, bytes, target))
}
