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
        });
    }
    Ok(StatusInfo {
        node_name: state.config.node_name(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        data_dir: crate::paths::data_dir(),
        backups,
    })
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

    tracing::info!(
        backup = %cfg.id,
        files = file_count,
        new_bytes = result.bytes_new,
        "snapshot complete"
    );
    Ok(info)
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

    let store = state.blobs.clone();
    let repo_key = state.repo_key.clone();
    let manifest_hash = burrow_core::BlobHash(info.manifest_hash);
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
