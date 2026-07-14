//! SQLite metadata store behind a dedicated blocking thread. All access goes
//! through `Db::call`, which runs a closure against the connection and returns
//! the result to async land.

use std::path::Path;

use anyhow::Context;
use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

#[derive(Clone)]
pub struct Db {
    tx: std::sync::mpsc::Sender<Job>,
}

fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(
        r#"
        CREATE TABLE snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            manifest_hash BLOB NOT NULL UNIQUE,
            file_count INTEGER NOT NULL,
            bytes_scanned INTEGER NOT NULL,
            bytes_new INTEGER NOT NULL,
            chunk_count INTEGER NOT NULL
        );
        CREATE INDEX idx_snapshots_backup ON snapshots(backup_id, created_at);
        "#,
    )])
}

impl Db {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut conn = Connection::open(path)
            .with_context(|| format!("opening database {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        migrations().to_latest(&mut conn).context("running database migrations")?;

        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("burrow-db".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job(&mut conn);
                }
            })
            .context("spawning db thread")?;
        Ok(Self { tx })
    }

    pub async fn call<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> anyhow::Result<T> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Box::new(move |conn| {
                let _ = tx.send(f(conn));
            }))
            .map_err(|_| anyhow::anyhow!("db thread is gone"))?;
        rx.await.context("db thread dropped the reply")?
    }
}

/// Row helpers shared by ops.
pub mod rows {
    use burrow_proto::ctrl::SnapshotInfo;

    pub fn snapshot_info(row: &rusqlite::Row<'_>) -> rusqlite::Result<SnapshotInfo> {
        let hash_vec: Vec<u8> = row.get("manifest_hash")?;
        let manifest_hash: [u8; 32] = hash_vec.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Blob,
                "manifest_hash is not 32 bytes".into(),
            )
        })?;
        Ok(SnapshotInfo {
            backup_id: row.get("backup_id")?,
            created_at: row.get("created_at")?,
            manifest_hash,
            file_count: row.get("file_count")?,
            bytes_scanned: row.get("bytes_scanned")?,
            bytes_new: row.get("bytes_new")?,
            chunk_count: row.get("chunk_count")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_valid() {
        migrations().validate().unwrap();
    }

    #[tokio::test]
    async fn open_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("meta.db")).unwrap();
        let count: i64 = db
            .call(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM snapshots", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
