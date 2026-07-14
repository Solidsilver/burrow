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
    Migrations::new(vec![
        M::up(
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
        ),
        M::up(
            r#"
            CREATE TABLE peers (
                endpoint_id BLOB PRIMARY KEY,       -- 32 bytes
                name TEXT NOT NULL UNIQUE,          -- local nickname
                state TEXT NOT NULL,                -- 'pending_in' | 'active'
                ticket TEXT,                        -- pairing ticket (dial hints)
                hello_name TEXT,                    -- their self-reported name
                approved_by_them INTEGER,           -- 0/1, from last contact
                added_at INTEGER NOT NULL,
                last_seen INTEGER
            );
            CREATE TABLE grants (
                peer BLOB NOT NULL REFERENCES peers(endpoint_id) ON DELETE CASCADE,
                direction TEXT NOT NULL,            -- 'given' | 'received'
                granted_bytes INTEGER NOT NULL,
                used_bytes INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (peer, direction)
            );
            CREATE TABLE held (                     -- chunks I store for peers
                owner BLOB NOT NULL REFERENCES peers(endpoint_id) ON DELETE CASCADE,
                blob_hash BLOB NOT NULL,
                size INTEGER NOT NULL,
                is_manifest INTEGER NOT NULL DEFAULT 0,
                stored_at INTEGER NOT NULL,
                PRIMARY KEY (owner, blob_hash)
            );
            CREATE INDEX idx_held_hash ON held(blob_hash);
            CREATE TABLE space_requests (
                peer BLOB PRIMARY KEY REFERENCES peers(endpoint_id) ON DELETE CASCADE,
                bytes INTEGER NOT NULL,
                given_total INTEGER NOT NULL DEFAULT 0,
                received_total INTEGER NOT NULL DEFAULT 0,
                requested_at INTEGER NOT NULL
            );
            CREATE TABLE transfer_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer BLOB NOT NULL,
                direction TEXT NOT NULL,            -- 'sent' | 'received'
                bytes INTEGER NOT NULL,
                at INTEGER NOT NULL
            );
            "#,
        ),
        M::up(
            r#"
            -- Every blob my backups reference (data chunks + manifests),
            -- with stored size; the replication planner's work list.
            CREATE TABLE chunk_refs (
                backup_id TEXT NOT NULL,
                blob_hash BLOB NOT NULL,
                size INTEGER NOT NULL,
                is_manifest INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (backup_id, blob_hash)
            );
            CREATE INDEX idx_chunk_refs_hash ON chunk_refs(blob_hash);
            -- Where my blobs live remotely.
            CREATE TABLE placements (
                blob_hash BLOB NOT NULL,
                peer BLOB NOT NULL REFERENCES peers(endpoint_id) ON DELETE CASCADE,
                size INTEGER NOT NULL,
                state TEXT NOT NULL,   -- 'pending' | 'stored' | 'verified' | 'lost'
                updated_at INTEGER NOT NULL,
                last_verified INTEGER,
                PRIMARY KEY (blob_hash, peer)
            );
            CREATE INDEX idx_placements_peer ON placements(peer);
            "#,
        ),
        M::up(
            // When a grant shrinks below current usage, the holder gives the
            // owner until this deadline to evacuate before forced eviction.
            "ALTER TABLE grants ADD COLUMN shrink_deadline INTEGER;",
        ),
    ])
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
