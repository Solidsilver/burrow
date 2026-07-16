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
        // (v6 appended below the v5 rebuild.)
        M::up(
            // v5: owner-identity model. Peer-keyed tables are dropped and
            // recreated keyed by owner (person) with devices underneath.
            // Pre-release schema: dropping runtime peer state is acceptable.
            r#"
            DROP TABLE IF EXISTS space_requests;
            DROP TABLE IF EXISTS held;
            DROP TABLE IF EXISTS grants;
            DROP TABLE IF EXISTS placements;
            DROP TABLE IF EXISTS transfer_ledger;
            DROP TABLE IF EXISTS peers;

            CREATE TABLE owners (
                owner_pk BLOB PRIMARY KEY,          -- 32 bytes, ed25519
                name TEXT NOT NULL UNIQUE,          -- local nickname
                state TEXT NOT NULL,                -- 'pending_in' | 'active' | 'self'
                added_at INTEGER NOT NULL,
                last_seen INTEGER
            );
            CREATE TABLE devices (
                endpoint_id BLOB PRIMARY KEY,       -- 32 bytes
                owner_pk BLOB NOT NULL REFERENCES owners(owner_pk) ON DELETE CASCADE,
                device_name TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'host',  -- 'host' | 'client'
                ticket TEXT,                        -- dial hints
                last_seen INTEGER
            );
            CREATE INDEX idx_devices_owner ON devices(owner_pk);
            -- Space THIS DEVICE promises to an owner (their any device may use it).
            CREATE TABLE grants_given (
                owner_pk BLOB PRIMARY KEY REFERENCES owners(owner_pk) ON DELETE CASCADE,
                granted_bytes INTEGER NOT NULL,
                used_bytes INTEGER NOT NULL DEFAULT 0,
                shrink_deadline INTEGER,
                updated_at INTEGER NOT NULL
            );
            -- Space a specific remote device promises OUR owner.
            CREATE TABLE grants_received (
                device BLOB PRIMARY KEY REFERENCES devices(endpoint_id) ON DELETE CASCADE,
                owner_pk BLOB NOT NULL,
                granted_bytes INTEGER NOT NULL,
                used_bytes INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            );
            -- Blobs this device stores, per owning person.
            CREATE TABLE held (
                owner_pk BLOB NOT NULL REFERENCES owners(owner_pk) ON DELETE CASCADE,
                blob_hash BLOB NOT NULL,
                size INTEGER NOT NULL,
                is_manifest INTEGER NOT NULL DEFAULT 0,
                stored_at INTEGER NOT NULL,
                PRIMARY KEY (owner_pk, blob_hash)
            );
            CREATE INDEX idx_held_hash ON held(blob_hash);
            CREATE TABLE space_requests (
                owner_pk BLOB PRIMARY KEY REFERENCES owners(owner_pk) ON DELETE CASCADE,
                bytes INTEGER NOT NULL,
                given_total INTEGER NOT NULL DEFAULT 0,
                received_total INTEGER NOT NULL DEFAULT 0,
                requested_at INTEGER NOT NULL
            );
            -- Where my blobs live: physical device, with owner for diversity.
            CREATE TABLE placements (
                blob_hash BLOB NOT NULL,
                device BLOB NOT NULL REFERENCES devices(endpoint_id) ON DELETE CASCADE,
                owner_pk BLOB NOT NULL,
                size INTEGER NOT NULL,
                state TEXT NOT NULL,   -- 'pending' | 'stored' | 'verified' | 'lost'
                updated_at INTEGER NOT NULL,
                last_verified INTEGER,
                PRIMARY KEY (blob_hash, device)
            );
            CREATE INDEX idx_placements_device ON placements(device);
            CREATE INDEX idx_placements_owner ON placements(owner_pk);
            CREATE TABLE transfer_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_pk BLOB NOT NULL,
                direction TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                at INTEGER NOT NULL
            );
            -- mtime cache: skip re-chunking unchanged files (P2-M3).
            CREATE TABLE file_cache (
                backup_id TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                chunks BLOB NOT NULL,               -- postcard Vec<ChunkRef>
                PRIMARY KEY (backup_id, path)
            );
            "#,
        ),
        M::up(
            // v6: transfer_ledger was never read or written — drop it. Small
            // daemon key/value state (e.g. paused_until) lives in kv.
            r#"
            DROP TABLE IF EXISTS transfer_ledger;
            CREATE TABLE kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        ),
        M::up(
            // v7: blobs whose local bytes failed bao validation (bit rot).
            // Quarantined hashes are EXCLUDED from GC protection so the store
            // reclaims them; the next fetch then transfers fresh bytes and
            // lifts the quarantine. (iroh-blobs has no direct delete API —
            // GC is the only deletion path, so this is how a corrupt local
            // copy gets replaced.)
            r#"
            CREATE TABLE quarantine (
                blob_hash BLOB PRIMARY KEY,
                at INTEGER NOT NULL
            );
            "#,
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
        migrations()
            .to_latest(&mut conn)
            .context("running database migrations")?;

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
            files_cached: 0,
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

    /// Every SQL statement must prepare against the *migrated* schema. This
    /// catches queries left behind by a schema rename (a v5 column rename
    /// silently broke pruning for months: `placements.peer` → `device`).
    #[test]
    fn queries_match_schema() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_latest(&mut conn).unwrap();
        conn.prepare(crate::ops::ORPHAN_PLACEMENTS_SQL).unwrap();
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
