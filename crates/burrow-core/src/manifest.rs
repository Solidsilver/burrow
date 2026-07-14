//! The snapshot manifest: an encrypted, postcard-encoded description of one
//! point-in-time backup — every file, its metadata, and the chunk list needed
//! to rebuild it. Manifests are sealed and stored exactly like data chunks, so
//! holding peers can't distinguish them; owners pin them with named tags.

use serde::{Deserialize, Serialize};

use crate::crypto::{BlobHash, PlainId, RepoKey, SealedChunk};
use crate::error::Result;

pub const MANIFEST_FORMAT: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Bump-on-breaking-change format marker, checked on decode.
    pub format: u16,
    /// User-assigned backup id from config, e.g. "photos".
    pub backup_id: String,
    /// Machine that produced the snapshot.
    pub node_name: String,
    /// Unix seconds. Supplied by the caller (core stays clock-free).
    pub created_at: u64,
    /// Absolute source roots (manifest path form, no leading slash).
    pub roots: Vec<String>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Path relative to the backup root, `/`-separated, no leading slash.
    pub path: String,
    pub kind: EntryKind,
    /// Unix mode bits (permissions); best-effort on non-unix.
    pub mode: u32,
    /// Modification time, unix seconds.
    pub mtime: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryKind {
    File { size: u64, chunks: Vec<ChunkRef> },
    Dir,
    Symlink { target: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub plain_id: PlainId,
    pub blob_hash: BlobHash,
    /// Plaintext length of this chunk.
    pub size: u32,
}

impl Manifest {
    /// Serialize and encrypt into a sealed blob (same format as data chunks).
    pub fn seal(&self, key: &RepoKey) -> SealedChunk {
        let bytes = postcard::to_allocvec(self).expect("manifest serialization cannot fail");
        key.seal_chunk(&bytes)
    }

    pub fn open(key: &RepoKey, blob: &[u8]) -> Result<Self> {
        let bytes = key.open_chunk(blob)?;
        let m: Manifest = postcard::from_bytes(&bytes)?;
        if m.format != MANIFEST_FORMAT {
            return Err(crate::CoreError::UnsupportedVersion(m.format as u8));
        }
        Ok(m)
    }

    /// Every blob hash this snapshot depends on, manifest excluded,
    /// deduplicated, in deterministic order.
    pub fn referenced_blobs(&self) -> Vec<BlobHash> {
        let mut hashes: Vec<BlobHash> = self
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                EntryKind::File { chunks, .. } => Some(chunks.iter().map(|c| c.blob_hash)),
                _ => None,
            })
            .flatten()
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        hashes
    }

    pub fn total_bytes(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| match &e.kind {
                EntryKind::File { size, .. } => *size,
                _ => 0,
            })
            .sum()
    }
}
