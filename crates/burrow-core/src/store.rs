//! Minimal blob-store abstraction the snapshot pipeline writes to / reads
//! from. The daemon implements this over iroh-blobs; tests use `MemStore`.

use std::collections::HashMap;

use crate::crypto::BlobHash;
use crate::error::{CoreError, Result};

pub trait BlobStore {
    /// Store a blob. Must be idempotent (content-addressed).
    fn put(&mut self, bytes: Vec<u8>) -> Result<BlobHash>;
    fn get(&self, hash: &BlobHash) -> Result<Vec<u8>>;
    fn contains(&self, hash: &BlobHash) -> bool;
}

#[derive(Default)]
pub struct MemStore {
    blobs: HashMap<BlobHash, Vec<u8>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }
}

impl BlobStore for MemStore {
    fn put(&mut self, bytes: Vec<u8>) -> Result<BlobHash> {
        let hash = BlobHash::of(&bytes);
        self.blobs.insert(hash, bytes);
        Ok(hash)
    }

    fn get(&self, hash: &BlobHash) -> Result<Vec<u8>> {
        let bytes = self.blobs.get(hash).ok_or(CoreError::BlobMissing(*hash))?;
        // Defensive integrity check — mirrors iroh's verified reads.
        if &BlobHash::of(bytes) != hash {
            return Err(CoreError::HashMismatch { hash: *hash });
        }
        Ok(bytes.clone())
    }

    fn contains(&self, hash: &BlobHash) -> bool {
        self.blobs.contains_key(hash)
    }
}
