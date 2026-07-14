//! Bridges burrow-core's synchronous `BlobStore` trait onto the async
//! iroh-blobs store. The snapshot pipeline runs inside `spawn_blocking`, so
//! blocking on the runtime handle here is safe.

use burrow_core::store::BlobStore;
use burrow_core::{BlobHash, CoreError};
use iroh_blobs::api::Store;
use iroh_blobs::Hash;

pub struct IrohBlobStore {
    store: Store,
    handle: tokio::runtime::Handle,
}

impl IrohBlobStore {
    /// Must be constructed on a runtime thread (captures the current handle);
    /// used from `spawn_blocking` threads.
    pub fn new(store: Store) -> Self {
        Self { store, handle: tokio::runtime::Handle::current() }
    }
}

pub fn to_iroh_hash(h: &BlobHash) -> Hash {
    Hash::from_bytes(h.0)
}

pub fn from_iroh_hash(h: &Hash) -> BlobHash {
    BlobHash(*h.as_bytes())
}

fn io_err(e: impl std::fmt::Display) -> CoreError {
    CoreError::Io(std::io::Error::other(e.to_string()))
}

impl BlobStore for IrohBlobStore {
    fn put(&mut self, bytes: Vec<u8>) -> Result<BlobHash, CoreError> {
        let store = self.store.clone();
        self.handle.block_on(async move {
            let tag = store.blobs().add_bytes(bytes).await.map_err(io_err)?;
            Ok(from_iroh_hash(&tag.hash))
        })
    }

    fn get(&self, hash: &BlobHash) -> Result<Vec<u8>, CoreError> {
        let store = self.store.clone();
        let hash = to_iroh_hash(hash);
        self.handle.block_on(async move {
            let bytes = store.blobs().get_bytes(hash).await.map_err(io_err)?;
            Ok(bytes.to_vec())
        })
    }

    fn contains(&self, hash: &BlobHash) -> bool {
        let store = self.store.clone();
        let hash = to_iroh_hash(hash);
        self.handle
            .block_on(async move { store.blobs().has(hash).await })
            .unwrap_or(false)
    }
}
