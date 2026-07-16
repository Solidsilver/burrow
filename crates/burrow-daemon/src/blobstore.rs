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
    /// One temp tag per blob written through `put`. Blobs written by a
    /// snapshot are not yet in `chunk_refs`, so the GC protect callback
    /// doesn't know about them; these tags keep them alive until the caller
    /// has committed that metadata (then drops them via `into_temp_tags`).
    /// A persistent tag here instead would pin every chunk forever and GC
    /// could never reclaim pruned data.
    temp_tags: Vec<iroh_blobs::api::TempTag>,
}

impl IrohBlobStore {
    /// Must be constructed on a runtime thread (captures the current handle);
    /// used from `spawn_blocking` threads.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            handle: tokio::runtime::Handle::current(),
            temp_tags: Vec::new(),
        }
    }

    /// Hand over the GC guards for everything written so far. The caller must
    /// keep them alive until the blobs are protected by metadata (chunk_refs
    /// rows / a snapshot tag).
    pub fn into_temp_tags(self) -> Vec<iroh_blobs::api::TempTag> {
        self.temp_tags
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
        // temp_tag(), not the default with_tag(): the awaited form creates a
        // persistent auto tag per blob, which would defeat GC forever.
        let tag = self.handle.block_on(async move {
            store
                .blobs()
                .add_bytes(bytes)
                .temp_tag()
                .await
                .map_err(io_err)
        })?;
        let hash = from_iroh_hash(tag.as_ref());
        self.temp_tags.push(tag);
        Ok(hash)
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
