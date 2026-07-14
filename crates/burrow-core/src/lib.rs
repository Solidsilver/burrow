//! Core logic for burrow: content-defined chunking, deterministic chunk
//! encryption, the snapshot/manifest model, and snapshot build/restore against
//! an abstract blob store. This crate is pure — no networking, no iroh — so
//! everything here is testable in-process.

pub mod chunk;
pub mod crypto;
pub mod error;
pub mod manifest;
pub mod snapshot;
pub mod store;

pub use crypto::{BlobHash, PlainId, RepoKey};
pub use error::CoreError;
