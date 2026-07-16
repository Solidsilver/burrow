//! Deterministic authenticated chunk encryption.
//!
//! Design: every chunk is encrypted with a key derived from (repo key,
//! plaintext), so the same plaintext under the same repo key always produces
//! byte-identical ciphertext — and therefore the same content address in
//! iroh-blobs. This makes replica tracking stable across index loss: re-running
//! a backup reconverges on the blob hashes peers already hold.
//!
//! Because key derivation is keyed by the secret repo key, this is NOT public
//! convergent encryption: without the repo key, no one can confirm a guessed
//! plaintext. Peers learn only equality among a single user's chunks, which
//! content addressing reveals anyway.
//!
//! Blob wire format (version 1):
//!   [0x01][plain_id: 32 bytes][xchacha20poly1305 ciphertext + 16-byte tag]
//! The embedded plain_id is what lets the owner re-derive the chunk key at
//! decrypt time with nothing but the repo key — manifests stay optional for
//! recovery of any individual blob.

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{CoreError, Result};

pub const BLOB_FORMAT_V1: u8 = 1;
/// version byte + plain_id + AEAD tag
pub const BLOB_OVERHEAD: usize = 1 + 32 + 16;

const CTX_PLAIN_ID: &str = "burrow v1 plain id";
const CTX_CHUNK_KEY: &str = "burrow v1 chunk key";
const CTX_CHUNK_NONCE: &str = "burrow v1 chunk nonce";

/// The repository master secret. Everything an owner stores is derived from
/// this single 32-byte value; losing it means losing the backups.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct RepoKey([u8; 32]);

impl std::fmt::Debug for RepoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RepoKey(..)")
    }
}

impl RepoKey {
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).expect("OS RNG unavailable");
        Self(key)
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// 24-word BIP-39 recovery phrase encoding the full 32 bytes of entropy.
    pub fn to_recovery_phrase(&self) -> String {
        bip39::Mnemonic::from_entropy(&self.0)
            .expect("32 bytes is a valid mnemonic entropy size")
            .to_string()
    }

    pub fn from_recovery_phrase(phrase: &str) -> Result<Self> {
        let m = bip39::Mnemonic::parse_normalized(phrase)
            .map_err(|e| CoreError::RecoveryPhrase(e.to_string()))?;
        let entropy = m.to_entropy();
        let bytes: [u8; 32] = entropy.as_slice().try_into().map_err(|_| {
            CoreError::RecoveryPhrase("phrase must encode 32 bytes (24 words)".into())
        })?;
        Ok(Self(bytes))
    }

    /// Keyed identity of a plaintext chunk. Reveals nothing without the repo
    /// key; used as the dedup key and embedded in the sealed blob.
    pub fn plain_id(&self, plaintext: &[u8]) -> PlainId {
        let key = blake3::derive_key(CTX_PLAIN_ID, &self.0);
        PlainId(*blake3::keyed_hash(&key, plaintext).as_bytes())
    }

    fn chunk_key(&self, plain_id: &PlainId) -> [u8; 32] {
        let mut material = [0u8; 64];
        material[..32].copy_from_slice(&self.0);
        material[32..].copy_from_slice(&plain_id.0);
        let key = blake3::derive_key(CTX_CHUNK_KEY, &material);
        material.zeroize();
        key
    }

    fn chunk_nonce(&self, plain_id: &PlainId) -> [u8; 24] {
        let mut material = [0u8; 64];
        material[..32].copy_from_slice(&self.0);
        material[32..].copy_from_slice(&plain_id.0);
        let full = blake3::derive_key(CTX_CHUNK_NONCE, &material);
        material.zeroize();
        full[..24].try_into().unwrap()
    }

    /// Encrypt a plaintext chunk into the self-describing blob format.
    /// Deterministic: same plaintext + same repo key => identical output.
    pub fn seal_chunk(&self, plaintext: &[u8]) -> SealedChunk {
        let plain_id = self.plain_id(plaintext);
        let key = self.chunk_key(&plain_id);
        let nonce = self.chunk_nonce(&plain_id);
        let cipher = XChaCha20Poly1305::new((&key).into());
        let ct = cipher
            .encrypt(XNonce::from_slice(&nonce), plaintext)
            .expect("XChaCha20Poly1305 encryption is infallible for in-memory data");
        let mut blob = Vec::with_capacity(BLOB_OVERHEAD + plaintext.len());
        blob.push(BLOB_FORMAT_V1);
        blob.extend_from_slice(&plain_id.0);
        blob.extend_from_slice(&ct);
        SealedChunk {
            plain_id,
            blob_hash: BlobHash::of(&blob),
            blob,
        }
    }

    /// Decrypt a sealed blob. Authenticates via the AEAD tag and verifies the
    /// recovered plaintext matches the embedded plain_id.
    pub fn open_chunk(&self, blob: &[u8]) -> Result<Vec<u8>> {
        if blob.len() < BLOB_OVERHEAD {
            return Err(CoreError::MalformedBlob("blob shorter than header"));
        }
        if blob[0] != BLOB_FORMAT_V1 {
            return Err(CoreError::UnsupportedVersion(blob[0]));
        }
        let plain_id = PlainId(blob[1..33].try_into().unwrap());
        let key = self.chunk_key(&plain_id);
        let nonce = self.chunk_nonce(&plain_id);
        let cipher = XChaCha20Poly1305::new((&key).into());
        let plaintext = cipher
            .decrypt(XNonce::from_slice(&nonce), &blob[33..])
            .map_err(|_| CoreError::Decrypt)?;
        if self.plain_id(&plaintext) != plain_id {
            return Err(CoreError::MalformedBlob("plain_id mismatch after decrypt"));
        }
        Ok(plaintext)
    }
}

/// Keyed BLAKE3 identity of a plaintext chunk (secret-keyed; safe to embed in
/// ciphertext and share in manifests).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlainId(pub [u8; 32]);

/// BLAKE3 hash of a sealed blob — identical to the iroh-blobs content address
/// for the same bytes (iroh-blobs also hashes with BLAKE3).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobHash(pub [u8; 32]);

impl BlobHash {
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

pub struct SealedChunk {
    pub plain_id: PlainId,
    pub blob_hash: BlobHash,
    pub blob: Vec<u8>,
}

macro_rules! hex_display {
    ($ty:ty) => {
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for b in &self.0 {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($ty), self)
            }
        }
    };
}
hex_display!(PlainId);
hex_display!(BlobHash);

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn key_a() -> RepoKey {
        RepoKey::from_bytes([7u8; 32])
    }
    fn key_b() -> RepoKey {
        RepoKey::from_bytes([8u8; 32])
    }

    proptest! {
        #[test]
        fn roundtrip(data in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let sealed = key_a().seal_chunk(&data);
            let opened = key_a().open_chunk(&sealed.blob).unwrap();
            prop_assert_eq!(opened, data);
        }

        #[test]
        fn deterministic_same_key(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let a = key_a().seal_chunk(&data);
            let b = key_a().seal_chunk(&data);
            prop_assert_eq!(a.blob, b.blob);
            prop_assert_eq!(a.blob_hash, b.blob_hash);
        }

        #[test]
        fn different_key_different_ciphertext(data in proptest::collection::vec(any::<u8>(), 1..4096)) {
            let a = key_a().seal_chunk(&data);
            let b = key_b().seal_chunk(&data);
            prop_assert_ne!(a.blob_hash, b.blob_hash);
            prop_assert!(key_b().open_chunk(&a.blob).is_err());
        }
    }

    #[test]
    fn tamper_detected() {
        let sealed = key_a().seal_chunk(b"attack at dawn");
        for i in 0..sealed.blob.len() {
            let mut t = sealed.blob.clone();
            t[i] ^= 0x01;
            assert!(key_a().open_chunk(&t).is_err(), "flip at byte {i} accepted");
        }
    }

    #[test]
    fn recovery_phrase_roundtrip() {
        let k = RepoKey::generate();
        let phrase = k.to_recovery_phrase();
        assert_eq!(phrase.split_whitespace().count(), 24);
        let k2 = RepoKey::from_recovery_phrase(&phrase).unwrap();
        assert_eq!(k.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn ciphertext_format_stable() {
        // Golden test: the sealed blob for a fixed key+plaintext must never
        // change — a change means old backups can't be deduplicated against.
        let sealed = RepoKey::from_bytes([1u8; 32]).seal_chunk(b"burrow format freeze");
        assert_eq!(sealed.blob[0], BLOB_FORMAT_V1);
        assert_eq!(sealed.blob.len(), BLOB_OVERHEAD + 20);
        // Recorded at format-freeze:
        const GOLDEN_BLOB_HASH: &str =
            "b17aa644a0c7b46544931a74eb6e3fffab64290e1821305d66544abedec641a8";
        assert_eq!(sealed.blob_hash.to_string(), GOLDEN_BLOB_HASH);
    }
}
