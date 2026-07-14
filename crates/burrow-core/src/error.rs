use crate::crypto::BlobHash;

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("chunk decryption failed (wrong key or corrupt blob)")]
    Decrypt,
    #[error("blob is malformed: {0}")]
    MalformedBlob(&'static str),
    #[error("blob {0} not found in store")]
    BlobMissing(BlobHash),
    #[error("blob {hash} content does not match its hash")]
    HashMismatch { hash: BlobHash },
    #[error("manifest decode failed: {0}")]
    ManifestDecode(#[from] postcard::Error),
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u8),
    #[error("invalid recovery phrase: {0}")]
    RecoveryPhrase(String),
    #[error("path {0:?} escapes the restore target")]
    PathEscape(std::path::PathBuf),
}

pub type Result<T, E = CoreError> = std::result::Result<T, E>;
