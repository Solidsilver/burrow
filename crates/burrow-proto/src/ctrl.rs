//! CLI ↔ daemon control protocol: postcard-encoded frames over a unix socket.
//! (irpc 0.17 only supports noq/iroh QUIC streams, so the local control plane
//! uses this small hand-rolled framing instead; semantics live here so the
//! transport could be swapped without touching callers.)

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Frames larger than this are rejected (corrupt stream / protocol mismatch).
pub const MAX_FRAME: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CtrlRequest {
    Ping,
    Status,
    /// Run a configured backup now.
    BackupRun { backup_id: String },
    /// List snapshots, optionally filtered by backup id.
    SnapshotList { backup_id: Option<String> },
    Restore {
        backup_id: String,
        /// Restore this snapshot (unix seconds); latest if unset.
        snapshot: Option<u64>,
        target: PathBuf,
    },
}

pub type CtrlResult = Result<CtrlOk, CtrlError>;

/// Errors crossing the control socket are plain strings — the CLI only ever
/// prints them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CtrlError(pub String);

impl std::fmt::Display for CtrlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for CtrlError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CtrlOk {
    Pong,
    Status(StatusInfo),
    BackupDone(SnapshotInfo),
    Snapshots(Vec<SnapshotInfo>),
    RestoreDone { files: u64, bytes: u64, target: PathBuf },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub node_name: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub backups: Vec<BackupStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupStatus {
    pub backup_id: String,
    pub paths: Vec<PathBuf>,
    pub replicas: u32,
    pub snapshot_count: u64,
    pub last_snapshot: Option<SnapshotInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub backup_id: String,
    pub created_at: u64,
    pub manifest_hash: [u8; 32],
    pub file_count: u64,
    pub bytes_scanned: u64,
    pub bytes_new: u64,
    pub chunk_count: u64,
}

pub async fn write_frame<W, T>(w: &mut W, msg: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = postcard::to_allocvec(msg).map_err(std::io::Error::other)?;
    let len = u32::try_from(bytes.len()).map_err(std::io::Error::other)?;
    if len > MAX_FRAME {
        return Err(std::io::Error::other("frame exceeds MAX_FRAME"));
    }
    w.write_all(&len.to_le_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await
}

pub async fn read_frame<R, T>(r: &mut R) -> std::io::Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len_bytes = [0u8; 4];
    r.read_exact(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes);
    if len > MAX_FRAME {
        return Err(std::io::Error::other("frame exceeds MAX_FRAME"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    postcard::from_bytes(&buf).map_err(std::io::Error::other)
}
