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
    BackupRun {
        backup_id: String,
    },
    /// List snapshots, optionally filtered by backup id.
    SnapshotList {
        backup_id: Option<String>,
    },
    Restore {
        backup_id: String,
        /// Restore this snapshot (unix seconds); latest if unset.
        snapshot: Option<u64>,
        target: PathBuf,
    },
    /// Produce a pairing ticket for this node.
    PeerInvite,
    /// Add a friend from their pairing ticket under a local nickname.
    PeerAdd {
        ticket: String,
        name: String,
    },
    /// List peers (live-refreshes grant/liveness info from reachable peers).
    PeerList,
    PeerRemove {
        name: String,
    },
    /// Pending inbound peerings and space requests.
    PendingList,
    /// Approve a pending inbound peer.
    Approve {
        name: String,
    },
    /// Deny/remove a pending inbound peer or clear their space request.
    Deny {
        name: String,
    },
    /// Reserve space for a peer (grow/shrink/revoke with bytes=0). Also the
    /// way a space request is granted.
    Grant {
        name: String,
        bytes: u64,
    },
    /// Ask a peer to reserve space for us.
    RequestSpace {
        name: String,
        bytes: u64,
    },
    /// Force a replication + verification pass now.
    RepairNow,
    /// Rebuild the snapshot catalog from what peers hold (disaster recovery).
    Resync,
    /// Link this device to another of the same owner via its ticket.
    DeviceJoin {
        ticket: String,
    },
    /// Suspend scheduled backups + replication (until resumed, or for a
    /// duration in seconds).
    Pause {
        seconds: Option<u64>,
    },
    Resume,
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
    RestoreDone {
        files: u64,
        bytes: u64,
        target: PathBuf,
    },
    Ticket(String),
    Peers(Vec<PeerInfo>),
    Pending {
        peers: Vec<PeerInfo>,
        space_requests: Vec<SpaceRequestInfo>,
    },
    /// Generic success with a human-readable summary.
    Done(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Local nickname for the OWNER (person).
    pub name: String,
    pub owner_pk: [u8; 32],
    /// "active" | "pending_in" | "self"
    pub state: String,
    /// Bytes this device reserves for them / of theirs it holds.
    pub given_bytes: u64,
    pub given_used: u64,
    /// Bytes their devices reserve for me / of mine they hold (summed).
    pub received_bytes: u64,
    pub received_used: u64,
    /// Whether they've approved us (from last contact).
    pub approved_by_them: Option<bool>,
    pub devices: Vec<DeviceInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub device_name: String,
    pub endpoint_id: [u8; 32],
    /// "host" | "client"
    pub mode: String,
    pub last_seen: Option<u64>,
    /// Result of the live refresh; None = not attempted.
    pub online: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceRequestInfo {
    pub peer_name: String,
    pub bytes: u64,
    /// Requester's self-reported give/take totals (advisory).
    pub given_total: u64,
    pub received_total: u64,
    pub requested_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub node_name: String,
    pub device_name: String,
    /// "host" | "client"
    pub mode: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub endpoint_id: [u8; 32],
    pub owner_pk: [u8; 32],
    pub backups: Vec<BackupStatus>,
    /// Hosting overview: space offered/used on THIS device.
    pub hosting: HostingInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HostingInfo {
    pub offer_max: Option<u64>,
    /// Total bytes held for everyone (self + friends).
    pub held_total: u64,
    /// (owner name, granted, used) for each granted owner.
    pub grants: Vec<(String, u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupStatus {
    pub backup_id: String,
    pub paths: Vec<PathBuf>,
    pub replicas: u32,
    pub snapshot_count: u64,
    pub last_snapshot: Option<SnapshotInfo>,
    pub health: ReplicationHealth,
}

/// Replication standing across every blob a backup references.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationHealth {
    pub total_blobs: u64,
    /// Blobs at or above their replica target.
    pub satisfied: u64,
    /// Blobs below target but with at least one remote replica.
    pub degraded: u64,
    /// Blobs with zero remote replicas.
    pub critical: u64,
}

impl ReplicationHealth {
    pub fn summary(&self) -> String {
        if self.total_blobs == 0 {
            "no data yet".to_string()
        } else if self.critical == self.total_blobs {
            "local only".to_string()
        } else if self.satisfied == self.total_blobs {
            "healthy".to_string()
        } else if self.critical > 0 {
            format!(
                "CRITICAL ({}/{} unreplicated)",
                self.critical, self.total_blobs
            )
        } else {
            format!(
                "degraded ({}/{} below target)",
                self.degraded, self.total_blobs
            )
        }
    }
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
    /// Files skipped via the unchanged-file cache (fresh runs only; 0 when
    /// listed from history).
    #[serde(default)]
    pub files_cached: u64,
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
