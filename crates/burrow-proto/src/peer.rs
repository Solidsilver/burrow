//! Peer ↔ peer control protocol. One request per QUIC bi-stream: the client
//! writes a postcard-encoded `PeerRequest` and finishes the stream; the server
//! replies with a `PeerReply`. The remote's identity comes from the QUIC
//! connection (`Connection::remote_id()`), never from the payload.

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

/// Cap on encoded request/reply size (these are small control messages).
pub const MAX_PEER_MSG: usize = 64 * 1024;

/// Identity a device presents (and mirrors back) in Hello. `cert` is the
/// owner key's signature over the device's endpoint id — the receiver checks
/// it against the TLS-authenticated connection, so devices prove ownership
/// with no shared state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceIdentity {
    pub owner_pk: [u8; 32],
    pub device_name: String,
    /// Owner's display name (config `node.name`).
    pub owner_name: String,
    /// "host" or "client".
    pub mode: String,
    #[serde(with = "BigArray")]
    pub cert: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerRequest {
    /// Introduce yourself; also used to re-sync names and approval state.
    Hello { identity: DeviceIdentity, proto_version: u32 },
    /// Ask the remote to reserve space for us. Totals are self-reported
    /// context for the human who approves (advisory ratio).
    RequestSpace { bytes: u64, given_total: u64, received_total: u64 },
    /// Notify that our grant to the remote changed (grow/shrink/revoke).
    GrantChanged { granted_bytes: u64 },
    /// Ask what the remote currently grants us and how much we use.
    QuotaStatus,
    /// Ask the remote to hold a blob for us. On accept, the remote pulls the
    /// blob from us over iroh-blobs before replying, so a success reply means
    /// the replica exists.
    RequestStore { hash: [u8; 32], size: u64, is_manifest: bool },
    /// Tell the remote it may drop blobs of ours it holds.
    Release { hashes: Vec<[u8; 32]> },
    /// List blobs of ours the remote holds (paginated; disaster recovery).
    ListHeld { offset: u64 },
    /// Same-owner only: share your view of owners and devices, so every
    /// device of a person knows the friends any one of them added.
    SyncPeers,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnerEntry {
    pub owner_pk: [u8; 32],
    pub name: String,
    /// "active" | "pending_in" (self is never sent).
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceEntry {
    pub endpoint_id: [u8; 32],
    pub owner_pk: [u8; 32],
    pub device_name: String,
    pub mode: String,
    pub ticket: Option<String>,
}

/// Page size for ListHeld replies (fits comfortably in MAX_PEER_MSG).
pub const HELD_PAGE: u64 = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeldEntry {
    pub hash: [u8; 32],
    pub size: u64,
    pub is_manifest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerReply {
    Hello(HelloReply),
    /// Space request recorded, pending human approval on the remote.
    RequestSpaceRecorded,
    GrantChangedAck,
    QuotaStatus(QuotaReply),
    /// Blob fetched and stored (or already present).
    StoreDone,
    ReleaseAck { dropped: u32 },
    HeldPage { entries: Vec<HeldEntry>, more: bool },
    PeersSnapshot { owners: Vec<OwnerEntry>, devices: Vec<DeviceEntry> },
    /// Request refused (unknown peer, not approved, malformed…).
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloReply {
    /// The responder's own identity, verified by the dialer the same way.
    pub identity: DeviceIdentity,
    pub proto_version: u32,
    /// Whether the responder's human has approved our OWNER as a peer
    /// (always true between devices of the same owner).
    pub approved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaReply {
    pub name: String,
    pub approved: bool,
    /// Bytes this device reserves for our owner (for same-owner requesters:
    /// remaining hosting capacity).
    pub granted_to_you: u64,
    /// Bytes of our owner's data this device currently holds.
    pub used_by_you: u64,
}
