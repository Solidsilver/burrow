//! Peer ↔ peer control protocol. One request per QUIC bi-stream: the client
//! writes a postcard-encoded `PeerRequest` and finishes the stream; the server
//! replies with a `PeerReply`. The remote's identity comes from the QUIC
//! connection (`Connection::remote_id()`), never from the payload.

use serde::{Deserialize, Serialize};

/// Cap on encoded request/reply size (these are small control messages).
pub const MAX_PEER_MSG: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerRequest {
    /// Introduce yourself; also used to re-sync names and approval state.
    Hello { name: String, proto_version: u32 },
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
    /// Request refused (unknown peer, not approved, malformed…).
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloReply {
    pub name: String,
    pub proto_version: u32,
    /// Whether the remote's human has approved us as a peer.
    pub approved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaReply {
    pub name: String,
    pub approved: bool,
    /// Bytes the remote reserves for us.
    pub granted_to_you: u64,
    /// Bytes of ours the remote currently holds.
    pub used_by_you: u64,
}
