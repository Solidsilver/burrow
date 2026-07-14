//! Shared protocol surface: ALPNs, the CLI↔daemon control protocol, and (from
//! M3) the peer↔peer irpc service definitions.

pub mod ctrl;

/// ALPN for burrow's peer control protocol (contracts, quotas, repair).
pub const PEER_ALPN: &[u8] = b"burrow/peer/0";

/// Version negotiated in `Hello`; bump on incompatible PeerProto changes.
pub const PROTO_VERSION: u32 = 0;
