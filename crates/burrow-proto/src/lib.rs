//! Shared protocol surface: ALPNs and wire types. The irpc service enums
//! (PeerProto for peer↔peer, CtrlProto for CLI↔daemon) land here in M2/M3.

/// ALPN for burrow's peer control protocol (contracts, quotas, repair).
pub const PEER_ALPN: &[u8] = b"burrow/peer/0";

/// Version negotiated in `Hello`; bump on incompatible PeerProto changes.
pub const PROTO_VERSION: u32 = 0;
