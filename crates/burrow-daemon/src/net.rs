//! iroh endpoint/router assembly and the peer-protocol server + client.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use burrow_proto::peer::{PeerReply, PeerRequest, MAX_PEER_MSG};
use burrow_proto::PEER_ALPN;
use iroh::endpoint::presets;
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};

use crate::daemon::AppState;

/// Identity model: everything derives from the one repo key (master phrase).
///
/// - The OWNER key identifies the person. Friends approve and grant space to
///   the owner, not to machines.
/// - Each DEVICE gets its own endpoint identity, derived from the repo key
///   plus its device name, and carries a certificate — the owner key's
///   signature over its endpoint id — so any receiver can verify "this
///   connection belongs to that owner" without shared state.
///
/// Deriving identities from the repo secret is sound here: whoever holds the
/// phrase can already read every backup, so impersonation grants nothing
/// further. And it means one phrase recovers everything — data, owner
/// identity, and (with any device name) a working device identity.
pub fn owner_key(repo_key: &burrow_core::RepoKey) -> SecretKey {
    let bytes = blake3::derive_key("burrow v1 owner key", repo_key.as_bytes());
    SecretKey::from_bytes(&bytes)
}

pub fn device_key(repo_key: &burrow_core::RepoKey, device_name: &str) -> SecretKey {
    let mut material = Vec::with_capacity(32 + device_name.len());
    material.extend_from_slice(repo_key.as_bytes());
    material.extend_from_slice(device_name.as_bytes());
    let bytes = blake3::derive_key("burrow v1 device key", &material);
    SecretKey::from_bytes(&bytes)
}

const CERT_CONTEXT: &[u8] = b"burrow device v1";

/// Owner's signature binding a device endpoint id to the owner identity.
pub fn device_cert(repo_key: &burrow_core::RepoKey, device: EndpointId) -> [u8; 64] {
    let mut msg = Vec::with_capacity(CERT_CONTEXT.len() + 32);
    msg.extend_from_slice(CERT_CONTEXT);
    msg.extend_from_slice(device.as_bytes());
    owner_key(repo_key).sign(&msg).to_bytes()
}

pub fn verify_device_cert(owner_pk: &[u8; 32], device: EndpointId, cert: &[u8; 64]) -> bool {
    let Ok(owner) = iroh::PublicKey::from_bytes(owner_pk) else {
        return false;
    };
    let mut msg = Vec::with_capacity(CERT_CONTEXT.len() + 32);
    msg.extend_from_slice(CERT_CONTEXT);
    msg.extend_from_slice(device.as_bytes());
    let sig = iroh::Signature::from_bytes(cert);
    owner.verify(&msg, &sig).is_ok()
}

pub async fn build_endpoint(secret_key: SecretKey) -> anyhow::Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await
        .context("binding iroh endpoint")
}

/// The peer control protocol: one request per bi-stream.
#[derive(Debug, Clone)]
pub struct PeerProtocol {
    state: std::sync::Weak<AppState>,
}

impl PeerProtocol {
    pub fn new(state: &Arc<AppState>) -> Self {
        Self { state: Arc::downgrade(state) }
    }
}

impl ProtocolHandler for PeerProtocol {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let remote: EndpointId = connection.remote_id();
        let Some(state) = self.state.upgrade() else {
            return Ok(()); // daemon shutting down
        };
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(pair) => pair,
                Err(_) => break, // connection closed by peer
            };
            let state = state.clone();
            tokio::spawn(async move {
                let reply = async {
                    let bytes = recv
                        .read_to_end(MAX_PEER_MSG)
                        .await
                        .map_err(|e| anyhow::anyhow!("reading peer request: {e}"))?;
                    let req: PeerRequest = postcard::from_bytes(&bytes)?;
                    tracing::debug!(peer = %remote.fmt_short(), ?req, "peer request");
                    Ok::<PeerReply, anyhow::Error>(
                        crate::peers::handle_peer_request(&state, remote, req).await,
                    )
                }
                .await
                .unwrap_or_else(|e| PeerReply::Error(format!("{e:#}")));
                let encoded = match postcard::to_allocvec(&reply) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let _ = send.write_all(&encoded).await;
                let _ = send.finish();
                // Keep the task alive until the peer has read the reply.
                let _ = send.stopped().await;
            });
        }
        connection.closed().await;
        Ok(())
    }
}

/// Fetch one blob from a specific peer into the local store (verified,
/// resumable, no-op if already present).
pub async fn fetch_blob(
    state: &Arc<AppState>,
    from: EndpointId,
    hash: iroh_blobs::Hash,
) -> anyhow::Result<()> {
    let content = iroh_blobs::HashAndFormat::raw(hash);
    let local = state.blobs.remote().local(content).await?;
    if local.is_complete() {
        return Ok(());
    }
    let conn = state
        .endpoint
        .connect(from, iroh_blobs::protocol::ALPN)
        .await
        .context("connecting for blob fetch")?;
    state.blobs.remote().execute_get(conn, local.missing()).await.context("fetching blob")?;
    Ok(())
}

/// Dial a peer and perform one request/reply exchange.
pub async fn peer_call(
    endpoint: &Endpoint,
    addr: impl Into<EndpointAddr>,
    req: &PeerRequest,
) -> anyhow::Result<PeerReply> {
    // RequestStore replies only after the remote has pulled the blob, so its
    // budget scales with size (a large manifest on a slow uplink must still
    // fit, or it would time out and retry forever); everything else is a
    // quick round trip.
    let secs = match req {
        // 180s base + 1s per 256 KiB ≈ assumes a worst case of ~2 Mbit/s.
        PeerRequest::RequestStore { size, .. } => 180 + size / (256 * 1024),
        _ => 15,
    };
    peer_call_with_timeout(endpoint, addr, req, Duration::from_secs(secs)).await
}

pub async fn peer_call_with_timeout(
    endpoint: &Endpoint,
    addr: impl Into<EndpointAddr>,
    req: &PeerRequest,
    timeout: Duration,
) -> anyhow::Result<PeerReply> {
    let fut = async {
        let conn = endpoint.connect(addr, PEER_ALPN).await.context("connecting to peer")?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&postcard::to_allocvec(req)?).await?;
        send.finish()?;
        let bytes = recv.read_to_end(MAX_PEER_MSG).await.context("reading peer reply")?;
        conn.close(0u32.into(), b"done");
        Ok::<PeerReply, anyhow::Error>(postcard::from_bytes(&bytes)?)
    };
    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| anyhow::anyhow!("peer did not answer within {}s", timeout.as_secs()))?
}
