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

/// The node's iroh identity, derived from the repo key. One recovery phrase
/// therefore restores both the data AND the node identity — a recovered
/// machine keeps its EndpointId, so peers still recognize it and its stored
/// contracts/placements on their side remain valid.
///
/// Deriving identity from the repo secret is sound here: whoever holds the
/// phrase can already read every backup, so impersonating the node grants
/// nothing further.
pub fn node_key(repo_key: &burrow_core::RepoKey) -> SecretKey {
    let bytes = blake3::derive_key("burrow v1 node key", repo_key.as_bytes());
    SecretKey::from_bytes(&bytes)
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
    // RequestStore replies only after the remote has pulled the blob, so it
    // gets a transfer-sized budget; everything else is a quick round trip.
    let secs = match req {
        PeerRequest::RequestStore { .. } => 180,
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
