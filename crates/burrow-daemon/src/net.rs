//! iroh endpoint/router assembly and the peer-protocol server + client.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use burrow_proto::peer::{PeerReply, PeerRequest, MAX_PEER_MSG};
use burrow_proto::PEER_ALPN;
use iroh::endpoint::presets;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};

use crate::daemon::AppState;

/// Load or create the node's iroh identity (distinct from the repo key: this
/// is the *machine's* identity; the repo key encrypts the data).
pub fn load_or_create_node_key(path: &Path) -> anyhow::Result<SecretKey> {
    if path.exists() {
        let text = std::fs::read_to_string(path)?;
        let text = text.trim();
        let mut bytes = [0u8; 32];
        if text.len() != 64 {
            bail!("node key file {} is malformed", path.display());
        }
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
                .with_context(|| format!("node key file {} is not hex", path.display()))?;
        }
        Ok(SecretKey::from_bytes(&bytes))
    } else {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("OS RNG unavailable");
        let key = SecretKey::from_bytes(&bytes);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        std::fs::write(path, hex + "\n")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(key)
    }
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

/// Dial a peer and perform one request/reply exchange.
pub async fn peer_call(
    endpoint: &Endpoint,
    addr: impl Into<EndpointAddr>,
    req: &PeerRequest,
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
    tokio::time::timeout(Duration::from_secs(15), fut)
        .await
        .map_err(|_| anyhow::anyhow!("peer did not answer within 15s"))?
}
