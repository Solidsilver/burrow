//! Per-peer authorization for the iroh-blobs data plane.
//!
//! Connections are only accepted from approved peers; blob GETs are only
//! served for hashes the requesting peer owns (rows in `held`). Transfer
//! completions feed the byte ledger.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::EndpointId;
use iroh_blobs::provider::events::{
    AbortReason, ConnectMode, EventMask, ProviderMessage, RequestMode,
};

use crate::daemon::AppState;

pub fn event_mask() -> EventMask {
    EventMask {
        connected: ConnectMode::Intercept,
        get: RequestMode::Intercept,
        ..EventMask::DEFAULT
    }
}

pub fn spawn_auth_loop(
    state: std::sync::Weak<AppState>,
    mut rx: tokio::sync::mpsc::Receiver<ProviderMessage>,
) {
    tokio::spawn(async move {
        // connection id -> authenticated peer, for request-level checks.
        let mut conn_peer: HashMap<u64, EndpointId> = HashMap::new();
        while let Some(msg) = rx.recv().await {
            let Some(state) = state.upgrade() else { break };
            match msg {
                ProviderMessage::ClientConnected(m) => {
                    let allowed = match m.endpoint_id {
                        Some(id) => {
                            if is_active_peer(&state, id).await {
                                conn_peer.insert(m.connection_id, id);
                                true
                            } else {
                                false
                            }
                        }
                        None => false,
                    };
                    tracing::debug!(
                        peer = ?m.endpoint_id.map(|id| id.fmt_short().to_string()),
                        allowed,
                        "blobs connection"
                    );
                    let res = if allowed { Ok(()) } else { Err(AbortReason::Permission) };
                    m.tx.send(res).await.ok();
                }
                ProviderMessage::ConnectionClosed(m) => {
                    conn_peer.remove(&m.connection_id);
                }
                ProviderMessage::GetRequestReceived(m) => {
                    let res = match conn_peer.get(&m.connection_id) {
                        Some(peer) => {
                            if owns_blob(&state, *peer, m.request.hash.as_bytes()).await {
                                Ok(())
                            } else {
                                Err(AbortReason::Permission)
                            }
                        }
                        None => Err(AbortReason::Permission),
                    };
                    m.tx.send(res).await.ok();
                }
                _ => {}
            }
        }
    });
}

async fn is_active_peer(state: &Arc<AppState>, id: EndpointId) -> bool {
    let bytes = id.as_bytes().to_vec();
    state
        .db
        .call(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM peers WHERE endpoint_id = ?1 AND state = 'active'",
                [&bytes],
                |r| r.get::<_, i64>(0),
            )? > 0)
        })
        .await
        .unwrap_or(false)
}

async fn owns_blob(state: &Arc<AppState>, peer: EndpointId, hash: &[u8; 32]) -> bool {
    let peer_bytes = peer.as_bytes().to_vec();
    let hash_bytes = hash.to_vec();
    state
        .db
        .call(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM held WHERE owner = ?1 AND blob_hash = ?2",
                rusqlite::params![peer_bytes, hash_bytes],
                |r| r.get::<_, i64>(0),
            )? > 0)
        })
        .await
        .unwrap_or(false)
}
