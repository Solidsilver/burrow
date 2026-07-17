//! Optional web UI + JSON API (`--features web`, on by default).
//!
//! This is a thin HTTP front over the same operations the unix control
//! socket dispatches: the daemon's core loops never touch it, and building
//! with `--no-default-features` removes it entirely. Static assets (the
//! Svelte SPA) are embedded from `web-dist/`; a placeholder page is written
//! by build.rs when the frontend hasn't been built.
//!
//! Auth model: loopback clients are trusted (same as the control socket's
//! filesystem permissions). Any other client must send
//! `Authorization: Bearer <token>` with the token from
//! `~/.config/burrow/web.token` (`burrow web token` prints it).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Request, State};
use axum::http::{header, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use burrow_proto::ctrl::{PeerInfo, SnapshotInfo, SpaceRequestInfo, StatusInfo};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};

use crate::daemon::AppState;

#[derive(RustEmbed)]
#[folder = "web-dist/"]
struct Assets;

/// Everything the API needs beyond the shared daemon state.
#[derive(Clone)]
struct WebState {
    app: Arc<AppState>,
    token: String,
}

/// Bind and spawn the server; returns the address it is listening on.
/// Failures are logged by the caller — the daemon runs fine without the UI.
pub async fn start(
    app: Arc<AppState>,
) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let bind: SocketAddr = app
        .config
        .web
        .bind
        .parse()
        .with_context(|| format!("web.bind {:?}", app.config.web.bind))?;
    let token = crate::token::load_or_create(&crate::paths::web_token_file())?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding web UI to {bind}"))?;
    let addr = listener.local_addr()?;
    let state = WebState { app, token };
    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            tracing::error!("web UI server ended: {e:#}");
        }
    });
    Ok((addr, handle))
}

fn router(state: WebState) -> Router {
    let api = Router::new()
        .route("/server", get(server))
        .route("/status", get(status))
        .route("/peers", get(peers))
        .route("/pending", get(pending))
        .route("/snapshots", get(all_snapshots))
        .route("/backups", get(backup_configs))
        .route("/backups/{id}/snapshots", get(backup_snapshots))
        .route("/backups/{id}/run", post(backup_run))
        .route("/restore", post(restore))
        .route("/peers/invite", post(peer_invite))
        .route("/peers/add", post(peer_add))
        .route("/peers/{name}/remove", post(peer_remove))
        .route("/peers/{name}/grant", post(peer_grant))
        .route("/peers/{name}/request", post(peer_request))
        .route("/requests/{name}/approve", post(request_approve))
        .route("/requests/{name}/deny", post(request_deny))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/repair", post(repair))
        .route("/resync", post(resync))
        .route("/devices/join", post(device_join))
        .layer(middleware::from_fn_with_state(state.clone(), auth));
    Router::new()
        .nest("/api/v1", api)
        .fallback(static_or_spa)
        .with_state(state)
}

/// Loopback skips auth (same trust model as the control socket) unless
/// `[web] trust_loopback = false` (reverse-proxy setups); everyone else
/// needs `Authorization: Bearer <web.token>`.
async fn auth(
    State(state): State<WebState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let loopback = state.app.config.web.trust_loopback
        && req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip().is_loopback())
            // No connect info (tests, unusual transports): treat as local
            // rather than lock out.
            .unwrap_or(true);
    if loopback {
        return Ok(next.run(req).await);
    }
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(t) if crate::token::matches(t.trim(), &state.token) => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

// ---------------------------------------------------------------------------
// Handlers: thin adapters over the same ops the control socket dispatches.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ServerInfo {
    version: &'static str,
    bind: String,
    /// Active pause deadline (u64::MAX = until resumed), null if running.
    paused_until: Option<u64>,
}

async fn server(State(s): State<WebState>) -> Json<ServerInfo> {
    Json(ServerInfo {
        version: env!("CARGO_PKG_VERSION"),
        bind: s.app.config.web.bind.clone(),
        paused_until: s.app.paused_until(),
    })
}

async fn status(State(s): State<WebState>) -> Result<Json<StatusInfo>, ApiError> {
    Ok(Json(crate::ops::status(&s.app).await?))
}

async fn peers(State(s): State<WebState>) -> Result<Json<Vec<PeerInfo>>, ApiError> {
    Ok(Json(crate::peers::list(&s.app).await?))
}

#[derive(Serialize)]
struct PendingResponse {
    peers: Vec<PeerInfo>,
    space_requests: Vec<SpaceRequestInfo>,
}

async fn pending(State(s): State<WebState>) -> Result<Json<PendingResponse>, ApiError> {
    let (peers, space_requests) = crate::peers::pending(&s.app).await?;
    Ok(Json(PendingResponse {
        peers,
        space_requests,
    }))
}

async fn all_snapshots(State(s): State<WebState>) -> Result<Json<Vec<SnapshotInfo>>, ApiError> {
    Ok(Json(crate::ops::snapshot_list(&s.app, None).await?))
}

/// The configured `[[backup]]` sections (schedule, retention, excludes) —
/// runtime state like health and snapshot counts comes from `/status`.
#[derive(Serialize)]
struct BackupConfigView {
    id: String,
    paths: Vec<std::path::PathBuf>,
    exclude: Vec<String>,
    replicas: u32,
    schedule: Option<String>,
    keep_last: Option<u32>,
    min_offsite: u32,
}

async fn backup_configs(State(s): State<WebState>) -> Json<Vec<BackupConfigView>> {
    Json(
        s.app
            .config
            .backups
            .iter()
            .map(|b| BackupConfigView {
                id: b.id.clone(),
                paths: b.paths.clone(),
                exclude: b.exclude.clone(),
                replicas: b.replicas,
                schedule: b.schedule.clone(),
                keep_last: b.keep_last,
                min_offsite: b.min_offsite,
            })
            .collect(),
    )
}

async fn backup_snapshots(
    State(s): State<WebState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<SnapshotInfo>>, ApiError> {
    Ok(Json(crate::ops::snapshot_list(&s.app, Some(id)).await?))
}

async fn backup_run(
    State(s): State<WebState>,
    Path(id): Path<String>,
) -> Result<Json<SnapshotInfo>, ApiError> {
    Ok(Json(crate::ops::backup_run(&s.app, &id).await?))
}

#[derive(Deserialize)]
struct RestoreBody {
    backup_id: String,
    snapshot: Option<u64>,
    target: std::path::PathBuf,
}

#[derive(Serialize)]
struct RestoreResponse {
    files: u64,
    bytes: u64,
    target: std::path::PathBuf,
}

async fn restore(
    State(s): State<WebState>,
    Json(body): Json<RestoreBody>,
) -> Result<Json<RestoreResponse>, ApiError> {
    let (files, bytes, target) =
        crate::ops::restore(&s.app, &body.backup_id, body.snapshot, body.target).await?;
    Ok(Json(RestoreResponse {
        files,
        bytes,
        target,
    }))
}

#[derive(Serialize)]
struct TicketResponse {
    ticket: String,
}

async fn peer_invite(State(s): State<WebState>) -> Result<Json<TicketResponse>, ApiError> {
    Ok(Json(TicketResponse {
        ticket: crate::peers::invite(&s.app).await?,
    }))
}

#[derive(Deserialize)]
struct PeerAddBody {
    ticket: String,
    name: String,
}

async fn peer_add(
    State(s): State<WebState>,
    Json(body): Json<PeerAddBody>,
) -> Result<Json<Message>, ApiError> {
    message(crate::peers::add(&s.app, &body.ticket, &body.name).await)
}

async fn peer_remove(
    State(s): State<WebState>,
    Path(name): Path<String>,
) -> Result<Json<Message>, ApiError> {
    message(crate::peers::remove(&s.app, &name).await)
}

#[derive(Deserialize)]
struct BytesBody {
    bytes: u64,
}

async fn peer_grant(
    State(s): State<WebState>,
    Path(name): Path<String>,
    Json(body): Json<BytesBody>,
) -> Result<Json<Message>, ApiError> {
    message(crate::peers::grant(&s.app, &name, body.bytes).await)
}

async fn peer_request(
    State(s): State<WebState>,
    Path(name): Path<String>,
    Json(body): Json<BytesBody>,
) -> Result<Json<Message>, ApiError> {
    message(crate::peers::request_space(&s.app, &name, body.bytes).await)
}

async fn request_approve(
    State(s): State<WebState>,
    Path(name): Path<String>,
) -> Result<Json<Message>, ApiError> {
    message(crate::peers::approve(&s.app, &name).await)
}

async fn request_deny(
    State(s): State<WebState>,
    Path(name): Path<String>,
) -> Result<Json<Message>, ApiError> {
    message(crate::peers::deny(&s.app, &name).await)
}

#[derive(Deserialize)]
struct PauseBody {
    seconds: Option<u64>,
}

async fn pause(
    State(s): State<WebState>,
    Json(body): Json<PauseBody>,
) -> Result<Json<Message>, ApiError> {
    message(crate::ops::pause(&s.app, body.seconds).await)
}

async fn resume(State(s): State<WebState>) -> Result<Json<Message>, ApiError> {
    message(crate::ops::resume(&s.app).await)
}

async fn repair(State(s): State<WebState>) -> Result<Json<Message>, ApiError> {
    let (ok, lost) = crate::verify::verify_round(&s.app).await?;
    let placed = crate::replicate::tick(&s.app).await?;
    message(Ok(format!(
        "verified {ok} replicas ({lost} lost), placed {placed} new replicas"
    )))
}

async fn resync(State(s): State<WebState>) -> Result<Json<Message>, ApiError> {
    message(crate::ops::resync(&s.app).await)
}

#[derive(Deserialize)]
struct DeviceJoinBody {
    ticket: String,
}

async fn device_join(
    State(s): State<WebState>,
    Json(body): Json<DeviceJoinBody>,
) -> Result<Json<Message>, ApiError> {
    let (reply, _) = crate::peers::hello_via_ticket(&s.app, &body.ticket).await?;
    if reply.identity.owner_pk != s.app.owner_pk {
        return Err(ApiError(anyhow::anyhow!(
            "that ticket belongs to {:?}, not to you — use peer add for friends",
            reply.identity.owner_name
        )));
    }
    message(Ok(format!(
        "linked with your device {:?}",
        reply.identity.device_name
    )))
}

// ---------------------------------------------------------------------------
// Shared response shapes
// ---------------------------------------------------------------------------

/// Generic success with a human-readable summary (mirrors CtrlOk::Done).
#[derive(Serialize)]
struct Message {
    message: String,
}

fn message(result: anyhow::Result<String>) -> Result<Json<Message>, ApiError> {
    Ok(Json(Message { message: result? }))
}

struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("{:#}", self.0) })),
        )
            .into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError(e.into())
    }
}

// ---------------------------------------------------------------------------
// Static assets (embedded SPA with client-side fallback)
// ---------------------------------------------------------------------------

async fn static_or_spa(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    if let Some(file) = Assets::get(path) {
        return embedded(path, &file.data, cache_for(path));
    }
    // Client-side routes fall through to the SPA (or the build.rs placeholder
    // when the frontend hasn't been built).
    match Assets::get("index.html") {
        Some(file) => embedded("index.html", &file.data, "no-cache"),
        None => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            PLACEHOLDER,
        )
            .into_response(),
    }
}

fn embedded(path: &str, data: &[u8], cache: &str) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, content_type(path))
        .header(header::CACHE_CONTROL, cache.to_string())
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(Body::from(data.to_vec()))
        .expect("static response")
}

/// Vite emits hashed filenames under assets/ — cache them forever; revalidate
/// everything else.
fn cache_for(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("json") | Some("webmanifest") => "application/json",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Last-resort page when `web-dist/` is empty (shouldn't happen: build.rs
/// writes a placeholder index.html there before compile).
const PLACEHOLDER: &str =
    "<!doctype html><title>burrow</title><p>burrow web UI assets not built.</p>";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_types() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("assets/app-abc123.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("x.bin"), "application/octet-stream");
    }

    #[test]
    fn cache_policy() {
        assert!(cache_for("assets/app-abc.js").contains("immutable"));
        assert_eq!(cache_for("index.html"), "no-cache");
    }
}
