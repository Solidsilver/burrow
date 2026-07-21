//! Optional web UI + JSON API (`--features web`, on by default).
//!
//! This is a thin HTTP front over the same operations the unix control
//! socket dispatches: the daemon's core loops never touch it, and building
//! with `--no-default-features` removes it entirely. Static assets (the
//! Svelte SPA) are embedded from `web-dist/`; a placeholder page is written
//! by build.rs when the frontend hasn't been built.
//!
//! Auth model: every request must carry a `Host` we recognize (an IP literal,
//! `localhost`/`*.localhost`, or a name in `[web] allowed_hosts`) — that is
//! what stops DNS rebinding, since a rebound attack page always keeps its own
//! domain in the URL. Mutating requests additionally reject cross-site
//! `Origin`/`Sec-Fetch-Site`. Loopback clients are then trusted without a
//! token (same as the control socket's filesystem permissions). Any other
//! client must send `Authorization: Bearer <token>` with the token from
//! `~/.config/burrow/web.token` (`burrow web token` prints it).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Request, State};
use axum::http::{header, Method, StatusCode, Uri};
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
        // Outermost layer: runs before auth, on API and static alike.
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state)
}

/// DNS-rebinding / cross-site guard.
///
/// * `Host` must be an IP literal, `localhost`/`*.localhost`, or a name in
///   `[web] allowed_hosts`. A rebound attack page keeps its own domain in
///   the URL, so its fetches arrive as `Host: attacker.example` and die
///   here — while users who browsed to an IP or localhost are unaffected.
/// * Mutating methods additionally reject cross-site requests: a present
///   `Origin` must match the `Host` name (or be allowlisted), and
///   `Sec-Fetch-Site: cross-site` is refused. Classic CSRF is already
///   impossible — no cookies, and the `Json` extractors force a preflighted
///   content type with no CORS layer — so these are defense in depth.
async fn guard(
    State(state): State<WebState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let allowed = &state.app.config.web.allowed_hosts;
    if !host_allowed(host, allowed) {
        return Err(StatusCode::FORBIDDEN);
    }
    let method = req.method();
    if !(method == Method::GET || method == Method::HEAD || method == Method::OPTIONS) {
        if req
            .headers()
            .get("sec-fetch-site")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|site| site.eq_ignore_ascii_case("cross-site"))
        {
            return Err(StatusCode::FORBIDDEN);
        }
        if let Some(origin) = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
        {
            // Proxies that don't forward the original Host still pass when
            // the Origin's name is explicitly allowlisted.
            if !origin_matches_host(origin, host) && !origin_allowlisted(origin, allowed) {
                return Err(StatusCode::FORBIDDEN);
            }
        }
    }
    Ok(next.run(req).await)
}

/// The host part of a `Host` header value, without the port:
/// "127.0.0.1:8385" → "127.0.0.1", "[::1]:8385" → "::1". None for malformed
/// values (empty host, unbracketed IPv6).
fn host_name(host_header: &str) -> Option<&str> {
    let h = host_header.trim();
    if let Some(rest) = h.strip_prefix('[') {
        let end = rest.find(']')?;
        return Some(&rest[..end]);
    }
    match h.matches(':').count() {
        0 => Some(h),
        1 => h.split(':').next(),
        _ => None,
    }
}

/// Is this `Host` value one we serve? IP literals (any — a rebounded attack
/// page cannot mint a literal-IP `Host`) and localhost names are always fine;
/// DNS names must be explicitly allowlisted in `[web] allowed_hosts`.
fn host_allowed(host_header: &str, extra: &[String]) -> bool {
    let Some(name) = host_name(host_header) else {
        return false;
    };
    if name.is_empty() {
        return false;
    }
    if name.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    let lower = name.to_ascii_lowercase();
    lower == "localhost"
        || lower.ends_with(".localhost")
        || extra.iter().any(|h| h.trim().eq_ignore_ascii_case(&lower))
}

/// The authority (host[:port]) of an `Origin` header value, e.g.
/// "http://127.0.0.1:8385" → "127.0.0.1:8385". None without a scheme
/// separator (covers the "null" origin of sandboxed frames).
fn origin_host(origin: &str) -> Option<&str> {
    let authority = origin.split("://").nth(1)?;
    Some(authority.split('/').next().unwrap_or(""))
}

/// Does an `Origin` belong to the same host as the request's `Host`?
/// Compares names only: scheme and port legitimately differ behind a
/// TLS-terminating proxy.
fn origin_matches_host(origin: &str, host_header: &str) -> bool {
    match (
        origin_host(origin).and_then(host_name),
        host_name(host_header),
    ) {
        (Some(a), Some(b)) => !a.is_empty() && a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

/// Is the `Origin`'s host name in the `[web] allowed_hosts` list?
fn origin_allowlisted(origin: &str, extra: &[String]) -> bool {
    origin_host(origin)
        .and_then(host_name)
        .is_some_and(|name| extra.iter().any(|h| h.trim().eq_ignore_ascii_case(name)))
}

/// Loopback skips auth (same trust model as the control socket) unless
/// `[web] trust_loopback = false` (reverse-proxy setups); everyone else
/// needs `Authorization: Bearer <web.token>`. Runs after `guard`, so the
/// `Host` here is already known-good.
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

    #[test]
    fn host_names() {
        assert_eq!(host_name("127.0.0.1:8385"), Some("127.0.0.1"));
        assert_eq!(host_name("localhost"), Some("localhost"));
        assert_eq!(host_name("[::1]:8385"), Some("::1"));
        assert_eq!(host_name("[::1]"), Some("::1"));
        assert_eq!(host_name(":8385"), Some(""));
        assert_eq!(host_name("::1:8385"), None); // unbracketed v6: malformed
    }

    #[test]
    fn host_allowlist() {
        let extra = vec!["burrow.example.com".to_string()];
        // IP literals always pass (loopback, LAN, v6) — a rebound attack
        // page can't mint a literal-IP Host.
        assert!(host_allowed("127.0.0.1:8385", &extra));
        assert!(host_allowed("127.0.0.1", &extra));
        assert!(host_allowed("192.168.1.5:8385", &extra));
        assert!(host_allowed("[::1]:8385", &extra));
        // localhost names pass.
        assert!(host_allowed("localhost:8385", &extra));
        assert!(host_allowed("ui.localhost:8385", &extra));
        assert!(host_allowed("LOCALHOST:8385", &extra));
        // Allowlisted DNS names pass.
        assert!(host_allowed("burrow.example.com", &extra));
        assert!(host_allowed("Burrow.Example.COM:443", &extra));
        // Everything else — notably the attacker's rebound domain — fails.
        assert!(!host_allowed("attacker.example", &extra));
        assert!(!host_allowed("burrow.example.com", &[]));
        assert!(!host_allowed("", &extra));
        assert!(!host_allowed(":8385", &extra));
        assert!(!host_allowed("::1:8385", &extra));
    }

    #[test]
    fn origin_checks() {
        assert!(origin_matches_host(
            "http://127.0.0.1:8385",
            "127.0.0.1:8385"
        ));
        // Scheme/port may differ behind a TLS-terminating proxy.
        assert!(origin_matches_host(
            "https://burrow.example.com",
            "burrow.example.com"
        ));
        assert!(origin_matches_host("http://[::1]:8385", "[::1]:8385"));
        assert!(!origin_matches_host(
            "http://attacker.example",
            "127.0.0.1:8385"
        ));
        assert!(!origin_matches_host("null", "127.0.0.1:8385"));
        assert!(!origin_matches_host("", "127.0.0.1:8385"));

        let extra = vec!["burrow.example.com".to_string()];
        assert!(origin_allowlisted("https://burrow.example.com", &extra));
        assert!(!origin_allowlisted("https://attacker.example", &extra));
        assert!(!origin_allowlisted("null", &extra));
    }
}
