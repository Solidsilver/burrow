//! Daemon assembly and lifecycle.

use std::sync::Arc;

use anyhow::Context;
use burrow_core::RepoKey;
use iroh_blobs::store::fs::FsStore;
use tokio::net::UnixListener;

use crate::config::Config;
use crate::db::Db;

pub struct AppState {
    pub config: Config,
    pub repo_key: RepoKey,
    pub db: Db,
    /// iroh-blobs store (derefs from FsStore); cheap to clone.
    pub blobs: iroh_blobs::api::Store,
    /// Held for the FsStore's lifetime.
    pub fs_store: FsStore,
    /// This device's iroh endpoint.
    pub endpoint: iroh::Endpoint,
    /// The person this device belongs to (derived from the repo key).
    pub owner_pk: [u8; 32],
    pub device_name: String,
    /// Precomputed identity presented in Hello exchanges.
    pub identity: burrow_proto::peer::DeviceIdentity,
    /// Serializes backup runs.
    pub backup_lock: tokio::sync::Mutex<()>,
    /// Serializes replication passes.
    pub replicate_lock: tokio::sync::Mutex<()>,
    /// Background work suspended until this unix time (u64::MAX = until
    /// resumed). Manual commands ignore this.
    pub paused_until: std::sync::Mutex<Option<u64>>,
}

impl AppState {
    pub fn is_paused(&self) -> bool {
        let mut guard = self.paused_until.lock().expect("pause lock poisoned");
        match *guard {
            None => false,
            Some(until) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                if now >= until {
                    *guard = None;
                    false
                } else {
                    true
                }
            }
        }
    }
}

/// Run the daemon until ctrl-c / SIGTERM.
pub async fn run(config: Config) -> anyhow::Result<()> {
    let repo_key = crate::keys::load(&crate::paths::repo_key_file())?;
    let db = Db::open(&crate::paths::db_file())?;
    let blobs_dir = crate::paths::blobs_dir();
    std::fs::create_dir_all(&blobs_dir)?;

    // GC deletes anything unprotected: the callback protects every blob our
    // metadata says we need (own snapshots' chunks + blobs held for peers).
    // On a metadata read error we abort the GC run rather than delete blindly.
    let protect_db = db.clone();
    let protect: iroh_blobs::store::GcConfig = iroh_blobs::store::GcConfig {
        interval: std::time::Duration::from_secs(300),
        add_protected: Some(std::sync::Arc::new(move |set| {
            let db = protect_db.clone();
            Box::pin(async move {
                let hashes = db
                    .call(|conn| {
                        let mut stmt = conn.prepare(
                            "SELECT blob_hash FROM chunk_refs
                             UNION SELECT blob_hash FROM held",
                        )?;
                        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
                        let mut out = Vec::new();
                        for row in rows {
                            out.push(row?);
                        }
                        Ok(out)
                    })
                    .await;
                match hashes {
                    Ok(hashes) => {
                        for h in hashes {
                            if let Ok(arr) = <[u8; 32]>::try_from(h) {
                                set.insert(iroh_blobs::Hash::from_bytes(arr));
                            }
                        }
                        iroh_blobs::store::ProtectOutcome::Continue
                    }
                    Err(e) => {
                        tracing::error!("GC protect query failed; aborting GC run: {e:#}");
                        iroh_blobs::store::ProtectOutcome::Abort
                    }
                }
            })
        })),
    };
    let mut store_opts = iroh_blobs::store::fs::options::Options::new(&blobs_dir);
    store_opts.gc = Some(protect);
    let fs_store = FsStore::load_with_opts(blobs_dir.join("blobs.db"), store_opts)
        .await
        .with_context(|| format!("opening blob store {}", blobs_dir.display()))?;
    let blobs: iroh_blobs::api::Store = (*fs_store).clone();

    let device_name =
        crate::keys::load_or_create_device_name(&crate::paths::device_name_file(), None)?;
    let endpoint =
        crate::net::build_endpoint(crate::net::device_key(&repo_key, &device_name)).await?;
    let owner_pk = *crate::net::owner_key(&repo_key).public().as_bytes();
    let identity = burrow_proto::peer::DeviceIdentity {
        owner_pk,
        device_name: device_name.clone(),
        owner_name: config.node_name(),
        mode: config.device.mode.as_str().to_string(),
        cert: crate::net::device_cert(&repo_key, endpoint.id()),
    };

    let state = Arc::new(AppState {
        config,
        repo_key,
        db,
        blobs,
        fs_store,
        endpoint: endpoint.clone(),
        owner_pk,
        device_name: device_name.clone(),
        identity,
        backup_lock: tokio::sync::Mutex::new(()),
        replicate_lock: tokio::sync::Mutex::new(()),
        paused_until: std::sync::Mutex::new(None),
    });

    // Register ourselves: the self owner row and this device.
    {
        let pk = owner_pk.to_vec();
        let my_id = state.endpoint.id().as_bytes().to_vec();
        let name = state.config.node_name();
        let dev = device_name.clone();
        let mode = state.config.device.mode.as_str().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_secs();
        state
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO owners (owner_pk, name, state, added_at, last_seen)
                     VALUES (?1, ?2, 'self', ?3, ?3)
                     ON CONFLICT(owner_pk) DO UPDATE SET name = excluded.name, state = 'self'",
                    rusqlite::params![pk, name, now],
                )?;
                conn.execute(
                    "INSERT INTO devices (endpoint_id, owner_pk, device_name, mode, last_seen)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(endpoint_id) DO UPDATE SET
                       device_name = excluded.device_name, mode = excluded.mode",
                    rusqlite::params![my_id, pk, dev, mode, now],
                )?;
                Ok(())
            })
            .await?;
    }

    // Data plane: iroh-blobs gated by the per-peer auth loop.
    let (events_tx, events_rx) = iroh_blobs::provider::events::EventSender::channel(
        32,
        crate::auth::event_mask(),
    );
    crate::auth::spawn_auth_loop(Arc::downgrade(&state), events_rx);
    let blobs_proto = iroh_blobs::BlobsProtocol::new(&state.blobs, Some(events_tx));

    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs_proto)
        .accept(burrow_proto::PEER_ALPN, crate::net::PeerProtocol::new(&state))
        .spawn();
    tracing::info!(endpoint_id = %state.endpoint.id(), "iroh endpoint up");

    let socket = crate::paths::socket_path();
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
        // The socket dir may live in a shared /tmp — lock it to this user.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    // A stale socket file from an unclean shutdown blocks bind; if nothing is
    // listening on it, remove it.
    if socket.exists() {
        match tokio::net::UnixStream::connect(&socket).await {
            Ok(_) => anyhow::bail!(
                "another burrow daemon is already listening on {}",
                socket.display()
            ),
            Err(_) => std::fs::remove_file(&socket)?,
        }
    }
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("binding control socket {}", socket.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    }
    tracing::info!(
        node = %state.config.node_name(),
        socket = %socket.display(),
        "burrow daemon up"
    );

    let ctrl = tokio::spawn(crate::ctrl::serve(state.clone(), listener));
    crate::replicate::spawn_replication_loop(Arc::downgrade(&state));
    crate::verify::spawn_verify_loop(Arc::downgrade(&state));
    crate::scheduler::spawn_scheduler(Arc::downgrade(&state));

    // Consume a pending join/pair ticket left by `burrow device join` or
    // `burrow peer add` on a machine whose daemon wasn't running yet.
    {
        let ticket_file = crate::paths::join_ticket_file();
        if let Ok(ticket) = std::fs::read_to_string(&ticket_file) {
            let state = state.clone();
            tokio::spawn(async move {
                match crate::peers::hello_via_ticket(&state, ticket.trim()).await {
                    Ok((reply, _)) => {
                        tracing::info!(
                            with = %reply.identity.device_name,
                            same_owner = reply.identity.owner_pk == state.owner_pk,
                            "pending join ticket consumed"
                        );
                        let _ = std::fs::remove_file(&ticket_file);
                    }
                    Err(e) => {
                        tracing::warn!("pending join ticket failed (kept for retry): {e:#}")
                    }
                }
            });
        }
    }

    shutdown_signal().await;
    tracing::info!("shutting down");
    // Unlink the socket immediately: a replacement daemon may bind the same
    // path while we finish the slow parts of shutdown, and removing it last
    // would delete *their* socket.
    let _ = std::fs::remove_file(&socket);
    ctrl.abort();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), router.shutdown()).await;
    state.fs_store.shutdown().await.ok();
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("installing SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
