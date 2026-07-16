//! Directory layout. XDG-style on every unix (self-hosters expect
//! `~/.config`/`~/.local/share` even on macOS):
//!
//!   config: $BURROW_CONFIG_DIR | $XDG_CONFIG_HOME/burrow | ~/.config/burrow
//!   data:   $BURROW_DATA_DIR   | $XDG_DATA_HOME/burrow   | ~/.local/share/burrow

use std::path::PathBuf;

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn config_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("BURROW_CONFIG_DIR") {
        return PathBuf::from(dir);
    }
    match std::env::var_os("XDG_CONFIG_HOME") {
        Some(x) if !x.is_empty() => PathBuf::from(x).join("burrow"),
        _ => home().join(".config/burrow"),
    }
}

pub fn data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("BURROW_DATA_DIR") {
        return PathBuf::from(dir);
    }
    match std::env::var_os("XDG_DATA_HOME") {
        Some(x) if !x.is_empty() => PathBuf::from(x).join("burrow"),
        _ => home().join(".local/share/burrow"),
    }
}

pub fn config_file() -> PathBuf {
    config_dir().join("config.toml")
}

pub fn repo_key_file() -> PathBuf {
    config_dir().join("repo.key")
}

/// The device's stable name (state, not config — renaming would change the
/// derived device identity). Written once by init / device join.
pub fn device_name_file() -> PathBuf {
    config_dir().join("device.name")
}

/// A pending `device join` / `peer add` ticket the daemon consumes at startup
/// (lets joining work before the daemon has ever run — headless friendly).
pub fn join_ticket_file() -> PathBuf {
    config_dir().join("join.ticket")
}

/// Unix sockets are limited to ~104 bytes of path (SUN_LEN), so the socket
/// can't live under an arbitrarily deep data dir. It goes in a short runtime
/// dir instead, named by a hash of the data dir so distinct daemons never
/// collide. Override with $BURROW_SOCKET.
pub fn socket_path() -> PathBuf {
    if let Some(sock) = std::env::var_os("BURROW_SOCKET") {
        return PathBuf::from(sock);
    }
    let runtime = match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(x) if !x.is_empty() => PathBuf::from(x),
        _ => PathBuf::from("/tmp"),
    };
    let data = data_dir();
    let tag = &blake3::hash(data.as_os_str().as_encoded_bytes()).to_hex()[..8];
    #[cfg(unix)]
    let uid = unsafe { libc::getuid() };
    #[cfg(not(unix))]
    let uid = 0;
    runtime
        .join(format!("burrow-{uid}"))
        .join(format!("{tag}.sock"))
}

/// Bulk blob storage (own chunks + data held for friends, plus iroh's blob
/// index). $BURROW_BLOBS_DIR relocates it independently of the metadata dir —
/// typical for servers that keep metadata on fast storage and blobs on a
/// large pool.
pub fn blobs_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("BURROW_BLOBS_DIR") {
        return PathBuf::from(dir);
    }
    data_dir().join("blobs")
}

pub fn db_file() -> PathBuf {
    data_dir().join("meta.db")
}
