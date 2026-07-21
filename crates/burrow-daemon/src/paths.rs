//! Directory layout. XDG-style on every unix (self-hosters expect
//! `~/.config`/`~/.local/share` even on macOS):
//!
//!   config: $BURROW_CONFIG_DIR | $XDG_CONFIG_HOME/burrow | ~/.config/burrow
//!   data:   $BURROW_DATA_DIR   | $XDG_DATA_HOME/burrow   | ~/.local/share/burrow

use std::path::Path;
use std::path::PathBuf;

/// Create `dir` (+ parents) and lock it to the owner (0700) on unix. An
/// existing directory is tightened only when group/other hold any bits, so
/// manual runs under a loose umask get fixed without churning already-safe
/// layouts. Applied at daemon startup and before every secret write; the
/// systemd unit's UMask=0077 makes it a no-op there.
pub fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir)?.permissions().mode();
        if mode & 0o077 != 0 {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
            tracing::info!(path = %dir.display(), "tightened directory permissions to 0700");
        }
    }
    Ok(())
}

/// Write `contents` to `path` with owner-only permissions from the moment
/// the file is created — no umask window. An existing file is truncated and
/// re-tightened through the open handle (its old mode survives open()).
#[cfg(unix)]
pub fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(contents.as_bytes())?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Non-unix has no mode bits; the platform's user profile ACLs apply.
#[cfg(not(unix))]
pub fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    std::fs::write(path, contents)
}

/// Like [`write_private`] but refuses to overwrite: created atomically with
/// mode 0600, `AlreadyExists` reported to the caller.
#[cfg(unix)]
pub fn create_private(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(contents.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
pub fn create_private(path: &Path, contents: &str) -> std::io::Result<()> {
    if path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "file exists",
        ));
    }
    std::fs::write(path, contents)
}

/// Warn at load time if a secret file is readable beyond the owner (e.g.
/// written by an older version under a loose umask).
#[cfg(unix)]
pub fn check_private_file(path: &Path, what: &str) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            tracing::warn!(
                path = %path.display(),
                mode = format!("0{mode:o}"),
                "{what} is readable by group/other — fix with: chmod 0600 {}",
                path.display()
            );
        }
    }
}

#[cfg(not(unix))]
pub fn check_private_file(_path: &Path, _what: &str) {}

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

/// Bearer token for the optional web UI when bound beyond loopback. State,
/// not config: auto-generated on first start with `[web] enable = true`.
pub fn web_token_file() -> PathBuf {
    config_dir().join("web.token")
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode(p: &Path) -> u32 {
        std::fs::metadata(p).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn private_writes_skip_the_umask_window() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("secret");
        write_private(&f, "one").unwrap();
        assert_eq!(mode(&f), 0o600);
        // Pre-existing loose file is re-tightened through the handle.
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&f, "two").unwrap();
        assert_eq!(mode(&f), 0o600);
        assert_eq!(std::fs::read_to_string(&f).unwrap(), "two");

        let g = dir.path().join("once");
        create_private(&g, "x").unwrap();
        assert_eq!(mode(&g), 0o600);
        let err = create_private(&g, "y").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read_to_string(&g).unwrap(), "x");
    }

    #[test]
    fn ensure_private_dir_tightens_but_keeps_safe_modes() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("d");
        std::fs::create_dir(&sub).unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
        ensure_private_dir(&sub).unwrap();
        assert_eq!(mode(&sub), 0o700);
        // Already owner-only: untouched.
        ensure_private_dir(&sub).unwrap();
        assert_eq!(mode(&sub), 0o700);
    }
}
