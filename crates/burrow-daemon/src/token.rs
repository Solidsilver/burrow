//! Access token for the optional web UI. Loopback connections are trusted
//! (same trust model as the control socket); anything else must present this
//! token as `Authorization: Bearer …`. (All requests first pass the
//! Host/Origin rebinding guard — see web.rs.) The token is state, not
//! config: it is generated once into `web.token` next to the repo key, mode
//! 0600.

use std::path::Path;

use anyhow::Context;

/// Read the token from `path`, generating and saving a fresh one if the file
/// is missing or empty. Idempotent — the daemon and `burrow web token` always
/// agree on the value. Created atomically at 0600 (no umask window); if two
/// processes race the creation, the loser adopts the winner's token.
pub fn load_or_create(path: &Path) -> anyhow::Result<String> {
    if let Ok(text) = std::fs::read_to_string(path) {
        let token = text.trim();
        if !token.is_empty() {
            crate::paths::check_private_file(path, "web token");
            return Ok(token.to_string());
        }
    }
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("OS RNG unavailable");
    let token = hex(&bytes);
    if let Some(parent) = path.parent() {
        crate::paths::ensure_private_dir(parent)?;
    }
    match crate::paths::create_private(path, &format!("{token}\n")) {
        Ok(()) => Ok(token),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading web token {}", path.display()))?;
            Ok(text.trim().to_string())
        }
        Err(e) => Err(e).with_context(|| format!("writing web token {}", path.display())),
    }
}

/// Constant-time-ish check: hash both sides so the comparison is over fixed
/// 32-byte values regardless of token length.
pub fn matches(presented: &str, expected: &str) -> bool {
    blake3::hash(presented.as_bytes()) == blake3::hash(expected.as_bytes())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("web.token");
        let a = load_or_create(&path).unwrap();
        let b = load_or_create(&path).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(matches(&a, &b));
        assert!(!matches(&a, "wrong"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "token must be owner-only from creation");
        }
    }
}
