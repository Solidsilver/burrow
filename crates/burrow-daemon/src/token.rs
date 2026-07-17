//! Access token for the optional web UI. Loopback connections are trusted
//! (same trust model as the control socket); anything else must present this
//! token as `Authorization: Bearer …`. The token is state, not config: it is
//! generated once into `web.token` next to the repo key, mode 0600.

use std::path::Path;

use anyhow::Context;

/// Read the token from `path`, generating and saving a fresh one if the file
/// is missing or empty. Idempotent — the daemon and `burrow web token` always
/// agree on the value.
pub fn load_or_create(path: &Path) -> anyhow::Result<String> {
    if let Ok(text) = std::fs::read_to_string(path) {
        let token = text.trim();
        if !token.is_empty() {
            return Ok(token.to_string());
        }
    }
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("OS RNG unavailable");
    let token = hex(&bytes);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, format!("{token}\n"))
        .with_context(|| format!("writing web token {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(token)
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
    }
}
