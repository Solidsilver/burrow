//! Repo key on disk: 64 hex chars in a 0600 file under the config dir.

use std::path::Path;

use anyhow::{bail, Context};
use burrow_core::RepoKey;

pub fn generate_and_save(path: &Path) -> anyhow::Result<RepoKey> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_private_dir(parent)?;
    }
    let key = RepoKey::generate();
    let hex: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    // create_new: the no-overwrite check and the 0600 create are atomic.
    crate::paths::create_private(path, &(hex + "\n")).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            anyhow::anyhow!(
                "repo key already exists at {} — refusing to overwrite it",
                path.display()
            )
        } else {
            anyhow::Error::new(e).context(format!("writing repo key {}", path.display()))
        }
    })?;
    Ok(key)
}

pub fn load(path: &Path) -> anyhow::Result<RepoKey> {
    crate::paths::check_private_file(path, "repo key");
    let text = std::fs::read_to_string(path).with_context(|| {
        format!(
            "reading repo key {} (run `burrow init` first?)",
            path.display()
        )
    })?;
    let text = text.trim();
    if text.len() != 64 {
        bail!("repo key file {} is malformed", path.display());
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .with_context(|| format!("repo key file {} is not hex", path.display()))?;
    }
    Ok(RepoKey::from_bytes(bytes))
}

/// Load the device's stable name, or create it (defaulting to the hostname).
pub fn load_or_create_device_name(path: &Path, preferred: Option<&str>) -> anyhow::Result<String> {
    if path.exists() {
        let name = std::fs::read_to_string(path)?.trim().to_string();
        if name.is_empty() {
            bail!("device name file {} is empty", path.display());
        }
        if let Some(p) = preferred {
            if p != name {
                bail!(
                    "device is already named {name:?} (in {}); renaming would change \
                     its identity — move the file away if you really mean to",
                    path.display()
                );
            }
        }
        return Ok(name);
    }
    let name = match preferred {
        Some(p) => {
            // Explicit names must be clean; defaults get sanitized below.
            if p.is_empty()
                || !p
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                bail!("device name {p:?} must be non-empty [a-zA-Z0-9_-]");
            }
            p.to_string()
        }
        None => {
            let host = gethostname::gethostname().to_string_lossy().into_owned();
            let base = host.split('.').next().unwrap_or("device");
            let clean: String = base
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '-'
                    }
                })
                .collect();
            if clean.is_empty() {
                "device".to_string()
            } else {
                clean
            }
        }
    };
    if let Some(parent) = path.parent() {
        crate::paths::ensure_private_dir(parent)?;
    }
    std::fs::write(path, format!("{name}\n"))?;
    Ok(name)
}

/// Write a recovered repo key (shared by init and `burrow recover`).
pub fn save_key(path: &Path, key: &RepoKey) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_private_dir(parent)?;
    }
    let hex: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    crate::paths::write_private(path, &(hex + "\n"))
        .with_context(|| format!("writing repo key {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repo.key");
        let key = generate_and_save(&path).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(key.as_bytes(), loaded.as_bytes());
        assert!(
            generate_and_save(&path).is_err(),
            "must refuse to overwrite"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "key must be owner-only from creation");
        }
    }
}
