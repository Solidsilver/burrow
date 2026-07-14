//! Build a snapshot of a directory tree into a blob store, and restore one
//! back out. Pure filesystem + store logic; the daemon supplies scheduling,
//! placement, and networking around this.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, Write};
use std::path::{Component, Path, PathBuf};

use crate::chunk::chunk_stream;
use crate::crypto::{BlobHash, RepoKey};
use crate::error::{CoreError, Result};
use crate::manifest::{ChunkRef, Entry, EntryKind, Manifest, MANIFEST_FORMAT};
use crate::store::BlobStore;

pub struct SnapshotOptions {
    pub backup_id: String,
    pub node_name: String,
    /// Unix seconds; injected so core stays deterministic/clock-free.
    pub created_at: u64,
    /// Glob-free exclusion for now: any path component equal to one of these
    /// is skipped. Real glob matching arrives with the daemon config layer.
    pub exclude_names: Vec<String>,
}

pub struct SnapshotResult {
    pub manifest: Manifest,
    /// Hash of the sealed manifest blob (already in the store).
    pub manifest_hash: BlobHash,
    /// Blobs newly written by this snapshot (excludes pre-existing dedup hits
    /// and the manifest itself).
    pub new_blobs: Vec<BlobHash>,
    pub bytes_scanned: u64,
    pub bytes_new: u64,
}

/// Walk `root`, chunk+seal every file, store blobs, and store the sealed
/// manifest. Deduplication is inherent: sealing is deterministic, so unchanged
/// content maps to blobs the store already has.
pub fn create_snapshot<S: BlobStore>(
    store: &mut S,
    key: &RepoKey,
    root: &Path,
    opts: &SnapshotOptions,
) -> Result<SnapshotResult> {
    // BTreeMap keyed by relative path => deterministic manifest ordering.
    let mut entries: BTreeMap<String, Entry> = BTreeMap::new();
    let mut new_blobs = Vec::new();
    let mut bytes_scanned = 0u64;
    let mut bytes_new = 0u64;

    let walker = walkdir::WalkDir::new(root).follow_links(false).into_iter();
    for item in walker.filter_entry(|e| {
        e.depth() == 0
            || !opts
                .exclude_names
                .iter()
                .any(|x| e.file_name().to_string_lossy().as_ref() == x)
    }) {
        let item = item.map_err(|e| CoreError::Io(std::io::Error::other(e.to_string())))?;
        if item.depth() == 0 {
            continue; // the root itself is implicit
        }
        let rel = relative_path_string(item.path(), root)?;
        let meta = item.metadata().map_err(|e| CoreError::Io(std::io::Error::other(e.to_string())))?;
        let (mode, mtime) = mode_and_mtime(&meta);

        let kind = if item.file_type().is_dir() {
            EntryKind::Dir
        } else if item.file_type().is_symlink() {
            let target = fs::read_link(item.path())?;
            EntryKind::Symlink { target: target.to_string_lossy().into_owned() }
        } else if item.file_type().is_file() {
            let mut chunks = Vec::new();
            let reader = BufReader::new(fs::File::open(item.path())?);
            for chunk in chunk_stream(reader) {
                let plaintext = chunk?;
                bytes_scanned += plaintext.len() as u64;
                let sealed = key.seal_chunk(&plaintext);
                if !store.contains(&sealed.blob_hash) {
                    bytes_new += sealed.blob.len() as u64;
                    let stored = store.put(sealed.blob)?;
                    debug_assert_eq!(stored, sealed.blob_hash);
                    new_blobs.push(sealed.blob_hash);
                }
                chunks.push(ChunkRef {
                    plain_id: sealed.plain_id,
                    blob_hash: sealed.blob_hash,
                    size: plaintext.len() as u32,
                });
            }
            EntryKind::File { size: meta.len(), chunks }
        } else {
            continue; // sockets, fifos, devices: not backed up
        };

        entries.insert(rel.clone(), Entry { path: rel, kind, mode, mtime });
    }

    let manifest = Manifest {
        format: MANIFEST_FORMAT,
        backup_id: opts.backup_id.clone(),
        node_name: opts.node_name.clone(),
        created_at: opts.created_at,
        entries: entries.into_values().collect(),
    };
    let sealed = manifest.seal(key);
    let manifest_hash = store.put(sealed.blob)?;

    Ok(SnapshotResult { manifest, manifest_hash, new_blobs, bytes_scanned, bytes_new })
}

/// Restore a snapshot into `target` (created if missing; must be empty or
/// non-existent to avoid clobbering).
pub fn restore_snapshot<S: BlobStore>(
    store: &S,
    key: &RepoKey,
    manifest_hash: &BlobHash,
    target: &Path,
) -> Result<Manifest> {
    let manifest = Manifest::open(key, &store.get(manifest_hash)?)?;
    fs::create_dir_all(target)?;

    for entry in &manifest.entries {
        let dest = safe_join(target, &entry.path)?;
        match &entry.kind {
            EntryKind::Dir => fs::create_dir_all(&dest)?,
            EntryKind::Symlink { target: link_target } => {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)?;
                }
                #[cfg(unix)]
                std::os::unix::fs::symlink(link_target, &dest)?;
                #[cfg(not(unix))]
                let _ = link_target; // symlink restore is unix-only for now
            }
            EntryKind::File { size, chunks } => {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut f = fs::File::create(&dest)?;
                let mut written = 0u64;
                for c in chunks {
                    let plaintext = key.open_chunk(&store.get(&c.blob_hash)?)?;
                    f.write_all(&plaintext)?;
                    written += plaintext.len() as u64;
                }
                f.flush()?;
                if written != *size {
                    return Err(CoreError::Io(std::io::Error::other(format!(
                        "restored {written} bytes for {} but manifest says {size}",
                        entry.path
                    ))));
                }
            }
        }
        apply_metadata(&dest, entry);
    }
    Ok(manifest)
}

fn relative_path_string(path: &Path, root: &Path) -> Result<String> {
    let rel = path.strip_prefix(root).map_err(|_| CoreError::PathEscape(path.to_path_buf()))?;
    Ok(rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

/// Join a manifest-relative path onto the target, rejecting traversal.
fn safe_join(target: &Path, rel: &str) -> Result<PathBuf> {
    let rel_path = Path::new(rel);
    for comp in rel_path.components() {
        match comp {
            Component::Normal(_) => {}
            _ => return Err(CoreError::PathEscape(rel_path.to_path_buf())),
        }
    }
    Ok(target.join(rel_path))
}

fn mode_and_mtime(meta: &fs::Metadata) -> (u32, i64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (meta.mode(), meta.mtime())
    }
    #[cfg(not(unix))]
    {
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        (0o644, mtime)
    }
}

fn apply_metadata(dest: &Path, entry: &Entry) {
    // Best-effort: metadata restore failures shouldn't abort a data restore.
    if matches!(entry.kind, EntryKind::Symlink { .. }) {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(dest, fs::Permissions::from_mode(entry.mode));
    }
    let _ = filetime::set_file_mtime(dest, filetime::FileTime::from_unix_time(entry.mtime, 0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    fn testkey() -> RepoKey {
        RepoKey::from_bytes([3u8; 32])
    }

    fn opts() -> SnapshotOptions {
        SnapshotOptions {
            backup_id: "test".into(),
            node_name: "unit".into(),
            created_at: 1_700_000_000,
            exclude_names: vec![".cache".into()],
        }
    }

    fn build_tree(root: &Path) {
        fs::create_dir_all(root.join("sub/deep")).unwrap();
        fs::create_dir_all(root.join(".cache")).unwrap();
        fs::write(root.join("hello.txt"), b"hello burrow").unwrap();
        fs::write(root.join("sub/deep/data.bin"), vec![0xAB; 3 * 1024 * 1024]).unwrap();
        fs::write(root.join(".cache/skipme"), b"ephemeral").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("hello.txt", root.join("link")).unwrap();
    }

    #[test]
    fn snapshot_restore_roundtrip() {
        let src = tempfile::tempdir().unwrap();
        build_tree(src.path());
        let mut store = MemStore::new();
        let result = create_snapshot(&mut store, &testkey(), src.path(), &opts()).unwrap();

        assert!(result.manifest.entries.iter().all(|e| e.path != ".cache/skipme"));

        let dst = tempfile::tempdir().unwrap();
        let target = dst.path().join("restored");
        restore_snapshot(&store, &testkey(), &result.manifest_hash, &target).unwrap();

        assert_eq!(fs::read(target.join("hello.txt")).unwrap(), b"hello burrow");
        assert_eq!(
            fs::read(target.join("sub/deep/data.bin")).unwrap(),
            vec![0xAB; 3 * 1024 * 1024]
        );
        assert!(!target.join(".cache").exists());
        #[cfg(unix)]
        assert_eq!(fs::read_link(target.join("link")).unwrap(), Path::new("hello.txt"));
    }

    #[test]
    fn incremental_snapshot_dedups_unchanged_content() {
        let src = tempfile::tempdir().unwrap();
        build_tree(src.path());
        let mut store = MemStore::new();
        let first = create_snapshot(&mut store, &testkey(), src.path(), &opts()).unwrap();
        assert!(!first.new_blobs.is_empty());

        // No changes: second snapshot must write zero new data blobs.
        let second = create_snapshot(&mut store, &testkey(), src.path(), &opts()).unwrap();
        assert!(second.new_blobs.is_empty(), "unchanged tree re-uploaded {:?}", second.new_blobs);
        assert_eq!(second.bytes_new, 0);

        // Touch one small file: only that file's chunk should be new.
        fs::write(src.path().join("hello.txt"), b"hello again").unwrap();
        let third = create_snapshot(&mut store, &testkey(), src.path(), &opts()).unwrap();
        assert_eq!(third.new_blobs.len(), 1);
    }

    #[test]
    fn restore_rejects_traversal() {
        let mut store = MemStore::new();
        let manifest = Manifest {
            format: MANIFEST_FORMAT,
            backup_id: "evil".into(),
            node_name: "unit".into(),
            created_at: 0,
            entries: vec![Entry {
                path: "../escape".into(),
                kind: EntryKind::Dir,
                mode: 0o755,
                mtime: 0,
            }],
        };
        let sealed = manifest.seal(&testkey());
        let hash = store.put(sealed.blob).unwrap();
        let dst = tempfile::tempdir().unwrap();
        let err = restore_snapshot(&store, &testkey(), &hash, &dst.path().join("out"));
        assert!(matches!(err, Err(CoreError::PathEscape(_))));
    }
}
