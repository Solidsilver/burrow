//! Build a snapshot of one or more directory trees into a blob store, and
//! restore back out. Pure filesystem + store logic; the daemon supplies
//! scheduling, placement, and networking around this.
//!
//! A snapshot may cover multiple roots (like `tar /a /b`): every entry's
//! manifest path is the root's absolute path with the leading slash removed,
//! so `/home/luke/photos/x.jpg` restores under `<target>/home/luke/photos/`.

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
    /// Glob patterns matched against each entry's root-relative path
    /// (e.g. `*.tmp`, `.cache/**`, `node_modules`).
    pub exclude: Vec<String>,
}

pub struct SnapshotResult {
    pub manifest: Manifest,
    /// Hash of the sealed manifest blob (already in the store).
    pub manifest_hash: BlobHash,
    /// Blobs newly written by this snapshot (excludes pre-existing dedup hits
    /// and the manifest itself).
    pub new_blobs: Vec<BlobHash>,
    /// Stored (sealed) size of the manifest blob.
    pub manifest_size: u64,
    pub bytes_scanned: u64,
    pub bytes_new: u64,
}

/// Walk each root, chunk+seal every file, store blobs, and store the sealed
/// manifest. Deduplication is inherent: sealing is deterministic, so unchanged
/// content maps to blobs the store already has.
pub fn create_snapshot<S: BlobStore>(
    store: &mut S,
    key: &RepoKey,
    roots: &[PathBuf],
    opts: &SnapshotOptions,
) -> Result<SnapshotResult> {
    let excludes = build_globset(&opts.exclude)?;
    // BTreeMap keyed by manifest path => deterministic manifest ordering.
    let mut entries: BTreeMap<String, Entry> = BTreeMap::new();
    let mut new_blobs = Vec::new();
    let mut bytes_scanned = 0u64;
    let mut bytes_new = 0u64;
    let mut manifest_roots = Vec::new();

    for root in roots {
        let root_abs = std::path::absolute(root)?;
        let prefix = path_to_manifest_string(&root_abs);
        manifest_roots.push(prefix.clone());

        let walker = walkdir::WalkDir::new(&root_abs)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter();
        let iter = walker.filter_entry(|e| {
            e.depth() == 0
                || e.path()
                    .strip_prefix(&root_abs)
                    .map(|rel| !excludes.is_match(rel))
                    .unwrap_or(true)
        });
        for item in iter {
            let item = item.map_err(|e| CoreError::Io(std::io::Error::other(e.to_string())))?;
            let rel_in_root = item
                .path()
                .strip_prefix(&root_abs)
                .map_err(|_| CoreError::PathEscape(item.path().to_path_buf()))?
                .to_path_buf();
            let manifest_path = if rel_in_root.as_os_str().is_empty() {
                prefix.clone()
            } else {
                format!("{prefix}/{}", path_to_manifest_string(&rel_in_root))
            };
            let meta =
                item.metadata().map_err(|e| CoreError::Io(std::io::Error::other(e.to_string())))?;
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

            entries.insert(
                manifest_path.clone(),
                Entry { path: manifest_path, kind, mode, mtime },
            );
        }
    }

    let manifest = Manifest {
        format: MANIFEST_FORMAT,
        backup_id: opts.backup_id.clone(),
        node_name: opts.node_name.clone(),
        created_at: opts.created_at,
        roots: manifest_roots,
        entries: entries.into_values().collect(),
    };
    let sealed = manifest.seal(key);
    let manifest_size = sealed.blob.len() as u64;
    let manifest_hash = store.put(sealed.blob)?;

    Ok(SnapshotResult { manifest, manifest_hash, new_blobs, manifest_size, bytes_scanned, bytes_new })
}

/// Restore a snapshot into `target` (created if missing). Existing files in
/// the way are overwritten; the caller decides whether that's acceptable.
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
                {
                    if dest.symlink_metadata().is_ok() {
                        fs::remove_file(&dest)?;
                    }
                    std::os::unix::fs::symlink(link_target, &dest)?;
                }
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

fn build_globset(patterns: &[String]) -> Result<globset::GlobSet> {
    let mut builder = globset::GlobSetBuilder::new();
    for p in patterns {
        let glob = globset::Glob::new(p)
            .map_err(|e| CoreError::Pattern(format!("bad exclude pattern {p:?}: {e}")))?;
        builder.add(glob);
    }
    builder.build().map_err(|e| CoreError::Pattern(e.to_string()))
}

/// `/`-joined path string with no leading separator (manifest path form).
fn path_to_manifest_string(path: &Path) -> String {
    path.components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
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
            exclude: vec![".cache/**".into(), ".cache".into(), "*.tmp".into()],
        }
    }

    fn build_tree(root: &Path) {
        fs::create_dir_all(root.join("sub/deep")).unwrap();
        fs::create_dir_all(root.join(".cache")).unwrap();
        fs::write(root.join("hello.txt"), b"hello burrow").unwrap();
        fs::write(root.join("scratch.tmp"), b"tempfile").unwrap();
        fs::write(root.join("sub/deep/data.bin"), vec![0xAB; 3 * 1024 * 1024]).unwrap();
        fs::write(root.join(".cache/skipme"), b"ephemeral").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("hello.txt", root.join("link")).unwrap();
    }

    fn prefixed(root: &Path, rel: &str) -> String {
        let abs = std::path::absolute(root).unwrap();
        format!("{}/{}", path_to_manifest_string(&abs), rel)
    }

    #[test]
    fn snapshot_restore_roundtrip() {
        let src = tempfile::tempdir().unwrap();
        build_tree(src.path());
        let mut store = MemStore::new();
        let result =
            create_snapshot(&mut store, &testkey(), &[src.path().to_path_buf()], &opts()).unwrap();

        let paths: Vec<&str> = result.manifest.entries.iter().map(|e| e.path.as_str()).collect();
        assert!(!paths.iter().any(|p| p.contains(".cache") || p.ends_with(".tmp")), "{paths:?}");

        let dst = tempfile::tempdir().unwrap();
        let target = dst.path().join("restored");
        restore_snapshot(&store, &testkey(), &result.manifest_hash, &target).unwrap();

        let restored_root = target.join(prefixed(src.path(), "hello.txt"));
        assert_eq!(fs::read(&restored_root).unwrap(), b"hello burrow");
        assert_eq!(
            fs::read(target.join(prefixed(src.path(), "sub/deep/data.bin"))).unwrap(),
            vec![0xAB; 3 * 1024 * 1024]
        );
        #[cfg(unix)]
        assert_eq!(
            fs::read_link(target.join(prefixed(src.path(), "link"))).unwrap(),
            Path::new("hello.txt")
        );
    }

    #[test]
    fn multi_root_snapshot() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        fs::write(a.path().join("a.txt"), b"aaa").unwrap();
        fs::write(b.path().join("b.txt"), b"bbb").unwrap();
        let mut store = MemStore::new();
        let result = create_snapshot(
            &mut store,
            &testkey(),
            &[a.path().to_path_buf(), b.path().to_path_buf()],
            &opts(),
        )
        .unwrap();
        assert_eq!(result.manifest.roots.len(), 2);

        let dst = tempfile::tempdir().unwrap();
        restore_snapshot(&store, &testkey(), &result.manifest_hash, dst.path()).unwrap();
        assert_eq!(fs::read(dst.path().join(prefixed(a.path(), "a.txt"))).unwrap(), b"aaa");
        assert_eq!(fs::read(dst.path().join(prefixed(b.path(), "b.txt"))).unwrap(), b"bbb");
    }

    #[test]
    fn incremental_snapshot_dedups_unchanged_content() {
        let src = tempfile::tempdir().unwrap();
        build_tree(src.path());
        let roots = [src.path().to_path_buf()];
        let mut store = MemStore::new();
        let first = create_snapshot(&mut store, &testkey(), &roots, &opts()).unwrap();
        assert!(!first.new_blobs.is_empty());

        // No changes: second snapshot must write zero new data blobs.
        let second = create_snapshot(&mut store, &testkey(), &roots, &opts()).unwrap();
        assert!(second.new_blobs.is_empty(), "unchanged tree re-uploaded {:?}", second.new_blobs);
        assert_eq!(second.bytes_new, 0);

        // Touch one small file: only that file's chunk should be new.
        fs::write(src.path().join("hello.txt"), b"hello again").unwrap();
        let third = create_snapshot(&mut store, &testkey(), &roots, &opts()).unwrap();
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
            roots: vec![],
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
