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
    /// Exclude globs. Patterns without `/` match any single path component at
    /// any depth (`node_modules` prunes every node_modules dir; `*.tmp` any
    /// tmp file). Patterns with `/` are anchored to the backup root and `*`
    /// stops at separators (`.cache/**`, `build/*.o`).
    pub exclude: Vec<String>,
    /// Files whose mtime (unix seconds) is at or after this cutoff are never
    /// entered into the skip-cache: an mtime that fresh can't prove the file
    /// won't change again within the filesystem's timestamp granularity.
    /// Callers pass "now"; tests pass i64::MAX to cache everything.
    pub cache_cutoff: i64,
}

/// Skip-unchanged-files cache: manifest path -> (size, mtime, chunk refs).
/// A hit means the file wasn't even read; its chunk refs are reused verbatim.
pub type FileCache = std::collections::HashMap<String, FileCacheEntry>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileCacheEntry {
    pub size: u64,
    /// Modification time in unix NANOseconds (whole seconds are too coarse:
    /// a same-second rewrite with equal size would be silently skipped).
    pub mtime: i64,
    pub chunks: Vec<ChunkRef>,
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
    /// Bytes actually read and chunked (cache hits don't count).
    pub bytes_scanned: u64,
    pub bytes_new: u64,
    /// Files skipped via the mtime cache.
    pub files_cached: u64,
    /// Fresh cache reflecting exactly this snapshot's files; persist it and
    /// pass it to the next run.
    pub cache: FileCache,
}

/// Walk each root, chunk+seal every file, store blobs, and store the sealed
/// manifest. Deduplication is inherent: sealing is deterministic, so unchanged
/// content maps to blobs the store already has.
pub fn create_snapshot<S: BlobStore>(
    store: &mut S,
    key: &RepoKey,
    roots: &[PathBuf],
    opts: &SnapshotOptions,
    old_cache: &FileCache,
) -> Result<SnapshotResult> {
    let excludes = build_globset(&opts.exclude)?;
    // BTreeMap keyed by manifest path => deterministic manifest ordering.
    let mut entries: BTreeMap<String, Entry> = BTreeMap::new();
    let mut new_blobs = Vec::new();
    let mut bytes_scanned = 0u64;
    let mut bytes_new = 0u64;
    let mut files_cached = 0u64;
    let mut new_cache = FileCache::new();
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
            let (mode, mtime, mtime_nanos) = mode_and_mtime(&meta);

            let kind = if item.file_type().is_dir() {
                EntryKind::Dir
            } else if item.file_type().is_symlink() {
                let target = fs::read_link(item.path())?;
                EntryKind::Symlink { target: target.to_string_lossy().into_owned() }
            } else if item.file_type().is_file() {
                // Cache hit: same size+mtime and every chunk still stored —
                // reuse the refs without reading the file at all.
                let cached = old_cache.get(&manifest_path).filter(|c| {
                    c.size == meta.len()
                        && c.mtime == mtime_nanos
                        && c.chunks.iter().all(|r| store.contains(&r.blob_hash))
                });
                let (chunks, file_size) = if let Some(hit) = cached {
                    files_cached += 1;
                    (hit.chunks.clone(), hit.size)
                } else {
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
                    // The manifest size must describe the bytes we actually
                    // chunked, not the earlier stat: a file appended to
                    // between the two would otherwise fail its own restore.
                    let read_size = chunks.iter().map(|c| c.size as u64).sum();
                    (chunks, read_size)
                };
                if mtime < opts.cache_cutoff {
                    new_cache.insert(
                        manifest_path.clone(),
                        FileCacheEntry { size: file_size, mtime: mtime_nanos, chunks: chunks.clone() },
                    );
                }
                EntryKind::File { size: file_size, chunks }
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

    Ok(SnapshotResult {
        manifest,
        manifest_hash,
        new_blobs,
        manifest_size,
        bytes_scanned,
        bytes_new,
        files_cached,
        cache: new_cache,
    })
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
        // Directory metadata waits for the second pass below: applying a
        // read-only mode now would make writing the children fail, and the
        // children's writes would clobber the directory mtime anyway.
        if !matches!(entry.kind, EntryKind::Dir) {
            apply_metadata(&dest, entry);
        }
    }
    // Entries are path-sorted, so reverse order visits children before their
    // parents (deepest first).
    for entry in manifest.entries.iter().rev() {
        if matches!(entry.kind, EntryKind::Dir) {
            if let Ok(dest) = safe_join(target, &entry.path) {
                apply_metadata(&dest, entry);
            }
        }
    }
    Ok(manifest)
}

/// See `SnapshotOptions::exclude` for the matching rules.
struct Excludes {
    /// Patterns containing `/`: anchored to the backup root, `*` does not
    /// cross separators (use `**` to descend).
    anchored: globset::GlobSet,
    /// Patterns without `/`: matched against every individual path component.
    component: globset::GlobSet,
}

impl Excludes {
    fn is_match(&self, rel: &Path) -> bool {
        self.anchored.is_match(rel)
            || rel.components().any(|c| match c {
                Component::Normal(s) => self.component.is_match(Path::new(s)),
                _ => false,
            })
    }
}

fn build_globset(patterns: &[String]) -> Result<Excludes> {
    let mut anchored = globset::GlobSetBuilder::new();
    let mut component = globset::GlobSetBuilder::new();
    for p in patterns {
        let has_sep = p.contains('/');
        let glob = globset::GlobBuilder::new(p)
            .literal_separator(has_sep)
            .build()
            .map_err(|e| CoreError::Pattern(format!("bad exclude pattern {p:?}: {e}")))?;
        if has_sep {
            anchored.add(glob);
        } else {
            component.add(glob);
        }
    }
    Ok(Excludes {
        anchored: anchored.build().map_err(|e| CoreError::Pattern(e.to_string()))?,
        component: component.build().map_err(|e| CoreError::Pattern(e.to_string()))?,
    })
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

/// (mode bits, mtime in unix seconds, mtime in unix nanoseconds).
/// Seconds go in the manifest; nanoseconds feed the skip-cache, where
/// second granularity would miss same-second rewrites.
fn mode_and_mtime(meta: &fs::Metadata) -> (u32, i64, i64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let nanos = meta.mtime().saturating_mul(1_000_000_000).saturating_add(meta.mtime_nsec());
        (meta.mode(), meta.mtime(), nanos)
    }
    #[cfg(not(unix))]
    {
        let nanos = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        (0o644, nanos / 1_000_000_000, nanos)
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
            cache_cutoff: i64::MAX,
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
            create_snapshot(&mut store, &testkey(), &[src.path().to_path_buf()], &opts(), &FileCache::new()).unwrap();

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
            &FileCache::new(),
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
        let first = create_snapshot(&mut store, &testkey(), &roots, &opts(), &FileCache::new()).unwrap();
        assert!(!first.new_blobs.is_empty());

        // No changes: second snapshot must write zero new data blobs.
        let second = create_snapshot(&mut store, &testkey(), &roots, &opts(), &first.cache).unwrap();
        assert!(second.new_blobs.is_empty(), "unchanged tree re-uploaded {:?}", second.new_blobs);
        assert_eq!(second.bytes_new, 0);
        assert_eq!(second.bytes_scanned, 0, "cache hits must not re-read files");
        assert_eq!(second.files_cached, 2, "both regular files should be cache hits");
        assert_eq!(second.manifest_hash, first.manifest_hash);

        // Touch one small file: only that file's chunk should be new.
        fs::write(src.path().join("hello.txt"), b"hello again").unwrap();
        let third = create_snapshot(&mut store, &testkey(), &roots, &opts(), &second.cache).unwrap();
        assert_eq!(third.new_blobs.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn restores_read_only_directories() {
        use std::os::unix::fs::PermissionsExt;
        let src = tempfile::tempdir().unwrap();
        let ro_dir = src.path().join("frozen");
        fs::create_dir(&ro_dir).unwrap();
        fs::write(ro_dir.join("keep.txt"), b"contents").unwrap();
        fs::set_permissions(&ro_dir, fs::Permissions::from_mode(0o555)).unwrap();

        let mut store = MemStore::new();
        let result = create_snapshot(
            &mut store,
            &testkey(),
            &[src.path().to_path_buf()],
            &opts(),
            &FileCache::new(),
        )
        .unwrap();

        let dst = tempfile::tempdir().unwrap();
        let target = dst.path().join("out");
        restore_snapshot(&store, &testkey(), &result.manifest_hash, &target).unwrap();

        let restored_dir = target.join(prefixed(src.path(), "frozen"));
        assert_eq!(
            fs::read(restored_dir.join("keep.txt")).unwrap(),
            b"contents",
            "file inside read-only dir must be restored"
        );
        assert_eq!(
            fs::metadata(&restored_dir).unwrap().permissions().mode() & 0o777,
            0o555,
            "directory mode must still be restored"
        );

        // Cleanup so tempdir can delete.
        fs::set_permissions(&ro_dir, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&restored_dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn fresh_files_are_not_cached() {
        // Files modified at/after cache_cutoff must be re-read next run: their
        // mtime can't prove stability within fs timestamp granularity.
        let src = tempfile::tempdir().unwrap();
        fs::write(src.path().join("hot.txt"), b"just written").unwrap();
        let roots = [src.path().to_path_buf()];
        let mut store = MemStore::new();
        let hot_opts = SnapshotOptions { cache_cutoff: 0, ..opts() };
        let first =
            create_snapshot(&mut store, &testkey(), &roots, &hot_opts, &FileCache::new()).unwrap();
        assert!(first.cache.is_empty(), "hot file must not enter the cache");

        let second = create_snapshot(&mut store, &testkey(), &roots, &hot_opts, &first.cache).unwrap();
        assert_eq!(second.files_cached, 0);
        assert!(second.bytes_scanned > 0, "hot file must be re-read");
        // Content unchanged, so dedup still means no new blobs.
        assert!(second.new_blobs.is_empty());
    }

    #[test]
    fn excludes_match_components_at_any_depth() {
        let src = tempfile::tempdir().unwrap();
        fs::create_dir_all(src.path().join("app/node_modules/dep")).unwrap();
        fs::create_dir_all(src.path().join("app/src")).unwrap();
        fs::write(src.path().join("app/node_modules/dep/big.js"), b"dep").unwrap();
        fs::write(src.path().join("app/src/main.rs"), b"code").unwrap();
        fs::write(src.path().join("app/src/junk.tmp"), b"junk").unwrap();

        let mut store = MemStore::new();
        let snapshot_opts = SnapshotOptions {
            exclude: vec!["node_modules".into(), "*.tmp".into()],
            ..opts()
        };
        let result = create_snapshot(
            &mut store,
            &testkey(),
            &[src.path().to_path_buf()],
            &snapshot_opts,
            &FileCache::new(),
        )
        .unwrap();
        let paths: Vec<&str> = result.manifest.entries.iter().map(|e| e.path.as_str()).collect();
        assert!(
            !paths.iter().any(|p| p.contains("node_modules") || p.ends_with(".tmp")),
            "nested excludes must apply: {paths:?}"
        );
        assert!(paths.iter().any(|p| p.ends_with("main.rs")));
    }

    #[test]
    fn anchored_excludes_stay_anchored() {
        let src = tempfile::tempdir().unwrap();
        fs::create_dir_all(src.path().join("cache")).unwrap();
        fs::create_dir_all(src.path().join("keep/cache")).unwrap();
        fs::write(src.path().join("cache/drop.txt"), b"x").unwrap();
        fs::write(src.path().join("keep/cache/keep.txt"), b"y").unwrap();

        let mut store = MemStore::new();
        let snapshot_opts = SnapshotOptions { exclude: vec!["cache/**".into()], ..opts() };
        let result = create_snapshot(
            &mut store,
            &testkey(),
            &[src.path().to_path_buf()],
            &snapshot_opts,
            &FileCache::new(),
        )
        .unwrap();
        let paths: Vec<&str> = result.manifest.entries.iter().map(|e| e.path.as_str()).collect();
        assert!(!paths.iter().any(|p| p.ends_with("drop.txt")), "{paths:?}");
        assert!(
            paths.iter().any(|p| p.ends_with("keep/cache/keep.txt")),
            "anchored pattern must not match nested dirs: {paths:?}"
        );
    }

    #[test]
    fn same_second_rewrite_is_detected() {
        // Rewrite with equal size and equal seconds-mtime but different
        // nanoseconds must invalidate the cache entry.
        let src = tempfile::tempdir().unwrap();
        let file = src.path().join("data.txt");
        fs::write(&file, b"version one!").unwrap();
        filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1_600_000_000, 111)).unwrap();
        let roots = [src.path().to_path_buf()];
        let mut store = MemStore::new();
        let first =
            create_snapshot(&mut store, &testkey(), &roots, &opts(), &FileCache::new()).unwrap();

        fs::write(&file, b"version two!").unwrap(); // same length
        filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1_600_000_000, 222)).unwrap();
        let second = create_snapshot(&mut store, &testkey(), &roots, &opts(), &first.cache).unwrap();
        assert_eq!(second.files_cached, 0, "same-second rewrite must not hit the cache");
        assert_eq!(second.new_blobs.len(), 1, "changed content must be stored");
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
