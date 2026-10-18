//! Disk cache with an SQLite index.
//!
//! Two kinds of records are kept:
//! * `files`   – finished files, keyed by the SHA-256 of the cache key (URL).
//! * `chunks`  – byte ranges of an *in progress* download, used to resume after
//!   a crash or a network error.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use crate::util::{format_unix, now_unix};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS files (
    digest       TEXT PRIMARY KEY,
    path         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    created      INTEGER NOT NULL,
    accessed     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_accessed ON files(accessed);

CREATE TABLE IF NOT EXISTS chunks (
    url     TEXT NOT NULL,
    total   INTEGER NOT NULL,
    start   INTEGER NOT NULL,
    "end"   INTEGER NOT NULL,
    updated INTEGER NOT NULL,
    PRIMARY KEY (url, start, "end")
);
CREATE INDEX IF NOT EXISTS idx_chunks_updated ON chunks(updated);
"#;

#[derive(Debug, Clone)]
pub struct CachedFile {
    pub path: PathBuf,
    pub size: u64,
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    pub count: u64,
    pub size_bytes: u64,
    pub size_gb: f64,
    pub first_cached: Option<String>,
    pub last_accessed: Option<String>,
}

pub struct CacheManager {
    dir: PathBuf,
    db: Mutex<Connection>,
    max_bytes: u64,
    lru_enabled: bool,
    enabled: bool,
}

impl CacheManager {
    pub fn open(dir: PathBuf, max_size_gb: f64, lru_enabled: bool, enabled: bool) -> Result<Self> {
        fs::create_dir_all(&dir)
            .with_context(|| format!("cannot create cache directory {}", dir.display()))?;

        let db_path = dir.join("meta.db");
        let conn = if enabled {
            Connection::open(&db_path)
                .with_context(|| format!("cannot open cache index {}", db_path.display()))?
        } else {
            // --no-cache: keep the index in memory so nothing is persisted.
            Connection::open_in_memory().context("cannot open the in-memory cache index")?
        };
        // WAL keeps readers from blocking the downloader.
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .unwrap_or_else(|_| "delete".to_string());
        debug!(journal_mode = %mode, enabled, "cache index opened");
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;

        Ok(Self {
            dir,
            db: Mutex::new(conn),
            max_bytes: (max_size_gb.max(0.0) * 1024.0 * 1024.0 * 1024.0) as u64,
            lru_enabled,
            enabled,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Whether entries may be read from and written to the cache.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn digest(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Deterministic scratch file used while a file is being downloaded.
    pub fn tmp_path(&self, key: &str) -> PathBuf {
        let digest = Self::digest(key);
        self.dir.join(format!("tmp_{}", &digest[..16]))
    }

    pub fn get(&self, key: &str) -> Option<CachedFile> {
        if !self.enabled {
            return None;
        }
        let digest = Self::digest(key);
        let row = {
            let conn = self.conn();
            conn.query_row(
                "SELECT path, size, content_type FROM files WHERE digest = ?1",
                params![digest],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .ok()
            .flatten()
        };

        let (path, size, content_type) = row?;
        let path = PathBuf::from(path);

        match fs::metadata(&path) {
            Ok(meta) if meta.len() == size.max(0) as u64 => {}
            _ => {
                debug!(%digest, "cache entry points at a missing/short file; dropping it");
                self.forget(&digest);
                return None;
            }
        }

        {
            let conn = self.conn();
            let _ = conn.execute(
                "UPDATE files SET accessed = ?1 WHERE digest = ?2",
                params![now_unix(), digest],
            );
        }

        Some(CachedFile {
            path,
            size: size.max(0) as u64,
            content_type,
        })
    }

    fn forget(&self, digest: &str) {
        let conn = self.conn();
        let _ = conn.execute("DELETE FROM files WHERE digest = ?1", params![digest]);
    }

    /// Move a finished download into the cache (hard link when possible, copy
    /// otherwise) and evict old entries if needed.
    pub fn put(&self, key: &str, source: &Path, content_type: &str) -> Result<CachedFile> {
        let size = fs::metadata(source)
            .with_context(|| format!("cannot stat {}", source.display()))?
            .len();

        if !self.enabled {
            // Nothing is stored; the caller keeps serving from the scratch file.
            return Ok(CachedFile {
                path: source.to_path_buf(),
                size,
                content_type: content_type.to_string(),
            });
        }

        let digest = Self::digest(key);
        let target = self.dir.join(&digest);

        let already_there = fs::metadata(&target)
            .map(|m| m.len() == size)
            .unwrap_or(false);
        if !already_there {
            let _ = fs::remove_file(&target);
            match fs::hard_link(source, &target) {
                Ok(()) => {}
                Err(link_err) => {
                    debug!(error = %link_err, "hard link failed, copying instead");
                    fs::copy(source, &target).with_context(|| {
                        format!("cannot store {} as {}", source.display(), target.display())
                    })?;
                }
            }
        }

        let now = now_unix();
        {
            let conn = self.conn();
            conn.execute(
                "INSERT INTO files (digest, path, size, content_type, created, accessed)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                 ON CONFLICT(digest) DO UPDATE SET
                     path = excluded.path,
                     size = excluded.size,
                     content_type = excluded.content_type,
                     accessed = excluded.accessed",
                params![
                    digest,
                    target.to_string_lossy().to_string(),
                    size as i64,
                    content_type,
                    now
                ],
            )?;
        }

        self.evict_if_needed();

        Ok(CachedFile {
            path: target,
            size,
            content_type: content_type.to_string(),
        })
    }

    /// Drop the least recently used files until the cache fits in `max_bytes`.
    pub fn evict_if_needed(&self) {
        if !self.enabled || !self.lru_enabled {
            return;
        }
        loop {
            let total: i64 = {
                let conn = self.conn();
                conn.query_row("SELECT COALESCE(SUM(size), 0) FROM files", [], |row| {
                    row.get(0)
                })
                .unwrap_or(0)
            };
            if total.max(0) as u64 <= self.max_bytes {
                return;
            }

            let victim = {
                let conn = self.conn();
                conn.query_row(
                    "SELECT digest, path, size FROM files ORDER BY accessed ASC, created ASC LIMIT 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()
                .unwrap_or(None)
            };

            let Some((digest, path, size)) = victim else {
                return;
            };

            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    // Most likely the file is still open on Windows; try again
                    // on the next maintenance pass instead of spinning.
                    warn!(path = %path, error = %err, "cannot evict cache file");
                    return;
                }
            }

            let conn = self.conn();
            let _ = conn.execute("DELETE FROM files WHERE digest = ?1", params![digest]);
            debug!(digest = %digest, freed = size, "evicted cache entry");
        }
    }

    pub fn stats(&self) -> CacheStats {
        let conn = self.conn();
        let row = conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(size), 0), MIN(created), MAX(accessed) FROM files",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .unwrap_or((0, 0, None, None));

        let (count, size, first, last) = row;
        CacheStats {
            count: count.max(0) as u64,
            size_bytes: size.max(0) as u64,
            size_gb: (size.max(0) as f64) / (1024.0 * 1024.0 * 1024.0),
            first_cached: first.map(format_unix),
            last_accessed: last.map(format_unix),
        }
    }

    // ---------------------------------------------------------------- chunks

    pub fn downloaded_chunks(&self, url: &str, total: u64, ttl_hours: u64) -> Vec<(u64, u64)> {
        if !self.enabled {
            return Vec::new();
        }
        let cutoff = now_unix() - (ttl_hours as i64).saturating_mul(3600);
        let conn = self.conn();
        let mut stmt = match conn.prepare(
            r#"SELECT start, "end" FROM chunks
               WHERE url = ?1 AND total = ?2 AND updated >= ?3
               ORDER BY start"#,
        ) {
            Ok(stmt) => stmt,
            Err(_) => return Vec::new(),
        };

        let rows = stmt.query_map(params![url, total as i64, cutoff], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        });

        match rows {
            Ok(iter) => iter
                .filter_map(|r| r.ok())
                .map(|(start, end)| (start.max(0) as u64, end.max(0) as u64))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn mark_chunk(&self, url: &str, total: u64, start: u64, end: u64) {
        if !self.enabled {
            return;
        }
        let conn = self.conn();
        let _ = conn.execute(
            r#"INSERT INTO chunks (url, total, start, "end", updated)
               VALUES (?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(url, start, "end") DO UPDATE SET
                   total = excluded.total,
                   updated = excluded.updated"#,
            params![url, total as i64, start as i64, end as i64, now_unix()],
        );
    }

    pub fn clear_chunks(&self, url: &str) {
        let conn = self.conn();
        let _ = conn.execute("DELETE FROM chunks WHERE url = ?1", params![url]);
    }

    pub fn cleanup_chunks(&self, ttl_hours: u64) -> usize {
        let cutoff = now_unix() - (ttl_hours as i64).saturating_mul(3600);
        let conn = self.conn();
        conn.execute("DELETE FROM chunks WHERE updated < ?1", params![cutoff])
            .unwrap_or(0)
    }

    /// Periodic housekeeping: expire resume records and delete scratch files
    /// that no download is using (leftovers from a crash).
    pub fn maintenance(&self, chunk_ttl_hours: u64, tmp_ttl: Duration, active: &HashSet<PathBuf>) {
        let removed = self.cleanup_chunks(chunk_ttl_hours);
        if removed > 0 {
            debug!(removed, "expired resume records");
        }

        let now = std::time::SystemTime::now();
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with("tmp_") {
                continue;
            }
            if active.contains(&path) {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|meta| meta.modified())
                .map(|modified| {
                    now.duration_since(modified)
                        .map(|age| age > tmp_ttl)
                        .unwrap_or(false)
                })
                .unwrap_or(false);
            if stale {
                if let Err(err) = fs::remove_file(&path) {
                    debug!(path = %path.display(), error = %err, "cannot remove stale scratch file");
                } else {
                    debug!(path = %path.display(), "removed stale scratch file");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(dir: &Path) -> CacheManager {
        CacheManager::open(dir.to_path_buf(), 1.0, true, true).unwrap()
    }

    #[test]
    fn put_and_get_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = manager(tmp.path());
        let src = tmp.path().join("download.bin");
        fs::write(&src, b"hello world").unwrap();

        let stored = cache
            .put("https://example.com/a", &src, "text/plain")
            .unwrap();
        assert_eq!(stored.size, 11);
        assert!(stored.path.exists());

        let hit = cache.get("https://example.com/a").unwrap();
        assert_eq!(hit.size, 11);
        assert_eq!(fs::read(&hit.path).unwrap(), b"hello world");
        assert_eq!(hit.content_type, "text/plain");

        assert!(cache.get("https://example.com/other").is_none());
    }

    #[test]
    fn missing_file_invalidates_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = manager(tmp.path());
        let src = tmp.path().join("f.bin");
        fs::write(&src, b"x").unwrap();
        let stored = cache.put("k", &src, "").unwrap();
        fs::remove_file(&stored.path).unwrap();
        assert!(cache.get("k").is_none());
        assert_eq!(cache.stats().count, 0);
    }

    #[test]
    fn lru_evicts_oldest() {
        let tmp = tempfile::tempdir().unwrap();
        // 1 byte of capacity would evict everything, so use a small real budget.
        let cache = CacheManager::open(tmp.path().to_path_buf(), 1.0, true, true).unwrap();
        let a = tmp.path().join("a.bin");
        let b = tmp.path().join("b.bin");
        fs::write(&a, vec![0u8; 600 * 1024 * 1024]).unwrap();
        fs::write(&b, vec![0u8; 600 * 1024 * 1024]).unwrap();

        cache.put("a", &a, "").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        cache.put("b", &b, "").unwrap();

        assert!(cache.get("b").is_some());
        assert!(cache.get("a").is_none(), "oldest entry should be evicted");
    }

    #[test]
    fn chunk_records_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = manager(tmp.path());
        cache.mark_chunk("u", 100, 0, 49);
        cache.mark_chunk("u", 100, 50, 99);
        cache.mark_chunk("u", 100, 0, 49); // duplicate upsert
        let chunks = cache.downloaded_chunks("u", 100, 48);
        assert_eq!(chunks, vec![(0, 49), (50, 99)]);
        assert!(cache.downloaded_chunks("u", 101, 48).is_empty());
        cache.clear_chunks("u");
        assert!(cache.downloaded_chunks("u", 100, 48).is_empty());
    }

    #[test]
    fn tmp_path_is_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = manager(tmp.path());
        assert_eq!(cache.tmp_path("key"), cache.tmp_path("key"));
        assert_ne!(cache.tmp_path("key"), cache.tmp_path("other"));
    }

    #[test]
    fn disabled_cache_stores_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = CacheManager::open(tmp.path().to_path_buf(), 1.0, true, false).unwrap();
        assert!(!cache.enabled());

        let src = tmp.path().join("x.bin");
        fs::write(&src, b"data").unwrap();
        let stored = cache.put("k", &src, "text/plain").unwrap();
        assert_eq!(stored.path, src, "no copy should be made");

        assert!(cache.get("k").is_none());
        cache.mark_chunk("u", 10, 0, 9);
        assert!(cache.downloaded_chunks("u", 10, 48).is_empty());
        assert_eq!(cache.stats().count, 0);
        assert!(!tmp.path().join(CacheManager::digest("k")).exists());
    }
}
