//! Parallel ranged downloader.
//!
//! A download runs as a detached task that writes byte ranges straight into a
//! pre-allocated scratch file. While it runs, any number of clients can stream
//! the file: readers are only ever handed bytes that are already on disk and
//! contiguous from their current offset.

use std::collections::VecDeque;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use axum::http::header::{ACCEPT_ENCODING, RANGE};
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::stream::{self, FuturesUnordered};
use futures_util::StreamExt;
use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use url::Url;

use crate::cache::CacheManager;
use crate::util::{format_bytes, RangeSet};

/// Largest read handed to a streaming client in one chunk.
const MAX_READ: u64 = 256 * 1024;
/// Give up when a download makes no progress for this long.
const STALL_TIMEOUT: Duration = Duration::from_secs(600);
/// Never split a file into more range requests than this.
const MAX_CHUNKS: u64 = 100_000;
/// Attempts per range request.
const CHUNK_ATTEMPTS: u32 = 3;

#[derive(Debug)]
struct JobState {
    ranges: RangeSet,
    finished: bool,
    error: Option<String>,
    version: u64,
}

/// Shared state of one in-flight download.
pub struct DownloadJob {
    pub key: String,
    pub path: PathBuf,
    pub total: u64,
    pub content_type: String,
    state: Mutex<JobState>,
    tx: watch::Sender<u64>,
}

impl DownloadJob {
    pub fn new(key: String, path: PathBuf, total: u64, content_type: String) -> Arc<Self> {
        let (tx, _rx) = watch::channel(0u64);
        Arc::new(Self {
            key,
            path,
            total,
            content_type,
            state: Mutex::new(JobState {
                ranges: RangeSet::new(),
                finished: false,
                error: None,
                version: 0,
            }),
            tx,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, JobState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Exclusive end of the contiguous run of downloaded bytes starting at `pos`.
    pub fn available_from(&self, pos: u64) -> u64 {
        self.lock().ranges.end_from(pos)
    }

    pub fn downloaded_bytes(&self) -> u64 {
        self.lock().ranges.len()
    }

    pub fn is_finished(&self) -> bool {
        self.lock().finished
    }

    pub fn error(&self) -> Option<String> {
        self.lock().error.clone()
    }

    pub fn progress(&self) -> f64 {
        if self.total == 0 {
            return 1.0;
        }
        (self.downloaded_bytes() as f64 / self.total as f64).clamp(0.0, 1.0)
    }

    pub fn mark_range(&self, start: u64, end: u64) {
        let version = {
            let mut state = self.lock();
            state.ranges.insert(start, end);
            state.version += 1;
            state.version
        };
        self.tx.send_replace(version);
    }

    pub fn finish_ok(&self) {
        let version = {
            let mut state = self.lock();
            state.finished = true;
            state.version += 1;
            state.version
        };
        self.tx.send_replace(version);
    }

    pub fn finish_err(&self, message: impl Into<String>) {
        let version = {
            let mut state = self.lock();
            state.finished = true;
            state.error = Some(message.into());
            state.version += 1;
            state.version
        };
        self.tx.send_replace(version);
    }
}

/// Everything a download task needs. Cheap to clone (all handles are shared).
#[derive(Clone)]
pub struct DownloadPlan {
    pub key: String,
    pub url: Url,
    pub path: PathBuf,
    pub total: u64,
    pub concurrency: usize,
    pub chunk_size: i64,
    pub headers: HeaderMap,
    pub client: Client,
    pub cache: Arc<CacheManager>,
    pub chunk_ttl_hours: u64,
    pub content_type: String,
}

/// Split a file into inclusive byte ranges.
///
/// `chunk_size <= 0` reproduces the original "auto" mode: the file is divided
/// into `concurrency - 10` (at least one) equal parts, which keeps the number of
/// range requests small for multi-gigabyte Docker layers.
pub fn plan_chunks(total: u64, concurrency: usize, chunk_size: i64) -> Vec<(u64, u64)> {
    if total == 0 {
        return Vec::new();
    }
    let mut size = if chunk_size <= 0 {
        let parts = (concurrency.saturating_sub(10)).max(1) as u64;
        (total / parts).max(1)
    } else {
        chunk_size as u64
    };
    if total / size > MAX_CHUNKS {
        size = total / MAX_CHUNKS + 1;
    }

    let mut chunks = Vec::new();
    let mut start = 0u64;
    while start < total {
        let end = (start + size - 1).min(total - 1);
        chunks.push((start, end));
        start = end + 1;
    }
    chunks
}

async fn ensure_file(path: &Path, total: u64) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.len() == total => Ok(false),
        _ => {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .await
                .with_context(|| format!("cannot create {}", path.display()))?;
            file.set_len(total).await.with_context(|| {
                format!("cannot pre-allocate {} bytes in {}", total, path.display())
            })?;
            Ok(true)
        }
    }
}

/// Run the download to completion. Chunk progress is persisted as it goes, so a
/// crash or a network failure can be resumed later.
pub async fn run_download(plan: DownloadPlan, job: Arc<DownloadJob>) -> Result<()> {
    let total = plan.total;
    let url_key = plan.url.as_str().to_string();
    let started = std::time::Instant::now();

    if total == 0 {
        return Ok(());
    }

    let fresh = ensure_file(&plan.path, total).await?;
    if fresh {
        plan.cache.clear_chunks(&url_key);
    }

    let chunks = plan_chunks(total, plan.concurrency, plan.chunk_size);
    let already: RangeSet = if fresh {
        RangeSet::new()
    } else {
        plan.cache
            .downloaded_chunks(&url_key, total, plan.chunk_ttl_hours)
            .into_iter()
            .collect()
    };

    let mut pending: VecDeque<(u64, u64)> = VecDeque::new();
    let mut resumed = 0u64;
    for &(start, end) in &chunks {
        if already.covers(start, end) {
            job.mark_range(start, end);
            resumed += end - start + 1;
        } else {
            pending.push_back((start, end));
        }
    }

    info!(
        key = %plan.key,
        size = %format_bytes(total),
        chunks = chunks.len(),
        pending = pending.len(),
        resumed = %format_bytes(resumed),
        "download started"
    );

    let progress = Arc::new(AtomicU64::new(resumed));
    let concurrency = plan.concurrency.max(1);
    let mut inflight = FuturesUnordered::new();
    let mut failure: Option<anyhow::Error> = None;
    let mut completed = 0usize;
    let total_chunks = chunks.len();
    let log_every = (total_chunks / 10).max(1);

    loop {
        while inflight.len() < concurrency {
            let Some((start, end)) = pending.pop_front() else {
                break;
            };
            inflight.push(fetch_range_with_retry(
                plan.clone(),
                job.clone(),
                start,
                end,
                progress.clone(),
            ));
        }
        if inflight.is_empty() {
            break;
        }
        match inflight.next().await {
            Some(Ok(())) => {
                completed += 1;
                if completed % log_every == 0 || completed == total_chunks {
                    debug!(
                        key = %plan.key,
                        progress = format!("{:.1}%", completed as f64 / total_chunks as f64 * 100.0),
                        chunks = format!("{completed}/{total_chunks}"),
                        "download progress"
                    );
                }
            }
            Some(Err(err)) => {
                failure = Some(err);
                break;
            }
            None => break,
        }
    }
    drop(inflight);

    if let Some(err) = failure {
        return Err(err);
    }

    let actual = tokio::fs::metadata(&plan.path)
        .await
        .with_context(|| format!("cannot stat {}", plan.path.display()))?
        .len();
    if actual != total {
        bail!("size mismatch after download: expected {total}, got {actual}");
    }

    let cache = plan.cache.clone();
    let key = plan.key.clone();
    let path = plan.path.clone();
    let content_type = plan.content_type.clone();
    tokio::task::spawn_blocking(move || cache.put(&key, &path, &content_type))
        .await
        .context("cache writer panicked")??;

    let _ = tokio::fs::remove_file(&plan.path).await;
    plan.cache.clear_chunks(&url_key);

    let elapsed = started.elapsed().as_secs_f64();
    info!(
        key = %plan.key,
        size = %format_bytes(total),
        elapsed = format!("{elapsed:.1}s"),
        speed = format!("{:.1} MB/s", total as f64 / 1024.0 / 1024.0 / elapsed.max(0.001)),
        "download complete"
    );
    Ok(())
}

async fn fetch_range_with_retry(
    plan: DownloadPlan,
    job: Arc<DownloadJob>,
    start: u64,
    end: u64,
    progress: Arc<AtomicU64>,
) -> Result<()> {
    let url_key = plan.url.as_str().to_string();
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 0..CHUNK_ATTEMPTS {
        match fetch_range(&plan, &job, start, end, &progress).await {
            Ok(()) => {
                plan.cache.mark_chunk(&url_key, plan.total, start, end);
                return Ok(());
            }
            Err(err) => {
                warn!(
                    key = %plan.key,
                    range = format!("{start}-{end}"),
                    attempt = attempt + 1,
                    error = %err,
                    "range request failed"
                );
                last_error = Some(err);
                if attempt + 1 < CHUNK_ATTEMPTS {
                    sleep(Duration::from_secs(1 << attempt)).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("range request failed")))
}

async fn fetch_range(
    plan: &DownloadPlan,
    job: &Arc<DownloadJob>,
    start: u64,
    end: u64,
    progress: &AtomicU64,
) -> Result<()> {
    let expected = end - start + 1;

    let mut request = plan
        .client
        .get(plan.url.clone())
        .header(RANGE, format!("bytes={start}-{end}"))
        // Ranges must be raw bytes, never a compressed representation.
        .header(ACCEPT_ENCODING, "identity");
    for (name, value) in plan.headers.iter() {
        if name == RANGE || name == ACCEPT_ENCODING {
            continue;
        }
        request = request.header(name, value);
    }

    let response = request.send().await.context("range request failed")?;
    let status = response.status();

    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        write_body(plan, job, start, expected, response, progress).await
    } else if status == reqwest::StatusCode::OK && start == 0 && expected == plan.total {
        // Upstream ignored the Range header but the whole file was requested in
        // one piece, so the body is still usable.
        write_body(plan, job, 0, expected, response, progress).await
    } else {
        Err(anyhow!(
            "unexpected upstream status {status} for range {start}-{end}"
        ))
    }
}

async fn write_body(
    plan: &DownloadPlan,
    job: &Arc<DownloadJob>,
    start: u64,
    expected: u64,
    response: reqwest::Response,
    progress: &AtomicU64,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(&plan.path)
        .await
        .with_context(|| format!("cannot open {} for writing", plan.path.display()))?;
    file.seek(SeekFrom::Start(start)).await?;

    let mut written = 0u64;
    let mut body = response.bytes_stream();
    while let Some(item) = body.next().await {
        let chunk: Bytes = item.context("error while reading the upstream body")?;
        if chunk.is_empty() {
            continue;
        }
        if written + chunk.len() as u64 > expected {
            bail!("upstream sent more bytes than requested for range at {start}");
        }
        file.write_all(&chunk).await?;
        // `tokio::fs::File::write` hands the buffer to a blocking task and
        // reports success before the bytes reach the OS, so the range must not
        // be published until `flush` has confirmed the write. Otherwise a
        // streaming reader could see pre-allocated zeros.
        file.flush().await?;
        written += chunk.len() as u64;
        job.mark_range(start, start + written - 1);
        progress.fetch_add(chunk.len() as u64, Ordering::Relaxed);
    }
    file.flush().await?;

    if written != expected {
        bail!("short range: expected {expected} bytes, received {written}");
    }
    Ok(())
}

/// Stream a growing file to a client.
///
/// Bytes are yielded only when they are contiguous from the reader's offset, so
/// a client never sees a hole filled with zeros while later chunks are still in
/// flight.
pub fn stream_job(
    job: Arc<DownloadJob>,
    file: tokio::fs::File,
    rx: watch::Receiver<u64>,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    struct Reader {
        job: Arc<DownloadJob>,
        file: tokio::fs::File,
        rx: watch::Receiver<u64>,
        pos: u64,
    }

    let state = Reader {
        job,
        file,
        rx,
        pos: 0,
    };

    stream::unfold(state, |mut reader| async move {
        loop {
            let end = reader.job.available_from(reader.pos);
            if end > reader.pos {
                let want = (end - reader.pos).min(MAX_READ) as usize;
                let mut buf = vec![0u8; want];
                match reader.file.read(&mut buf).await {
                    Ok(0) => {
                        let err = std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "download file ended early",
                        );
                        return Some((Err(err), reader));
                    }
                    Ok(n) => {
                        buf.truncate(n);
                        reader.pos += n as u64;
                        return Some((Ok(Bytes::from(buf)), reader));
                    }
                    Err(err) => return Some((Err(err), reader)),
                }
            }

            if reader.job.is_finished() {
                return match reader.job.error() {
                    Some(message) if reader.pos < reader.job.total => {
                        Some((Err(std::io::Error::other(message)), reader))
                    }
                    _ => None,
                };
            }

            match tokio::time::timeout(STALL_TIMEOUT, reader.rx.changed()).await {
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => return None,
                Err(_) => {
                    let err = std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "download stalled, no progress for 600s",
                    );
                    return Some((Err(err), reader));
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_chunk_size_splits_evenly() {
        let chunks = plan_chunks(25, 4, 10);
        assert_eq!(chunks, vec![(0, 9), (10, 19), (20, 24)]);
    }

    #[test]
    fn auto_chunk_size_uses_concurrency_minus_ten() {
        // 20 concurrent workers -> 10 parts
        let chunks = plan_chunks(1000, 20, 0);
        assert_eq!(chunks.len(), 10);
        assert_eq!(chunks[0], (0, 99));
        assert_eq!(chunks[9], (900, 999));

        // Fewer than 10 workers still produces a single part.
        let chunks = plan_chunks(1000, 4, 0);
        assert_eq!(chunks, vec![(0, 999)]);
    }

    #[test]
    fn chunk_count_is_bounded() {
        let chunks = plan_chunks(1_000_000_000, 8, 1);
        assert!(chunks.len() as u64 <= MAX_CHUNKS + 1);
    }

    #[test]
    fn empty_file_has_no_chunks() {
        assert!(plan_chunks(0, 4, 1024).is_empty());
    }

    #[test]
    fn job_tracks_progress_and_finish() {
        let job = DownloadJob::new("k".into(), PathBuf::from("x"), 100, String::new());
        assert_eq!(job.progress(), 0.0);
        job.mark_range(0, 49);
        assert_eq!(job.downloaded_bytes(), 50);
        assert!((job.progress() - 0.5).abs() < f64::EPSILON);
        job.finish_ok();
        assert!(job.is_finished());
        assert!(job.error().is_none());
    }

    #[test]
    fn job_reports_errors() {
        let job = DownloadJob::new("k".into(), PathBuf::from("x"), 10, String::new());
        job.finish_err("boom");
        assert!(job.is_finished());
        assert_eq!(job.error().as_deref(), Some("boom"));
    }
}
