//! Deterministic proof that parallel chunks add up when the upstream throttles
//! per connection.
//!
//! A local mock server streams 2 MiB/s *per connection*. With one connection
//! the 16 MiB file takes ~8 s; with eight connections it should take ~1 s.
//! The test is `#[ignore]`d because it measures time (CI machines are noisy),
//! run it with:
//!
//! ```text
//! cargo test --test parallel_speed -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use tokio::net::TcpListener;

use pacac::config::{Config, RuleConfig, Strategy};
use pacac::server::{self, AppState};

const FILE_SIZE: usize = 16 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;
/// 64 KiB every 32 ms ~= 2 MiB/s per connection.
const TICK: Duration = Duration::from_millis(32);

fn payload() -> Vec<u8> {
    (0..FILE_SIZE).map(|i| ((i * 31 + 7) % 251) as u8).collect()
}

fn parse_range(header: &str, total: u64) -> (u64, u64) {
    let spec = header.trim_start_matches("bytes=");
    let (start, end) = spec.split_once('-').unwrap();
    let start: u64 = start.parse().unwrap();
    let end = if end.is_empty() {
        total - 1
    } else {
        end.parse::<u64>().unwrap().min(total - 1)
    };
    (start, end)
}

/// Streams the requested slice at a fixed rate for this connection only.
async fn slow_handler(State(data): State<Arc<Vec<u8>>>, headers: HeaderMap) -> Response<Body> {
    let total = data.len() as u64;
    let (start, end, status) = match headers.get(RANGE).and_then(|v| v.to_str().ok()) {
        Some(range) => {
            let (s, e) = parse_range(range, total);
            (s, e, StatusCode::PARTIAL_CONTENT)
        }
        None => (0, total - 1, StatusCode::OK),
    };

    let slice = data[start as usize..=end as usize].to_vec();
    let len = slice.len();
    let body = stream::unfold((slice, 0usize), |(slice, offset)| async move {
        if offset >= slice.len() {
            return None;
        }
        let next = (offset + CHUNK).min(slice.len());
        let piece = Bytes::copy_from_slice(&slice[offset..next]);
        tokio::time::sleep(TICK).await;
        Some((Ok::<Bytes, std::io::Error>(piece), (slice, next)))
    });

    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, len)
        .header("accept-ranges", "bytes");
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(CONTENT_RANGE, format!("bytes {start}-{end}/{total}"));
    }
    builder.body(Body::from_stream(body)).unwrap()
}

async fn start_mock() -> String {
    let data = Arc::new(payload());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().fallback(slow_handler).with_state(data);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn start_pacac(upstream: &str, concurrency: usize) -> String {
    let cache = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mut cfg = Config::default();
    cfg.server.host = "127.0.0.1".into();
    cfg.server.public_host = Some(addr.to_string());
    cfg.cache.dir = Some(cache.path().to_string_lossy().into_owned());
    cfg.rules = vec![RuleConfig {
        name: "slow".into(),
        path_prefix: Some("/".into()),
        upstream: upstream.to_string(),
        strategy: Strategy::Parallel,
        min_size: 1,
        concurrency,
        chunk_size: 2 * 1024 * 1024,
        ..Default::default()
    }];

    let state = AppState::new(cfg).unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, server::app(state)).await;
    });
    // Keep the temp dir alive for the process lifetime.
    std::mem::forget(cache);
    format!("http://{addr}")
}

async fn download(base: &str) -> Duration {
    let client = reqwest::Client::new();
    let started = Instant::now();
    let response = client.get(format!("{base}/file")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.unwrap();
    assert_eq!(body.len(), FILE_SIZE, "incomplete body");
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "performance check, run manually"]
async fn parallel_chunks_scale_with_per_connection_throttling() {
    let upstream = start_mock().await;

    let serial_base = start_pacac(&upstream, 1).await;
    let serial = download(&serial_base).await;
    println!("1 connection : {:>6.2}s", serial.as_secs_f64());

    let parallel_base = start_pacac(&upstream, 8).await;
    let parallel = download(&parallel_base).await;
    println!("8 connections: {:>6.2}s", parallel.as_secs_f64());

    let speedup = serial.as_secs_f64() / parallel.as_secs_f64();
    println!("speedup      : {:>6.2}x", speedup);

    assert!(
        speedup > 4.0,
        "expected parallel chunks to be much faster, got {speedup:.2}x"
    );
}
