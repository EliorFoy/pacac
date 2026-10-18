//! Streaming race tests: the reader must never see bytes that the downloader
//! has not written yet.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use axum::http::{HeaderMap, Response, StatusCode, Uri};
use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use tokio::net::TcpListener;

use pacac::config::{Config, RuleConfig, Strategy};
use pacac::server::{self, AppState};

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 37 + 11) % 251) as u8).collect()
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

/// Streams the requested slice slowly, in small pieces, like a real CDN.
async fn slow_handler(
    State(data): State<Arc<Vec<u8>>>,
    uri: Uri,
    headers: HeaderMap,
) -> Response<Body> {
    let total = data.len() as u64;
    let range = headers
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .map(|value| parse_range(value, total));

    let (start, end, status) = match range {
        Some((start, end)) => (start, end, StatusCode::PARTIAL_CONTENT),
        None => (0, total - 1, StatusCode::OK),
    };
    let _ = uri;

    let slice = data[start as usize..=end as usize].to_vec();
    let len = slice.len();
    let stream = stream::unfold((slice, 0usize), |(slice, offset)| async move {
        if offset >= slice.len() {
            return None;
        }
        let next = (offset + 16 * 1024).min(slice.len());
        let chunk = Bytes::copy_from_slice(&slice[offset..next]);
        tokio::time::sleep(Duration::from_millis(1)).await;
        Some((Ok::<Bytes, std::io::Error>(chunk), (slice, next)))
    });

    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, len)
        .header("accept-ranges", "bytes");
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(CONTENT_RANGE, format!("bytes {start}-{end}/{total}"));
    }
    builder.body(Body::from_stream(stream)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streamed_response_has_no_holes() {
    let data = Arc::new(payload(8 * 1024 * 1024));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(slow_handler)
        .with_state(data.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

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
        upstream: format!("http://{upstream_addr}"),
        strategy: Strategy::Parallel,
        min_size: 1,
        concurrency: 2,
        chunk_size: 2 * 1024 * 1024,
        ..Default::default()
    }];

    let state = AppState::new(cfg).unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, server::app(state)).await;
    });

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{addr}/file"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.unwrap();

    assert_eq!(body.len(), data.len(), "wrong body length");
    let mut bad = Vec::new();
    for (index, (got, want)) in body.iter().zip(data.iter()).enumerate() {
        if got != want {
            bad.push(index);
            if bad.len() > 5 {
                break;
            }
        }
    }
    assert!(bad.is_empty(), "mismatched bytes at offsets {bad:?}");
}
