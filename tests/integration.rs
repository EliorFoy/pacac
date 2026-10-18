//! End-to-end tests: a mock upstream, a real pacac instance and a real HTTP
//! client.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, HOST, LOCATION, RANGE,
};
use axum::http::{HeaderMap, Method, Response, StatusCode, Uri};
use axum::Router;
use reqwest::redirect::Policy;
use serde_json::Value;
use tokio::net::TcpListener;

use pacac::config::{Config, ContentRewrite, RuleConfig, Strategy};
use pacac::server::{self, AppState};

// ---------------------------------------------------------------- mock server

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    query: String,
    range: Option<String>,
}

struct Mock {
    big: Vec<u8>,
    small: Vec<u8>,
    requests: Mutex<Vec<Recorded>>,
}

impl Mock {
    fn record(&self, method: &Method, path: &str, query: &str, headers: &HeaderMap) {
        self.requests.lock().unwrap().push(Recorded {
            method: method.to_string(),
            path: path.to_string(),
            query: query.to_string(),
            range: headers
                .get(RANGE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        });
    }

    fn take(&self) -> Vec<Recorded> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }

    fn range_requests(&self) -> Vec<Recorded> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.range.is_some())
            .cloned()
            .collect()
    }
}

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect()
}

fn parse_range(header: &str, total: u64) -> Option<(u64, u64)> {
    let spec = header.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() {
        total - 1
    } else {
        end.parse().ok()?
    };
    if start > end || end >= total {
        return None;
    }
    Some((start, end))
}

fn file_response(data: &[u8], method: &Method, headers: &HeaderMap) -> Response<Body> {
    let total = data.len() as u64;

    if let Some(range) = headers.get(RANGE).and_then(|value| value.to_str().ok()) {
        if let Some((start, end)) = parse_range(range, total) {
            let slice = data[start as usize..=end as usize].to_vec();
            return Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, slice.len())
                .header(CONTENT_RANGE, format!("bytes {start}-{end}/{total}"))
                .header(ACCEPT_RANGES, "bytes")
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(slice))
                .unwrap();
        }
    }

    let body = if *method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(data.to_vec())
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, total)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .unwrap()
}

async fn mock_handler(
    State(mock): State<Arc<Mock>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response<Body> {
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();
    mock.record(&method, &path, &query, &headers);

    // Optional artificial latency, used to make concurrent requests overlap.
    if let Some(delay) = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("delay="))
        .and_then(|value| value.parse::<u64>().ok())
    {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    // `hold=1` only slows down the actual range transfers, so the HEAD request
    // of a second client still returns quickly.
    if query.contains("hold=1") && headers.get(RANGE).is_some() {
        tokio::time::sleep(Duration::from_millis(2000)).await;
    }

    match path.as_str() {
        "/v2/" | "/v2" => {
            let host = headers
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("127.0.0.1");
            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(
                    "www-authenticate",
                    format!("Bearer realm=\"http://{host}/token\",service=\"mock-registry\""),
                )
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"errors":[{"code":"UNAUTHORIZED","message":"auth required"}]}"#,
                ))
                .unwrap()
        }
        "/token" => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"token":"mock-token","expires_in":300}"#))
            .unwrap(),
        "/redirect" => Response::builder()
            .status(StatusCode::FOUND)
            .header(LOCATION, "/big")
            .body(Body::empty())
            .unwrap(),
        "/html" => {
            let host = headers
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("127.0.0.1");
            let body = format!(
                r#"<html><body><a href="https://files.pythonhosted.org/pkg.whl">pkg</a><a href="http://{host}/simple/pkg/">pkg</a></body></html>"#
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/html; charset=utf-8")
                .header(CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap()
        }
        "/json" => {
            let body = r#"{"url":"https://pypi.org/simple/","name":"demo"}"#;
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .header(CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap()
        }
        "/echo-query" => {
            let body = query.clone();
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/plain")
                .header(CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap()
        }
        "/small" => file_response(&mock.small, &method, &headers),
        "/no-range" => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, mock.big.len())
            .body(Body::from(mock.big.clone()))
            .unwrap(),
        "/error" => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("upstream exploded"))
            .unwrap(),
        _ => file_response(&mock.big, &method, &headers),
    }
}

async fn start_mock() -> (String, Arc<Mock>) {
    let mock = Arc::new(Mock {
        big: payload(4 * 1024 * 1024),
        small: payload(1024),
        requests: Mutex::new(Vec::new()),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(mock_handler)
        .with_state(mock.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{addr}"), mock)
}

// ------------------------------------------------------------------ pacac app

struct TestServer {
    base: String,
    state: Arc<AppState>,
    _cache: tempfile::TempDir,
    client: reqwest::Client,
    no_redirect: reqwest::Client,
}

async fn start_pacac(upstream: &str) -> TestServer {
    start_pacac_with(upstream, |_| {}).await
}

async fn start_pacac_with(upstream: &str, mutate: impl FnOnce(&mut Config)) -> TestServer {
    let cache = tempfile::tempdir().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let mut cfg = test_config(upstream, &addr, cache.path().to_path_buf());
    mutate(&mut cfg);
    let state = AppState::new(cfg).unwrap();
    let app = server::app(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    TestServer {
        base: format!("http://{addr}"),
        state,
        _cache: cache,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap(),
        no_redirect: reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap(),
    }
}

fn test_config(upstream: &str, addr: &SocketAddr, cache_dir: PathBuf) -> Config {
    let mut cfg = Config::default();
    cfg.server.host = "127.0.0.1".to_string();
    cfg.server.public_host = Some(addr.to_string());
    cfg.server.max_concurrent_downloads = 8;
    cfg.server.queue_timeout_secs = 5;
    cfg.cache.dir = Some(cache_dir.to_string_lossy().into_owned());
    cfg.cache.max_size_gb = 1.0;
    cfg.content_rewrite = Some(ContentRewrite {
        content_types: vec!["text/html".into(), "application/json".into()],
        targets: vec![
            upstream.to_string(),
            "https://files.pythonhosted.org".into(),
            "https://pypi.org".into(),
        ],
    });

    cfg.rules = vec![
        RuleConfig {
            name: "pypi-simple".into(),
            path_prefix: Some("/simple".into()),
            upstream: upstream.into(),
            strategy: Strategy::Proxy,
            ..Default::default()
        },
        RuleConfig {
            name: "big".into(),
            path_prefix: Some("/big".into()),
            upstream: upstream.into(),
            strategy: Strategy::Parallel,
            min_size: 1,
            concurrency: 4,
            chunk_size: 1024 * 1024,
            ..Default::default()
        },
        RuleConfig {
            name: "single-chunk".into(),
            path_prefix: Some("/single".into()),
            upstream: upstream.into(),
            strategy: Strategy::Parallel,
            min_size: 1,
            concurrency: 1,
            chunk_size: 0,
            ..Default::default()
        },
        RuleConfig {
            name: "small".into(),
            path_prefix: Some("/small".into()),
            upstream: upstream.into(),
            strategy: Strategy::Parallel,
            min_size: 10 * 1024 * 1024,
            concurrency: 4,
            chunk_size: 1024 * 1024,
            ..Default::default()
        },
        RuleConfig {
            name: "docker-blob".into(),
            pattern: Some("/v2/.*/blobs/sha256:[a-f0-9]+".into()),
            upstream: upstream.into(),
            strategy: Strategy::Parallel,
            min_size: 1,
            concurrency: 2,
            chunk_size: 0,
            ..Default::default()
        },
        RuleConfig {
            name: "docker-registry".into(),
            pattern: Some("/v2/.*".into()),
            upstream: upstream.into(),
            strategy: Strategy::Proxy,
            handler: Some("handlers.docker".into()),
            ..Default::default()
        },
        RuleConfig {
            name: "default".into(),
            path_prefix: Some("/".into()),
            upstream: upstream.into(),
            strategy: Strategy::Proxy,
            ..Default::default()
        },
    ];

    cfg
}

// ---------------------------------------------------------------------- tests

#[tokio::test]
async fn proxies_plain_get() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let response = server
        .client
        .get(format!("{}/simple/demo/", server.base))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.unwrap();
    assert_eq!(body.len(), 4 * 1024 * 1024);
    assert_eq!(&body[..8], &payload(8)[..]);
}

#[tokio::test]
async fn rewrites_html_and_json_links() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let html = server
        .client
        .get(format!("{}/html", server.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains(&format!("{}/pkg.whl", server.base)), "{html}");
    assert!(!html.contains("files.pythonhosted.org"), "{html}");

    let json = server
        .client
        .get(format!("{}/json", server.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(json.contains(&format!("{}/simple/", server.base)), "{json}");
}

#[tokio::test]
async fn parallel_download_uses_ranges_and_caches() {
    let (upstream, mock) = start_mock().await;
    let server = start_pacac(&upstream).await;
    mock.take();

    let first = server
        .client
        .get(format!("{}/big", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first.headers().get(CONTENT_LENGTH).unwrap(),
        &(4 * 1024 * 1024).to_string()
    );
    let body = first.bytes().await.unwrap();
    assert_eq!(body.as_ref(), payload(4 * 1024 * 1024).as_slice());

    let ranges = mock.range_requests();
    assert_eq!(ranges.len(), 4, "expected 4 range requests, got {ranges:?}");

    // Second request must be served from cache without touching the upstream.
    mock.take();
    let second = server
        .client
        .get(format!("{}/big", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.bytes().await.unwrap().as_ref(),
        payload(4 * 1024 * 1024).as_slice()
    );
    assert!(
        mock.range_requests().is_empty(),
        "cache hit should not download again"
    );

    let stats: Value = server
        .client
        .get(format!("{}/stats", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["cache"]["count"], 1);
    assert!(stats["cache"]["size_bytes"].as_u64().unwrap() >= 4 * 1024 * 1024);
}

#[tokio::test]
async fn concurrent_clients_share_one_download() {
    let (upstream, mock) = start_mock().await;
    let server = start_pacac(&upstream).await;
    mock.take();

    let url = format!("{}/big?delay=60", server.base);
    let (a, b) = tokio::join!(
        server.client.get(&url).send(),
        server.client.get(&url).send()
    );

    let a = a.unwrap().bytes().await.unwrap();
    let b = b.unwrap().bytes().await.unwrap();
    assert_eq!(a.as_ref(), payload(4 * 1024 * 1024).as_slice());
    assert_eq!(b.as_ref(), payload(4 * 1024 * 1024).as_slice());

    let ranges = mock.range_requests();
    assert_eq!(
        ranges.len(),
        4,
        "the second client should attach to the running download, saw {ranges:?}"
    );
}

#[tokio::test]
async fn head_requests_are_forwarded() {
    let (upstream, mock) = start_mock().await;
    let server = start_pacac(&upstream).await;
    mock.take();

    let response = server
        .client
        .head(format!("{}/big", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("4194304")
    );
    assert!(response.bytes().await.unwrap().is_empty());
    assert!(
        mock.take().iter().any(|r| r.method == "HEAD"),
        "HEAD should reach the upstream as a HEAD request"
    );
}

#[tokio::test]
async fn small_files_fall_back_to_proxying() {
    let (upstream, mock) = start_mock().await;
    let server = start_pacac(&upstream).await;
    mock.take();

    let response = server
        .client
        .get(format!("{}/small", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        payload(1024).as_slice()
    );
    assert!(
        mock.range_requests().is_empty(),
        "a file below min_size must not be downloaded in ranges"
    );
}

#[tokio::test]
async fn query_strings_are_forwarded() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let body = server
        .client
        .get(format!("{}/echo-query?download=true&x=1", server.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "download=true&x=1");
}

#[tokio::test]
async fn redirects_are_followed() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let response = server
        .client
        .get(format!("{}/redirect", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().len(), 4 * 1024 * 1024);
}

#[tokio::test]
async fn upstream_errors_are_passed_through() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let response = server
        .client
        .get(format!("{}/error", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response.text().await.unwrap(), "upstream exploded");
}

#[tokio::test]
async fn health_and_stats_endpoints() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let health: Value = server
        .client
        .get(format!("{}/health", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["active_downloads"], 0);
    assert!(health["version"].is_string());

    let stats: Value = server
        .client
        .get(format!("{}/stats", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["cache"]["count"], 0);
}

#[tokio::test]
async fn docker_returns_401_pointing_at_the_proxy() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let response = server
        .no_redirect
        .get(format!("{}/v2/", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let challenge = response
        .headers()
        .get("www-authenticate")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_string();
    assert!(
        challenge.contains(&format!("{}/v2/auth", server.base)),
        "challenge should point at the proxy: {challenge}"
    );
}

#[tokio::test]
async fn docker_token_is_proxied_with_scope() {
    let (upstream, mock) = start_mock().await;
    let server = start_pacac(&upstream).await;
    mock.take();

    let response = server
        .client
        .get(format!(
            "{}/v2/auth?scope=repository:library/nginx:pull&service=registry.docker.io",
            server.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert!(body.contains("mock-token"), "{body}");

    let token_request = mock
        .take()
        .into_iter()
        .find(|r| r.path == "/token")
        .expect("token endpoint should be called");
    assert!(
        token_request.query.contains("service=mock-registry"),
        "{token_request:?}"
    );
    assert!(
        token_request
            .query
            .contains("scope=repository%3Alibrary%2Fnginx%3Apull")
            || token_request
                .query
                .contains("scope=repository:library/nginx:pull"),
        "{token_request:?}"
    );
}

#[tokio::test]
async fn docker_official_images_are_redirected_to_library() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let response = server
        .no_redirect
        .get(format!("{}/v2/nginx/manifests/latest", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
    assert_eq!(
        response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok()),
        Some(format!("{}/v2/library/nginx/manifests/latest", server.base).as_str())
    );
}

#[tokio::test]
async fn server_ignoring_ranges_is_tolerated() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    // `single-chunk` uses chunk_size 0 with concurrency 1, so exactly one range
    // is requested; the upstream answers 200 with the whole body.
    let response = server
        .client
        .get(format!("{}/single", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        payload(4 * 1024 * 1024).as_slice()
    );
}

#[tokio::test]
async fn interrupted_download_resumes_from_chunk_records() {
    let (upstream, mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    // Simulate a crash after the first two of four chunks were written.
    let key = format!("{upstream}/big");
    let scratch = server.state.cache.tmp_path(&key);
    let data = payload(4 * 1024 * 1024);
    std::fs::write(&scratch, &data).unwrap();
    server
        .state
        .cache
        .mark_chunk(&key, 4 * 1024 * 1024, 0, 1024 * 1024 - 1);
    server
        .state
        .cache
        .mark_chunk(&key, 4 * 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024 - 1);
    mock.take();

    let response = server
        .client
        .get(format!("{}/big", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), data.as_slice());

    let ranges = mock.range_requests();
    assert_eq!(
        ranges.len(),
        2,
        "only the missing chunks should be requested, saw {ranges:?}"
    );
}

#[tokio::test]
async fn unknown_paths_still_reach_the_default_rule() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac(&upstream).await;

    let response = server
        .client
        .get(format!("{}/completely/unknown", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn download_slots_report_503_and_health_shows_progress() {
    let (upstream, _mock) = start_mock().await;
    let server = start_pacac_with(&upstream, |cfg| {
        cfg.server.max_concurrent_downloads = 1;
        cfg.server.queue_timeout_secs = 1;
    })
    .await;

    let url = format!("{}/big?hold=1", server.base);
    let client = server.client.clone();
    let first = tokio::spawn(async move { client.get(&url).send().await });

    tokio::time::sleep(Duration::from_millis(400)).await;
    let health: Value = server
        .client
        .get(format!("{}/health", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["active_downloads"], 1, "health: {health}");

    let second = server
        .client
        .get(format!("{}/big?hold=1&other=1", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(second.headers().get("retry-after").is_some());

    let first = first.await.unwrap().unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.bytes().await.unwrap().len(), 4 * 1024 * 1024);
}
