//! HTTP server: routing, dispatch, cache lookup and download orchestration.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::http::header::{ACCEPT_RANGES, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router as AxumRouter;
use reqwest::redirect::Policy;
use serde_json::json;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};
use url::Url;

use crate::cache::CacheManager;
use crate::config::{Config, Strategy};
use crate::downloader::{run_download, stream_job, DownloadJob, DownloadPlan};
use crate::handlers::{self, HandlerCtx};
use crate::proxy::{self, ProxyRequest};
use crate::router::{build_target_url, Router, Rule};
use crate::util::{format_bytes, header_value};

/// Longest a request waits for a free download slot before getting a 503.
const JANITOR_INTERVAL: Duration = Duration::from_secs(300);
/// Scratch files older than this are considered abandoned.
const SCRATCH_TTL: Duration = Duration::from_secs(3600);

pub struct AppState {
    pub cfg: Arc<Config>,
    pub router: Arc<Router>,
    pub cache: Arc<CacheManager>,
    pub client: reqwest::Client,
    pub head_client: reqwest::Client,
    /// Never follows redirects; used to read metadata headers on HEAD.
    pub meta_client: reqwest::Client,
    /// Client used for range requests (HTTP/1.1 unless configured otherwise).
    pub range_client: reqwest::Client,
    pub download_limit: Arc<Semaphore>,
    pub sequential_limit: Arc<Semaphore>,
    pub active: Arc<Mutex<HashMap<String, Arc<DownloadJob>>>>,
    pub started: Instant,
    pub requests: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub bytes_served: AtomicU64,
}

impl AppState {
    pub fn new(cfg: Config) -> Result<Arc<Self>> {
        let router = Router::build(&cfg.rules)?;
        let cache = Arc::new(CacheManager::open(
            cfg.cache_dir(),
            cfg.cache.max_size_gb,
            cfg.cache.lru_enabled,
            cfg.cache.enabled,
        )?);

        let (client, head_client, meta_client, range_client) = build_clients(&cfg)?;
        let download_limit = Arc::new(Semaphore::new(cfg.server.max_concurrent_downloads));
        let sequential_limit = Arc::new(Semaphore::new(cfg.server.sequential_downloads));

        info!(
            cache_dir = %cache.dir().display(),
            cache_limit = %format_bytes(cache.max_bytes()),
            rules = router.rules().len(),
            "cache ready"
        );

        Ok(Arc::new(Self {
            cfg: Arc::new(cfg),
            router: Arc::new(router),
            cache,
            client,
            head_client,
            meta_client,
            range_client,
            download_limit,
            sequential_limit,
            active: Arc::new(Mutex::new(HashMap::new())),
            started: Instant::now(),
            requests: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            bytes_served: AtomicU64::new(0),
        }))
    }
}

fn build_clients(
    cfg: &Config,
) -> Result<(
    reqwest::Client,
    reqwest::Client,
    reqwest::Client,
    reqwest::Client,
)> {
    let proxy = cfg.server.resolve_proxy();
    if let Some(setting) = &proxy {
        info!(
            proxy = %setting.url,
            from_system = setting.from_system,
            "using an upstream proxy"
        );
    }

    let base = || -> Result<reqwest::ClientBuilder> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(cfg.server.connect_timeout_secs))
            .read_timeout(Duration::from_secs(cfg.server.read_timeout_secs))
            .pool_max_idle_per_host(cfg.server.max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(30))
            .redirect(Policy::limited(10))
            .user_agent(format!("pacac/{}", env!("CARGO_PKG_VERSION")))
            .gzip(true)
            .brotli(true)
            .deflate(true);

        if let Some(setting) = &proxy {
            let mut upstream = reqwest::Proxy::all(&setting.url)
                .with_context(|| format!("invalid server.upstream_proxy {:?}", setting.url))?;
            if setting.from_system {
                // Never send loopback traffic through the system proxy.
                upstream = upstream.no_proxy(reqwest::NoProxy::from_string(
                    "localhost,127.0.0.1,::1,*.local",
                ));
            }
            builder = builder.proxy(upstream);
        }
        Ok(builder)
    };

    let client = base()?
        .build()
        .context("failed to build the upstream HTTP client")?;

    let head_client = base()?
        .timeout(Duration::from_secs(cfg.server.head_timeout_secs))
        .build()
        .context("failed to build the metadata HTTP client")?;

    let meta_client = base()?
        .timeout(Duration::from_secs(cfg.server.head_timeout_secs))
        .redirect(Policy::none())
        .build()
        .context("failed to build the no-redirect HTTP client")?;

    // Range requests are multiplexed by HTTP/2 onto a single TCP connection,
    // which cancels out the point of parallel chunks when the upstream
    // throttles per connection. HTTP/1.1 gives every chunk its own connection;
    // `server.download_http2: true` opts back in for upstreams that do not
    // throttle (single-connection speed is then higher).
    let range_client = if cfg.server.download_http2 {
        base()?
    } else {
        base()?.http1_only()
    }
    .build()
    .context("failed to build the range download client")?;

    Ok((client, head_client, meta_client, range_client))
}

pub fn app(state: Arc<AppState>) -> AxumRouter {
    let router: AxumRouter<Arc<AppState>> = AxumRouter::new()
        .route("/health", get(health))
        .route("/stats", get(stats))
        .fallback(dispatch);
    router.with_state(state)
}

/// Start the HTTP server and run until Ctrl+C / SIGTERM.
pub async fn run(cfg: Config, banner: bool) -> Result<()> {
    let state = AppState::new(cfg)?;
    spawn_janitor(state.clone());

    let addr = format!("{}:{}", state.cfg.server.host, state.cfg.server.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("cannot listen on {addr}"))?;
    let local = listener
        .local_addr()
        .unwrap_or_else(|_| addr.parse().unwrap());

    if local.ip().is_unspecified() {
        warn!("listening on all interfaces; pacac has no authentication");
    }

    if banner {
        print_banner(&state, &local);
    }
    info!("pacac listening on http://{local}");

    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    info!("pacac stopped");
    Ok(())
}

fn print_banner(state: &AppState, local: &std::net::SocketAddr) {
    use crate::config::Strategy;
    use crate::util::{format_bytes, local_ipv4};

    let base = public_addr(state);
    let rules = state.router.rules();
    let parallel: Vec<&Rule> = rules
        .iter()
        .filter(|rule| rule.strategy == Strategy::Parallel)
        .collect();
    let concurrency = parallel
        .iter()
        .map(|rule| rule.concurrency)
        .max()
        .unwrap_or(0);
    let chunk = parallel.first().map(|rule| rule.chunk_size).unwrap_or(0);

    let listen = if local.ip().is_unspecified() {
        match local_ipv4() {
            Some(ip) => format!("0.0.0.0:{}  (LAN {ip})", local.port()),
            None => format!("0.0.0.0:{}  (all interfaces)", local.port()),
        }
    } else {
        local.to_string()
    };

    let upstream = match &state.cfg.server.resolve_proxy() {
        Some(setting) if setting.from_system => format!("system proxy ({})", setting.url),
        Some(setting) => setting.url.clone(),
        None => "direct".to_string(),
    };

    let cache = if state.cache.enabled() {
        format!(
            "{}  ({}, LRU)",
            state.cache.dir().display(),
            format_bytes(state.cache.max_bytes())
        )
    } else {
        "disabled (--no-cache)".to_string()
    };

    let chunk_text = if chunk <= 0 {
        "auto".to_string()
    } else {
        format_bytes(chunk as u64)
    };

    let host = default_host(&base);
    let rule_names = rules
        .iter()
        .map(|rule| rule.name.as_str())
        .take(4)
        .collect::<Vec<_>>()
        .join(", ");

    println!();
    println!(
        "  pacac {}  -  Package Accelerator & Cache",
        env!("CARGO_PKG_VERSION")
    );
    println!("  {}", "=".repeat(64));
    println!("  listen     {listen}");
    println!("  public     {base}");
    println!("  upstream   {upstream}");
    println!(
        "  downloads  {concurrency} chunks/file, {} files at once, {chunk_text} chunks",
        state.cfg.server.max_concurrent_downloads
    );
    println!("  cache      {cache}");
    println!("  rules      {} ({rule_names}, ...)", rules.len());
    println!();
    println!("  pip        pip install -i {base}/simple --trusted-host {host} <package>");
    println!("  hf         HF_ENDPOINT={base}");
    println!("  docker     \"registry-mirrors\": [\"{base}\"]");
    println!("  health     {base}/health");
    println!();
    println!("  Ctrl+C to stop");
    println!();
}

fn default_host(base: &str) -> String {
    base.trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or(base)
        .to_string()
}

fn public_addr(state: &AppState) -> String {
    let host = state.cfg.server.public_host();
    if host.starts_with("http://") || host.starts_with("https://") {
        host
    } else {
        format!("http://{host}")
    }
}

fn spawn_janitor(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(JANITOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let active_paths: HashSet<PathBuf> = {
                let active = state.active.lock().await;
                active.values().map(|job| job.path.clone()).collect()
            };
            let cache = state.cache.clone();
            let ttl = state.cfg.cache.chunk_ttl_hours;
            let _ = tokio::task::spawn_blocking(move || {
                cache.maintenance(ttl, SCRATCH_TTL, &active_paths);
                cache.evict_if_needed();
            })
            .await;
        }
    });
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown requested");
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let active = state.active.lock().await;
    let downloads: Vec<String> = active.keys().cloned().collect();
    let in_flight: Vec<_> = active
        .values()
        .map(|job| {
            json!({
                "key": job.key,
                "size": job.total,
                "progress": format!("{:.1}%", job.progress() * 100.0),
            })
        })
        .collect();

    Json(json!({
        "status": "ok",
        "active_downloads": downloads.len(),
        "downloads": downloads,
        "in_flight": in_flight,
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.started.elapsed().as_secs(),
        "requests": state.requests.load(Ordering::Relaxed),
        "cache_hits": state.cache_hits.load(Ordering::Relaxed),
        "cache_misses": state.cache_misses.load(Ordering::Relaxed),
        "bytes_served": state.bytes_served.load(Ordering::Relaxed),
    }))
}

async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cache = state.cache.clone();
    let stats = tokio::task::spawn_blocking(move || cache.stats())
        .await
        .unwrap_or(crate::cache::CacheStats {
            count: 0,
            size_bytes: 0,
            size_gb: 0.0,
            first_cached: None,
            last_accessed: None,
        });
    Json(json!({ "cache": stats }))
}

async fn dispatch(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    state.requests.fetch_add(1, Ordering::Relaxed);

    if method == Method::CONNECT {
        return proxy::error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "CONNECT tunnelling is not supported; use HTTP_PROXY pointing at this server",
        );
    }

    let path = uri.path().to_string();
    let query = uri.query().map(str::to_string);
    let full_path = path.trim_start_matches('/').to_string();

    let Some((rule, processed)) = state.router.match_path(&path) else {
        return proxy::error_response(StatusCode::NOT_FOUND, format!("no rule matches {path}"));
    };

    debug!(method = %method, path = %path, rule = %rule.name, "dispatch");

    if let Some(kind) = rule.handler {
        let ctx = HandlerCtx {
            client: &state.client,
            cfg: &state.cfg,
            rule,
            full_path: &full_path,
            query: query.as_deref(),
            method: &method,
            headers: &headers,
        };
        if let Some(response) = handlers::exec(kind, &ctx).await {
            return response;
        }
    }

    let target = match build_target_url(rule.upstream(), &processed, query.as_deref()) {
        Ok(url) => url,
        Err(err) => {
            return proxy::error_response(StatusCode::BAD_REQUEST, err);
        }
    };

    let wants_parallel = rule.strategy == Strategy::Parallel && method == Method::GET;
    if !wants_parallel {
        let ctx = ProxyRequest {
            client: &state.client,
            meta_client: &state.meta_client,
            cfg: &state.cfg,
            rule: Some(rule),
            method: &method,
            headers: &headers,
            target: &target,
        };
        return proxy::proxy_request(ctx, body).await;
    }

    parallel_dispatch(&state, &headers, &target, rule, &method).await
}

async fn parallel_dispatch(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    target: &Url,
    rule: &Rule,
    method: &Method,
) -> Response<Body> {
    // 1. Ask the upstream for the size and the final URL.
    let mut head_request = state.head_client.head(target.clone());
    if let Some(auth) = headers.get(AUTHORIZATION) {
        head_request = head_request.header(AUTHORIZATION, auth);
    }
    let head = match head_request.send().await {
        Ok(response) => response,
        Err(err) => {
            warn!(url = %target, error = %err, "HEAD failed, falling back to proxying");
            return proxy_fallback(state, headers, target, rule, method).await;
        }
    };

    let status = head.status();
    let total = head
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let content_type = head
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let final_url = head.url().clone();

    if !status.is_success() || total == 0 || total < rule.min_size {
        debug!(
            url = %target,
            %status,
            total,
            min_size = rule.min_size,
            "not suitable for parallel download, proxying"
        );
        return proxy_fallback(state, headers, target, rule, method).await;
    }

    // 2. Cache lookup.
    let key = match rule.cache_key_source {
        crate::config::CacheKeySource::Original => target.as_str().to_string(),
        crate::config::CacheKeySource::Final => final_url.as_str().to_string(),
    };

    if let Some(cached) = state.cache.get(&key) {
        state.cache_hits.fetch_add(1, Ordering::Relaxed);
        state.bytes_served.fetch_add(cached.size, Ordering::Relaxed);
        info!(key = %key, size = %format_bytes(cached.size), "cache hit");
        let content_type = if cached.content_type.is_empty() {
            content_type
        } else {
            cached.content_type
        };
        return proxy::serve_file(&cached.path, cached.size, &content_type).await;
    }
    state.cache_misses.fetch_add(1, Ordering::Relaxed);

    // 3. Join an in-flight download when possible.
    {
        let active = state.active.lock().await;
        if let Some(job) = active.get(&key) {
            let job = job.clone();
            drop(active);
            debug!(key = %key, "attaching to the running download");
            return serve_job(state, job).await;
        }
    }

    // 4. Take a download slot.
    let permit =
        match acquire_slot(&state.download_limit, state.cfg.server.queue_timeout_secs).await {
            Some(permit) => permit,
            None => {
                warn!(key = %key, "download queue is full");
                return busy_response(state.cfg.server.queue_timeout_secs);
            }
        };

    // Auto-mode downloads (chunk_size <= 0) are serialised, like the original
    // implementation, to keep registry tokens from expiring mid-download.
    let sequential_permit = if rule.chunk_size <= 0 {
        match acquire_slot(&state.sequential_limit, state.cfg.server.queue_timeout_secs).await {
            Some(permit) => Some(permit),
            None => {
                warn!(key = %key, "sequential download slot is busy");
                return busy_response(state.cfg.server.queue_timeout_secs);
            }
        }
    } else {
        None
    };

    // 5. Register the job and start it.
    let mut active = state.active.lock().await;
    if let Some(job) = active.get(&key) {
        let job = job.clone();
        drop(active);
        return serve_job(state, job).await;
    }

    let scratch = state.cache.tmp_path(&key);
    let freshly_created = match ensure_scratch(&scratch, total).await {
        Ok(fresh) => fresh,
        Err(err) => {
            return proxy::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("cannot prepare the download file: {err}"),
            );
        }
    };
    if freshly_created {
        state.cache.clear_chunks(final_url.as_str());
    }

    let job = DownloadJob::new(key.clone(), scratch.clone(), total, content_type.clone());
    active.insert(key.clone(), job.clone());
    drop(active);

    let plan = DownloadPlan {
        key: key.clone(),
        url: final_url,
        path: scratch,
        total,
        concurrency: rule.concurrency,
        chunk_size: rule.chunk_size,
        headers: auth_headers(headers),
        client: state.range_client.clone(),
        cache: state.cache.clone(),
        chunk_ttl_hours: state.cfg.cache.chunk_ttl_hours,
        content_type,
    };

    let task_job = job.clone();
    let active_map = state.active.clone();
    let task_key = key.clone();
    tokio::spawn(async move {
        let result = run_download(plan, task_job.clone()).await;
        match &result {
            Ok(()) => task_job.finish_ok(),
            Err(err) => {
                warn!(key = %task_key, error = %err, "download failed");
                task_job.finish_err(err.to_string());
            }
        }
        active_map.lock().await.remove(&task_key);
        drop(permit);
        drop(sequential_permit);
    });

    serve_job(state, job).await
}

fn auth_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    if let Some(value) = headers.get(AUTHORIZATION) {
        out.insert(AUTHORIZATION, value.clone());
    }
    out
}

async fn acquire_slot(
    semaphore: &Arc<Semaphore>,
    timeout_secs: u64,
) -> Option<OwnedSemaphorePermit> {
    if let Ok(permit) = semaphore.clone().try_acquire_owned() {
        return Some(permit);
    }
    match tokio::time::timeout(
        Duration::from_secs(timeout_secs.max(1)),
        semaphore.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => Some(permit),
        _ => None,
    }
}

fn busy_response(retry_after: u64) -> Response<Body> {
    let mut response = proxy::error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "all download slots are busy, please retry shortly",
    );
    if let Ok(value) = HeaderValue::from_str(&retry_after.max(1).to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

async fn proxy_fallback(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    target: &Url,
    rule: &Rule,
    method: &Method,
) -> Response<Body> {
    // A GET carries no body worth forwarding, so an empty one is rebuilt here.
    let ctx = ProxyRequest {
        client: &state.client,
        meta_client: &state.meta_client,
        cfg: &state.cfg,
        rule: Some(rule),
        method,
        headers,
        target,
    };
    proxy::proxy_request(ctx, Body::empty()).await
}

async fn ensure_scratch(path: &Path, total: u64) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.len() == total => Ok(false),
        _ => {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .await?;
            file.set_len(total).await?;
            Ok(true)
        }
    }
}

async fn serve_job(state: &Arc<AppState>, job: Arc<DownloadJob>) -> Response<Body> {
    match tokio::fs::File::open(&job.path).await {
        Ok(file) => {
            state.bytes_served.fetch_add(job.total, Ordering::Relaxed);
            job_response(job, file)
        }
        Err(err) => {
            // The job may have just finished and moved the file into the cache.
            debug!(key = %job.key, error = %err, "scratch file vanished, checking the cache");
            if let Some(cached) = state.cache.get(&job.key) {
                state.cache_hits.fetch_add(1, Ordering::Relaxed);
                state.bytes_served.fetch_add(cached.size, Ordering::Relaxed);
                let content_type = if cached.content_type.is_empty() {
                    job.content_type.clone()
                } else {
                    cached.content_type.clone()
                };
                return proxy::serve_file(&cached.path, cached.size, &content_type).await;
            }
            proxy::error_response(
                StatusCode::BAD_GATEWAY,
                format!("download is not available: {err}"),
            )
        }
    }
}

fn job_response(job: Arc<DownloadJob>, file: tokio::fs::File) -> Response<Body> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_LENGTH, HeaderValue::from(job.total));
    if let Some(value) = header_value(&job.content_type) {
        headers.insert(CONTENT_TYPE, value);
    }
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    let stream = stream_job(job.clone(), file, job.subscribe());
    proxy::build(StatusCode::OK, headers, Body::from_stream(stream))
}
