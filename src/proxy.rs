//! Forwarding requests upstream and rewriting responses.

use std::path::Path;

use axum::body::Body;
use axum::http::header::{
    HeaderName, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HOST,
    TRANSFER_ENCODING,
};
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use reqwest::Client;
use tracing::{debug, warn};
use url::Url;

use crate::config::Config;
use crate::router::Rule;
use crate::util::{copy_headers, header_value, is_hop_by_hop};

/// Responses up to this size may be buffered in memory so their links can be
/// rewritten. Anything larger is streamed untouched.
const REWRITE_CAP: usize = 32 * 1024 * 1024;
/// Request bodies (uploads) are buffered like the original implementation.
const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;

pub struct ProxyRequest<'a> {
    pub client: &'a Client,
    /// Client that never follows redirects (used to read metadata headers).
    pub meta_client: &'a Client,
    pub cfg: &'a Config,
    pub rule: Option<&'a Rule>,
    pub method: &'a Method,
    pub headers: &'a HeaderMap,
    pub target: &'a Url,
}

/// Forward a request to `target` and stream the response back.
pub async fn proxy_request(ctx: ProxyRequest<'_>, body: Body) -> Response<Body> {
    let mut meta_headers = HeaderMap::new();

    // HEAD responses sometimes carry metadata headers that are lost when
    // following a redirect (HuggingFace sends x-repo-commit etc. from the API
    // host, then redirects to a CDN). Fetch them without following redirects.
    if *ctx.method == Method::HEAD {
        if let Some(rule) = ctx.rule {
            if !rule.head_meta_headers.is_empty() {
                let mut request = ctx.meta_client.head(ctx.target.clone());
                if let Some(auth) = ctx.headers.get(AUTHORIZATION) {
                    request = request.header(AUTHORIZATION, auth);
                }
                match request.send().await {
                    Ok(response) => {
                        for name in &rule.head_meta_headers {
                            let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
                                continue;
                            };
                            if let Some(value) = response.headers().get(&name) {
                                meta_headers.insert(name, value.clone());
                            }
                        }
                    }
                    Err(err) => debug!(error = %err, "metadata HEAD request failed"),
                }
            }
        }
    }

    let mut request = ctx.client.request(ctx.method.clone(), ctx.target.clone());
    request = request.headers(forward_request_headers(ctx.headers));

    let has_body =
        ctx.headers.contains_key(CONTENT_LENGTH) || ctx.headers.contains_key(TRANSFER_ENCODING);
    if has_body && *ctx.method != Method::HEAD {
        match axum::body::to_bytes(body, MAX_REQUEST_BODY).await {
            Ok(bytes) if !bytes.is_empty() => {
                request = request.body(bytes);
            }
            Ok(_) => {}
            Err(_) => {
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body is too large for this proxy",
                )
            }
        }
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => {
            warn!(url = %ctx.target, error = %err, "upstream request failed");
            return error_response(
                StatusCode::BAD_GATEWAY,
                format!("upstream request failed: {err}"),
            );
        }
    };

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();

    let mut out_headers = forward_response_headers(response.headers());
    for (name, value) in meta_headers {
        if let Some(name) = name {
            out_headers.insert(name, value);
        }
    }

    // Rewriting only makes sense for successful, non-empty GET responses.
    let rewrite = *ctx.method != Method::HEAD
        && status.is_success()
        && should_rewrite(&content_type, ctx.cfg);

    if !rewrite {
        let body = Body::from_stream(response.bytes_stream());
        return build(status, out_headers, body);
    }

    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut too_large = false;
    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => {
                buffer.extend_from_slice(&chunk);
                if buffer.len() > REWRITE_CAP {
                    too_large = true;
                    break;
                }
            }
            Err(err) => {
                warn!(url = %ctx.target, error = %err, "upstream body error");
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    format!("upstream body error: {err}"),
                );
            }
        }
    }

    if too_large {
        // Hand over what we already read plus the rest of the stream untouched.
        let head = Bytes::from(buffer);
        let rest = stream.map(|item| item.map_err(std::io::Error::other));
        let body = Body::from_stream(
            stream::once(async move { Ok::<Bytes, std::io::Error>(head) }).chain(rest),
        );
        out_headers.remove(CONTENT_LENGTH);
        return build(status, out_headers, body);
    }

    let rewritten = rewrite_body(buffer, ctx.cfg);
    if let Ok(value) = HeaderValue::from_str(&rewritten.len().to_string()) {
        out_headers.insert(CONTENT_LENGTH, value);
    }
    build(status, out_headers, Body::from(rewritten))
}

fn forward_request_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        let lower = name.as_str();
        if is_hop_by_hop(lower)
            || lower == HOST.as_str()
            || lower == CONTENT_LENGTH.as_str()
            || lower == ACCEPT_ENCODING.as_str()
        {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

pub(crate) fn forward_response_headers(headers: &HeaderMap) -> HeaderMap {
    // `content-encoding` is dropped because reqwest transparently decompresses
    // gzip/brotli/deflate bodies (and removes `content-length` with them).
    copy_headers(
        headers,
        &[
            "connection",
            "keep-alive",
            "transfer-encoding",
            "content-encoding",
            "upgrade",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
        ],
    )
}

pub fn should_rewrite(content_type: &str, cfg: &Config) -> bool {
    let Some(rewrite) = &cfg.content_rewrite else {
        return false;
    };
    if rewrite.targets.is_empty() || rewrite.content_types.is_empty() {
        return false;
    }
    rewrite
        .content_types
        .iter()
        .any(|candidate| !candidate.is_empty() && content_type.contains(candidate.as_str()))
}

/// Replace upstream hosts with this proxy's address so that links keep working.
pub fn rewrite_body(data: Vec<u8>, cfg: &Config) -> Bytes {
    let Some(rewrite) = &cfg.content_rewrite else {
        return Bytes::from(data);
    };
    if rewrite.targets.is_empty() {
        return Bytes::from(data);
    }

    let text = match String::from_utf8(data) {
        Ok(text) => text,
        Err(err) => return Bytes::from(err.into_bytes()),
    };

    let base = format!("http://{}", cfg.server.public_host());
    let mut text = text;
    for target in &rewrite.targets {
        let host = target.host();
        if host.is_empty() || !text.contains(host) {
            continue;
        }
        // Mirrors mounted under a path prefix need it back in the rewritten URL.
        let prefix = target.prefix();
        let replacement = if prefix.is_empty() {
            base.clone()
        } else if prefix.starts_with('/') {
            format!("{base}{prefix}")
        } else {
            format!("{base}/{prefix}")
        };
        text = text.replace(host, &replacement);
    }

    Bytes::from(text.into_bytes())
}

pub fn build(status: StatusCode, headers: HeaderMap, body: Body) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

pub fn error_response(status: StatusCode, message: impl std::fmt::Display) -> Response<Body> {
    let mut response = (status, format!("{message}\n")).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Serve a file from disk with a known length.
pub async fn serve_file(path: &Path, size: u64, content_type: &str) -> Response<Body> {
    match tokio::fs::File::open(path).await {
        Ok(file) => {
            let mut headers = HeaderMap::new();
            if let Some(value) = header_value(&size.to_string()) {
                headers.insert(CONTENT_LENGTH, value);
            }
            if !content_type.is_empty() {
                if let Some(value) = header_value(content_type) {
                    headers.insert(CONTENT_TYPE, value);
                }
            }
            headers.insert(
                axum::http::header::ACCEPT_RANGES,
                HeaderValue::from_static("bytes"),
            );
            let stream = tokio_util::io::ReaderStream::new(file);
            build(StatusCode::OK, headers, Body::from_stream(stream))
        }
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot open cached file");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cached file is no longer readable",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ContentRewrite;

    fn config() -> Config {
        let mut cfg = Config::default();
        cfg.server.port = 8081;
        cfg.content_rewrite = Some(ContentRewrite {
            content_types: vec!["text/html".into(), "application/json".into()],
            targets: vec![
                "https://files.pythonhosted.org".into(),
                "https://pypi.org".into(),
                crate::config::RewriteTarget::Prefixed {
                    host: "https://download-r2.pytorch.org".into(),
                    prefix: Some("/torch-r2".into()),
                },
            ],
        });
        cfg
    }

    #[test]
    fn rewrites_links_to_proxy_host() {
        let cfg = config();
        let input = br#"<a href="https://files.pythonhosted.org/packages/x.whl">x</a>"#.to_vec();
        let out = rewrite_body(input, &cfg);
        let text = std::str::from_utf8(&out).unwrap();
        assert!(text.contains("http://127.0.0.1:8081/packages/x.whl"));
        assert!(!text.contains("files.pythonhosted.org"));
    }

    #[test]
    fn prefixed_targets_keep_their_prefix() {
        let cfg = config();
        let input =
            br#"<a href="https://download-r2.pytorch.org/whl/cu126/torch.whl">t</a>"#.to_vec();
        let text = String::from_utf8(rewrite_body(input, &cfg).to_vec()).unwrap();
        assert_eq!(
            text,
            r#"<a href="http://127.0.0.1:8081/torch-r2/whl/cu126/torch.whl">t</a>"#
        );
    }

    #[test]
    fn leaves_binary_untouched() {
        let cfg = config();
        let input = vec![0xff, 0xfe, 0x00, 0x01];
        assert_eq!(rewrite_body(input.clone(), &cfg).as_ref(), input.as_slice());
    }

    #[test]
    fn content_type_matching_is_substring_based() {
        let cfg = config();
        assert!(should_rewrite("text/html; charset=utf-8", &cfg));
        assert!(!should_rewrite("application/vnd.pypi.simple.v1+json", &cfg));
        assert!(should_rewrite("application/json", &cfg));
        assert!(!should_rewrite("application/octet-stream", &cfg));
    }
}
