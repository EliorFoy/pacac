//! Docker Registry v2 handling.
//!
//! Docker clients expect a token dance: they call `/v2/`, receive `401` with a
//! `WWW-Authenticate` header pointing at a token service, then fetch a token and
//! retry. When the registry is reached through a mirror the token service URL
//! must point back at the mirror, otherwise clients would talk to Docker Hub
//! directly and the blobs would not be cached.
//!
//! Three things happen here:
//! * `/v2/auth` proxies the token endpoint.
//! * `/v2/` rewrites the `401` challenge to point at this proxy.
//! * `/v2/<image>/...` (no namespace) is redirected to `/v2/library/<image>/...`
//!   because Docker Hub requires the `library/` prefix.

use std::collections::HashMap;
use std::sync::OnceLock;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, LOCATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use regex::Regex;
use tracing::{debug, info, warn};
use url::Url;

use super::{passthrough, query_param, HandlerCtx};
use crate::proxy::{build, error_response};

pub async fn exec(ctx: &HandlerCtx<'_>) -> Option<Response<Body>> {
    let upstream = ctx
        .cfg
        .rule_upstream("docker-registry")
        .unwrap_or_else(|| ctx.rule.upstream())
        .trim_end_matches('/')
        .to_string();

    if ctx.full_path == "v2/auth" || ctx.full_path.starts_with("v2/auth") {
        return Some(handle_auth(ctx, &upstream).await);
    }

    if ctx.full_path == "v2/" || ctx.full_path == "v2" {
        return Some(handle_v2_root(ctx, &upstream).await);
    }

    if ctx.full_path.starts_with("v2/") {
        if let Some(response) = library_redirect(ctx) {
            return Some(response);
        }
    }

    None
}

fn authorization_header(ctx: &HandlerCtx<'_>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(value) = ctx.headers.get(AUTHORIZATION) {
        headers.insert(AUTHORIZATION, value.clone());
    }
    headers
}

fn public_base(ctx: &HandlerCtx<'_>) -> String {
    let scheme = ctx
        .headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| *value == "https")
        .unwrap_or("http");
    format!("{scheme}://{}", ctx.cfg.server.public_host())
}

/// `/v2/auth`: ask the registry what token endpoint it uses, then proxy the
/// token request with the client's `scope`.
async fn handle_auth(ctx: &HandlerCtx<'_>, upstream: &str) -> Response<Body> {
    let probe_url = format!("{upstream}/v2/");
    let auth_headers = authorization_header(ctx);

    let probe = match ctx
        .client
        .get(&probe_url)
        .headers(auth_headers.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(url = %probe_url, error = %err, "docker auth probe failed");
            return error_response(
                StatusCode::BAD_GATEWAY,
                format!("cannot reach the registry: {err}"),
            );
        }
    };

    if probe.status() != StatusCode::UNAUTHORIZED {
        return passthrough(probe);
    }

    let Some(challenge) = probe
        .headers()
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
    else {
        return passthrough(probe);
    };

    let info = parse_www_authenticate(&challenge);
    let Some(realm) = info.get("realm") else {
        return passthrough(probe);
    };
    let service = info.get("service").cloned().unwrap_or_default();

    let mut token_url = match Url::parse(realm) {
        Ok(url) => url,
        Err(err) => {
            warn!(realm = %realm, error = %err, "invalid token realm");
            return error_response(StatusCode::BAD_GATEWAY, "registry sent an invalid realm");
        }
    };

    let existing: Vec<(String, String)> = token_url
        .query_pairs()
        .filter(|(key, _)| key != "service" && key != "scope")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    token_url.set_query(None);
    {
        let mut pairs = token_url.query_pairs_mut();
        for (key, value) in existing {
            pairs.append_pair(&key, &value);
        }
        if !service.is_empty() {
            pairs.append_pair("service", &service);
        }
        if let Some(scope) = query_param(ctx.query, "scope") {
            if !scope.is_empty() {
                pairs.append_pair("scope", &scope);
            }
        }
    }

    debug!(url = %token_url, "requesting registry token");

    match ctx
        .client
        .get(token_url.clone())
        .headers(auth_headers)
        .send()
        .await
    {
        Ok(response) => passthrough(response),
        Err(err) => {
            warn!(url = %token_url, error = %err, "token request failed");
            error_response(
                StatusCode::BAD_GATEWAY,
                format!("token request failed: {err}"),
            )
        }
    }
}

/// `/v2/`: answer the authentication challenge ourselves so the client asks
/// *this* proxy for a token.
async fn handle_v2_root(ctx: &HandlerCtx<'_>, upstream: &str) -> Response<Body> {
    let probe_url = format!("{upstream}/v2/");
    let probe = match ctx
        .client
        .get(&probe_url)
        .headers(authorization_header(ctx))
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(url = %probe_url, error = %err, "docker /v2/ probe failed");
            return error_response(
                StatusCode::BAD_GATEWAY,
                format!("cannot reach the registry: {err}"),
            );
        }
    };

    if probe.status() != StatusCode::UNAUTHORIZED {
        return passthrough(probe);
    }

    let challenge = format!(
        "Bearer realm=\"{}/v2/auth\",service=\"docker-proxy\"",
        public_base(ctx)
    );
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Ok(value) = HeaderValue::from_str(&challenge) {
        headers.insert(WWW_AUTHENTICATE, value);
    }
    build(
        StatusCode::UNAUTHORIZED,
        headers,
        Body::from(r#"{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}"#),
    )
}

/// `/v2/nginx/manifests/latest` -> `/v2/library/nginx/manifests/latest`.
fn library_redirect(ctx: &HandlerCtx<'_>) -> Option<Response<Body>> {
    let parts: Vec<&str> = ctx.full_path.split('/').collect();
    if parts.len() != 4 {
        return None;
    }
    let (repo, kind, reference) = (parts[1], parts[2], parts[3]);
    if kind != "manifests" && kind != "blobs" {
        return None;
    }
    if repo.contains('/') || repo.starts_with("library") || repo.is_empty() {
        return None;
    }

    let location = format!("{}/v2/library/{repo}/{kind}/{reference}", public_base(ctx));
    info!(from = %ctx.full_path, to = %location, "redirecting official image to library/");
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&location) {
        headers.insert(LOCATION, value);
    }
    Some(build(StatusCode::MOVED_PERMANENTLY, headers, Body::empty()))
}

fn parse_www_authenticate(header: &str) -> HashMap<String, String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r#"([a-zA-Z]+)="([^"]*)""#).expect("valid regex"));
    re.captures_iter(header)
        .filter_map(|cap| {
            let key = cap.get(1)?.as_str().to_ascii_lowercase();
            let value = cap.get(2)?.as_str().to_string();
            Some((key, value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bearer_challenge() {
        let info = parse_www_authenticate(
            r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io""#,
        );
        assert_eq!(info.get("realm").unwrap(), "https://auth.docker.io/token");
        assert_eq!(info.get("service").unwrap(), "registry.docker.io");
    }

    #[test]
    fn parses_challenge_with_error_field() {
        let info = parse_www_authenticate(
            r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",error="insufficient_scope""#,
        );
        assert_eq!(info.len(), 3);
        assert_eq!(info.get("error").unwrap(), "insufficient_scope");
    }
}
