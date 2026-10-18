//! Pluggable request handlers for upstreams that need more than plain proxying.

pub mod docker;

use axum::body::Body;
use axum::http::{HeaderMap, Method, Response};
use reqwest::Client;

use crate::config::Config;
use crate::router::{HandlerKind, Rule};

pub struct HandlerCtx<'a> {
    pub client: &'a Client,
    pub cfg: &'a Config,
    pub rule: &'a Rule,
    /// Request path without the leading slash, e.g. `v2/library/nginx/manifests/latest`.
    pub full_path: &'a str,
    pub query: Option<&'a str>,
    pub method: &'a Method,
    pub headers: &'a HeaderMap,
}

/// Returns `Some(response)` when the handler fully answered the request.
pub async fn exec(kind: HandlerKind, ctx: &HandlerCtx<'_>) -> Option<Response<Body>> {
    match kind {
        HandlerKind::Docker => docker::exec(ctx).await,
    }
}

/// Stream an upstream response back to the client unchanged.
pub(crate) fn passthrough(response: reqwest::Response) -> Response<Body> {
    let status = response.status();
    let headers = crate::proxy::forward_response_headers(response.headers());
    crate::proxy::build(status, headers, Body::from_stream(response.bytes_stream()))
}

/// Extract a query parameter without pulling in a full URL parser.
pub(crate) fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    let query = query?;
    for pair in query.split('&') {
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        if key == name {
            return Some(percent_decode(value));
        }
    }
    None
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(value) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(value);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_params_are_extracted() {
        assert_eq!(
            query_param(
                Some("scope=repository:library/nginx:pull&service=registry"),
                "scope"
            )
            .as_deref(),
            Some("repository:library/nginx:pull")
        );
        assert_eq!(query_param(Some("a=1&b=2"), "b").as_deref(), Some("2"));
        assert_eq!(query_param(Some("a=1"), "c"), None);
        assert_eq!(query_param(None, "a"), None);
    }

    #[test]
    fn percent_decoding_works() {
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("a+b"), "a b");
        assert_eq!(percent_decode("%zz"), "%zz");
    }
}
