//! Rule based routing: decide which upstream a request goes to and how it is
//! fetched (`proxy` or `parallel`).

use anyhow::{Context, Result};
use regex::Regex;
use url::Url;

use crate::config::{CacheKeySource, RuleConfig, Strategy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerKind {
    /// Docker Registry v2 special handling (`/v2/auth`, 401 rewrite, ...).
    Docker,
}

impl HandlerKind {
    /// Accepts `handlers.docker`, `docker` and `handlers/docker.py`.
    pub fn parse(spec: &str) -> Option<Self> {
        let spec = spec.trim().trim_end_matches(".py");
        let last = spec.rsplit(['.', '/', '\\']).next().unwrap_or(spec);
        match last {
            "docker" => Some(HandlerKind::Docker),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub name: String,
    pub upstream: String,
    pub strategy: Strategy,
    pub min_size: u64,
    pub max_size: Option<u64>,
    pub strip_prefix: bool,
    pub concurrency: usize,
    pub chunk_size: i64,
    pub cache_key_source: CacheKeySource,
    pub handler: Option<HandlerKind>,
    /// Lowercase header names to preserve on HEAD responses.
    pub head_meta_headers: Vec<String>,
    prefix: Option<String>,
    regex: Option<Regex>,
    path_rewrite: Vec<(String, String)>,
}

impl Rule {
    pub fn from_config(cfg: &RuleConfig) -> Result<Self> {
        let regex = match cfg.pattern.as_deref() {
            Some(pattern) => Some(
                Regex::new(pattern)
                    .with_context(|| format!("rule {:?}: invalid pattern {pattern:?}", cfg.name))?,
            ),
            None => None,
        };

        let mut prefix = cfg.path_prefix.clone();
        if let Some(p) = prefix.as_mut() {
            let trimmed = p.trim();
            *p = if trimmed.starts_with('/') {
                trimmed.to_string()
            } else {
                format!("/{trimmed}")
            };
        }

        let handler = cfg.handler.as_deref().and_then(HandlerKind::parse);
        if cfg.handler.is_some() && handler.is_none() {
            tracing::warn!(
                rule = %cfg.name,
                handler = ?cfg.handler,
                "unknown handler; the rule will be handled generically"
            );
        }

        Ok(Self {
            name: cfg.name.clone(),
            upstream: cfg.upstream.trim().trim_end_matches('/').to_string(),
            strategy: cfg.strategy,
            min_size: cfg.min_size,
            max_size: cfg.max_size,
            strip_prefix: cfg.strip_prefix,
            concurrency: cfg.concurrency.max(1),
            chunk_size: cfg.chunk_size,
            cache_key_source: cfg.cache_key_source,
            handler,
            head_meta_headers: cfg
                .head_meta_headers
                .iter()
                .map(|h| h.trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .collect(),
            prefix,
            regex,
            path_rewrite: cfg
                .path_rewrite
                .iter()
                .filter(|r| !r.search.is_empty())
                .map(|r| (r.search.clone(), r.replace.clone()))
                .collect(),
        })
    }

    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    /// Returns the path to append to `upstream`, or `None` when the rule does
    /// not match.
    pub fn match_path(&self, path: &str) -> Option<String> {
        let mut matched_prefix = "";
        let matched = if let Some(prefix) = self.prefix.as_deref() {
            if prefix_matches(path, prefix) {
                matched_prefix = prefix;
                true
            } else {
                false
            }
        } else if let Some(regex) = &self.regex {
            regex.is_match(path)
        } else {
            // No matcher configured: catch-all rule.
            true
        };

        if !matched {
            return None;
        }

        let mut processed = path.to_string();
        // `strip_prefix` only applies to prefix rules, matching the original
        // implementation.
        if self.strip_prefix && !matched_prefix.is_empty() {
            let rest = &path[matched_prefix.len().min(path.len())..];
            processed = if rest.starts_with('/') {
                rest.to_string()
            } else {
                format!("/{rest}")
            };
        }
        for (search, replace) in &self.path_rewrite {
            if processed.contains(search.as_str()) {
                processed = processed.replace(search, replace);
            }
        }
        Some(processed)
    }
}

fn prefix_matches(path: &str, prefix: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    if path == prefix {
        return true;
    }
    match path.strip_prefix(prefix) {
        Some(rest) => rest.starts_with('/') || prefix.ends_with('/'),
        None => false,
    }
}

#[derive(Debug)]
pub struct Router {
    rules: Vec<Rule>,
}

impl Router {
    pub fn build(rules: &[RuleConfig]) -> Result<Self> {
        let mut built = Vec::with_capacity(rules.len());
        for rule in rules {
            built.push(Rule::from_config(rule)?);
        }
        Ok(Self { rules: built })
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    /// First matching rule wins.
    pub fn match_path(&self, path: &str) -> Option<(&Rule, String)> {
        for rule in &self.rules {
            if let Some(processed) = rule.match_path(path) {
                return Some((rule, processed));
            }
        }
        None
    }

    pub fn rule_by_name(&self, name: &str) -> Option<&Rule> {
        self.rules.iter().find(|r| r.name == name)
    }

    pub fn default_rule(&self) -> Option<&Rule> {
        self.rule_by_name("default").or_else(|| self.rules.last())
    }
}

/// Join `upstream` + path + query into an absolute URL.
pub fn build_target_url(upstream: &str, path: &str, query: Option<&str>) -> Result<Url> {
    let base = upstream.trim_end_matches('/');
    let mut raw = String::with_capacity(base.len() + path.len() + 32);
    raw.push_str(base);
    if !path.starts_with('/') {
        raw.push('/');
    }
    raw.push_str(path);
    if let Some(query) = query {
        if !query.is_empty() {
            raw.push('?');
            raw.push_str(query);
        }
    }
    Url::parse(&raw).with_context(|| format!("cannot build an upstream URL from {raw:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(prefix: Option<&str>, pattern: Option<&str>, strip: bool) -> Rule {
        Rule::from_config(&RuleConfig {
            name: "test".into(),
            upstream: "https://example.com/".into(),
            path_prefix: prefix.map(str::to_string),
            pattern: pattern.map(str::to_string),
            strip_prefix: strip,
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn prefix_requires_path_boundary() {
        let rule = rule(Some("/simple"), None, false);
        assert_eq!(
            rule.match_path("/simple/foo").as_deref(),
            Some("/simple/foo")
        );
        assert_eq!(rule.match_path("/simple").as_deref(), Some("/simple"));
        assert!(rule.match_path("/simplex/foo").is_none());
        assert!(rule.match_path("/other").is_none());
    }

    #[test]
    fn strip_prefix_rewrites_path() {
        let rule = rule(Some("/nvidia"), None, true);
        assert_eq!(
            rule.match_path("/nvidia/pkg-1.0.whl").as_deref(),
            Some("/pkg-1.0.whl")
        );
    }

    #[test]
    fn regex_rules_and_path_rewrite() {
        let mut r = rule(None, Some(r"/.*/(blob|resolve)/[^/]+/.+"), false);
        r.path_rewrite = vec![("/blob/".to_string(), "/resolve/".to_string())];
        assert_eq!(
            r.match_path("/meta-llama/Llama-2/blob/main/model.gguf")
                .as_deref(),
            Some("/meta-llama/Llama-2/resolve/main/model.gguf")
        );
        assert!(r.match_path("/api/models/foo").is_none());
    }

    #[test]
    fn catch_all_rule_matches_everything() {
        let r = rule(None, None, false);
        assert_eq!(
            r.match_path("/anything/at/all").as_deref(),
            Some("/anything/at/all")
        );
    }

    #[test]
    fn router_first_match_wins() {
        let cfg = vec![
            RuleConfig {
                name: "metadata".into(),
                upstream: "https://files.pythonhosted.org".into(),
                pattern: Some(r"/packages/.+\.metadata$".into()),
                ..Default::default()
            },
            RuleConfig {
                name: "packages".into(),
                upstream: "https://files.pythonhosted.org".into(),
                path_prefix: Some("/packages".into()),
                strategy: Strategy::Parallel,
                ..Default::default()
            },
            RuleConfig {
                name: "default".into(),
                upstream: "https://pypi.org".into(),
                ..Default::default()
            },
        ];
        let router = Router::build(&cfg).unwrap();
        let (rule, path) = router.match_path("/packages/ab/cd/pkg.whl").unwrap();
        assert_eq!(rule.name, "packages");
        assert_eq!(path, "/packages/ab/cd/pkg.whl");

        let (rule, _) = router
            .match_path("/packages/ab/cd/pkg.whl.metadata")
            .unwrap();
        assert_eq!(rule.name, "metadata");

        let (rule, _) = router.match_path("/simple/torch/").unwrap();
        assert_eq!(rule.name, "default");
    }

    #[test]
    fn target_url_keeps_query_and_strips_trailing_slash() {
        let url = build_target_url("https://example.com/", "/a/b", Some("x=1&y=2")).unwrap();
        assert_eq!(url.as_str(), "https://example.com/a/b?x=1&y=2");
        let url = build_target_url("https://example.com", "a/b", None).unwrap();
        assert_eq!(url.as_str(), "https://example.com/a/b");
    }

    #[test]
    fn handler_spec_parsing() {
        assert_eq!(
            HandlerKind::parse("handlers.docker"),
            Some(HandlerKind::Docker)
        );
        assert_eq!(HandlerKind::parse("docker"), Some(HandlerKind::Docker));
        assert_eq!(HandlerKind::parse("handlers/custom.py"), None);
    }

    #[test]
    fn path_rewrite_entries_are_applied() {
        let mut r = rule(Some("/x"), None, false);
        r.path_rewrite = vec![("/x".to_string(), "/y".to_string())];
        assert_eq!(r.match_path("/x/a").as_deref(), Some("/y/a"));
    }
}
