//! Configuration model.
//!
//! The YAML layout is intentionally compatible with the original Python
//! `aimirror` / `fast_proxy` `config.yaml`, so an existing config file can be
//! reused as-is. Only the *paths* default to platform specific directories when
//! they are omitted.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PORT: u16 = 8081;
pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 100;
pub const DEFAULT_SEQUENTIAL_DOWNLOADS: usize = 1;
pub const DEFAULT_CONCURRENCY: usize = 4;
pub const DEFAULT_CHUNK_SIZE: i64 = 5 * 1024 * 1024;
pub const DEFAULT_MAX_SIZE_GB: f64 = 100.0;
pub const DEFAULT_CHUNK_TTL_HOURS: u64 = 48;
pub const DEFAULT_QUEUE_TIMEOUT_SECS: u64 = 15;
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 15;
pub const DEFAULT_HEAD_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_READ_TIMEOUT_SECS: u64 = 120;

/// Used when no `config.yaml` can be found next to the binary or in the user
/// config directory.
pub const EMBEDDED_CONFIG: &str = include_str!("../config.yaml");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    /// Stream the upstream response through untouched (plus optional rewriting).
    #[default]
    Proxy,
    /// Split the file into byte ranges and download them concurrently.
    Parallel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheKeySource {
    /// Key the cache on the URL after following redirects (default).
    #[default]
    Final,
    /// Key the cache on the URL the client requested (for signed/temporary URLs).
    Original,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Upstream HTTP(S) proxy, empty string means "connect directly".
    pub upstream_proxy: Option<String>,
    /// Host:port clients use to reach this server; used when rewriting links.
    pub public_host: Option<String>,
    /// Maximum number of files being downloaded at the same time.
    pub max_concurrent_downloads: usize,
    /// Maximum number of `chunk_size: 0` (auto/sequential) downloads at once.
    pub sequential_downloads: usize,
    /// How long a request waits for a download slot before a 503 is returned.
    pub queue_timeout_secs: u64,
    pub connect_timeout_secs: u64,
    pub head_timeout_secs: u64,
    /// Abort an upstream transfer when no bytes arrive for this long.
    pub read_timeout_secs: u64,
    pub max_idle_per_host: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: DEFAULT_PORT,
            upstream_proxy: None,
            public_host: None,
            max_concurrent_downloads: DEFAULT_MAX_CONCURRENT_DOWNLOADS,
            sequential_downloads: DEFAULT_SEQUENTIAL_DOWNLOADS,
            queue_timeout_secs: DEFAULT_QUEUE_TIMEOUT_SECS,
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            head_timeout_secs: DEFAULT_HEAD_TIMEOUT_SECS,
            read_timeout_secs: DEFAULT_READ_TIMEOUT_SECS,
            max_idle_per_host: 50,
        }
    }
}

impl ServerConfig {
    pub fn public_host(&self) -> String {
        match self.public_host.as_deref() {
            Some(host) if !host.trim().is_empty() => host.trim().to_string(),
            _ => format!("127.0.0.1:{}", self.port),
        }
    }

    /// Resolve `upstream_proxy`, including the special value `"system"`.
    pub fn resolve_proxy(&self) -> Option<ProxySetting> {
        match self.upstream_proxy.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(value) if value.eq_ignore_ascii_case("system") => {
                crate::sysproxy::detect().map(|url| ProxySetting {
                    url,
                    from_system: true,
                })
            }
            Some(value) => Some(ProxySetting {
                url: value.to_string(),
                from_system: false,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxySetting {
    pub url: String,
    /// True when the proxy came from the OS configuration rather than the file.
    pub from_system: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Cache directory. `None` means the platform cache directory.
    pub dir: Option<String>,
    /// `false` turns the disk cache off (`--no-cache`).
    pub enabled: bool,
    pub max_size_gb: f64,
    pub lru_enabled: bool,
    /// How long partially downloaded chunks stay resumable, in hours.
    pub chunk_ttl_hours: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: None,
            enabled: true,
            max_size_gb: DEFAULT_MAX_SIZE_GB,
            lru_enabled: true,
            chunk_ttl_hours: DEFAULT_CHUNK_TTL_HOURS,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ContentRewrite {
    #[serde(default)]
    pub content_types: Vec<String>,
    #[serde(default)]
    pub targets: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PathRewrite {
    #[serde(default)]
    pub search: String,
    #[serde(default)]
    pub replace: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RuleConfig {
    pub name: String,
    pub upstream: String,
    pub strategy: Strategy,
    /// Regex matched against the request path (search semantics, like Python).
    pub pattern: Option<String>,
    /// Simple prefix match, e.g. `/simple`.
    pub path_prefix: Option<String>,
    /// Remove the matched prefix before appending the rest to `upstream`.
    pub strip_prefix: bool,
    /// Extra literal replacements applied to the path.
    pub path_rewrite: Vec<PathRewrite>,
    /// Files smaller than this are proxied instead of downloaded in chunks.
    pub min_size: u64,
    pub max_size: Option<u64>,
    pub concurrency: usize,
    /// Bytes per range request; `<= 0` means "split into `concurrency - 10`
    /// roughly equal parts" (the original auto mode).
    pub chunk_size: i64,
    pub cache_key_source: CacheKeySource,
    /// Named handler, e.g. `handlers.docker`.
    pub handler: Option<String>,
    /// Extra response headers to preserve when answering a HEAD request.
    pub head_meta_headers: Vec<String>,
    /// Deprecated in the original project; accepted and ignored.
    pub content_rewrite: Option<serde_yaml::Value>,
}

impl Default for RuleConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            upstream: String::new(),
            strategy: Strategy::Proxy,
            pattern: None,
            path_prefix: None,
            strip_prefix: false,
            path_rewrite: Vec::new(),
            min_size: 0,
            max_size: None,
            concurrency: DEFAULT_CONCURRENCY,
            chunk_size: DEFAULT_CHUNK_SIZE,
            cache_key_source: CacheKeySource::Final,
            handler: None,
            head_meta_headers: Vec::new(),
            content_rewrite: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    /// Optional log file; `None` logs to stdout only.
    pub file: Option<String>,
    pub ansi: Option<bool>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: None,
            ansi: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub cache: CacheConfig,
    pub content_rewrite: Option<ContentRewrite>,
    pub rules: Vec<RuleConfig>,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone)]
pub enum ConfigOrigin {
    File(PathBuf),
    Embedded,
}

impl ConfigOrigin {
    pub fn describe(&self) -> String {
        match self {
            ConfigOrigin::File(path) => path.display().to_string(),
            ConfigOrigin::Embedded => "<built-in defaults>".to_string(),
        }
    }

    pub fn base_dir(&self) -> PathBuf {
        match self {
            ConfigOrigin::File(path) => path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            ConfigOrigin::Embedded => {
                std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
            }
        }
    }
}

impl Config {
    pub fn from_yaml_str(text: &str) -> Result<Self> {
        serde_yaml::from_str(text).context("failed to parse YAML configuration")
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let mut cfg = Self::from_yaml_str(&text)
            .with_context(|| format!("invalid config file {}", path.display()))?;
        let base = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        cfg.resolve_paths(&base);
        cfg.validate()?;
        Ok(cfg)
    }

    /// Load the built-in configuration (relative paths resolve against the
    /// current working directory).
    pub fn load_embedded() -> Result<Self> {
        let mut cfg = Self::from_yaml_str(EMBEDDED_CONFIG)
            .context("built-in configuration is invalid (this is a bug)")?;
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        cfg.resolve_paths(&cwd);
        cfg.validate()?;
        Ok(cfg)
    }

    /// Resolve a config file, falling back to the built-in defaults.
    ///
    /// Search order:
    /// 1. `explicit` (CLI `--config` or `PACAC_CONFIG`)
    /// 2. `./config.yaml`, `./config.yml`, `./pacac.yaml`, `./pacac.yml`
    /// 3. `config.yaml` next to the executable (portable layout)
    /// 4. `<platform config dir>/pacac/config.yaml`
    /// 5. built-in defaults
    pub fn discover(explicit: Option<&Path>) -> Result<(Self, ConfigOrigin)> {
        if let Some(path) = explicit {
            let cfg = Self::load(path)?;
            return Ok((cfg, ConfigOrigin::File(path.to_path_buf())));
        }

        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let mut candidates: Vec<PathBuf> = ["config.yaml", "config.yml", "pacac.yaml", "pacac.yml"]
            .iter()
            .map(|name| cwd.join(name))
            .collect();
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                candidates.push(dir.join("config.yaml"));
                candidates.push(dir.join("pacac.yaml"));
            }
        }
        if let Some(dir) = dirs::config_dir() {
            candidates.push(dir.join("pacac").join("config.yaml"));
            candidates.push(dir.join("pacac").join("pacac.yaml"));
        }

        for candidate in candidates {
            if candidate.is_file() {
                let cfg = Self::load(&candidate)?;
                return Ok((cfg, ConfigOrigin::File(candidate)));
            }
        }

        Ok((Self::load_embedded()?, ConfigOrigin::Embedded))
    }

    fn resolve_paths(&mut self, base: &Path) {
        if let Some(dir) = self.cache.dir.take() {
            if !dir.trim().is_empty() {
                self.cache.dir = Some(expand_path(&dir, base).to_string_lossy().into_owned());
            }
        }
        if let Some(file) = self.logging.file.take() {
            if !file.trim().is_empty() {
                self.logging.file = Some(expand_path(&file, base).to_string_lossy().into_owned());
            }
        }
    }

    pub fn cache_dir(&self) -> PathBuf {
        match self.cache.dir.as_deref() {
            Some(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
            _ => default_cache_dir(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.server.port == 0 {
            bail!("server.port must be between 1 and 65535");
        }
        if self.server.max_concurrent_downloads == 0 {
            bail!("server.max_concurrent_downloads must be >= 1");
        }
        if self.server.sequential_downloads == 0 {
            bail!("server.sequential_downloads must be >= 1");
        }
        if self.cache.max_size_gb < 0.0 {
            bail!("cache.max_size_gb must be >= 0");
        }
        if self.rules.is_empty() {
            bail!("no rules configured: add at least a `default` rule");
        }

        for (idx, rule) in self.rules.iter().enumerate() {
            let label = if rule.name.trim().is_empty() {
                format!("rules[{idx}]")
            } else {
                format!("rules[{idx}] ({})", rule.name)
            };
            if rule.upstream.trim().is_empty() {
                bail!("{label}: `upstream` is required");
            }
            let upstream = rule.upstream.trim();
            if !(upstream.starts_with("http://") || upstream.starts_with("https://")) {
                bail!("{label}: `upstream` must start with http:// or https:// (got {upstream:?})");
            }
            if rule.concurrency == 0 {
                bail!("{label}: `concurrency` must be >= 1");
            }
            if let Some(pattern) = rule.pattern.as_deref() {
                regex::Regex::new(pattern)
                    .with_context(|| format!("{label}: invalid `pattern` regex {pattern:?}"))?;
            }
            if rule.strategy == Strategy::Parallel
                && rule.chunk_size > 0
                && rule.chunk_size < 64 * 1024
            {
                tracing::warn!(
                    rule = %rule.name,
                    chunk_size = rule.chunk_size,
                    "chunk_size is very small; this creates a lot of range requests"
                );
            }
        }

        if let Some(rewrite) = &self.content_rewrite {
            if rewrite.targets.is_empty() {
                tracing::warn!("content_rewrite.targets is empty; response rewriting is disabled");
            }
        }

        Ok(())
    }

    /// Look up a rule's upstream by name (used by the docker handler).
    pub fn rule_upstream(&self, name: &str) -> Option<&str> {
        self.rules
            .iter()
            .find(|r| r.name == name)
            .map(|r| r.upstream.trim())
    }
}

/// `<platform cache dir>/pacac/cache`.
pub fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .map(|dir| dir.join("pacac").join("cache"))
        .unwrap_or_else(|| PathBuf::from("cache"))
}

/// `<platform config dir>/pacac`.
pub fn default_config_dir() -> PathBuf {
    dirs::config_dir()
        .map(|dir| dir.join("pacac"))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Expand `~`, `${VAR}`, `$VAR` and `%VAR%`, then resolve relative paths
/// against `base`.
pub fn expand_path(raw: &str, base: &Path) -> PathBuf {
    let expanded = expand_env(raw.trim());
    let path = if let Some(rest) = expanded.strip_prefix("~") {
        let rest = rest.trim_start_matches(['/', '\\']);
        match dirs::home_dir() {
            Some(home) if rest.is_empty() => home,
            Some(home) => home.join(rest),
            None => PathBuf::from(&expanded),
        }
    } else {
        PathBuf::from(&expanded)
    };

    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn expand_env(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '$' && i + 1 < chars.len() {
            if chars[i + 1] == '{' {
                if let Some(end) = chars[i + 2..].iter().position(|&ch| ch == '}') {
                    let name: String = chars[i + 2..i + 2 + end].iter().collect();
                    if let Ok(value) = std::env::var(&name) {
                        out.push_str(&value);
                        i = i + 3 + end;
                        continue;
                    }
                }
            } else {
                let mut j = i + 1;
                while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                if j > i + 1 {
                    let name: String = chars[i + 1..j].iter().collect();
                    if let Ok(value) = std::env::var(&name) {
                        out.push_str(&value);
                        i = j;
                        continue;
                    }
                }
            }
        } else if c == '%' {
            if let Some(end) = chars[i + 1..].iter().position(|&ch| ch == '%') {
                let name: String = chars[i + 1..i + 1 + end].iter().collect();
                if !name.is_empty()
                    && name
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                {
                    if let Ok(value) = std::env::var(&name) {
                        out.push_str(&value);
                        i = i + 2 + end;
                        continue;
                    }
                }
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
server:
  host: "0.0.0.0"
  port: 9000
  upstream_proxy: ""
  public_host: "mirror.local:9000"
  max_concurrent_downloads: 42

cache:
  max_size_gb: 12.5
  lru_enabled: false
  chunk_ttl_hours: 6

content_rewrite:
  content_types: ["text/html"]
  targets: ["https://files.pythonhosted.org"]

rules:
  - name: pypi
    path_prefix: "/simple"
    upstream: "https://pypi.org"
    strategy: proxy
  - name: hf
    pattern: '/.*/(blob|resolve)/[^/]+/.+'
    upstream: "https://huggingface.co"
    strategy: parallel
    min_size: 102400
    concurrency: 8
    chunk_size: 0
    cache_key_source: original
    path_rewrite:
      - search: "/blob/"
        replace: "/resolve/"
    head_meta_headers: ["etag"]
"#;

    #[test]
    fn parses_compatible_config() {
        let cfg = Config::from_yaml_str(SAMPLE).unwrap();
        assert_eq!(cfg.server.port, 9000);
        assert_eq!(cfg.server.max_concurrent_downloads, 42);
        assert_eq!(cfg.cache.max_size_gb, 12.5);
        assert!(!cfg.cache.lru_enabled);
        assert_eq!(cfg.cache.chunk_ttl_hours, 6);
        assert_eq!(cfg.rules.len(), 2);
        assert_eq!(cfg.rules[1].strategy, Strategy::Parallel);
        assert_eq!(cfg.rules[1].cache_key_source, CacheKeySource::Original);
        assert_eq!(cfg.rules[1].concurrency, 8);
        assert_eq!(cfg.rules[1].chunk_size, 0);
        assert_eq!(cfg.rules[1].path_rewrite[0].search, "/blob/");
        assert_eq!(cfg.rules[0].concurrency, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn empty_proxy_means_none() {
        let cfg = Config::from_yaml_str(SAMPLE).unwrap();
        assert!(cfg.server.resolve_proxy().is_none());
        assert_eq!(cfg.server.public_host(), "mirror.local:9000");
    }

    #[test]
    fn validation_rejects_bad_upstream() {
        let mut cfg = Config::from_yaml_str(SAMPLE).unwrap();
        cfg.rules[0].upstream = "pypi.org".to_string();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("upstream"), "{err}");
    }

    #[test]
    fn validation_rejects_bad_regex() {
        let mut cfg = Config::from_yaml_str(SAMPLE).unwrap();
        cfg.rules[1].pattern = Some("([a-z".to_string());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("pattern"), "{err}");
    }

    #[test]
    fn validation_rejects_empty_rules() {
        let mut cfg = Config::from_yaml_str(SAMPLE).unwrap();
        cfg.rules.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn env_expansion_works() {
        std::env::set_var("PACAC_TEST_DIR", "sub/dir");
        assert_eq!(expand_env("${PACAC_TEST_DIR}/cache"), "sub/dir/cache");
        assert_eq!(expand_env("$PACAC_TEST_DIR"), "sub/dir");
        assert_eq!(expand_env("%PACAC_TEST_DIR%/x"), "sub/dir/x");
    }

    #[test]
    fn embedded_config_is_valid() {
        let cfg = Config::from_yaml_str(EMBEDDED_CONFIG).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.rules.len() >= 5);
    }
}
