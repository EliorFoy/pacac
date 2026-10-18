//! Command line interface.
//!
//! Every flag overrides the corresponding `config.yaml` value, so the tool can
//! be driven entirely from the command line when no configuration file is used.

use std::path::PathBuf;

use clap::Parser;

use crate::config::{Config, Strategy};

const EXAMPLES: &str = "\
EXAMPLES:
  pacac                                     local only, port 8081
  pacac -p 9000                             another port
  pacac --lan                               accept connections from the LAN
  pacac --proxy system                      use the OS proxy as upstream
  pacac -j 32 --max-downloads 200          32 chunks per file, 200 files at once
  pacac --chunk-size 8MB --cache-size 50    8 MiB chunks, 50 GiB cache
  pacac --no-cache                          disable the disk cache
  pacac --config my.yaml --check-config     validate a config and exit

ENVIRONMENT:
  PACAC_CONFIG      configuration file
  PACAC_PORT        listen port
  PACAC_LOG_LEVEL   log level
";

#[derive(Debug, Parser)]
#[command(
    name = "pacac",
    version,
    about = "Package Accelerator & Cache - parallel download mirror for PyPI, Docker Hub, HuggingFace, CRAN and any HTTP source",
    after_help = EXAMPLES,
    disable_help_subcommand = true
)]
pub struct Cli {
    // ------------------------------------------------------------- general
    /// Configuration file (default: ./config.yaml, next to the exe, then the user config dir)
    #[arg(short, long, env = "PACAC_CONFIG", value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Log level: trace, debug, info, warn, error
    #[arg(long, env = "PACAC_LOG_LEVEL", value_name = "LEVEL")]
    pub log_level: Option<String>,

    /// More verbose output (same as --log-level debug)
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Only warnings and errors, no banner
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Validate the configuration and exit
    #[arg(long)]
    pub check_config: bool,

    /// Print the effective configuration and exit
    #[arg(long)]
    pub print_config: bool,

    // -------------------------------------------------------------- network
    /// Listen port
    #[arg(short = 'p', long, env = "PACAC_PORT", value_name = "PORT", value_parser = parse_port)]
    pub port: Option<u16>,

    /// Listen address, overrides --lan/--local-only and the config file
    #[arg(long, value_name = "ADDR")]
    pub host: Option<String>,

    /// Accept connections from other machines on the LAN (bind 0.0.0.0)
    #[arg(long, conflicts_with = "local_only")]
    pub lan: bool,

    /// Only accept connections from this machine (bind 127.0.0.1)
    #[arg(long, conflicts_with = "lan")]
    pub local_only: bool,

    /// Address clients use to reach this server, e.g. 192.168.1.10:8081
    #[arg(long, value_name = "HOST:PORT")]
    pub public_host: Option<String>,

    // ------------------------------------------------------------- upstream
    /// Upstream proxy: a URL, "system" for the OS proxy, or "none" for direct
    #[arg(long, value_name = "URL|system|none", conflicts_with = "no_proxy")]
    pub proxy: Option<String>,

    /// Connect directly, ignoring any configured upstream proxy
    #[arg(long)]
    pub no_proxy: bool,

    // ------------------------------------------------------------ downloads
    /// Parallel chunks per file (overrides every parallel rule)
    #[arg(short = 'j', long, value_name = "N", value_parser = parse_count)]
    pub concurrency: Option<usize>,

    /// Bytes per range request, e.g. 5MB, 1048576 or 0 for automatic
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    pub chunk_size: Option<i64>,

    /// Maximum number of files downloaded at the same time
    #[arg(long, value_name = "N", value_parser = parse_count)]
    pub max_downloads: Option<usize>,

    /// How many `chunk_size: 0` (auto mode) downloads may run at once
    #[arg(long, value_name = "N", value_parser = parse_count)]
    pub sequential: Option<usize>,

    // ---------------------------------------------------------------- cache
    /// Cache directory
    #[arg(long, value_name = "DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Maximum cache size in GiB
    #[arg(long, value_name = "GB", value_parser = parse_positive_f64)]
    pub cache_size: Option<f64>,

    /// Do not read or write the download cache
    #[arg(long)]
    pub no_cache: bool,
}

impl Cli {
    /// Apply the command line overrides on top of the loaded configuration.
    pub fn apply(&self, cfg: &mut Config) {
        // --- listen address -------------------------------------------------
        if let Some(port) = self.port {
            cfg.server.port = port;
        }
        if let Some(host) = self.host.as_deref() {
            cfg.server.host = host.trim().to_string();
        } else if self.lan {
            cfg.server.host = "0.0.0.0".to_string();
        } else if self.local_only {
            cfg.server.host = "127.0.0.1".to_string();
        }

        // --- public address -------------------------------------------------
        if let Some(public) = self.public_host.as_deref() {
            cfg.server.public_host = Some(public.trim().to_string());
        } else if self.lan || is_wildcard(&cfg.server.host) {
            // Clients on other machines must not be told to use 127.0.0.1.
            if let Some(ip) = crate::util::local_ipv4() {
                cfg.server.public_host = Some(format!("{ip}:{}", cfg.server.port));
            }
        }

        // --- upstream proxy -------------------------------------------------
        if self.no_proxy {
            cfg.server.upstream_proxy = Some(String::new());
        } else if let Some(proxy) = self.proxy.as_deref() {
            cfg.server.upstream_proxy = Some(normalize_proxy(proxy));
        }

        // --- download tuning ------------------------------------------------
        for rule in cfg.rules.iter_mut() {
            if rule.strategy != Strategy::Parallel {
                continue;
            }
            if let Some(concurrency) = self.concurrency {
                rule.concurrency = concurrency;
            }
            if let Some(chunk_size) = self.chunk_size {
                rule.chunk_size = chunk_size;
            }
        }
        if let Some(max) = self.max_downloads {
            cfg.server.max_concurrent_downloads = max;
        }
        if let Some(sequential) = self.sequential {
            cfg.server.sequential_downloads = sequential;
        }

        // --- cache ----------------------------------------------------------
        if let Some(dir) = self.cache_dir.as_ref() {
            cfg.cache.dir = Some(dir.to_string_lossy().into_owned());
        }
        if let Some(gb) = self.cache_size {
            cfg.cache.max_size_gb = gb;
        }
        if self.no_cache {
            cfg.cache.enabled = false;
        }

        // --- logging --------------------------------------------------------
        if self.quiet {
            cfg.logging.level = "warn".to_string();
        } else if self.verbose {
            cfg.logging.level = "debug".to_string();
        }
        if let Some(level) = self.log_level.as_deref() {
            cfg.logging.level = level.trim().to_string();
        }
    }

    pub fn banner_enabled(&self) -> bool {
        !self.quiet && !self.print_config && !self.check_config
    }
}

fn is_wildcard(host: &str) -> bool {
    matches!(host.trim(), "0.0.0.0" | "::" | "[::]" | "*")
}

fn normalize_proxy(raw: &str) -> String {
    let value = raw.trim();
    if value.eq_ignore_ascii_case("none") || value.eq_ignore_ascii_case("off") {
        String::new()
    } else if value.eq_ignore_ascii_case("system") {
        "system".to_string()
    } else if value.contains("://") {
        value.to_string()
    } else {
        // `--proxy 127.0.0.1:7890` is a common shorthand.
        format!("http://{value}")
    }
}

/// Parse sizes such as `5MB`, `512k`, `1.5GiB`, `1048576`, `auto` or `0`.
///
/// All suffixes are binary (1 KB = 1024 B), which is what chunk sizes mean in
/// practice.
pub fn parse_size(raw: &str) -> Result<i64, String> {
    let text = raw.trim().to_ascii_lowercase();
    if text.is_empty() {
        return Err("size must not be empty".to_string());
    }
    if text == "auto" {
        return Ok(0);
    }

    let (digits, multiplier) = split_unit(&text)?;
    let value: f64 = digits
        .parse()
        .map_err(|_| format!("invalid size {raw:?} (try 5MB, 512k or 0)"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("invalid size {raw:?}: must be positive"));
    }

    let bytes = value * multiplier;
    if bytes >= i64::MAX as f64 {
        return Err(format!("size {raw:?} is too large"));
    }
    let bytes = bytes as i64;
    if bytes == 0 && value > 0.0 {
        return Err(format!("size {raw:?} is smaller than one byte"));
    }
    Ok(bytes)
}

fn split_unit(text: &str) -> Result<(&str, f64), String> {
    const UNITS: [(&str, f64); 10] = [
        ("kib", 1024.0),
        ("kb", 1024.0),
        ("k", 1024.0),
        ("mib", 1024.0 * 1024.0),
        ("mb", 1024.0 * 1024.0),
        ("m", 1024.0 * 1024.0),
        ("gib", 1024.0 * 1024.0 * 1024.0),
        ("gb", 1024.0 * 1024.0 * 1024.0),
        ("g", 1024.0 * 1024.0 * 1024.0),
        ("b", 1.0),
    ];
    for (suffix, multiplier) in UNITS {
        if let Some(digits) = text.strip_suffix(suffix) {
            return Ok((digits, multiplier));
        }
    }
    Ok((text, 1.0))
}

fn parse_port(raw: &str) -> Result<u16, String> {
    let port: u32 = raw
        .trim()
        .parse()
        .map_err(|_| format!("invalid port {raw:?}"))?;
    if !(1..=65535).contains(&port) {
        return Err("port must be between 1 and 65535".to_string());
    }
    Ok(port as u16)
}

fn parse_count(raw: &str) -> Result<usize, String> {
    let value: usize = raw
        .trim()
        .parse()
        .map_err(|_| format!("invalid number {raw:?}"))?;
    if value == 0 {
        return Err("value must be at least 1".to_string());
    }
    Ok(value)
}

fn parse_positive_f64(raw: &str) -> Result<f64, String> {
    let value: f64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("invalid number {raw:?}"))?;
    if !value.is_finite() || value <= 0.0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuleConfig;

    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("pacac").chain(args.iter().copied()))
    }

    fn config() -> Config {
        Config {
            rules: vec![
                RuleConfig {
                    name: "parallel".into(),
                    upstream: "https://example.com".into(),
                    strategy: Strategy::Parallel,
                    concurrency: 4,
                    chunk_size: 1024,
                    ..Default::default()
                },
                RuleConfig {
                    name: "proxy".into(),
                    upstream: "https://example.com".into(),
                    strategy: Strategy::Proxy,
                    concurrency: 7,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn parses_sizes() {
        assert_eq!(parse_size("0"), Ok(0));
        assert_eq!(parse_size("auto"), Ok(0));
        assert_eq!(parse_size("1024"), Ok(1024));
        assert_eq!(parse_size("5MB"), Ok(5 * 1024 * 1024));
        assert_eq!(parse_size("5m"), Ok(5 * 1024 * 1024));
        assert_eq!(
            parse_size("1.5GiB"),
            Ok((1.5 * 1024.0 * 1024.0 * 1024.0) as i64)
        );
        assert_eq!(parse_size("512k"), Ok(512 * 1024));
        assert!(parse_size("abc").is_err());
        assert!(parse_size("-1").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn port_and_lan_flags_are_applied() {
        let mut cfg = config();
        cli(&["-p", "9000", "--lan"]).apply(&mut cfg);
        assert_eq!(cfg.server.port, 9000);
        assert_eq!(cfg.server.host, "0.0.0.0");
    }

    #[test]
    fn local_only_binds_loopback() {
        let mut cfg = config();
        cfg.server.host = "0.0.0.0".into();
        cli(&["--local-only"]).apply(&mut cfg);
        assert_eq!(cfg.server.host, "127.0.0.1");
    }

    #[test]
    fn explicit_host_wins_over_lan() {
        let mut cfg = config();
        cli(&["--lan", "--host", "10.0.0.5"]).apply(&mut cfg);
        assert_eq!(cfg.server.host, "10.0.0.5");
    }

    #[test]
    fn proxy_forms_are_normalized() {
        let mut cfg = config();
        cli(&["--proxy", "system"]).apply(&mut cfg);
        assert_eq!(cfg.server.upstream_proxy.as_deref(), Some("system"));

        cli(&["--proxy", "127.0.0.1:7890"]).apply(&mut cfg);
        assert_eq!(
            cfg.server.upstream_proxy.as_deref(),
            Some("http://127.0.0.1:7890")
        );

        cli(&["--proxy", "none"]).apply(&mut cfg);
        assert_eq!(cfg.server.upstream_proxy.as_deref(), Some(""));

        cli(&["--proxy", "socks5://127.0.0.1:1080"]).apply(&mut cfg);
        assert_eq!(
            cfg.server.upstream_proxy.as_deref(),
            Some("socks5://127.0.0.1:1080")
        );

        cli(&["--no-proxy"]).apply(&mut cfg);
        assert_eq!(cfg.server.upstream_proxy.as_deref(), Some(""));
    }

    #[test]
    fn download_flags_only_touch_parallel_rules() {
        let mut cfg = config();
        cli(&["-j", "32", "--chunk-size", "8MB", "--max-downloads", "200"]).apply(&mut cfg);
        assert_eq!(cfg.rules[0].concurrency, 32);
        assert_eq!(cfg.rules[0].chunk_size, 8 * 1024 * 1024);
        assert_eq!(cfg.rules[1].concurrency, 7, "proxy rules keep their values");
        assert_eq!(cfg.server.max_concurrent_downloads, 200);
    }

    #[test]
    fn cache_flags_are_applied() {
        let mut cfg = config();
        cli(&["--cache-size", "12.5", "--no-cache"]).apply(&mut cfg);
        assert_eq!(cfg.cache.max_size_gb, 12.5);
        assert!(!cfg.cache.enabled);
    }

    #[test]
    fn quiet_and_verbose_control_the_log_level() {
        let mut cfg = config();
        cli(&["--verbose"]).apply(&mut cfg);
        assert_eq!(cfg.logging.level, "debug");

        cli(&["--quiet"]).apply(&mut cfg);
        assert_eq!(cfg.logging.level, "warn");

        cli(&["--verbose", "--log-level", "trace"]).apply(&mut cfg);
        assert_eq!(cfg.logging.level, "trace");
    }

    #[test]
    fn conflicting_flags_are_rejected() {
        assert!(Cli::try_parse_from(["pacac", "--lan", "--local-only"]).is_err());
        assert!(Cli::try_parse_from(["pacac", "--proxy", "x", "--no-proxy"]).is_err());
        assert!(Cli::try_parse_from(["pacac", "-p", "0"]).is_err());
        assert!(Cli::try_parse_from(["pacac", "-j", "0"]).is_err());
    }
}
