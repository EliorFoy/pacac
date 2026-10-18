use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use pacac::cli::Cli;
use pacac::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // 1. Load the configuration file (or the built-in defaults).
    let (mut cfg, origin) = Config::discover(cli.config.as_deref())?;

    // 2. Command line flags win over the file.
    cli.apply(&mut cfg);
    cfg.validate()?;

    init_logging(&cfg)?;
    tracing::info!(source = %origin.describe(), "configuration loaded");

    if cli.print_config {
        println!("# source: {}", origin.describe());
        println!("# cache dir: {}", cfg.cache_dir().display());
        print!("{}", serde_yaml::to_string(&cfg)?);
        return Ok(());
    }

    if cli.check_config {
        println!("configuration OK ({})", origin.describe());
        println!("listen:  http://{}:{}", cfg.server.host, cfg.server.port);
        println!("public:  http://{}", cfg.server.public_host());
        println!("cache:   {}", cfg.cache_dir().display());
        println!("rules:   {}", cfg.rules.len());
        return Ok(());
    }

    pacac::server::run(cfg, cli.banner_enabled()).await
}

fn init_logging(cfg: &Config) -> Result<()> {
    use tracing_subscriber::fmt;

    let level = cfg.logging.level.trim().to_lowercase();
    let level = if level.is_empty() {
        "info".to_string()
    } else {
        level
    };

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("pacac={level},warn")));

    let ansi = cfg
        .logging
        .ansi
        .unwrap_or_else(|| std::io::IsTerminal::is_terminal(&std::io::stdout()));

    let stdout_layer = fmt::layer()
        .with_ansi(ansi)
        .with_target(false)
        .with_timer(timer());

    let file_layer = match cfg.logging.file.as_deref() {
        Some(path) if !path.trim().is_empty() => {
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("cannot create the log directory {}", parent.display())
                    })?;
                }
            }
            let dir = path.parent().unwrap_or_else(|| Path::new("."));
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| "pacac.log".to_string());
            let appender = tracing_appender::rolling::never(dir, name);
            Some(
                fmt::layer()
                    .with_writer(appender)
                    .with_ansi(false)
                    .with_target(false)
                    .with_timer(timer()),
            )
        }
        _ => None,
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(())
}

/// Local-time timestamps (`2026-01-01 12:00:00.123`), like the original tool.
fn timer() -> tracing_subscriber::fmt::time::ChronoLocal {
    tracing_subscriber::fmt::time::ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f".to_string())
}
