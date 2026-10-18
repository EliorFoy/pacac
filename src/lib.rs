//! pacac – Package Accelerator & Cache.
//!
//! A single HTTP service that accelerates (and caches) downloads from PyPI,
//! Docker Hub, HuggingFace, CRAN and any other HTTP source.

pub mod cache;
pub mod cli;
pub mod config;
pub mod downloader;
pub mod handlers;
pub mod proxy;
pub mod router;
pub mod server;
pub mod sysproxy;
pub mod util;
