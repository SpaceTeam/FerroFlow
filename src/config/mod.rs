//! Handles parsing and storing the configuration of FerroFlow

use anyhow::{Context, Result};
use config as config_builder;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub can_bus_interfaces: Vec<String>,
    pub heartbeat_period: u64,
    pub database_url: String,

    /// If set, FerroFlow will start an HTTP server on this address (e.g. "0.0.0.0:8080").
    ///
    /// This can be used to set parameters from external tools.
    #[serde(default)]
    pub http_listen: Option<String>,
}

pub fn load_config(path: &str) -> Result<Config> {
    let config = config_builder::Config::builder()
        .add_source(config::File::with_name(path))
        .build()?;

    config
        .try_deserialize()
        .with_context(|| format!("Failed to deserialize config from {}", path))
}
