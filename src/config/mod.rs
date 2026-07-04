//! Handles parsing and storing the configuration of FerroFlow

use anyhow::{Context, Result};
use config as config_builder;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub can_bus_interfaces: Vec<String>,
    pub heartbeat_period: u64,
    pub database_url: String,
    pub mapping_path: String,
    #[serde(default)]
    pub webserver_socket: WebserverSocketConfig,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WebserverSocketConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub reconnect_period_ms: u64,
}

impl Default for WebserverSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "127.0.0.1".to_string(),
            port: 8080,
            reconnect_period_ms: 3000,
        }
    }
}

pub fn load_config(path: &str) -> Result<Config> {
    let config = config_builder::Config::builder()
        .add_source(config::File::with_name(path))
        .build()?;

    config
        .try_deserialize()
        .with_context(|| format!("Failed to deserialize config from {}", path))
}
