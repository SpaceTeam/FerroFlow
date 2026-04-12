//! Handles parsing and storing the configuration of FerroFlow

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use config as config_builder;

#[derive(Deserialize,Serialize)]
pub struct Config {
    pub can_bus_interfaces: Vec<String>,
    pub heartbeat_interval: u64,
    pub database_url: String,
    }

pub fn load_config(path: &str) -> Result<Config> {
    let config = config_builder::Config::builder()
        .add_source(config::File::with_name(path))
        .build()?;

    config
        .try_deserialize()
        .with_context(|| format!("Failed to deserialize config from {}", path))
}

