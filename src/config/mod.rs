//! Handles parsing and storing the configuration of FerroFlow

use anyhow::{Context, Result};
use config as config_builder;
use serde::{Deserialize, Serialize};

const DEFAULT_HEARTBEAT_BACKOFF_MULTIPLIER: u32 = 2;
const DEFAULT_HEARTBEAT_MAX_PERIOD: u32 = 60;
const DEFAULT_HEARTBEAT_MAX_UNANSWERED: u32 = 5;

#[derive(Deserialize, Serialize, Debug)]
pub struct HeartbeatConfig {
    pub period: u32,
    #[serde(default = "default_heartbeat_backoff_multiplier")]
    pub backoff_multiplier: u32,
    #[serde(default = "default_heartbeat_max_period")]
    pub max_period: u32,
    #[serde(default = "default_heartbeat_max_unanswered")]
    pub max_unanswered: u32,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub can_bus_interfaces: Vec<String>,
    pub heartbeat: HeartbeatConfig,
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

fn default_heartbeat_backoff_multiplier() -> u32 {
    DEFAULT_HEARTBEAT_BACKOFF_MULTIPLIER
}

fn default_heartbeat_max_period() -> u32 {
    DEFAULT_HEARTBEAT_MAX_PERIOD
}

fn default_heartbeat_max_unanswered() -> u32 {
    DEFAULT_HEARTBEAT_MAX_UNANSWERED
}
