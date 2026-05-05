#![allow(clippy::single_match)]
use anyhow::Result;
use ferro_flow::{config, run_with_config};

fn main() -> Result<()> {
    let config = config::load_config("config.yml")?;

    run_with_config(config)
}
