#![allow(unused)] // TODO: Remove this when we have more code in place.

use anyhow::Result;

use crate::nodes::NodeManager;

mod can;
mod config;
mod db;
mod events;
mod nodes;
mod sequence;
mod socket;

fn main() -> Result<()> {
    let config = config::load_config()?;

    let mut node_manager = NodeManager::new();
    let event_dispatcher = events::EventDispatcher::new();

    std::thread::scope::<'_, _, Result<()>>(|scope| {
        can::spawn_can_threads(&["vcan0"], &event_dispatcher, scope)?;
        db::spawn_logging_worker(
            "postgres://postgres:@localhost/ferroflow".into(),
            &event_dispatcher,
            scope,
        )?;

        Ok(())
    });

    Ok(())
}
