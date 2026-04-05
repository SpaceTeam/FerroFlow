#![allow(clippy::single_match)]
use anyhow::Result;
use ferro_flow::{can, config, db, events, nodes};

fn main() -> Result<()> {
    let _config = config::load_config()?;

    let event_dispatcher = events::EventDispatcher::new();

    let _ = std::thread::scope::<'_, _, Result<()>>(|scope| {
        let node_manager = nodes::NodeManager::new(&event_dispatcher);
        can::spawn_can_threads(&["vcan0"], &event_dispatcher, scope)?;
        db::spawn_logging_worker(
            "postgres://postgres:@localhost/ferroflow".into(),
            &event_dispatcher,
            scope,
        )?;
        nodes::spawn_node_manager_thread(node_manager, &event_dispatcher, scope);

        Ok(())
    });

    Ok(())
}
