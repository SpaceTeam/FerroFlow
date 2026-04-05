#![allow(clippy::single_match)]
use anyhow::Result;
use ferro_flow::{can, config, db, events, nodes};

fn main() -> Result<()> {
    let _config = config::load_config()?;

    let event_dispatcher = events::EventDispatcher::new();

    let node_manager = nodes::NodeManager::new(&event_dispatcher);
    let _ = std::thread::scope::<'_, _, Result<()>>(|scope| {
        can::spawn_can_threads(&["vcan0"], &event_dispatcher, scope)?;
        db::spawn_logging_worker(
            "postgres://postgres:@localhost/ferroflow".into(),
            &event_dispatcher,
            scope,
        )?;
        nodes::spawn_can_msg_handler_thread(&node_manager, &event_dispatcher, scope);
        nodes::spawn_heartbeat_thread(
            &node_manager,
            std::time::Duration::from_secs(1),
            &event_dispatcher,
            scope,
        );

        Ok(())
    });

    Ok(())
}
