#![allow(unused)] // TODO: Remove this when we have more code in place.

use anyhow::Result;

use crate::nodes::NodeManager;

mod can;
mod config;
mod db;
mod nodes;
mod sequence;
mod socket;

fn main() -> Result<()> {
    let config = config::load_config()?;

    let mut node_manager = NodeManager::new();

    // Start the CAN threads
    let (can_receiver, can_sender, can_thread_handles) = can::spawn_can_threads("can0")?;

    // Start the database logging worker
    let (db_sender, db_thread_handle) =
        db::spawn_logging_worker("DATABASE_URL=postgres://postgres:@localhost/ferroflow".into())?;

    loop {
        let frame = match can_receiver.recv() {
            Ok(frame) => frame,
            Err(_) => break,
        };

        if let Err(error) = node_manager.handle_can_message_from_node(frame, &db_sender) {
            eprintln!("Failed to process CAN frame: {error:#}");
        }
    }

    Ok(())
}
