//! Contains code for managing the CAN nodes that are connected to FerroFlow, their fields and data types.

mod can_node;
mod node_manager;

use std::sync::mpsc;

pub use node_manager::NodeManager;

use crate::events;

pub fn spawn_node_manager_thread<'a>(
    node_manager: NodeManager<'a>,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a std::thread::Scope<'a, '_>,
) {
    let (tx, rx) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(tx);
    scope.spawn(move || {
        while let Ok(event) = rx.recv() {
            if let events::Event::CanMessageReceived { id, message } = event
                && let Err(error) = node_manager.handle_can_message_from_node(id, message)
            {
                eprintln!("Error handling CAN message in NodeManager: {error:#}");
            }
        }
    });
}
