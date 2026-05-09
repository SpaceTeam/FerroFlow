//! Contains code for managing the CAN nodes that are connected to FerroFlow, their fields and data types.

mod can_node;
mod heartbeat;
mod node_manager;

pub use heartbeat::spawn_heartbeat_thread;
pub use node_manager::NodeManager;
use std::sync::mpsc;

use crate::events;
use crate::events::EventKind;

pub fn spawn_can_msg_handler_thread<'a>(
    node_manager: &'a NodeManager<'a>,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a std::thread::Scope<'a, '_>,
) {
    let (tx, rx) = mpsc::channel::<events::Event>();
    let events = vec![EventKind::Shutdown, EventKind::CanMessageReceived];
    event_dispatcher.subscribe(tx, events, "Can message handler thread");
    scope.spawn(move || {
        while let Ok(event) = rx.recv() {
            match event {
                events::Event::Shutdown => break,
                events::Event::CanMessageReceived { id, message } => {
                    if let Err(error) = node_manager.handle_can_message_from_node(id, message) {
                        eprintln!("Error handling CAN message in NodeManager: {error:#}");
                    }
                }
                _ => {}
            }
        }
    });
}
