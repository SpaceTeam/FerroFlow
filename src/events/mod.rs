//! Events that are dispatched by one module and listened to by another.

use std::sync::{RwLock, mpsc::Sender};

use liquidcan::{CanMessage, CanMessageId};
use socketcan::CanAnyFrame;

#[derive(Clone, Debug)]
pub enum Event {
    CanMessageReceived {
        id: CanMessageId,
        message: CanMessage,
    },
    NodeFieldUpdated(crate::db::FieldLog),
    Shutdown,
    #[allow(unused)]
    SendCanMessage {
        receiver_node_id: u8,
        message: CanMessage,
    },
    RelayCanMessage {
        from_interface: String,
        frame: CanAnyFrame,
    },
}

struct EventListener {
    debug_name: String,
    sender: Sender<Event>,
}

pub struct EventDispatcher {
    listeners: RwLock<Vec<EventListener>>,
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
        }
    }

    pub fn subscribe(&self, listener: Sender<Event>, debug_name: impl Into<String>) {
        self.listeners.write().unwrap().push(EventListener {
            debug_name: debug_name.into(),
            sender: listener,
        });
    }

    pub fn dispatch(&self, event: Event) {
        for listener in self.listeners.read().unwrap().iter() {
            if let Err(e) = listener.sender.send(event.clone()) {
                eprintln!(
                    "Failed to send event to listener {}: {e}. Event content: {:#?}",
                    listener.debug_name, event
                );
            }
        }
    }
}
