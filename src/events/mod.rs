//! Events that are dispatched by one module and listened to by another.

use std::sync::{RwLock, mpsc::Sender};

use liquidcan::{CanMessage, CanMessageId};
use socketcan::CanAnyFrame;

#[derive(Clone)]
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

pub struct EventDispatcher {
    listeners: RwLock<Vec<Sender<Event>>>,
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

    pub fn subscribe(&self, listener: Sender<Event>) {
        self.listeners.write().unwrap().push(listener);
    }

    pub fn dispatch(&self, event: Event) {
        for listener in self.listeners.read().unwrap().iter() {
            let _ = listener.send(event.clone());
        }
    }
}
