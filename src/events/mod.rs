//! Events that are dispatched by one module and listened to by another.

use std::collections::HashSet;
use std::hash::Hash;
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

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum EventKind {
    CanMessageReceived,
    NodeFieldUpdated,
    Shutdown,
    SendCanMessage,
    RelayCanMessage,
}

impl From<Event> for EventKind {
    fn from(value: Event) -> Self {
        match value {
            Event::CanMessageReceived { .. } => EventKind::CanMessageReceived,
            Event::NodeFieldUpdated(_) => EventKind::NodeFieldUpdated,
            Event::Shutdown => EventKind::Shutdown,
            Event::SendCanMessage { .. } => EventKind::SendCanMessage,
            Event::RelayCanMessage { .. } => EventKind::RelayCanMessage,
        }
    }
}

struct EventListener {
    debug_name: String,
    sender: Sender<Event>,
    subscribed_events: HashSet<EventKind>,
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

    pub fn subscribe(
        &self,
        listener: Sender<Event>,
        subscribed_events: HashSet<EventKind>,
        debug_name: impl Into<String>,
    ) {
        self.listeners.write().unwrap().push(EventListener {
            debug_name: debug_name.into(),
            sender: listener,
            subscribed_events,
        });
    }

    pub fn dispatch(&self, event: Event) {
        for listener in self.listeners.read().unwrap().iter() {
            if !listener
                .subscribed_events
                .contains(&EventKind::from(event.clone()))
            {
                continue;
            }

            if let Err(e) = listener.sender.send(event.clone()) {
                eprintln!(
                    "Failed to send event to listener {}: {e}. Event content: {:#?}",
                    listener.debug_name, event
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Event, EventDispatcher, EventKind};
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn dispatch_only_notifies_subscribed_listeners() {
        let dispatcher = EventDispatcher::new();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let (can_tx, can_rx) = mpsc::channel();

        let mut shutdown_subscription = HashSet::new();
        shutdown_subscription.insert(EventKind::Shutdown);

        let mut can_subscription = HashSet::new();
        can_subscription.insert(EventKind::CanMessageReceived);

        dispatcher.subscribe(shutdown_tx, shutdown_subscription, "shutdown-listener");
        dispatcher.subscribe(can_tx, can_subscription, "can-listener");

        dispatcher.dispatch(Event::Shutdown);

        let received = shutdown_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("shutdown listener should receive Shutdown events");
        assert!(matches!(received, Event::Shutdown));

        let non_matching_result = can_rx.recv_timeout(Duration::from_millis(50));
        assert!(
            matches!(non_matching_result, Err(mpsc::RecvTimeoutError::Timeout)),
            "listener with non-matching subscription should not receive the event"
        );
    }

    #[test]
    fn dispatch_notifies_all_matching_listeners() {
        let dispatcher = EventDispatcher::new();
        let (listener_one_tx, listener_one_rx) = mpsc::channel();
        let (listener_two_tx, listener_two_rx) = mpsc::channel();

        let mut shutdown_subscription = HashSet::new();
        shutdown_subscription.insert(EventKind::Shutdown);

        let mut shutdown_subscription_two = HashSet::new();
        shutdown_subscription_two.insert(EventKind::Shutdown);

        dispatcher.subscribe(
            listener_one_tx,
            shutdown_subscription,
            "shutdown-listener-one",
        );
        dispatcher.subscribe(
            listener_two_tx,
            shutdown_subscription_two,
            "shutdown-listener-two",
        );

        dispatcher.dispatch(Event::Shutdown);

        let first_received = listener_one_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("first listener should receive Shutdown events");
        assert!(matches!(first_received, Event::Shutdown));

        let second_received = listener_two_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("second listener should receive Shutdown events");
        assert!(matches!(second_received, Event::Shutdown));
    }
}
