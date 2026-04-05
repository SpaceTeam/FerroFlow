//! Contains code for managing the CAN nodes that are connected to FerroFlow, their fields and data types.

mod can_node;
mod node_manager;

use std::{
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, Instant},
};

pub use node_manager::NodeManager;

use crate::events;

pub fn spawn_can_msg_handler_thread<'a>(
    node_manager: &'a NodeManager<'a>,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a std::thread::Scope<'a, '_>,
) {
    let (tx, rx) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(tx, "Can message handler thread");
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

pub fn spawn_heartbeat_thread<'a>(
    node_manager: &'a NodeManager<'a>,
    interval: Duration,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a std::thread::Scope<'a, '_>,
) {
    let (tx, rx) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(tx, "Heartbeat thread");

    scope.spawn(move || {
        if let Err(error) = node_manager.dispatch_heartbeat_requests() {
            eprintln!("Error dispatching heartbeat requests: {error:#}");
        }
        let mut next_heartbeat_at = Instant::now() + interval;

        loop {
            match rx.recv_timeout(next_heartbeat_at - Instant::now()) {
                Ok(events::Event::Shutdown) => break,
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(error) = node_manager.dispatch_heartbeat_requests() {
                        eprintln!("Error dispatching heartbeat requests: {error:#}");
                    }

                    next_heartbeat_at += interval;

                    // edge case: if next_heartbeat_at is already in the past, skip to now.
                    if next_heartbeat_at < Instant::now() {
                        next_heartbeat_at = Instant::now();
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    });
}
