//! Contains code related to sending/receiving CAN messages.mod can_thread;

use std::{
    sync::{Arc, mpsc},
    thread::{self, Scope},
};

use anyhow::{Context, Result, ensure};
use socketcan::{CanAnyFrame, CanFdSocket, Socket};

use crate::events::{self, Event, EventDispatcher};

pub fn spawn_can_threads<'a>(
    interfaces: &'a [&'a str],
    event_dispatcher: &'a EventDispatcher,
    scope: &'a Scope<'a, '_>,
) -> Result<()> {
    ensure!(
        !interfaces.is_empty(),
        "at least one CAN interface is required"
    );

    let sockets = interfaces
        .iter()
        .map(|&interface| {
            let socket = Arc::new(CanFdSocket::open(interface).with_context(|| {
                format!("failed to open can fd socket for interface {}", interface)
            })?);
            Ok((interface, socket))
        })
        .collect::<Result<Vec<_>>>()?;

    for (interface, socket) in &sockets {
        let interface = *interface;
        let socket = Arc::clone(socket);
        scope.spawn(move || can_recv_thread(interface, socket, event_dispatcher));
    }

    scope.spawn(move || can_send_thread(sockets, event_dispatcher));

    Ok(())
}

fn can_recv_thread(interface: &str, socket: Arc<CanFdSocket>, event_dispatcher: &EventDispatcher) {
    loop {
        if let Err(error) = receive_frame(interface, &socket, event_dispatcher) {
            eprintln!("CAN receive thread error on {interface}: {error:#}");
        }
    }
}

fn can_send_thread(sockets: Vec<(&str, Arc<CanFdSocket>)>, event_dispatcher: &EventDispatcher) {
    let (sender, receiver) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(sender);

    while let Ok(event) = receiver.recv() {
        let events::Event::SendCanMessage(frame) = event else {
            continue;
        };

        for (interface, socket) in &sockets {
            if let Err(error) = send_frame(interface, socket, &frame) {
                eprintln!("CAN send thread error on {interface}: {error:#}");
            }
        }
    }
}

fn receive_frame(
    interface: &str,
    socket: &CanFdSocket,
    event_dispatcher: &EventDispatcher,
) -> Result<()> {
    let frame = socket
        .read_frame()
        .with_context(|| format!("failed to read CAN frame on interface {}", interface))?;
    event_dispatcher.dispatch(Event::CanMessageReceived(frame));
    Ok(())
}

fn send_frame(interface: &str, socket: &CanFdSocket, frame: &CanAnyFrame) -> Result<()> {
    socket
        .write_frame(frame)
        .with_context(|| format!("failed to send CAN frame on interface {}", interface))?;
    Ok(())
}
