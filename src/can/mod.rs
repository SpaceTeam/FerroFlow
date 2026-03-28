//! Contains code related to sending/receiving CAN messages.mod can_thread;

use std::{
    sync::{Arc, mpsc},
    thread::{self, Scope},
};

use anyhow::{Context, Result};
use socketcan::{CanAnyFrame, CanFdSocket, Socket};

use crate::events::{self, Event, EventDispatcher};

type CanThreadHandles = [thread::JoinHandle<()>; 2];

pub fn spawn_can_threads<'a>(
    interface: &'a str,
    event_dispatcher: &'a EventDispatcher,
    scope: &'a Scope<'a, '_>,
) -> Result<()> {
    let socket =
        Arc::new(CanFdSocket::open(interface).with_context(|| {
            format!("failed to open can fd socket for interface {}", interface)
        })?);

    let socket_clone = Arc::clone(&socket);
    scope.spawn(move || can_recv_thread(socket_clone, event_dispatcher));
    scope.spawn(move || can_send_thread(socket, event_dispatcher));

    Ok(())
}

fn can_recv_thread(socket: Arc<CanFdSocket>, event_dispatcher: &EventDispatcher) {
    loop {
        if let Err(error) = receive_frame(&socket, event_dispatcher) {
            eprintln!("CAN receive thread error: {error:#}");
        }
    }
}

fn can_send_thread(socket: Arc<CanFdSocket>, event_dispatcher: &EventDispatcher) {
    let (sender, receiver) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(sender);
    while let Ok(event) = receiver.recv() {
        let events::Event::SendCanMessage(frame) = event else {
            continue;
        };
        if let Err(error) = send_frame(&socket, &frame) {
            eprintln!("CAN send thread error: {error:#}");
        }
    }
}

fn receive_frame(socket: &CanFdSocket, event_dispatcher: &EventDispatcher) -> Result<()> {
    let frame = socket.read_frame().context("failed to read CAN frame")?;
    event_dispatcher.dispatch(Event::CanMessageReceived(frame));
    Ok(())
}

fn send_frame(socket: &CanFdSocket, frame: &CanAnyFrame) -> Result<()> {
    socket
        .write_frame(frame)
        .context("failed to send CAN frame")?;
    Ok(())
}
