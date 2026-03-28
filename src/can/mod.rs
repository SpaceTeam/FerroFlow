//! Contains code related to sending/receiving CAN messages.mod can_thread;

use std::{
    sync::{Arc, mpsc},
    thread::Scope,
};

use anyhow::{Context, Result, bail, ensure};
use liquidcan::{CanMessage, CanMessageId, NODE_ID_BROADCAST, NODE_ID_INVALID, NODE_ID_SERVER};
use socketcan::{CanAnyFrame, CanFdFrame, CanFdSocket, EmbeddedFrame, Frame, Socket, StandardId};

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
        match event {
            events::Event::SendCanMessage {
                receiver_node_id,
                message,
            } => {
                for (interface, socket) in &sockets {
                    let mut frame: CanFdFrame = (&message).into();

                    let id = CanMessageId::new()
                        .with_receiver_id(receiver_node_id)
                        .with_sender_id(NODE_ID_SERVER);
                    let Some(can_id) = StandardId::new(id.into()) else {
                        eprintln!(
                            "Failed to convert CAN message ID to standard CAN ID for interface {interface}, message ID: {:#010x}",
                            u16::from(id)
                        );
                        continue;
                    };
                    frame.set_id(can_id);

                    let frame = CanAnyFrame::Fd(frame);

                    if let Err(error) = send_frame(interface, socket, &frame) {
                        eprintln!("CAN send thread error on {interface}: {error:#}");
                    }
                }
            }
            events::Event::RelayCanMessage {
                from_interface,
                frame,
            } => {
                for (interface, socket) in &sockets {
                    if *interface == from_interface {
                        continue; // Don't send back to the sender
                    }
                    if let Err(error) = send_frame(interface, socket, &frame) {
                        eprintln!("CAN send thread error on {interface}: {error:#}");
                    }
                }
            }
            _ => continue,
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

    let CanAnyFrame::Fd(frame) = frame else {
        anyhow::bail!(
            "received non-FD CAN frame on interface {}, {:?}",
            interface,
            frame
        );
    };

    let raw_id = match frame.id() {
        socketcan::Id::Standard(id) => id.as_raw(),
        socketcan::Id::Extended(id) => id.standard_id().as_raw(),
    };
    let message_id: liquidcan::CanMessageId = raw_id.into();

    if message_id.receiver_id() == NODE_ID_INVALID {
        bail!(
            "received CAN message with invalid receiver ID on interface {}, id: {raw_id:#010x}",
            interface
        );
    }

    if message_id.receiver_id() == NODE_ID_BROADCAST || message_id.receiver_id() == NODE_ID_SERVER {
        let message = CanMessage::try_from(frame).with_context(|| {
            format!(
                "failed to parse CAN frame into CanMessage for node {}",
                message_id.sender_id()
            )
        })?;

        event_dispatcher.dispatch(Event::CanMessageReceived {
            id: message_id,
            message,
        });
    } else {
        // broadcast this to all other interfaces.
        event_dispatcher.dispatch(Event::RelayCanMessage {
            from_interface: interface.to_string(),
            frame: CanAnyFrame::Fd(frame),
        });
    }
    Ok(())
}

fn send_frame(interface: &str, socket: &CanFdSocket, frame: &CanAnyFrame) -> Result<()> {
    socket
        .write_frame(frame)
        .with_context(|| format!("failed to send CAN frame on interface {}", interface))?;
    Ok(())
}
