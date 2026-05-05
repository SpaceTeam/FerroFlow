//! Contains code related to sending/receiving CAN messages.mod can_thread;

use anyhow::{Context, Result, bail, ensure};
use liquidcan::{CanMessage, CanMessageId, NODE_ID_BROADCAST, NODE_ID_INVALID, NODE_ID_SERVER};
use socketcan::{
    CanAnyFrame, CanFdFrame, CanFdSocket, EmbeddedFrame, Frame, Socket, SocketOptions, StandardId,
};
use std::{
    sync::{Arc, mpsc},
    thread::Scope,
    time::Duration,
};

use crate::events::{self, Event, EventDispatcher, EventKind};

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
            let socket = CanFdSocket::open(interface).with_context(|| {
                format!("failed to open can fd socket for interface {}", interface)
            })?;

            // Make receive threads responsive to shutdown by ensuring reads don't block forever.
            // On timeout the recv thread will just loop and check for Shutdown.
            socket.set_read_timeout(Some(Duration::from_millis(50)))?;

            let socket = Arc::new(socket);
            Ok((interface, socket))
        })
        .collect::<Result<Vec<_>>>()?;

    for (interface, socket) in &sockets {
        let interface = *interface;
        let socket = Arc::clone(socket);
        socket.set_recv_own_msgs(false)?;

        // Subscribe each recv thread so it can terminate on Event::Shutdown.
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<Event>();
        let events = vec![EventKind::Shutdown];

        event_dispatcher.subscribe(
            shutdown_tx,
            events,
            format!("CAN recv thread ({interface})"),
        );

        scope.spawn(move || can_recv_thread(interface, socket, event_dispatcher, shutdown_rx));
    }

    let (sender, receiver) = mpsc::channel::<events::Event>();
    let events = vec![
        EventKind::SendCanMessage,
        EventKind::RelayCanMessage,
        EventKind::Shutdown,
    ];
    event_dispatcher.subscribe(sender, events, "CAN send thread");

    scope.spawn(move || can_send_thread(sockets, receiver));

    Ok(())
}

fn can_send_thread(
    sockets: Vec<(&str, Arc<CanFdSocket>)>,
    event_receiver: mpsc::Receiver<events::Event>,
) {
    while let Ok(event) = event_receiver.recv() {
        match event {
            Event::Shutdown => break,
            Event::SendCanMessage {
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
            Event::RelayCanMessage {
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

fn can_recv_thread(
    interface: &str,
    socket: Arc<CanFdSocket>,
    event_dispatcher: &EventDispatcher,
    shutdown_rx: mpsc::Receiver<events::Event>,
) {
    use std::sync::mpsc::TryRecvError;

    loop {
        // Check for shutdown without blocking.
        match shutdown_rx.try_recv() {
            Ok(Event::Shutdown) => break,
            Ok(_) => {}
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => break,
        }

        if let Err(error) = receive_frame(interface, &socket, event_dispatcher) {
            // With the read timeout set, timeouts are expected; `receive_frame` turns them into Ok(()).
            eprintln!("CAN receive thread error on {interface}: {error:#}");
        }
    }
}

fn receive_frame(
    interface: &str,
    socket: &CanFdSocket,
    event_dispatcher: &EventDispatcher,
) -> Result<()> {
    let frame = match socket.read_frame() {
        Ok(frame) => frame,
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            // Timeouts are ok to allow for shutdown of the receive thread.
            return Ok(());
        }
        Err(e) => {
            return Err(anyhow::Error::from(e))
                .with_context(|| format!("failed to read CAN frame on interface {}", interface));
        }
    };

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
    let message_id: CanMessageId = raw_id.into();

    if message_id.receiver_id() == NODE_ID_INVALID {
        bail!(
            "received CAN message with invalid receiver ID on interface {}, id: {raw_id:#010x}",
            interface
        );
    }

    // TODO: Currently broadcast messages are not relayed. We should check if any client nodes ever need to broadcast messages and if so, what other nodes they need to reach.
    if message_id.receiver_id() == NODE_ID_BROADCAST || message_id.receiver_id() == NODE_ID_SERVER {
        let message = CanMessage::try_from(frame).with_context(|| {
            format!(
                "failed to parse CAN frame into CanMessage for node {}. Frame content: {:02x?}",
                message_id.sender_id(),
                frame.data()
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
