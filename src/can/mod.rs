//! Contains code related to sending/receiving CAN messages.mod can_thread;

use anyhow::{Context, Result, bail, ensure};
use liquidcan::{CanMessage, CanMessageId, NODE_ID_BROADCAST, NODE_ID_INVALID, NODE_ID_SERVER};
use socketcan::{
    CanAnyFrame, CanFdFrame, CanFdSocket, EmbeddedFrame, Frame, Socket, SocketOptions, StandardId,
};
use std::{
    sync::{Arc, OnceLock, mpsc},
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

    scope.spawn(move || can_send_thread(sockets, event_dispatcher));

    Ok(())
}

static CAN_RX_LOG_ENABLED: OnceLock<bool> = OnceLock::new();

fn can_rx_log_enabled() -> bool {
    *CAN_RX_LOG_ENABLED.get_or_init(|| {
        std::env::var("FERROFLOW_CAN_LOG")
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v.is_empty() || v == "0" || v == "false" || v == "off" || v == "no")
            })
            .unwrap_or(false)
    })
}

fn bytes_to_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    for (i, b) in data.iter().enumerate() {
        if i != 0 {
            out.push(' ');
        }
        let _ = write!(&mut out, "{:02X}", b);
    }
    out
}

fn bytes_to_bin(data: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    for (i, b) in data.iter().enumerate() {
        if i != 0 {
            out.push(' ');
        }
        let _ = write!(&mut out, "{:08b}", b);
    }
    out
}

fn log_can_rx(
    interface: &str,
    raw_id: u16,
    message_id: CanMessageId,
    data: &[u8],
    parsed: Option<&CanMessage>,
    action: &str,
) {
    if !can_rx_log_enabled() {
        return;
    }

    let data_hex = bytes_to_hex(data);
    let data_bin = bytes_to_bin(data);

    match parsed {
        Some(msg) => {
            println!(
                "CAN RX ({action}) iface={interface} can_id=0x{raw_id:03X} can_id_bin={raw_id:011b} sender={} receiver={} msg={msg:?} data_hex=[{data_hex}] data_bin=[{data_bin}]",
                message_id.sender_id(),
                message_id.receiver_id(),
            );
        }
        None => {
            println!(
                "CAN RX ({action}) iface={interface} can_id=0x{raw_id:03X} can_id_bin={raw_id:011b} sender={} receiver={} data_hex=[{data_hex}] data_bin=[{data_bin}]",
                message_id.sender_id(),
                message_id.receiver_id(),
            );
        }
    }
}

fn can_send_thread(sockets: Vec<(&str, Arc<CanFdSocket>)>, event_dispatcher: &EventDispatcher) {
    let (sender, receiver) = mpsc::channel::<events::Event>();
    let events = vec![
        EventKind::SendCanMessage,
        EventKind::RelayCanMessage,
        EventKind::Shutdown,
    ];
    event_dispatcher.subscribe(sender, events, "CAN send thread");

    while let Ok(event) = receiver.recv() {
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

    let data = frame.data().to_vec();

    // TODO: Currently broadcast messages are not relayed. We should check if any client nodes ever need to broadcast messages and if so, what other nodes they need to reach.
    if message_id.receiver_id() == NODE_ID_BROADCAST || message_id.receiver_id() == NODE_ID_SERVER {
        let message = CanMessage::try_from(&frame).with_context(|| {
            format!(
                "failed to parse CAN frame into CanMessage for node {}. Frame content hex=[{}] bin=[{}]",
                message_id.sender_id(),
                bytes_to_hex(&data),
                bytes_to_bin(&data)
            )
        })?;

        log_can_rx(
            interface,
            raw_id,
            message_id,
            &data,
            Some(&message),
            "local",
        );

        event_dispatcher.dispatch(Event::CanMessageReceived {
            id: message_id,
            message,
        });
    } else {
        log_can_rx(interface, raw_id, message_id, &data, None, "relay");

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
