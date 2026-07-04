//! Code for managing socket connections to the frontend.

use std::{
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::mpsc::{self, TryRecvError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    config::WebserverSocketConfig,
    events::{self, Event, EventKind},
    nodes::{FieldValueSnapshot, NodeManager, NodeTelemetrySnapshot, mapping::FieldType},
};

const SOCKET_POLL_PERIOD: Duration = Duration::from_millis(50);

pub fn spawn_webserver_socket_worker<'a>(
    config: WebserverSocketConfig,
    node_manager: &'a NodeManager<'a>,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a thread::Scope<'a, '_>,
) {
    let (tx, rx) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(
        tx,
        vec![
            EventKind::Shutdown,
            EventKind::NodeFieldUpdated,
            EventKind::NodeListUpdated,
        ],
        "Webserver socket thread",
    );

    scope.spawn(move || socket_worker(config, node_manager, rx));
}

fn socket_worker(
    config: WebserverSocketConfig,
    node_manager: &NodeManager<'_>,
    rx: mpsc::Receiver<events::Event>,
) {
    let reconnect_period = Duration::from_millis(config.reconnect_period_ms);
    let address = match resolve_socket_addr(&config) {
        Ok(address) => address,
        Err(error) => {
            eprintln!("Failed to resolve webserver socket address: {error:#}");
            return;
        }
    };

    loop {
        match TcpStream::connect(address) {
            Ok(mut stream) => {
                println!("Connected to webserver socket at {address}");
                if let Err(error) = stream.set_nonblocking(true) {
                    eprintln!("Failed to configure webserver socket as nonblocking: {error:#}");
                    return;
                }

                if let Err(error) = connected_socket_worker(&mut stream, node_manager, &rx) {
                    eprintln!("Error in webserver socket worker: {error:#}");
                } else {
                    println!("Webserver socket worker exiting gracefully.");
                    return;
                }
            }
            Err(error) => {
                eprintln!("Failed to connect to webserver socket at {address}: {error:#}");
            }
        }

        match rx.recv_timeout(reconnect_period) {
            Ok(Event::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn connected_socket_worker(
    stream: &mut TcpStream,
    node_manager: &NodeManager<'_>,
    rx: &mpsc::Receiver<events::Event>,
) -> Result<()> {
    let mut msg_buf = Vec::new();
    let mut last_telemetry_timestamp = Utc::now().timestamp_millis();

    send_nodes_message(stream, node_manager)?;
    send_telemetry_message(stream, node_manager, last_telemetry_timestamp)?;

    loop {
        match rx.recv_timeout(SOCKET_POLL_PERIOD) {
            Ok(event) => {
                handle_event(stream, node_manager, event, &mut last_telemetry_timestamp)?;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }

        loop {
            match rx.try_recv() {
                Ok(Event::Shutdown) => return Ok(()),
                Ok(event) => {
                    handle_event(stream, node_manager, event, &mut last_telemetry_timestamp)?;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        while let Some(msg_len) = read_message(stream, &mut msg_buf)? {
            msg_buf.drain(..2); // remove length prefix
            handle_command(
                stream,
                node_manager,
                &msg_buf[..msg_len],
                &mut last_telemetry_timestamp,
            )?;
            msg_buf.drain(..msg_len);
        }
    }
}

fn handle_event(
    stream: &mut TcpStream,
    node_manager: &NodeManager<'_>,
    event: Event,
    last_telemetry_timestamp: &mut i64,
) -> Result<()> {
    match event {
        Event::NodeListUpdated => send_nodes_message(stream, node_manager),
        Event::NodeFieldUpdated(field_log, source) => {
            let Some(field) = node_manager
                .field_value_snapshot_by_id(field_log.node_id as u8, field_log.field_id as u8)
            else {
                return Ok(());
            };

            let node_id = field_log.node_id as u8;
            match source {
                events::NodeFieldUpdateSource::FieldGetRes => {
                    let response = FieldGetResponseContent::from_field(node_id, field);
                    send_message(stream, "field_get_response", response)
                }
                events::NodeFieldUpdateSource::ParameterSetConfirmation => {
                    // TODO
                    Ok(())
                }
                events::NodeFieldUpdateSource::TelemetryUpdate => {
                    let previous_timestamp = *last_telemetry_timestamp;
                    let timestamp = Utc::now().timestamp_millis();
                    *last_telemetry_timestamp = timestamp;

                    let node = NodeTelemetrySnapshot {
                        id: node_id,
                        name: field.node_name.clone(),
                        telemetry: vec![field],
                    };
                    send_message(
                        stream,
                        "telemetry_delta",
                        TelemetryDeltaContent {
                            prev_timestamp: previous_timestamp,
                            timestamp,
                            nodes: telemetry_nodes_from_snapshots(vec![node]),
                        },
                    )
                }
            }
        }
        _ => Ok(()),
    }
}

fn handle_command(
    stream: &mut TcpStream,
    node_manager: &NodeManager<'_>,
    message: &[u8],
    last_telemetry_timestamp: &mut i64,
) -> Result<()> {
    let incoming: IncomingMessage = serde_json::from_slice(message)
        .with_context(|| "failed to decode webserver command as JSON")?;

    match incoming.message_type.as_str() {
        "set_parameter" => {
            let command: SetParameterCommand = serde_json::from_value(incoming.content)
                .with_context(|| "invalid set_parameter command content")?;
            match command.field {
                FieldReferenceCommand::Mapped { name } => {
                    let value = command
                        .value
                        .as_f64()
                        .with_context(|| format!("mapped parameter {name} requires a number"))?;
                    node_manager.set_mapped_value(&name, value)
                }
                FieldReferenceCommand::Raw {
                    node_name: _,
                    field_name,
                } => node_manager.set_raw_value(&field_name, command.value),
            }
        }
        "get_field" => {
            let command: GetFieldCommand = serde_json::from_value(incoming.content)
                .with_context(|| "invalid get_field command content")?;
            request_field(node_manager, &command.field)?;

            Ok(())
        }
        "get_nodes" => send_nodes_message(stream, node_manager),
        "get_telemetry" => {
            let timestamp = Utc::now().timestamp_millis();
            send_telemetry_message(stream, node_manager, timestamp)?;
            *last_telemetry_timestamp = timestamp;
            Ok(())
        }
        other => bail!("unsupported command type {other}"),
    }
}

fn send_nodes_message(stream: &mut TcpStream, node_manager: &NodeManager<'_>) -> Result<()> {
    let nodes = node_manager
        .nodes_snapshot()
        .into_iter()
        .map(|node| NodeContent {
            id: node.id,
            name: node.name,
            fields: node
                .fields
                .into_iter()
                .map(|field| FieldContent {
                    id: field.id,
                    name: field.name,
                    raw_name: field.raw_name,
                    mapped_name: field.mapped_name,
                    kind: match field.kind {
                        FieldType::Telemetry => "telemetry",
                        FieldType::Parameter => "parameter",
                    },
                })
                .collect(),
        })
        .collect();
    send_message(stream, "nodes", NodesContent { nodes })
}

fn request_field(
    node_manager: &NodeManager<'_>,
    field_reference: &FieldReferenceCommand,
) -> Result<()> {
    match field_reference {
        FieldReferenceCommand::Mapped { name } => node_manager.request_value(name),
        FieldReferenceCommand::Raw {
            node_name,
            field_name: _,
        } => node_manager.request_value(node_name),
    }
}

fn send_telemetry_message(
    stream: &mut TcpStream,
    node_manager: &NodeManager<'_>,
    timestamp: i64,
) -> Result<()> {
    send_message(
        stream,
        "telemetry",
        TelemetryContent {
            timestamp,
            nodes: telemetry_nodes_from_snapshots(node_manager.telemetry_snapshot()),
        },
    )
}

fn telemetry_nodes_from_snapshots(
    snapshots: Vec<NodeTelemetrySnapshot>,
) -> Vec<TelemetryNodeContent> {
    snapshots
        .into_iter()
        .map(|node| TelemetryNodeContent {
            id: node.id,
            name: node.name,
            telemetry: node
                .telemetry
                .into_iter()
                .map(TelemetryFieldContent::from)
                .collect(),
        })
        .collect()
}

fn send_message<T: Serialize>(
    stream: &mut TcpStream,
    message_type: &'static str,
    content: T,
) -> Result<()> {
    let payload = serde_json::to_vec(&OutgoingMessage {
        message_type,
        content,
    })?;
    let len = u16::try_from(payload.len())
        .with_context(|| format!("socket message too large: {} bytes", payload.len()))?;

    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&payload)?;
    Ok(())
}

/// Attempts to read a single message from the socket. Returns true if a message was placed in the message buffer, false otherwise.
fn read_message(stream: &mut TcpStream, message_buffer: &mut Vec<u8>) -> Result<Option<usize>> {
    // Do we already have a full message in the buffer from a previous read?
    if let Some(msg_len) = buffer_contains_full_message(message_buffer) {
        return Ok(Some(msg_len));
    }

    // Read more data
    let mut scratch = [0_u8; 4096];

    loop {
        match stream.read(&mut scratch) {
            Ok(0) => break, // EOF, socket closed
            Ok(bytes_read) => message_buffer.extend_from_slice(&scratch[..bytes_read]),
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }

    // Do we have a full message now?
    if let Some(msg_len) = buffer_contains_full_message(message_buffer) {
        Ok(Some(msg_len))
    } else {
        Ok(None)
    }
}

fn message_len_from_buffer(buffer: &[u8]) -> Option<usize> {
    if buffer.len() < 2 {
        None
    } else {
        Some(u16::from_be_bytes([buffer[0], buffer[1]]) as usize)
    }
}

fn buffer_contains_full_message(buffer: &[u8]) -> Option<usize> {
    let len = message_len_from_buffer(buffer);
    if let Some(len) = len
        && buffer.len() >= len + 2
    {
        Some(len)
    } else {
        None
    }
}

fn resolve_socket_addr(config: &WebserverSocketConfig) -> Result<SocketAddr> {
    (config.host.as_str(), config.port)
        .to_socket_addrs()?
        .next()
        .with_context(|| format!("{}:{} resolved to no addresses", config.host, config.port))
}

#[derive(Deserialize)]
struct IncomingMessage {
    #[serde(rename = "type")]
    message_type: String,
    content: serde_json::Value,
}

#[derive(Deserialize)]
struct SetParameterCommand {
    field: FieldReferenceCommand,
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct GetFieldCommand {
    field: FieldReferenceCommand,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FieldReferenceCommand {
    Mapped {
        name: String,
    },
    Raw {
        node_name: String,
        field_name: String,
    },
}

#[derive(Serialize)]
struct OutgoingMessage<T> {
    #[serde(rename = "type")]
    message_type: &'static str,
    content: T,
}

#[derive(Serialize)]
struct TelemetryContent {
    timestamp: i64,
    nodes: Vec<TelemetryNodeContent>,
}

#[derive(Serialize)]
struct TelemetryDeltaContent {
    prev_timestamp: i64,
    timestamp: i64,
    nodes: Vec<TelemetryNodeContent>,
}

#[derive(Serialize)]
struct TelemetryNodeContent {
    id: u8,
    name: String,
    telemetry: Vec<TelemetryFieldContent>,
}

#[derive(Serialize)]
struct TelemetryFieldContent {
    id: u8,
    name: String,
    raw_name: String,
    mapped_name: Option<String>,
    raw: serde_json::Value,
    value: serde_json::Value,
    unit: String,
    logical: serde_json::Value,
}

impl From<FieldValueSnapshot> for TelemetryFieldContent {
    fn from(value: FieldValueSnapshot) -> Self {
        Self {
            id: value.id,
            name: value.name,
            raw_name: value.raw_name,
            mapped_name: value.mapped_name,
            raw: value.raw,
            value: value.value,
            unit: value.unit,
            logical: value.logical,
        }
    }
}

#[derive(Serialize)]
struct NodesContent {
    nodes: Vec<NodeContent>,
}

#[derive(Serialize)]
struct NodeContent {
    id: u8,
    name: String,
    fields: Vec<FieldContent>,
}

#[derive(Serialize)]
struct FieldContent {
    id: u8,
    name: String,
    raw_name: String,
    mapped_name: Option<String>,
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Serialize)]
struct FieldGetResponseContent {
    node_id: u8,
    node_name: String,
    raw_name: String,
    mapped_name: Option<String>,
    name: String,
    raw: serde_json::Value,
    value: serde_json::Value,
    unit: String,
    logical: serde_json::Value,
}

impl FieldGetResponseContent {
    fn from_field(node_id: u8, field: FieldValueSnapshot) -> Self {
        Self {
            node_id,
            node_name: field.node_name,
            raw_name: field.raw_name,
            mapped_name: field.mapped_name,
            name: field.name,
            raw: field.raw,
            value: field.value,
            unit: field.unit,
            logical: field.logical,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mapped_field_command_reference() {
        let command: GetFieldCommand = serde_json::from_value(serde_json::json!({
            "field": {
                "type": "mapped",
                "name": "tank_pressure"
            }
        }))
        .expect("mapped command should parse");

        assert_eq!(
            command.field,
            FieldReferenceCommand::Mapped {
                name: "tank_pressure".to_string()
            }
        );
    }

    #[test]
    fn parses_raw_field_command_reference() {
        let command: SetParameterCommand = serde_json::from_value(serde_json::json!({
            "field": {
                "type": "raw",
                "node_name": "ECU",
                "field_name": "valve_raw"
            },
            "value": 42
        }))
        .expect("raw command should parse");

        assert_eq!(
            command.field,
            FieldReferenceCommand::Raw {
                node_name: "ECU".to_string(),
                field_name: "valve_raw".to_string()
            }
        );
        assert_eq!(command.value, serde_json::json!(42));
    }
}
