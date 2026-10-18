use std::{
    sync::mpsc,
    thread::Scope,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tiny_http::{Header, Method, Response, Server, StatusCode};

use crate::{
    events::{Event, EventDispatcher, EventKind},
    nodes::NodeManager,
};

#[derive(Debug, Deserialize)]
struct SetParameterRequest {
    node_id: u8,

    /// Parameter ID as provided by the node during registration.
    #[serde(default)]
    parameter_id: Option<u8>,

    /// Parameter name as provided by the node during registration.
    #[serde(default)]
    parameter_name: Option<String>,

    /// The value to set. The JSON type is interpreted based on the parameter's LiquidCAN type.
    value: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    ok: bool,
    error: String,
}

#[derive(Debug, Serialize)]
struct NodeInfoResponse {
    node_id: u8,
    device_name: String,
    parameters: Vec<FieldInfoResponse>,
}

#[derive(Debug, Serialize)]
struct FieldInfoResponse {
    id: u8,
    name: String,
    data_type: String,
}

#[derive(Debug, Deserialize)]
struct ReadFieldRequest {
    node_id: u8,

    /// Field ID as provided by the node during registration.
    #[serde(default)]
    field_id: Option<u8>,

    /// Telemetry or parameter name as provided by the node during registration.
    #[serde(default)]
    field_name: Option<String>,

    /// How long to wait for the node's response. Defaults to one second.
    #[serde(default = "default_read_timeout_ms")]
    timeout_ms: u64,
}

#[derive(Debug, Serialize)]
struct TelemetryNodeResponse {
    node_id: u8,
    device_name: String,
    telemetry: Vec<FieldValueResponse>,
}

#[derive(Debug, Serialize)]
struct FieldValueResponse {
    id: u8,
    name: String,
    data_type: String,
    timestamp_ms: Option<i64>,
    value: Option<serde_json::Value>,
}

fn default_read_timeout_ms() -> u64 {
    1_000
}

/// Spawns a small blocking HTTP server.
///
/// Endpoints:
/// - `GET /health` => "ok"
/// - `GET /api/nodes` => JSON list of nodes and their parameter definitions
/// - `GET /api/telemetry` => all cached telemetry values
/// - `POST /api/fields/read` => request and wait for a field value by id or name
/// - `POST /api/parameters/set` => set a parameter by id or name
pub fn spawn_http_server_thread<'a>(
    node_manager: &'a NodeManager<'a>,
    event_dispatcher: &'a EventDispatcher,
    listen_addr: String,
    scope: &'a Scope<'a, '_>,
) {
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<Event>();
    event_dispatcher.subscribe(
        shutdown_tx,
        vec![EventKind::Shutdown],
        format!("HTTP server ({listen_addr})"),
    );

    scope.spawn(move || {
        let server = match Server::http(&listen_addr) {
            Ok(server) => server,
            Err(e) => {
                eprintln!("Failed to start HTTP server on {listen_addr}: {e}");
                return;
            }
        };

        println!("HTTP server listening on http://{listen_addr}");

        loop {
            // Exit quickly on shutdown.
            match shutdown_rx.try_recv() {
                Ok(Event::Shutdown) => break,
                Ok(_) => {}
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => break,
            }

            let request = match server.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(req)) => req,
                Ok(None) => continue,
                Err(e) => {
                    eprintln!("HTTP server error: {e}");
                    continue;
                }
            };

            if let Err(e) = handle_request(node_manager, request) {
                eprintln!("HTTP handler error: {e:#}");
            }
        }

        println!("HTTP server stopped");
    });
}

fn handle_request(node_manager: &NodeManager<'_>, mut request: tiny_http::Request) -> Result<()> {
    let method = request.method().clone();
    let url = request.url().to_string();

    match (method, url.as_str()) {
        (Method::Get, "/health") => {
            let response = Response::from_string("ok");
            request.respond(response)?;
            Ok(())
        }

        (Method::Get, "/api/nodes") => {
            let nodes = node_manager
                .get_nodes()
                .iter()
                .map(|entry| {
                    let node_id = *entry.key();
                    let node = entry.value();
                    NodeInfoResponse {
                        node_id,
                        device_name: node.registration_info.device_name.clone(),
                        parameters: node
                            .parameter_fields
                            .iter()
                            .map(|(&id, info)| FieldInfoResponse {
                                id,
                                name: info.name.clone(),
                                data_type: format!("{:?}", info.data_type),
                            })
                            .collect(),
                    }
                })
                .collect::<Vec<_>>();

            respond_json(request, StatusCode(200), &nodes)
        }

        (Method::Get, "/api/telemetry") => {
            let mut nodes = node_manager
                .get_nodes()
                .iter()
                .map(|entry| {
                    let node_id = *entry.key();
                    let node = entry.value();
                    let mut telemetry = node
                        .telemetry_fields
                        .iter()
                        .map(|(&id, info)| {
                            let cached = node.values.get(&id);
                            FieldValueResponse {
                                id,
                                name: info.name.clone(),
                                data_type: format!("{:?}", info.data_type),
                                timestamp_ms: cached
                                    .as_ref()
                                    .map(|value| value.value().0.timestamp_millis()),
                                value: cached.as_ref().map(|value| {
                                    NodeManager::can_data_value_to_json(value.value().1.clone())
                                }),
                            }
                        })
                        .collect::<Vec<_>>();
                    telemetry.sort_by_key(|field| field.id);

                    TelemetryNodeResponse {
                        node_id,
                        device_name: node.registration_info.device_name.clone(),
                        telemetry,
                    }
                })
                .collect::<Vec<_>>();
            nodes.sort_by_key(|node| node.node_id);

            respond_json(request, StatusCode(200), &nodes)
        }

        (Method::Post, "/api/fields/read") => {
            let result: Result<FieldValueResponse> = (|| {
                let body = read_body_as_string(&mut request)?;
                let req: ReadFieldRequest = serde_json::from_str(&body)
                    .with_context(|| format!("invalid JSON body: {body}"))?;

                if req.timeout_ms > 30_000 {
                    bail!("timeout_ms cannot exceed 30000");
                }

                let field_id = match (req.field_id, req.field_name.as_deref()) {
                    (Some(id), _) => id,
                    (None, Some(name)) => {
                        node_manager.resolve_field_id_by_name(req.node_id, name)?
                    }
                    (None, None) => bail!("either 'field_id' or 'field_name' must be provided"),
                };

                let previous_timestamp = node_manager
                    .get_field_value(req.node_id, field_id)?
                    .map(|(timestamp, _)| timestamp);
                node_manager.request_field(req.node_id, field_id)?;

                let deadline = Instant::now() + Duration::from_millis(req.timeout_ms);
                let (timestamp, value) = loop {
                    if let Some((timestamp, value)) =
                        node_manager.get_field_value(req.node_id, field_id)?
                        && previous_timestamp.as_ref() != Some(&timestamp)
                    {
                        break (timestamp, value);
                    }

                    if Instant::now() >= deadline {
                        bail!(
                            "timed out waiting for node {} field {}",
                            req.node_id,
                            field_id
                        );
                    }
                    std::thread::sleep(Duration::from_millis(10));
                };

                let node = node_manager
                    .get_nodes()
                    .get(&req.node_id)
                    .with_context(|| format!("node {} is no longer registered", req.node_id))?;
                let info = node
                    .telemetry_fields
                    .get(&field_id)
                    .or_else(|| node.parameter_fields.get(&field_id))
                    .with_context(|| {
                        format!(
                            "field {} on node {} is no longer registered",
                            field_id, req.node_id
                        )
                    })?;

                Ok(FieldValueResponse {
                    id: field_id,
                    name: info.name.clone(),
                    data_type: format!("{:?}", info.data_type),
                    timestamp_ms: Some(timestamp.timestamp_millis()),
                    value: Some(NodeManager::can_data_value_to_json(value)),
                })
            })();

            match result {
                Ok(value) => respond_json(request, StatusCode(200), &value),
                Err(e) => respond_json(
                    request,
                    StatusCode(400),
                    &ErrorResponse {
                        ok: false,
                        error: format!("{e:#}"),
                    },
                ),
            }
        }

        (Method::Post, "/api/parameters/set") => {
            let result: Result<()> = (|| {
                let body = read_body_as_string(&mut request)?;
                let req: SetParameterRequest = serde_json::from_str(&body)
                    .with_context(|| format!("invalid JSON body: {body}"))?;

                let parameter_id = match (req.parameter_id, req.parameter_name.as_deref()) {
                    (Some(id), _) => id,
                    (None, Some(name)) => {
                        node_manager.resolve_parameter_id_by_name(req.node_id, name)?
                    }
                    (None, None) => {
                        bail!("either 'parameter_id' or 'parameter_name' must be provided")
                    }
                };

                node_manager.set_parameter(req.node_id, parameter_id, req.value)?;
                Ok(())
            })();

            match result {
                Ok(()) => respond_json(request, StatusCode(200), &OkResponse { ok: true }),
                Err(e) => respond_json(
                    request,
                    StatusCode(400),
                    &ErrorResponse {
                        ok: false,
                        error: format!("{e:#}"),
                    },
                ),
            }
        }

        _ => respond_json(
            request,
            StatusCode(404),
            &ErrorResponse {
                ok: false,
                error: "not found".to_string(),
            },
        ),
    }
}

fn read_body_as_string(request: &mut tiny_http::Request) -> Result<String> {
    let mut body = String::new();
    request
        .as_reader()
        .read_to_string(&mut body)
        .context("failed reading request body")?;
    Ok(body)
}

fn respond_json<T: ?Sized + Serialize>(
    request: tiny_http::Request,
    status: StatusCode,
    value: &T,
) -> Result<()> {
    let body = serde_json::to_string(value).context("failed to serialize JSON response")?;

    let header = Header::from_bytes("Content-Type", "application/json")
        .map_err(|_| anyhow::anyhow!("failed to create HTTP header"))?;

    let response = Response::from_string(body)
        .with_status_code(status)
        .with_header(header);

    request.respond(response)?;
    Ok(())
}
