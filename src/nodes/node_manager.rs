use std::{collections::HashMap, sync::Mutex};

use anyhow::anyhow;
use anyhow::{Context, Result, bail};
use chrono::Utc;
use dashmap::DashMap;
use liquidcan::{
    CanMessage, CanMessageId,
    payloads::{
        CanDataType, CanDataValue, FieldGetResPayload, FieldRegistrationPayload, HeartbeatPayload,
        NodeInfoResPayload, ParameterSetConfirmationPayload, ParameterSetReqPayload,
        ParameterSetStatus, TelemetryGroupDefinitionPayload, TelemetryGroupUpdatePayload,
    },
};

use crate::{db::FieldLog, events};

use super::can_node::{CanNode, FieldInfo, RegistrationInfo, TelemetryGroupDefinition};

pub struct NodeManager<'a> {
    can_nodes: DashMap<u8, CanNode>,

    // Nodes that did not yet receive all their field registrations.
    registering_nodes: Mutex<HashMap<u8, CanNode>>,
    event_dispatcher: &'a events::EventDispatcher,
}

impl<'a> NodeManager<'a> {
    pub fn new(event_dispatcher: &'a events::EventDispatcher) -> Self {
        Self {
            can_nodes: DashMap::new(),
            registering_nodes: Mutex::new(HashMap::new()),
            event_dispatcher,
        }
    }

    pub fn start_node_registration(&self) {
        self.event_dispatcher
            .dispatch(events::Event::SendCanMessage {
                receiver_node_id: liquidcan::NODE_ID_BROADCAST,
                message: CanMessage::NodeInfoReq,
            });
    }

    pub fn handle_can_message_from_node(
        &self,
        message_id: CanMessageId,
        message: CanMessage,
    ) -> Result<()> {
        match message {
            CanMessage::NodeInfoAnnouncement { payload } => {
                self.handle_node_info_announcement(message_id, payload)
            }
            CanMessage::TelemetryValueRegistration { payload } => {
                self.handle_field_registration(message_id, payload, true)
            }
            CanMessage::ParameterRegistration { payload } => {
                self.handle_field_registration(message_id, payload, false)
            }
            CanMessage::TelemetryGroupDefinition { payload } => {
                self.handle_telemetry_group_definition(message_id, payload)
            }
            CanMessage::TelemetryGroupUpdate { payload } => {
                self.handle_telemetry_group_update(message_id, payload)
            }
            CanMessage::FieldGetRes { payload } => self.handle_field_get_res(message_id, payload),
            CanMessage::HeartbeatRes { payload } => self.handle_heartbeat_res(message_id, payload),
            CanMessage::ParameterSetConfirmation { payload } => {
                self.handle_parameter_set_confirmation(message_id, payload)
            }
            _ => bail!(
                "received unsupported CAN message from node {}: {:?}",
                message_id.sender_id(),
                message
            ),
        }
    }

    pub fn handle_node_info_announcement(
        &self,
        can_msg_id: CanMessageId,
        node_info_res: NodeInfoResPayload,
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();
        let registration_info = RegistrationInfo {
            telemetry_count: node_info_res.tel_count,
            parameter_count: node_info_res.par_count,
            firmware_hash: node_info_res.firmware_hash,
            protocol_hash: node_info_res.liquid_hash,
            device_name: node_info_res.device_name.into(),
        };

        let node = CanNode::new(registration_info);

        if node.node_registration_complete() {
            self.can_nodes.insert(node_id, node);
        } else {
            self.registering_nodes
                .lock()
                .map_err(|e| anyhow!("Mutex was poisoned: {}", e))?
                .insert(node_id, node);
        }

        Ok(())
    }

    pub fn handle_field_registration(
        &self,
        can_msg_id: CanMessageId,
        field_registration: FieldRegistrationPayload,
        is_telemetry: bool,
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();
        let field_info = FieldInfo {
            name: field_registration.field_name.into(),
            data_type: field_registration.field_type,
        };

        let mut registering_nodes = self
            .registering_nodes
            .lock()
            .map_err(|e| anyhow!("Mutex was poisoned: {}", e))?;

        if let Some(node) = registering_nodes.get_mut(&node_id) {
            let id = field_registration.field_id;
            if is_telemetry {
                node.telemetry_fields.insert(id, field_info);
            } else {
                node.parameter_fields.insert(id, field_info);
            }

            if node.node_registration_complete() {
                let completed_node = registering_nodes.remove(&node_id).with_context(|| {
                    format!(
                        "node {} completed registration but was missing from the registering set",
                        node_id
                    )
                })?;
                self.can_nodes.insert(node_id, completed_node);
            }
            Ok(())
        } else {
            bail!(
                "Received field registration for node {} but it is not currently registering",
                node_id
            );
        }
    }

    pub fn handle_telemetry_group_definition(
        &self,
        can_msg_id: CanMessageId,
        group_definition: TelemetryGroupDefinitionPayload,
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();

        let mut registering_nodes = self
            .registering_nodes
            .lock()
            .map_err(|e| anyhow!("Mutex was poisoned: {}", e))?;

        if let Some(node) = registering_nodes.get_mut(&node_id) {
            let fields: &[u8] = (&group_definition.field_ids).into();
            let group = TelemetryGroupDefinition {
                fields: fields.into(),
            };
            node.telemetry_groups
                .insert(group_definition.group_id, group);

            if node.node_registration_complete() {
                let completed_node = registering_nodes.remove(&node_id).with_context(|| {
                    format!(
                        "node {} completed registration but was missing from the registering set",
                        node_id
                    )
                })?;
                self.can_nodes.insert(node_id, completed_node);
            }

            Ok(())
        } else {
            bail!(
                "Received telemetry group definition for node {} but it is not registered",
                node_id
            );
        }
    }

    pub fn handle_telemetry_group_update(
        &self,
        can_msg_id: CanMessageId,
        group_update: TelemetryGroupUpdatePayload,
    ) -> Result<()> {
        let timestamp = Utc::now();

        let node_id = can_msg_id.sender_id();

        let node = self.can_nodes.get(&node_id).with_context(|| {
            format!(
                "received telemetry group update for node {} but it is not registered",
                node_id
            )
        })?;

        let group_id = group_update.group_id;

        let field_ids = node
            .telemetry_groups
            .get(&group_id)
            .map(|group| group.fields.clone())
            .with_context(|| {
                format!(
                    "received telemetry group update for node {} and group {} but the group is not defined",
                    node_id, group_id
                )
            })?;

        let field_infos = field_ids.iter().map(|id| {
            node.telemetry_fields
                .get(id)
                .with_context(|| {
                format!(
                    "received telemetry group update for node {} and group {} but field {} is not defined",
                    node_id, group_id, id
                )
            })
        }).collect::<Result<Vec<&FieldInfo>>>()?;

        for (&id, value) in field_ids.iter().zip(
            group_update
                .values
                .unpack(field_infos.iter().map(|info| info.data_type)),
        ) {
            let value = value.with_context(|| {
                format!(
                    "failed to unpack value for node {} group {} field {}",
                    node_id, group_id, id
                )
            })?;
            node.values.insert(id, (timestamp, value.clone()));

            let field_info = node.telemetry_fields.get(&id).unwrap();

            let telemetry_log = FieldLog {
                timestamp,
                node_id: node_id as i16,
                field_id: id as i16,
                field_name: field_info.name.clone(),
                field_value: Self::can_data_value_to_json(value),
            };
            self.event_dispatcher
                .dispatch(events::Event::NodeFieldUpdated(telemetry_log));
        }

        Ok(())
    }

    pub fn handle_field_get_res(
        &self,
        can_msg_id: CanMessageId,
        res: FieldGetResPayload,
    ) -> Result<()> {
        let timestamp = Utc::now();

        let node_id = can_msg_id.sender_id();

        let node = self.can_nodes.get(&node_id).with_context(|| {
            format!(
                "received field get response for node {} but it is not registered",
                node_id
            )
        })?;

        let field_id = res.field_id;
        let field_info = node
            .telemetry_fields
            .get(&field_id)
            .or_else(|| node.parameter_fields.get(&field_id))
            .with_context(|| {
                format!(
                    "received field get response for node {} field {} but no field definition exists",
                    node_id, field_id
                )
            })?;

        let field_type = field_info.data_type;

        let value = res.value.convert_from_raw(field_type).with_context(|| {
            format!(
                "failed to convert field get response value for node {} field {} from {:?}",
                node_id, field_id, res.value
            )
        })?;

        node.values.insert(field_id, (timestamp, value.clone()));

        let telemetry_log = FieldLog {
            timestamp,
            node_id: node_id as i16,
            field_id: field_id as i16,
            field_name: field_info.name.clone(),
            field_value: Self::can_data_value_to_json(value),
        };

        self.event_dispatcher
            .dispatch(events::Event::NodeFieldUpdated(telemetry_log));

        Ok(())
    }

    pub fn handle_heartbeat_res(
        &self,
        can_msg_id: CanMessageId,
        payload: HeartbeatPayload,
    ) -> Result<()> {
        let timestamp = Utc::now();
        let node_id = can_msg_id.sender_id();

        let node = self.can_nodes.get(&node_id).with_context(|| {
            format!(
                "received heartbeat response for node {} but it is not registered",
                node_id
            )
        })?;

        let mut latest_heartbeat = node
            .latest_heartbeat_received
            .write()
            .map_err(|error| anyhow!("RwLock was poisoned: {}", error))?;

        *latest_heartbeat = Some((timestamp, payload.counter));

        Ok(())
    }

    pub fn dispatch_heartbeat_requests(&self) -> Result<()> {
        for node_entry in self.can_nodes.iter() {
            let node_id = *node_entry.key();
            let next_heartbeat = node_entry
                .latest_heartbeat_sent
                .read()
                .map_err(|error| anyhow!("RwLock was poisoned: {}", error))?
                .as_ref()
                .map(|(_, counter)| *counter + 1)
                .unwrap_or(0);

            self.event_dispatcher
                .dispatch(events::Event::SendCanMessage {
                    receiver_node_id: node_id,
                    message: CanMessage::HeartbeatReq {
                        payload: HeartbeatPayload {
                            counter: next_heartbeat,
                        },
                    },
                });
        }

        Ok(())
    }

    pub fn handle_parameter_set_confirmation(
        &self,
        can_msg_id: CanMessageId,
        payload: ParameterSetConfirmationPayload,
    ) -> Result<()> {
        let timestamp = Utc::now();
        let node_id = can_msg_id.sender_id();

        if payload.status != ParameterSetStatus::Success {
            eprintln!(
                "Parameter set confirmation from node {} for parameter {} reported status {:?}",
                node_id, payload.parameter_id, payload.status
            );
        }

        let node = self.can_nodes.get(&node_id).with_context(|| {
            format!(
                "received parameter set confirmation for node {} but it is not registered",
                node_id
            )
        })?;

        let parameter_id = payload.parameter_id;
        let field_info = node.parameter_fields.get(&parameter_id).with_context(|| {
            format!(
                "received parameter set confirmation for node {} parameter {} but no field definition exists",
                node_id, parameter_id
            )
        })?;

        let field_type = field_info.data_type;

        let value = match payload.value {
            raw @ CanDataValue::Raw(_) => raw.convert_from_raw(field_type).with_context(|| {
                format!(
                    "failed to convert parameter set confirmation value for node {} parameter {} from {:?}",
                    node_id, parameter_id, raw
                )
            })?,
            other => other,
        };

        node.values.insert(parameter_id, (timestamp, value.clone()));

        let log = FieldLog {
            timestamp,
            node_id: node_id as i16,
            field_id: parameter_id as i16,
            field_name: field_info.name.clone(),
            field_value: Self::can_data_value_to_json(value),
        };

        self.event_dispatcher
            .dispatch(events::Event::NodeFieldUpdated(log));

        Ok(())
    }

    /// Resolve a parameter id by its registered name.
    pub fn resolve_parameter_id_by_name(&self, node_id: u8, parameter_name: &str) -> Result<u8> {
        let node = self.can_nodes.get(&node_id).with_context(|| {
            format!(
                "cannot resolve parameter name for node {} because it is not registered",
                node_id
            )
        })?;

        for (&id, info) in node.parameter_fields.iter() {
            if info.name == parameter_name {
                return Ok(id);
            }
        }

        bail!(
            "node {} has no parameter with name '{}'",
            node_id,
            parameter_name
        );
    }

    /// Queue a LiquidCAN `ParameterSetReq` for the given node.
    pub fn set_parameter(
        &self,
        node_id: u8,
        parameter_id: u8,
        value: serde_json::Value,
    ) -> Result<()> {
        let node = self.can_nodes.get(&node_id).with_context(|| {
            format!(
                "cannot set parameter because node {} is not registered",
                node_id
            )
        })?;

        let field_info = node.parameter_fields.get(&parameter_id).with_context(|| {
            format!(
                "cannot set parameter {} on node {} because no parameter definition exists",
                parameter_id, node_id
            )
        })?;

        let can_value =
            Self::json_to_can_data_value(field_info.data_type, value).with_context(|| {
                format!(
                    "failed to convert JSON value for node {} parameter {} ('{}')",
                    node_id, parameter_id, field_info.name
                )
            })?;

        self.event_dispatcher
            .dispatch(events::Event::SendCanMessage {
                receiver_node_id: node_id,
                message: CanMessage::ParameterSetReq {
                    payload: ParameterSetReqPayload {
                        parameter_id,
                        value: can_value,
                    },
                },
            });

        Ok(())
    }

    pub fn get_nodes(&self) -> &DashMap<u8, CanNode> {
        &self.can_nodes
    }

    fn json_to_can_data_value(
        data_type: CanDataType,
        value: serde_json::Value,
    ) -> Result<CanDataValue> {
        let parse_u64 = |v: &serde_json::Value| -> Result<u64> {
            if let Some(n) = v.as_u64() {
                return Ok(n);
            }
            if let Some(n) = v.as_i64() {
                if n < 0 {
                    bail!("expected unsigned integer, got {n}");
                }
                return Ok(n as u64);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                    return Ok(u64::from_str_radix(hex, 16)
                        .with_context(|| format!("invalid hex integer '{s}'"))?);
                }
                return Ok(s
                    .parse::<u64>()
                    .with_context(|| format!("invalid integer '{s}'"))?);
            }
            bail!("expected integer, got {v}");
        };

        let parse_i64 = |v: &serde_json::Value| -> Result<i64> {
            if let Some(n) = v.as_i64() {
                return Ok(n);
            }
            if let Some(n) = v.as_u64() {
                return Ok(i64::try_from(n).context("integer out of range")?);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                    let n = i64::try_from(
                        u64::from_str_radix(hex, 16)
                            .with_context(|| format!("invalid hex integer '{s}'"))?,
                    )
                    .context("integer out of range")?;
                    return Ok(n);
                }
                return Ok(s
                    .parse::<i64>()
                    .with_context(|| format!("invalid integer '{s}'"))?);
            }
            bail!("expected integer, got {v}");
        };

        let parse_f64 = |v: &serde_json::Value| -> Result<f64> {
            if let Some(n) = v.as_f64() {
                return Ok(n);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                return Ok(s
                    .parse::<f64>()
                    .with_context(|| format!("invalid float '{s}'"))?);
            }
            bail!("expected float, got {v}");
        };

        let parse_bool = |v: &serde_json::Value| -> Result<bool> {
            if let Some(b) = v.as_bool() {
                return Ok(b);
            }
            if let Some(n) = v.as_i64() {
                return Ok(n != 0);
            }
            if let Some(n) = v.as_u64() {
                return Ok(n != 0);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim().to_ascii_lowercase();
                return match s.as_str() {
                    "true" | "1" | "yes" | "on" => Ok(true),
                    "false" | "0" | "no" | "off" => Ok(false),
                    _ => bail!("invalid boolean '{s}'"),
                };
            }
            bail!("expected boolean, got {v}");
        };

        match data_type {
            CanDataType::Float32 => Ok(CanDataValue::Float32(parse_f64(&value)? as f32)),
            CanDataType::Int32 => {
                let n = parse_i64(&value)?;
                if n < i32::MIN as i64 || n > i32::MAX as i64 {
                    bail!("int32 out of range: {n}");
                }
                Ok(CanDataValue::Int32(n as i32))
            }
            CanDataType::Int16 => {
                let n = parse_i64(&value)?;
                if n < i16::MIN as i64 || n > i16::MAX as i64 {
                    bail!("int16 out of range: {n}");
                }
                Ok(CanDataValue::Int16(n as i16))
            }
            CanDataType::Int8 => {
                let n = parse_i64(&value)?;
                if n < i8::MIN as i64 || n > i8::MAX as i64 {
                    bail!("int8 out of range: {n}");
                }
                Ok(CanDataValue::Int8(n as i8))
            }
            CanDataType::UInt32 => {
                let n = parse_u64(&value)?;
                if n > u32::MAX as u64 {
                    bail!("uint32 out of range: {n}");
                }
                Ok(CanDataValue::UInt32(n as u32))
            }
            CanDataType::UInt16 => {
                let n = parse_u64(&value)?;
                if n > u16::MAX as u64 {
                    bail!("uint16 out of range: {n}");
                }
                Ok(CanDataValue::UInt16(n as u16))
            }
            CanDataType::UInt8 => {
                let n = parse_u64(&value)?;
                if n > u8::MAX as u64 {
                    bail!("uint8 out of range: {n}");
                }
                Ok(CanDataValue::UInt8(n as u8))
            }
            CanDataType::Boolean => Ok(CanDataValue::Boolean(parse_bool(&value)?)),
        }
    }

    fn can_data_value_to_json(value: CanDataValue) -> serde_json::Value {
        match value {
            CanDataValue::Float32(v) => serde_json::json!(v),
            CanDataValue::Int32(v) => serde_json::json!(v),
            CanDataValue::Int16(v) => serde_json::json!(v),
            CanDataValue::Int8(v) => serde_json::json!(v),
            CanDataValue::UInt32(v) => serde_json::json!(v),
            CanDataValue::UInt16(v) => serde_json::json!(v),
            CanDataValue::UInt8(v) => serde_json::json!(v),
            CanDataValue::Boolean(v) => serde_json::json!(v),
            CanDataValue::Raw(items) => serde_json::json!(items),
        }
    }
}
