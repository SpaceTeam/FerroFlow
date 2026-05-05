use std::{collections::HashMap, sync::Mutex};

use anyhow::anyhow;
use anyhow::{Context, Result, bail};
use chrono::Utc;
use dashmap::DashMap;
use liquidcan::{
    CanMessage, CanMessageId,
    payloads::{
        CanDataValue, FieldGetResPayload, FieldRegistrationPayload, HeartbeatPayload,
        NodeInfoResPayload, TelemetryGroupDefinitionPayload, TelemetryGroupUpdatePayload,
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
    pub fn get_nodes(&self) -> &DashMap<u8, CanNode> {
        &self.can_nodes
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
