use std::{collections::HashMap, sync::Mutex};

use anyhow::anyhow;
use anyhow::{Context, Result, bail};
use chrono::Utc;
use dashmap::DashMap;
use liquidcan::{
    CanMessage, CanMessageId,
    payloads::{
        CanDataType, CanDataValue, FieldGetReqPayload, FieldGetResPayload,
        FieldRegistrationPayload, HeartbeatPayload, NodeInfoResPayload, ParameterSetReqPayload,
        TelemetryGroupDefinitionPayload, TelemetryGroupUpdatePayload,
    },
};

use crate::nodes::mapping::{self, LogicalValue, MappedValue, Mapping, MappingLookupResult};
use crate::{db::FieldLog, events};

use super::can_node::{CanNode, FieldInfo, RegistrationInfo, TelemetryGroupDefinition};

pub struct NodeManager<'a> {
    mapping: Mapping,
    can_nodes: DashMap<u8, CanNode>,

    // Nodes that did not yet receive all their field registrations.
    registering_nodes: Mutex<HashMap<u8, CanNode>>,
    event_dispatcher: &'a events::EventDispatcher,
}

impl<'a> NodeManager<'a> {
    pub fn new(event_dispatcher: &'a events::EventDispatcher, mapping: Mapping) -> Self {
        Self {
            mapping,
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

        let field_infos = field_ids
            .iter()
            .map(|id| {
                node.telemetry_fields.get(id).with_context(|| {
                    format!(
                        "received telemetry group update for node {} and group {} but field {} is not defined",
                        node_id, group_id, id
                    )
                })
            })
            .collect::<Result<Vec<&FieldInfo>>>()?;

        let raw_values = group_update
            .values
            .unpack(field_infos.iter().map(|info| info.data_type))
            .collect::<Vec<_>>();

        for ((&id, field_info), value) in field_ids.iter().zip(field_infos).zip(raw_values) {
            let value = value.with_context(|| {
                format!(
                    "failed to unpack value for node {} group {} field {}",
                    node_id, group_id, id
                )
            })?;
            node.values.insert(id, (timestamp, value.clone()));

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

    /// Returns the latest cached raw CAN value for a mapped field name.
    ///
    /// This does not send a CAN request. Call `request_value` first if a fresh value is needed.
    ///
    /// Use this `try_` variant to distinguish missing values from invalid mappings or fields
    /// that have not registered yet.
    pub fn try_get_raw_value(&self, mapped_name: &str) -> Result<Option<CanDataValue>> {
        let (_, target) = self.resolve_mapping_by_name(mapped_name)?;

        Ok(self.latest_raw_value(&target))
    }

    /// Convenience wrapper around `try_get_raw_value` that treats errors as missing values.
    pub fn get_raw_value(&self, mapped_name: &str) -> Option<CanDataValue> {
        self.try_get_raw_value(mapped_name).ok().flatten()
    }

    /// Returns the latest cached value after applying the mapping's slope/offset conversion.
    ///
    /// `Ok(None)` means the mapping and raw field exist, but no value has been received yet.
    pub fn try_get_mapped_value(&self, mapped_name: &str) -> Result<Option<MappedValue>> {
        let (mapping, target) = self.resolve_mapping_by_name(mapped_name)?;
        let Some(raw_value) = self.latest_raw_value(&target) else {
            return Ok(None);
        };

        Ok(Some(mapping.mapping_entry.mapped_value(&raw_value)?))
    }

    /// Convenience wrapper around `try_get_mapped_value` that treats errors as missing values.
    pub fn get_mapped_value(&self, mapped_name: &str) -> Option<MappedValue> {
        self.try_get_mapped_value(mapped_name).ok().flatten()
    }

    /// Returns the logical value associated with the current mapped value.
    ///
    /// Logical values are derived from the configured range table. If the mapping has no logical
    /// rules, this returns `Ok(None)` even when a mapped numeric value is available.
    pub fn try_get_logical_value(&self, mapped_name: &str) -> Result<Option<LogicalValue>> {
        let Some(mapped_value) = self.try_get_mapped_value(mapped_name)? else {
            return Ok(None);
        };

        let mapping_lookup = self.lookup_mapping(mapped_name)?;

        Ok(mapping_lookup
            .mapping_entry
            .logical_value(mapped_value.value))
    }

    /// Convenience wrapper around `try_get_logical_value` that treats errors as missing values.
    pub fn get_logical_value(&self, mapped_name: &str) -> Option<LogicalValue> {
        self.try_get_logical_value(mapped_name).ok().flatten()
    }

    /// Sends a `FieldGetReq` for the raw field behind a mapped name.
    ///
    /// The response is processed asynchronously by the normal CAN message handler and updates the
    /// cached value read by `get_raw_value`, `get_mapped_value`, and `get_logical_value`.
    pub fn request_value(&self, mapped_name: &str) -> Result<()> {
        let (_, target) = self.resolve_mapping_by_name(mapped_name)?;

        self.event_dispatcher
            .dispatch(events::Event::SendCanMessage {
                receiver_node_id: target.node_id,
                message: CanMessage::FieldGetReq {
                    payload: FieldGetReqPayload {
                        field_id: target.field_id,
                    },
                },
            });

        Ok(())
    }

    /// Writes a mapped value to a mapped parameter field.
    ///
    /// The value is converted back to the raw CAN type using the inverse of the configured linear
    /// mapping, then sent as a `ParameterSetReq`.
    pub fn set_mapped_value(&self, mapped_name: &str, mapped_value: f64) -> Result<()> {
        let (mapping_lookup, target) = self.resolve_mapping_by_name(mapped_name)?;

        if mapping_lookup.mapping_entry.field_type != mapping::FieldType::Parameter {
            bail!("mapped field {mapped_name} is not writable because it is not a parameter");
        }

        let raw_value = mapping_lookup
            .mapping_entry
            .raw_value_from_mapped(mapped_value, target.data_type)?;
        self.dispatch_parameter_set(target, raw_value);

        Ok(())
    }

    /// Writes a raw CAN value to a mapped parameter field.
    pub fn set_raw_value(&self, mapped_name: &str, raw_value: CanDataValue) -> Result<()> {
        let (mapping_lookup, target) = self.resolve_mapping_by_name(mapped_name)?;

        if mapping_lookup.mapping_entry.field_type != mapping::FieldType::Parameter {
            bail!("mapped field {mapped_name} is not writable because it is not a parameter");
        }

        self.dispatch_parameter_set(target, raw_value);

        Ok(())
    }

    fn lookup_mapping(&self, mapped_name: &str) -> Result<MappingLookupResult<'_>> {
        self.mapping
            .get_mapping_for_name(mapped_name)
            .with_context(|| format!("no mapping exists for {mapped_name}"))
    }

    fn resolve_mapping_by_name(
        &self,
        mapped_name: &str,
    ) -> Result<(MappingLookupResult<'_>, ResolvedMappingTarget)> {
        let mapping_lookup = self.lookup_mapping(mapped_name)?;
        let target = self
            .resolve_mapping_target(&mapping_lookup)
            .with_context(|| format!("mapped field {mapped_name} is not registered"))?;

        Ok((mapping_lookup, target))
    }

    fn latest_raw_value(&self, target: &ResolvedMappingTarget) -> Option<CanDataValue> {
        self.can_nodes.get(&target.node_id).and_then(|node| {
            node.values
                .get(&target.field_id)
                .map(|value| value.1.clone())
        })
    }

    fn dispatch_parameter_set(&self, target: ResolvedMappingTarget, raw_value: CanDataValue) {
        self.event_dispatcher
            .dispatch(events::Event::SendCanMessage {
                receiver_node_id: target.node_id,
                message: CanMessage::ParameterSetReq {
                    payload: ParameterSetReqPayload {
                        parameter_id: target.field_id,
                        value: raw_value,
                    },
                },
            });
    }

    /// Resolves a mapping entry to the currently registered node id, field id, and field type.
    ///
    /// Mappings are written against stable device/field names, but LiquidCAN requests need numeric
    /// ids learned during node registration.
    fn resolve_mapping_target(
        &self,
        mapping_lookup_result: &MappingLookupResult,
    ) -> Option<ResolvedMappingTarget> {
        self.can_nodes.iter().find_map(|node| {
            if node.registration_info.device_name != mapping_lookup_result.node_name {
                return None;
            }

            let fields = match mapping_lookup_result.mapping_entry.field_type {
                mapping::FieldType::Telemetry => &node.telemetry_fields,
                mapping::FieldType::Parameter => &node.parameter_fields,
            };

            fields
                .iter()
                .find(|(_, field)| field.name == mapping_lookup_result.mapping_entry.raw_field)
                .map(|(field_id, field)| ResolvedMappingTarget {
                    node_id: *node.key(),
                    field_id: *field_id,
                    data_type: field.data_type,
                })
        })
    }
}

struct ResolvedMappingTarget {
    node_id: u8,
    field_id: u8,
    data_type: CanDataType,
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use chrono::Utc;
    use liquidcan::payloads::{CanDataType, CanDataValue};
    use toml::Value;

    use crate::events::{Event, EventDispatcher, EventKind};

    use super::*;

    #[test]
    fn reads_raw_mapped_and_logical_values_by_mapping_name() {
        let dispatcher = EventDispatcher::new();
        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        assert_eq!(
            manager.get_raw_value("tank_pressure"),
            Some(CanDataValue::UInt16(198))
        );

        let mapped = manager
            .get_mapped_value("tank_pressure")
            .expect("mapped value should be available");
        assert_eq!(mapped.value, 100.0);
        assert_eq!(mapped.unit, "bar");

        let logical = manager
            .get_logical_value("tank_pressure")
            .expect("logical value should be available");
        assert_eq!(logical.value, Value::String("High".to_string()));
        assert_eq!(logical.color, Some("#ff0000".to_string()));
    }

    #[test]
    fn writes_mapped_parameter_values_as_raw_can_values() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        manager
            .set_mapped_value("valve_opening", 60.0)
            .expect("mapped parameter should be writable");

        assert_eq!(
            receive_parameter_set(&rx),
            (5, 20, CanDataValue::UInt8(100))
        );
    }

    #[test]
    fn writes_raw_parameter_values() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        manager
            .set_raw_value("valve_opening", CanDataValue::UInt8(42))
            .expect("raw parameter should be writable");

        assert_eq!(receive_parameter_set(&rx), (5, 20, CanDataValue::UInt8(42)));
    }

    #[test]
    fn requests_field_get_for_mapped_values() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        manager
            .request_value("tank_pressure")
            .expect("mapped field should be requestable");

        let event = rx
            .recv_timeout(Duration::from_millis(200))
            .expect("send event should be dispatched");

        match event {
            Event::SendCanMessage {
                receiver_node_id,
                message: CanMessage::FieldGetReq { payload },
            } => {
                assert_eq!(receiver_node_id, 5);
                assert_eq!(payload.field_id, 10);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn test_mapping() -> Mapping {
        Mapping::parse_mapping(
            r##"
[[mapping.ECU]]
name = "tank_pressure"
type = "telemetry"
raw_field = "pressure_adc"
value = { slope = 0.5, offset = 1.0, unit = "bar" }

[[mapping.ECU.logical]]
range = { min = 100 }
value = "High"
color = "#ff0000"

[[mapping.ECU.logical]]
range = { max = 100 }
value = "Normal"

[[mapping.ECU]]
name = "valve_opening"
type = "parameter"
raw_field = "valve_raw"
value = { slope = 0.5, offset = 10.0, unit = "%" }
"##,
        )
        .expect("mapping should parse")
    }

    fn receive_parameter_set(rx: &mpsc::Receiver<Event>) -> (u8, u8, CanDataValue) {
        let event = rx
            .recv_timeout(Duration::from_millis(200))
            .expect("send event should be dispatched");

        match event {
            Event::SendCanMessage {
                receiver_node_id,
                message:
                    CanMessage::ParameterSetReq {
                        payload:
                            ParameterSetReqPayload {
                                parameter_id,
                                value,
                            },
                    },
            } => (receiver_node_id, parameter_id, value),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn insert_test_node(manager: &NodeManager<'_>) {
        let mut node = CanNode::new(RegistrationInfo {
            telemetry_count: 1,
            parameter_count: 1,
            firmware_hash: 0,
            protocol_hash: 0,
            device_name: "ECU".to_string(),
        });
        node.telemetry_fields.insert(
            10,
            FieldInfo {
                data_type: CanDataType::UInt16,
                name: "pressure_adc".to_string(),
            },
        );
        node.parameter_fields.insert(
            20,
            FieldInfo {
                data_type: CanDataType::UInt8,
                name: "valve_raw".to_string(),
            },
        );
        node.values
            .insert(10, (Utc::now(), CanDataValue::UInt16(198)));

        manager.can_nodes.insert(5, node);
    }
}
