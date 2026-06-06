pub mod command;

use std::{collections::HashMap, sync::Mutex};

use anyhow::anyhow;
use anyhow::{Context, Result, bail};
use chrono::Utc;
use dashmap::DashMap;
use liquidcan::{
    CanMessage, CanMessageId,
    payloads::{
        CanDataType, CanDataValue, FieldGetResPayload, FieldRegistrationPayload, HeartbeatPayload,
        NodeInfoResPayload, TelemetryGroupDefinitionPayload, TelemetryGroupUpdatePayload,
    },
};

use crate::nodes::mapping::{self, LogicalValue, MappedValue, Mapping, MappingLookupResult};
use crate::{db::FieldLog, events};

use super::can_node::{CanNode, FieldInfo, RegistrationInfo, TelemetryGroupDefinition};

/// Manages the CAN nodes connected to FerroFlow.
///
/// By convention, methods taking a `field_name` argument expect either
/// - a name as defined in the mapping file
/// - a raw name in the format `node_name:raw_field_name`
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

    /// Returns the latest cached raw CAN value.
    ///
    /// This does not send a CAN request. Call `request_value` first if a fresh value is needed.
    ///
    /// Use this `try_` variant to distinguish missing values from invalid mappings or fields
    /// that have not registered yet.
    pub fn try_get_raw_value(&self, field_name: &str) -> Result<Option<CanDataValue>> {
        let (_, target) = self.resolve_mapping_by_name(field_name)?;

        Ok(self.latest_raw_value(&target))
    }

    /// Convenience wrapper around `try_get_raw_value` that treats errors as missing values.
    pub fn get_raw_value(&self, field_name: &str) -> Option<CanDataValue> {
        self.try_get_raw_value(field_name).ok().flatten()
    }

    /// Returns the latest cached value after applying the mapping's slope/offset conversion.
    ///
    /// `Ok(None)` means the mapping and raw field exist, but no value has been received yet.
    pub fn try_get_mapped_value(&self, field_name: &str) -> Result<Option<MappedValue>> {
        let (mapping, target) = self.resolve_mapping_by_name(field_name)?;
        let Some(raw_value) = self.latest_raw_value(&target) else {
            return Ok(None);
        };

        Ok(Some(mapping.mapping_entry.mapped_value(&raw_value)?))
    }

    /// Convenience wrapper around `try_get_mapped_value` that treats errors as missing values.
    pub fn get_mapped_value(&self, field_name: &str) -> Option<MappedValue> {
        self.try_get_mapped_value(field_name).ok().flatten()
    }

    /// Returns the logical value associated with the current mapped value.
    ///
    /// Logical values are derived from the configured range table. If the mapping has no logical
    /// rules, this returns `Ok(None)` even when a mapped numeric value is available.
    pub fn try_get_logical_value(&self, field_name: &str) -> Result<Option<LogicalValue>> {
        let Some(mapped_value) = self.try_get_mapped_value(field_name)? else {
            return Ok(None);
        };

        let mapping_lookup = self.lookup_mapping(field_name)?;

        Ok(mapping_lookup
            .mapping_entry
            .logical_value(mapped_value.value))
    }

    /// Convenience wrapper around `try_get_logical_value` that treats errors as missing values.
    pub fn get_logical_value(&self, field_name: &str) -> Option<LogicalValue> {
        self.try_get_logical_value(field_name).ok().flatten()
    }

    fn lookup_mapping(&self, field_name: &str) -> Result<MappingLookupResult<'_>> {
        if field_name.contains(':') {
            // This is a raw name
            let (node_name, raw_field_name) = field_name.split_once(':').unwrap();
            self.mapping
                .get_mapping_for_raw(node_name, raw_field_name)
                .with_context(|| format!("no mapping exists for {field_name}"))
        } else {
            self.mapping
                .get_mapping_for_name(field_name)
                .with_context(|| format!("no mapping exists for {field_name}"))
        }
    }

    fn resolve_mapping_by_name(
        &self,
        field_name: &str,
    ) -> Result<(MappingLookupResult<'_>, ResolvedMappingTarget)> {
        let mapping_lookup = self.lookup_mapping(field_name)?;
        let target = self
            .resolve_mapping_target(&mapping_lookup)
            .with_context(|| format!("mapped field {field_name} is not registered"))?;

        Ok((mapping_lookup, target))
    }

    fn latest_raw_value(&self, target: &ResolvedMappingTarget) -> Option<CanDataValue> {
        self.can_nodes.get(&target.node_id).and_then(|node| {
            node.values
                .get(&target.field_id)
                .map(|value| value.1.clone())
        })
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
    use liquidcan::payloads::{CanDataType, CanDataValue, ParameterSetReqPayload};
    use serde_json::json;
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

        let non_existant_mapped = manager.try_get_mapped_value("non_existent");
        assert!(
            non_existant_mapped
                .is_err_and(|e| { e.to_string() == "no mapping exists for non_existent" })
        );

        let non_existant_logical = manager.try_get_logical_value("non_existent");
        assert!(
            non_existant_logical
                .is_err_and(|e| { e.to_string() == "no mapping exists for non_existent" })
        );

        let non_existant_raw = manager.try_get_raw_value("non_existent");
        assert!(
            non_existant_raw
                .is_err_and(|e| { e.to_string() == "no mapping exists for non_existent" })
        );

        let non_registered_mapped = manager.try_get_mapped_value("tank_temp");
        assert!(
            non_registered_mapped
                .is_err_and(|e| { e.to_string() == "mapped field tank_temp is not registered" })
        );
    }

    #[test]
    fn try_get_mapped_value_returns_ok_none_when_no_value_cached() {
        let dispatcher = EventDispatcher::new();
        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        assert_eq!(manager.try_get_raw_value("valve_opening").unwrap(), None);
        assert_eq!(manager.try_get_mapped_value("valve_opening").unwrap(), None);
        assert_eq!(
            manager.try_get_logical_value("valve_opening").unwrap(),
            None
        );
    }

    #[test]
    fn get_returns_none_on_missing_mapping_or_unregistered() {
        let dispatcher = EventDispatcher::new();
        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        assert_eq!(manager.get_raw_value("non_existent"), None);
        assert_eq!(manager.get_mapped_value("non_existent"), None);
        assert_eq!(manager.get_logical_value("non_existent"), None);

        // mapping exists but is not registered on the inserted test node
        assert_eq!(manager.get_raw_value("tank_temp"), None);
        assert_eq!(manager.get_mapped_value("tank_temp"), None);
        assert_eq!(manager.get_logical_value("tank_temp"), None);
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
            .set_raw_value("valve_opening", json!(42))
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

    #[test]
    fn set_value_rejects_telemetry_values() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        let err = manager
            .set_mapped_value("tank_pressure", 10.0)
            .expect_err("telemetry mappings should not be writable");
        assert!(err.to_string().contains("is not writable"));

        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        let err = manager
            .set_raw_value("tank_pressure", json!(1))
            .expect_err("telemetry mappings should not be writable");
        assert!(err.to_string().contains("is not writable"));

        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn set_mapped_value_rejects_nan() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        assert!(manager.set_mapped_value("valve_opening", f64::NAN).is_err());
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn set_mapped_value_rejects_out_of_range() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        let err = manager
            .set_mapped_value("valve_opening", 1000.0)
            .expect_err("out of range values should fail");
        assert!(err.to_string().contains("out of range"));

        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn set_mapped_value_rejects_fractional_for_integer_type() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        let err = manager
            .set_mapped_value("valve_opening", 10.1)
            .expect_err("fractional inverse-mapped raw values should fail");
        assert!(err.to_string().contains("is not an integer"));

        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn request_value_errors_when_unregistered() {
        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");

        let manager = NodeManager::new(&dispatcher, test_mapping());
        insert_test_node(&manager);

        let err = manager
            .request_value("tank_temp")
            .expect_err("unregistered mappings should error");
        assert_eq!(err.to_string(), "mapped field tank_temp is not registered");

        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn telemetry_group_update_with_multiple_fields_pairs_values_correctly() {
        use liquidcan::payloads::PackedCanDataValues;

        let dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(
            tx,
            vec![EventKind::NodeFieldUpdated],
            "test-node-field-updated",
        );

        let manager = NodeManager::new(&dispatcher, Mapping::default());
        insert_two_field_test_node(&manager);

        let values = PackedCanDataValues::<62>::try_from(&[
            CanDataValue::UInt16(0x1234),
            CanDataValue::UInt32(0x89ABCDEF),
        ] as &[CanDataValue])
        .expect("values should pack");

        let payload = TelemetryGroupUpdatePayload {
            group_id: 1,
            values,
        };
        let msg_id = CanMessageId::new()
            .with_sender_id(5)
            .with_receiver_id(liquidcan::NODE_ID_SERVER);

        manager
            .handle_telemetry_group_update(msg_id, payload)
            .expect("update should succeed");

        let node = manager.can_nodes.get(&5).unwrap();
        assert_eq!(
            node.values.get(&10).unwrap().value().1,
            CanDataValue::UInt16(0x1234)
        );
        assert_eq!(
            node.values.get(&11).unwrap().value().1,
            CanDataValue::UInt32(0x89ABCDEF)
        );

        // Two update events with correct field ids and names.
        let evt1 = rx.recv_timeout(Duration::from_millis(200)).unwrap();
        let evt2 = rx.recv_timeout(Duration::from_millis(200)).unwrap();

        let mut logs = vec![];
        for evt in [evt1, evt2] {
            match evt {
                Event::NodeFieldUpdated(log) => logs.push(log),
                other => panic!("unexpected event: {other:?}"),
            }
        }
        logs.sort_by_key(|l| l.field_id);

        assert_eq!(logs[0].node_id, 5);
        assert_eq!(logs[0].field_id, 10);
        assert_eq!(logs[0].field_name, "a");
        assert_eq!(logs[0].field_value, serde_json::json!(0x1234u16));

        assert_eq!(logs[1].node_id, 5);
        assert_eq!(logs[1].field_id, 11);
        assert_eq!(logs[1].field_name, "b");
        assert_eq!(logs[1].field_value, serde_json::json!(0x89ABCDEFu32));
    }

    #[test]
    fn telemetry_group_update_unpacked_value_error_mentions_node_group_field() {
        use liquidcan::payloads::PackedCanDataValues;

        let dispatcher = EventDispatcher::new();
        let manager = NodeManager::new(&dispatcher, Mapping::default());
        insert_two_field_test_node(&manager);

        // Only pack one value, but the group expects two (UInt16 + UInt32).
        let values =
            PackedCanDataValues::<62>::try_from(&[CanDataValue::UInt16(0x1234)] as &[CanDataValue])
                .expect("values should pack");

        let payload = TelemetryGroupUpdatePayload {
            group_id: 1,
            values,
        };
        let msg_id = CanMessageId::new()
            .with_sender_id(5)
            .with_receiver_id(liquidcan::NODE_ID_SERVER);

        let err = manager
            .handle_telemetry_group_update(msg_id, payload)
            .expect_err("unpack should fail");

        assert!(format!("{err:#}").contains("failed to unpack value for node 5 group 1 field 11"));
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

[[mapping.ECU.logical]]
range = { max = 100 }
value = "Normal"

[[mapping.ECU]]
name = "valve_opening"
type = "parameter"
raw_field = "valve_raw"
value = { slope = 0.5, offset = 10.0, unit = "%" }

[[mapping.ECU]]
name = "tank_temp"
type = "telemetry"
raw_field = "temp_adc"
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

    fn insert_two_field_test_node(manager: &NodeManager<'_>) {
        let mut node = CanNode::new(RegistrationInfo {
            telemetry_count: 2,
            parameter_count: 0,
            firmware_hash: 0,
            protocol_hash: 0,
            device_name: "ECU".to_string(),
        });

        node.telemetry_fields.insert(
            10,
            FieldInfo {
                data_type: CanDataType::UInt16,
                name: "a".to_string(),
            },
        );
        node.telemetry_fields.insert(
            11,
            FieldInfo {
                data_type: CanDataType::UInt32,
                name: "b".to_string(),
            },
        );

        node.telemetry_groups.insert(
            1,
            TelemetryGroupDefinition {
                fields: vec![10, 11],
            },
        );

        manager.can_nodes.insert(5, node);
    }
}
