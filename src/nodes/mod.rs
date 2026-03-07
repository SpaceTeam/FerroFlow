//! Contains code for managing the CAN nodes that are connected to FerroFlow, their fields and data types.

mod can_node;
mod node_manager;

use std::collections::HashMap;

use liquidcan::{
    CanMessage, CanMessageId,
    payloads::{
        CanDataType, FieldGetResPayload, FieldRegistrationPayload, NodeInfoResPayload,
        TelemetryGroupDefinitionPayload, TelemetryGroupUpdatePayload,
    },
};
use socketcan::{CanAnyFrame, EmbeddedFrame, Frame, Id};

use can_node::{CanNode, FieldInfo, RegistrationInfo, TelemetryGroupDefinition};

pub struct NodeManager {
    can_nodes: HashMap<u8, CanNode>,

    // Nodes that did not yet receive all their field registrations.
    registering_nodes: HashMap<u8, CanNode>,
}

impl NodeManager {
    pub fn new() -> Self {
        Self {
            can_nodes: HashMap::new(),
            registering_nodes: HashMap::new(),
        }
    }

    pub fn handle_can_message_from_node(&mut self, frame: CanAnyFrame) {
        match frame {
            CanAnyFrame::Fd(frame) => {
                let raw_id = match frame.id() {
                    Id::Standard(id) => id.as_raw(),
                    Id::Extended(id) => id.standard_id().as_raw(),
                };
                let message_id = raw_id.into();
                let Ok(message) = CanMessage::try_from(frame) else {
                    eprintln!("Failed to parse CAN frame into CanMessage: {:?}", frame);
                    return;
                };

                match message {
                    CanMessage::NodeInfoAnnouncement { payload } => {
                        self.handle_node_info_announcement(message_id, payload);
                    }
                    CanMessage::TelemetryValueRegistration { payload } => {
                        self.handle_field_registration(message_id, payload, true);
                    }
                    CanMessage::ParameterRegistration { payload } => {
                        self.handle_field_registration(message_id, payload, false);
                    }
                    CanMessage::TelemetryGroupDefinition { payload } => {
                        self.handle_telemetry_group_definition(message_id, payload);
                    }
                    CanMessage::TelemetryGroupUpdate { payload } => {
                        self.handle_telemetry_group_update(message_id, payload);
                    }
                    CanMessage::FieldGetRes { payload } => {
                        self.handle_field_get_res(message_id, payload);
                    }
                    _ => {
                        eprintln!(
                            "Received unsupported CAN message from node {}: {:?}",
                            message_id.sender_id(),
                            message
                        );
                    }
                }
            }
            _ => {
                eprintln!(
                    "Received non-FD CAN frame, which is not supported: {:?}",
                    frame
                );
            }
        }
    }

    pub fn handle_node_info_announcement(
        &mut self,
        can_msg_id: CanMessageId,
        node_info_res: NodeInfoResPayload,
    ) {
        let node_id = can_msg_id.sender_id();
        let registration_info = RegistrationInfo {
            telemetry_count: node_info_res.tel_count,
            parameter_count: node_info_res.par_count,
            firmware_hash: node_info_res.firmware_hash,
            protocol_hash: node_info_res.liquid_hash,
            device_name: node_info_res.device_name.into(),
        };

        self.register_node(node_id, registration_info);
    }

    pub fn handle_field_registration(
        &mut self,
        can_msg_id: CanMessageId,
        field_registration: FieldRegistrationPayload,
        is_telemetry: bool,
    ) {
        let node_id = can_msg_id.sender_id();
        let field_info = FieldInfo {
            name: field_registration.field_name.into(),
            data_type: field_registration.field_type,
        };

        let node_id = node_id;
        if let Some(node) = self.registering_nodes.get_mut(&node_id) {
            let id = field_registration.field_id;
            if is_telemetry {
                node.telemetry_fields.insert(id, field_info);
            } else {
                node.parameter_fields.insert(id, field_info);
            }

            if node.field_registration_complete() {
                let completed_node = self.registering_nodes.remove(&node_id).unwrap();
                self.can_nodes.insert(node_id, completed_node);
            }
        } else {
            eprintln!(
                "Received field registration for node {} but it is not currently registering",
                node_id
            );
        }
    }

    pub fn handle_telemetry_group_definition(
        &mut self,
        can_msg_id: CanMessageId,
        group_definition: TelemetryGroupDefinitionPayload,
    ) {
        let node_id = can_msg_id.sender_id();

        if let Some(node) = self.can_nodes.get_mut(&node_id) {
            let fields: &[u8] = (&group_definition.field_ids).into();
            let group = TelemetryGroupDefinition {
                fields: fields.into(),
            };
            node.telemetry_groups
                .insert(group_definition.group_id, group);
        } else {
            eprintln!(
                "Received telemetry group definition for node {} but it is not registered",
                node_id
            );
        }
    }

    pub fn handle_telemetry_group_update(
        &mut self,
        can_msg_id: CanMessageId,
        group_update: TelemetryGroupUpdatePayload,
    ) {
        let node_id = can_msg_id.sender_id();

        let Some(node) = self.can_nodes.get_mut(&node_id) else {
            eprintln!(
                "Received telemetry group update for node {} but it is not registered",
                node_id
            );
            return;
        };

        let group_id = group_update.group_id;

        let Some(group_definition) = node.telemetry_groups.get(&group_id) else {
            eprintln!(
                "Received telemetry group update for node {} and group {} but the group is not defined",
                node_id, group_id
            );
            return;
        };

        // Check if we have all the definitions for the fields in this group, and if so unpack the data.
        if !group_definition
            .fields
            .iter()
            .all(|id| node.telemetry_fields.contains_key(id))
        {
            eprintln!(
                "Received telemetry group update for node {} and group {} but we don't have definitions for all the fields",
                node_id, group_id
            );
            return;
        }

        let field_types = group_definition
            .fields
            .iter()
            .map(|id| node.telemetry_fields.get(id).unwrap().data_type);

        for (id, value) in group_definition
            .fields
            .iter()
            .zip(group_update.values.unpack(field_types))
        {
            match value {
                Ok(value) => {
                    node.values.insert(*id, value);
                }
                Err(e) => {
                    eprintln!(
                        "Failed to unpack value for node {} group {} field {}: {:?}",
                        node_id, group_id, id, e
                    );
                }
            }
        }
    }

    pub fn handle_field_get_res(&mut self, can_msg_id: CanMessageId, res: FieldGetResPayload) {
        let node_id = can_msg_id.sender_id();

        let Some(node) = self.can_nodes.get_mut(&node_id) else {
            eprintln!(
                "Received field get response for node {} but it is not registered",
                node_id
            );
            return;
        };

        let field_id = res.field_id;
        let field_info = node
            .telemetry_fields
            .get_mut(&field_id)
            .or_else(|| node.parameter_fields.get_mut(&field_id));

        let Some(field_info) = field_info else {
            eprintln!(
                "Received field get response for node {} field {} but we don't have a definition for this field",
                node_id, field_id
            );
            return;
        };

        let field_type = field_info.data_type;

        let Ok(value) = res.value.convert_from_raw(field_type) else {
            eprintln!(
                "Failed to convert field get response value for node {} field {}: {:?}",
                node_id, field_id, res.value
            );
            return;
        };

        node.values.insert(field_id, value);
    }

    fn register_node(&mut self, node_id: u8, registration_info: RegistrationInfo) {
        let node = CanNode::new(registration_info);
        self.registering_nodes.insert(node_id, node);
    }
}
