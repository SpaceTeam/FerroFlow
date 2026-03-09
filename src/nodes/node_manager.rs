use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use liquidcan::{
    CanMessage, CanMessageId,
    payloads::{
        CanDataType, FieldGetResPayload, FieldRegistrationPayload, NodeInfoResPayload,
        TelemetryGroupDefinitionPayload, TelemetryGroupUpdatePayload,
    },
};
use socketcan::{CanAnyFrame, EmbeddedFrame, Frame, Id};

use super::can_node::{CanNode, FieldInfo, RegistrationInfo, TelemetryGroupDefinition};

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

    pub fn handle_can_message_from_node(&mut self, frame: CanAnyFrame) -> Result<()> {
        match frame {
            CanAnyFrame::Fd(frame) => {
                let raw_id = match frame.id() {
                    Id::Standard(id) => id.as_raw(),
                    Id::Extended(id) => id.standard_id().as_raw(),
                };
                let message_id: CanMessageId = raw_id.into();
                let message = CanMessage::try_from(frame).with_context(|| {
                    format!(
                        "failed to parse CAN frame into CanMessage for node {}",
                        message_id.sender_id()
                    )
                })?;

                match message {
                    CanMessage::NodeInfoAnnouncement { payload } => {
                        self.handle_node_info_announcement(message_id, payload);
                        Ok(())
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
                    CanMessage::FieldGetRes { payload } => {
                        self.handle_field_get_res(message_id, payload)
                    }
                    _ => bail!(
                        "received unsupported CAN message from node {}: {:?}",
                        message_id.sender_id(),
                        message
                    ),
                }
            }
            _ => bail!(
                "received non-FD CAN frame, which is not supported: {:?}",
                frame
            ),
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
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();
        let field_info = FieldInfo {
            name: field_registration.field_name.into(),
            data_type: field_registration.field_type,
        };

        if let Some(node) = self.registering_nodes.get_mut(&node_id) {
            let id = field_registration.field_id;
            if is_telemetry {
                node.telemetry_fields.insert(id, field_info);
            } else {
                node.parameter_fields.insert(id, field_info);
            }

            if node.field_registration_complete() {
                let completed_node = self.registering_nodes.remove(&node_id).with_context(|| {
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
        &mut self,
        can_msg_id: CanMessageId,
        group_definition: TelemetryGroupDefinitionPayload,
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();

        if let Some(node) = self.can_nodes.get_mut(&node_id) {
            let fields: &[u8] = (&group_definition.field_ids).into();
            let group = TelemetryGroupDefinition {
                fields: fields.into(),
            };
            node.telemetry_groups
                .insert(group_definition.group_id, group);
            Ok(())
        } else {
            bail!(
                "Received telemetry group definition for node {} but it is not registered",
                node_id
            );
        }
    }

    pub fn handle_telemetry_group_update(
        &mut self,
        can_msg_id: CanMessageId,
        group_update: TelemetryGroupUpdatePayload,
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();

        let node = self.can_nodes.get_mut(&node_id).with_context(|| {
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

        let (telemetry_fields, values) = (&node.telemetry_fields, &mut node.values);

        for id in &field_ids {
            // check if we can find the field definition for all fields in the group before trying to unpack any values.
            telemetry_fields.get(id).with_context(|| {
                format!(
                    "received telemetry group update for node {} and group {} but field {} is not defined",
                    node_id, group_id, id
                )
            })?;
        }

        let field_types = field_ids.iter().map(|id| {
            telemetry_fields
                .get(id)
                .map(|field| field.data_type)
                .expect("telemetry field existence validated above")
        });

        for (&id, value) in field_ids
            .iter()
            .zip(group_update.values.unpack(field_types))
        {
            let value = value.with_context(|| {
                format!(
                    "failed to unpack value for node {} group {} field {}",
                    node_id, group_id, id
                )
            })?;
            values.insert(id, value);
        }

        Ok(())
    }

    pub fn handle_field_get_res(
        &mut self,
        can_msg_id: CanMessageId,
        res: FieldGetResPayload,
    ) -> Result<()> {
        let node_id = can_msg_id.sender_id();

        let node = self.can_nodes.get_mut(&node_id).with_context(|| {
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

        node.values.insert(field_id, value);

        Ok(())
    }

    fn register_node(&mut self, node_id: u8, registration_info: RegistrationInfo) {
        let node = CanNode::new(registration_info);
        self.registering_nodes.insert(node_id, node);
    }
}
