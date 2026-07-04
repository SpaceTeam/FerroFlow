use anyhow::Result;
use liquidcan::payloads::CanDataValue;

use crate::nodes::mapping::FieldType;

use super::super::can_node::CanNode;
use super::NodeManager;

#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub id: u8,
    pub name: String,
    pub fields: Vec<FieldSnapshot>,
}

#[derive(Clone, Debug)]
pub struct FieldSnapshot {
    pub id: u8,
    pub name: String,
    pub raw_name: String,
    pub mapped_name: Option<String>,
    pub kind: FieldType,
}

#[derive(Clone, Debug)]
pub struct NodeTelemetrySnapshot {
    pub id: u8,
    pub name: String,
    pub telemetry: Vec<FieldValueSnapshot>,
}

#[derive(Clone, Debug)]
pub struct FieldValueSnapshot {
    pub node_id: u8,
    pub id: u8,
    pub node_name: String,
    pub raw_name: String,
    pub mapped_name: Option<String>,
    pub name: String,
    pub raw: serde_json::Value,
    pub value: serde_json::Value,
    pub unit: String,
    pub logical: serde_json::Value,
}

impl<'a> NodeManager<'a> {
    pub fn nodes_snapshot(&self) -> Vec<NodeSnapshot> {
        let mut nodes = self
            .can_nodes
            .iter()
            .map(|node| {
                let node_id = *node.key();
                let node_name = node.registration_info.device_name.clone();
                let mut fields = Vec::new();

                for (node_fields, field_type) in [
                    (&node.telemetry_fields, FieldType::Telemetry),
                    (&node.parameter_fields, FieldType::Parameter),
                ] {
                    fields.extend(node_fields.iter().map(|(&id, field)| {
                        // TODO: we should avoid having to look up the mapping every time
                        let mapped_name = self
                            .mapping
                            .get_mapping_for_raw(&node_name, &field.name)
                            .map(|mapping| mapping.mapping_entry.name.clone());
                        let name = mapped_name.clone().unwrap_or_else(|| field.name.clone());
                        FieldSnapshot {
                            id,
                            name,
                            raw_name: field.name.clone(),
                            mapped_name,
                            kind: field_type,
                        }
                    }))
                }

                NodeSnapshot {
                    id: node_id,
                    name: node_name,
                    fields,
                }
            })
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| node.id);
        nodes
    }

    pub fn telemetry_snapshot(&self) -> Vec<NodeTelemetrySnapshot> {
        self.can_nodes
            .iter()
            .filter_map(|node| {
                let telemetry = node
                    .values
                    .iter()
                    .filter_map(|value| {
                        self.field_value_snapshot_from_node(*node.key(), &node, *value.key())
                    })
                    .collect::<Vec<_>>();

                if telemetry.is_empty() {
                    return None;
                }

                Some(NodeTelemetrySnapshot {
                    id: *node.key(),
                    name: node.registration_info.device_name.clone(),
                    telemetry,
                })
            })
            .collect()
    }

    pub fn field_value_snapshot_by_id(
        &self,
        node_id: u8,
        field_id: u8,
    ) -> Option<FieldValueSnapshot> {
        self.can_nodes
            .get(&node_id)
            .and_then(|node| self.field_value_snapshot_from_node(node_id, &node, field_id))
    }

    pub fn field_value_snapshot_by_mapped_name(
        &self,
        mapped_name: &str,
    ) -> Result<Option<FieldValueSnapshot>> {
        let (_, target) = self.resolve_mapping_by_name(mapped_name)?;

        Ok(self.field_value_snapshot_by_id(target.node_id, target.field_id))
    }

    pub(super) fn can_data_value_to_json(value: CanDataValue) -> serde_json::Value {
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

    fn field_value_snapshot_from_node(
        &self,
        node_id: u8,
        node: &CanNode,
        field_id: u8,
    ) -> Option<FieldValueSnapshot> {
        let node_name = &node.registration_info.device_name;
        let field = node
            .telemetry_fields
            .get(&field_id)
            .or_else(|| node.parameter_fields.get(&field_id))?;

        let raw_value = node.values.get(&field_id).map(|value| value.1.clone())?;
        let raw_json = Self::can_data_value_to_json(raw_value.clone());
        let mapping = self.mapping.get_mapping_for_raw(node_name, &field.name);

        let (mapped_name, name, value, unit, logical) = if let Some(mapping) = mapping {
            let mapped = mapping.mapping_entry.mapped_value(&raw_value).ok();
            let value = mapped
                .as_ref()
                .map(|mapped| serde_json::json!(mapped.value))
                .unwrap_or_else(|| raw_json.clone());
            let unit = mapped
                .as_ref()
                .map(|mapped| mapped.unit.clone())
                .unwrap_or_default();
            let logical = mapped
                .and_then(|mapped| mapping.mapping_entry.logical_value(mapped.value))
                .and_then(|logical| serde_json::to_value(logical.value).ok())
                .unwrap_or(serde_json::Value::Null);

            (
                Some(mapping.mapping_entry.name.clone()),
                mapping.mapping_entry.name.clone(),
                value,
                unit,
                logical,
            )
        } else {
            (
                None,
                field.name.clone(),
                raw_json.clone(),
                String::new(),
                serde_json::Value::Null,
            )
        };

        Some(FieldValueSnapshot {
            node_id,
            id: field_id,
            node_name: node_name.clone(),
            raw_name: field.name.clone(),
            mapped_name,
            name,
            raw: raw_json,
            value,
            unit,
            logical,
        })
    }
}
