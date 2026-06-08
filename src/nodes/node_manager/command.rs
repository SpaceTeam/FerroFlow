use anyhow::{Context, Result, anyhow};
use liquidcan::{
    CanMessage,
    payloads::{CanDataType, CanDataValue, FieldGetReqPayload, ParameterSetReqPayload},
};
use serde_json::Value;

use crate::{events, nodes::mapping};

use super::{NodeManager, ResolvedMappingTarget};

impl<'a> NodeManager<'a> {
    /// Sends a `FieldGetReq` for the raw field.
    ///
    /// The response is processed asynchronously by the normal CAN message handler.
    pub fn request_value(&self, field_name: &str) -> Result<()> {
        let (_, target) = self.resolve_mapping_by_name(field_name)?;

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

    /// Writes a mapped value.
    ///
    /// The value is converted back to the raw CAN type using the inverse of the configured linear
    /// mapping, then sent as a `ParameterSetReq`.
    pub fn set_mapped_value(&self, field_name: &str, mapped_value: f64) -> Result<()> {
        let (mapping_lookup, target) = self.resolve_mapping_by_name(field_name)?;

        if mapping_lookup.mapping_entry.field_type != mapping::FieldType::Parameter {
            anyhow::bail!(
                "mapped field {field_name} is not writable because it is not a parameter"
            );
        }

        let raw_value = mapping_lookup
            .mapping_entry
            .raw_value_from_mapped(mapped_value, target.data_type)?;
        self.dispatch_parameter_set(target, raw_value);

        Ok(())
    }

    /// Writes a raw CAN value.
    pub fn set_raw_value(&self, field_name: &str, value: Value) -> Result<()> {
        let (mapping_lookup, target) = self.resolve_mapping_by_name(field_name)?;

        if mapping_lookup.mapping_entry.field_type != mapping::FieldType::Parameter {
            anyhow::bail!(
                "mapped field {field_name} is not writable because it is not a parameter"
            );
        }

        let raw_value = json_value_to_can_data_value(value, target.data_type)?;

        self.dispatch_parameter_set(target, raw_value);

        Ok(())
    }

    /// Writes a value, either raw or mapped depending on the field name format.
    ///
    /// If `field_name` is a raw name, `value` is converted directly to a `CanDataValue` and sent as-is.
    /// If `field_name` is a mapped name, `value` is converted using the inverse of the configured linear mapping before being sent.
    pub fn set_value(&self, field_name: &str, value: Value) -> Result<()> {
        if Self::is_mapped_name(field_name) {
            self.set_mapped_value(field_name, json_value_to_f64(&value)?)?;
        } else {
            self.set_raw_value(field_name, value)?;
        }
        Ok(())
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
}

pub fn json_value_to_can_data_value(
    value: serde_json::Value,
    data_type: CanDataType,
) -> Result<CanDataValue> {
    match data_type {
        CanDataType::Float32 => Ok(CanDataValue::Float32(json_value_to_f64(&value)? as f32)),
        CanDataType::Int32 => Ok(CanDataValue::Int32(json_value_to_integer(&value)?)),
        CanDataType::Int16 => Ok(CanDataValue::Int16(json_value_to_integer(&value)?)),
        CanDataType::Int8 => Ok(CanDataValue::Int8(json_value_to_integer(&value)?)),
        CanDataType::UInt32 => Ok(CanDataValue::UInt32(json_value_to_integer(&value)?)),
        CanDataType::UInt16 => Ok(CanDataValue::UInt16(json_value_to_integer(&value)?)),
        CanDataType::UInt8 => Ok(CanDataValue::UInt8(json_value_to_integer(&value)?)),
        CanDataType::Boolean => Ok(CanDataValue::Boolean(json_value_as_bool(&value)?)),
    }
}

fn json_value_to_f64(value: &serde_json::Value) -> Result<f64> {
    match value {
        Value::Number(num) => num
            .as_f64()
            .with_context(|| format!("expected numeric value, got {value}")),
        Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        _ => Err(anyhow!("expected numeric or boolean value, got {value}")),
    }
}

fn json_value_to_integer<T>(value: &serde_json::Value) -> Result<T>
where
    T: TryFrom<i64>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    let raw = match value {
        Value::Number(num) => num
            .as_i64()
            .with_context(|| format!("expected integer value, got {value}")),
        Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
        _ => Err(anyhow!("expected integer or boolean value, got {value}")),
    }?;
    T::try_from(raw).map_err(|_| anyhow!("integer value {raw} is out of range"))
}

fn json_value_as_bool(value: &serde_json::Value) -> Result<bool> {
    match value {
        Value::Bool(b) => Ok(*b),
        Value::Number(num) => num
            .as_i64()
            .map(|value| value != 0)
            .with_context(|| format!("expected boolean-compatible value, got {value}")),
        _ => Err(anyhow!("expected boolean-compatible value, got {value}")),
    }
}
