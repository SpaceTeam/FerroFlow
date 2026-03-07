use std::collections::HashMap;

use liquidcan::payloads::{CanDataType, CanDataValue};

pub struct CanNode {
    pub registration_info: RegistrationInfo,
    pub telemetry_fields: HashMap<u8, FieldInfo>,
    pub parameter_fields: HashMap<u8, FieldInfo>,
    pub telemetry_groups: HashMap<u8, TelemetryGroupDefinition>,
    pub values: HashMap<u8, CanDataValue>,
}

impl CanNode {
    pub fn new(registration_info: RegistrationInfo) -> Self {
        Self {
            registration_info,
            telemetry_fields: HashMap::new(),
            parameter_fields: HashMap::new(),
            telemetry_groups: HashMap::new(),
            values: HashMap::new(),
        }
    }

    pub fn field_registration_complete(&self) -> bool {
        self.telemetry_fields.len() == self.registration_info.telemetry_count as usize
            && self.parameter_fields.len() == self.registration_info.parameter_count as usize
    }
}

pub struct RegistrationInfo {
    pub telemetry_count: u8,
    pub parameter_count: u8,
    pub firmware_hash: u32,
    pub protocol_hash: u32,
    pub device_name: String,
}

pub struct FieldInfo {
    pub data_type: CanDataType,
    pub name: String,
}

pub struct TelemetryGroupDefinition {
    pub fields: Vec<u8>,
}
