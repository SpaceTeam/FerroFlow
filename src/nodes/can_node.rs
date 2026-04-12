use std::{collections::HashMap, sync::RwLock};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use liquidcan::payloads::{CanDataType, CanDataValue};

pub struct CanNode {
    pub registration_info: RegistrationInfo,
    pub telemetry_fields: HashMap<u8, FieldInfo>,
    pub parameter_fields: HashMap<u8, FieldInfo>,
    pub telemetry_groups: HashMap<u8, TelemetryGroupDefinition>,
    pub values: DashMap<u8, (DateTime<Utc>, CanDataValue)>,
    pub latest_heartbeat_sent: RwLock<Option<(DateTime<Utc>, u32)>>,
    pub latest_heartbeat_received: RwLock<Option<(DateTime<Utc>, u32)>>,
}

impl CanNode {
    pub fn new(registration_info: RegistrationInfo) -> Self {
        Self {
            registration_info,
            telemetry_fields: HashMap::new(),
            parameter_fields: HashMap::new(),
            telemetry_groups: HashMap::new(),
            values: DashMap::new(),
            latest_heartbeat_sent: RwLock::new(None),
            latest_heartbeat_received: RwLock::new(None),
        }
    }

    pub fn node_registration_complete(&self) -> bool {
        let all_fields_registered = self.telemetry_fields.len()
            == self.registration_info.telemetry_count as usize
            && self.parameter_fields.len() == self.registration_info.parameter_count as usize;

        let all_telemetry_groups_registered = self
            .telemetry_groups
            .values()
            .map(|group| group.fields.len())
            .sum::<usize>()
            == self.registration_info.telemetry_count as usize;

        all_fields_registered && all_telemetry_groups_registered
    }
}

#[allow(unused)]
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
