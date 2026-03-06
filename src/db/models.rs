use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde_json::Value;

use super::schema::sensor_logs;

#[derive(Insertable, Debug)]
#[diesel(table_name = sensor_logs)]
pub struct SensorLog {
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub payload: Value,
}
