use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde_json::Value;

use super::schema::telemetry_logs;

#[derive(Insertable, Debug)]
#[diesel(table_name = telemetry_logs)]
pub struct TelemetryLog {
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub payload: Value,
}
