use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde_json::Value;

use super::timescale_schema::field_logs;

#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = field_logs)]
pub struct FieldLog {
    pub timestamp: DateTime<Utc>,
    pub node_id: i16,
    pub field_id: i16,
    pub field_name: String,
    pub field_value: Value,
}
