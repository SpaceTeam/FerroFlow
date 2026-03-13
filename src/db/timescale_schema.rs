// Manually generated schema for TimescaleDB hypertable. This is necessary because Diesel's CLI tool doesn't support generating schemas for tables that do not include a primary key.

diesel::table! {
    field_logs (timestamp, node_id, field_id) {
        timestamp -> Timestamptz,
        node_id -> Nullable<Int2>,
        field_id -> Nullable<Int2>,
        field_name -> Nullable<Varchar>,
        field_value -> Jsonb,
    }
}
