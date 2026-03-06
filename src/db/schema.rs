// @generated automatically by Diesel CLI.

diesel::table! {
    sensor_logs (id) {
        id -> Int8,
        timestamp -> Timestamptz,
        event_type -> Varchar,
        payload -> Jsonb,
    }
}
