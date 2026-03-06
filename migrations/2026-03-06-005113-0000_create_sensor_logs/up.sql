CREATE TABLE sensor_logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type VARCHAR NOT NULL,
    payload JSONB NOT NULL
);

-- Index the timestamp for fast time-based querying later
CREATE INDEX idx_sensor_logs_timestamp ON sensor_logs(timestamp);