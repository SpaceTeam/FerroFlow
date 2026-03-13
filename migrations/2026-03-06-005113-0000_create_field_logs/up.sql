CREATE TABLE field_logs (
    timestamp TIMESTAMPTZ NOT NULL,
    node_id smallint not null,
    field_id smallint not null,
    field_name VARCHAR not null,
    field_value JSONB NOT NULL
);

-- convert to a TimescaleDB hypertable
SELECT create_hypertable('field_logs', by_range('timestamp'));

-- enable compression segmented by event_type
ALTER TABLE field_logs SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, field_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- compress data older than 7 days
SELECT add_compression_policy('field_logs', INTERVAL '7 days');