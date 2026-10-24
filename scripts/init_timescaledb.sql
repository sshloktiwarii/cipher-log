-- ============================================================================
-- Local Telemetry & Anomaly Ingestion Engine - TimescaleDB Initialization
-- ============================================================================

-- 1. Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 2. Core time-series hypertable
CREATE TABLE IF NOT EXISTS telemetry_events (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    metrics JSONB NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. Convert standard table to TimescaleDB hypertable partitioned by time (7-day chunks)
SELECT create_hypertable(
    'telemetry_events',
    'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- 4. Create composite high-throughput indexes
CREATE INDEX IF NOT EXISTS idx_telemetry_source_time ON telemetry_events (source, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_time ON telemetry_events (event_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_metrics_gin ON telemetry_events USING gin (metrics);
CREATE INDEX IF NOT EXISTS idx_telemetry_metadata_gin ON telemetry_events USING gin (metadata);

-- 5. Enable native TimescaleDB chunk compression (after 3 days)
ALTER TABLE telemetry_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'source, event_type',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('telemetry_events', INTERVAL '3 days', if_not_exists => TRUE);

-- 6. Enforce 30-day automated retention policy
SELECT add_retention_policy('telemetry_events', INTERVAL '30 days', if_not_exists => TRUE);
