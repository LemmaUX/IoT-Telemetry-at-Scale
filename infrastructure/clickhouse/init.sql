-- ============================================================================
-- PulseGrid ClickHouse Schema
-- ============================================================================
-- Design decisions:
-- 1. ReplacingMergeTree for idempotent upserts (handles Flink replay)
-- 2. PARTITION BY toYYYYMM for efficient time-range queries
-- 3. ORDER BY (device_id, event_time, seq) for device-specific lookups
-- 4. TTL for automatic data expiration (30 days hot retention)
-- ============================================================================

CREATE DATABASE IF NOT EXISTS pulsegrid;

-- Main telemetry table (hot storage)
CREATE TABLE IF NOT EXISTS pulsegrid.telemetry (
    event_id String,
    device_id String,
    event_time DateTime64(3),
    ingestion_time DateTime64(3) DEFAULT now64(3),
    device_sequence_number UInt64,
    
    -- Sensor measurements
    temperature Float32,
    vibration Float32,
    voltage Float32,
    
    -- Quality and metadata
    quality_flag Int32,
    schema_version String,
    
    -- Derived fields (for dashboards)
    device_type LowCardinality(String) DEFAULT 'unknown',
    location LowCardinality(String) DEFAULT 'unknown'
    
) ENGINE = ReplacingMergeTree(device_sequence_number)
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time, device_sequence_number)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Aggregated table for dashboards (pre-computed rollups)
CREATE MATERIALIZED VIEW IF NOT EXISTS pulsegrid.telemetry_1m_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (device_id, window_start)
AS SELECT
    device_id,
    toStartOfMinute(event_time) AS window_start,
    count() AS event_count,
    avgState(temperature) AS avg_temperature,
    maxState(temperature) AS max_temperature,
    avgState(vibration) AS avg_vibration,
    maxState(vibration) AS max_vibration,
    avgState(voltage) AS avg_voltage,
    minState(voltage) AS min_voltage
FROM pulsegrid.telemetry
GROUP BY device_id, window_start;

-- Alerts table (for operational dashboards)
CREATE TABLE IF NOT EXISTS pulsegrid.alerts (
    alert_id String,
    alert_type LowCardinality(String),
    severity Int32,
    device_id String,
    alert_time DateTime64(3),
    message String,
    schema_version String
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(alert_time)
ORDER BY (severity, alert_time, device_id)
TTL alert_time + INTERVAL 90 DAY;