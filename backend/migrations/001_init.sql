-- VoltGrid Fleet Telemetry Service - Migration 001
-- Initial schema setup for telemetry history and current vehicle state

-- 1. telemetry_history table
-- Stores immutable time-series readings for all vehicles.
-- Composite primary key (vehicle_id, seq) enforces database-level idempotency.
CREATE TABLE IF NOT EXISTS telemetry_history (
    vehicle_id      TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    ts              TEXT NOT NULL,
    soc_pct         REAL NOT NULL,
    battery_temp_c  REAL NOT NULL,
    speed_kmh       REAL NOT NULL,
    lat             REAL NOT NULL,
    lng             REAL NOT NULL,
    odometer_km     REAL NOT NULL,
    charging        INTEGER NOT NULL,
    fault_codes     TEXT NOT NULL,
    received_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (vehicle_id, seq)
);

-- Index for fast time-series retrieval by vehicle and timestamp
CREATE INDEX IF NOT EXISTS idx_history_vehicle_ts 
    ON telemetry_history (vehicle_id, ts);

-- 2. vehicles table
-- Maintains exactly 1 row per vehicle representing its latest known operational state
-- and sequence tracking metrics for completeness auditing.
CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id      TEXT PRIMARY KEY,
    last_ts         TEXT NOT NULL,
    last_seq        INTEGER NOT NULL,
    soc_pct         REAL NOT NULL,
    battery_temp_c  REAL NOT NULL,
    speed_kmh       REAL NOT NULL,
    lat             REAL NOT NULL,
    lng             REAL NOT NULL,
    odometer_km     REAL NOT NULL,
    charging        INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'online',
    min_seq         INTEGER NOT NULL,
    max_seq         INTEGER NOT NULL,
    received_count  INTEGER NOT NULL DEFAULT 1,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Index for filtering fleet by status (online, offline, maintenance)
CREATE INDEX IF NOT EXISTS idx_vehicles_status 
    ON vehicles (status);

-- Spatial index for geospatial bounding box and distance queries
CREATE INDEX IF NOT EXISTS idx_vehicles_lat_lng 
    ON vehicles (lat, lng);
