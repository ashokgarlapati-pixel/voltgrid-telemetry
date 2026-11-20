-- VoltGrid Fleet Telemetry Service - Migration 002
-- Schema setup for Alert Engine, Lifecycle Management, and Query Optimization

CREATE TABLE IF NOT EXISTS alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id  TEXT NOT NULL,
    rule        TEXT NOT NULL, -- 'overheat', 'rapid_discharge', 'geofence_breach', 'offline'
    status      TEXT NOT NULL DEFAULT 'open', -- 'open', 'acknowledged', 'resolved'
    version     INTEGER NOT NULL DEFAULT 1, -- Optimistic concurrency control for A4/ack
    details     TEXT, -- JSON context (e.g. peak temp, drop points, coordinates)
    opened_at   TEXT NOT NULL,
    resolved_at TEXT,
    updated_at  TEXT NOT NULL
);

-- Partial unique index guarantees that at most ONE active (open or acknowledged)
-- alert can exist per (vehicle_id, rule) at any point in time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_active_unique 
    ON alerts (vehicle_id, rule) 
    WHERE status != 'resolved';

-- Index for filtering alerts by lifecycle status and rule type
CREATE INDEX IF NOT EXISTS idx_alerts_status_rule 
    ON alerts (status, rule);

-- Index for fetching active alerts for a specific vehicle
CREATE INDEX IF NOT EXISTS idx_alerts_vehicle_status 
    ON alerts (vehicle_id, status);

-- Composite covering index for vehicle history subquery in alertEngine (ORDER BY ts DESC, seq DESC)
CREATE INDEX IF NOT EXISTS idx_history_vehicle_ts_seq 
    ON telemetry_history (vehicle_id, ts DESC, seq DESC);

-- Composite index for offline worker stale vehicle scans
CREATE INDEX IF NOT EXISTS idx_vehicles_status_updated 
    ON vehicles (status, updated_at);

-- Composite index for keyset pagination with status filter (eliminates temporary B-tree sorting)
CREATE INDEX IF NOT EXISTS idx_vehicles_status_id 
    ON vehicles (status, vehicle_id);

-- Index for chronologically sorting alert queries (GET /v1/alerts)
CREATE INDEX IF NOT EXISTS idx_alerts_opened_at 
    ON alerts (opened_at DESC, id DESC);

-- Partial index for active alert grouping in fleet summary (GET /v1/fleet/summary)
CREATE INDEX IF NOT EXISTS idx_alerts_active_rule 
    ON alerts (rule) 
    WHERE status != 'resolved';
