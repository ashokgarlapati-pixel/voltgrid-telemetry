-- VoltGrid Fleet Telemetry Service - Migration 005
-- Transactional Outbox for Crash-Resilient Alert Evaluation
-- Stores pending alert evaluation jobs in the same transaction as telemetry ingestion.
-- Ensures that if the application crashes before alert evaluation, the background
-- worker recovers and processes pending alerts upon restart without dropping events.

CREATE TABLE IF NOT EXISTS alert_outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    claimed_at    TEXT,
    processed_at  TEXT,
    error         TEXT
);

-- Index for background worker polling pending jobs in FIFO order
CREATE INDEX IF NOT EXISTS idx_alert_outbox_status_id 
    ON alert_outbox (status, id ASC);

-- Index for vehicle-specific lookups and status updates
CREATE INDEX IF NOT EXISTS idx_alert_outbox_vehicle_status 
    ON alert_outbox (vehicle_id, status);

-- Index for processing lease timeout and stale job reclamation
CREATE INDEX IF NOT EXISTS idx_alert_outbox_status_claimed 
    ON alert_outbox (status, claimed_at);
