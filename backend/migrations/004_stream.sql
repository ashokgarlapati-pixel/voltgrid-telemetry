-- Migration 004_stream.sql: Persistent Event Log (Outbox) for Live SSE Stream (Part A5)

-- Durable event store for Server-Sent Events (SSE)
-- Provides monotonic, restart-persistent event IDs and support for Last-Event-ID resumption.
CREATE TABLE IF NOT EXISTS stream_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  data TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Covering index on id for sub-millisecond range queries during Last-Event-ID replay
CREATE INDEX IF NOT EXISTS idx_stream_events_id ON stream_events (id ASC);
