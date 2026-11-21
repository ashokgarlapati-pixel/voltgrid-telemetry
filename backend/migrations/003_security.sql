-- Migration 003: Security tables for HMAC Authentication and Distributed Rate Limiting

-- Device group API keys and secret credentials
CREATE TABLE IF NOT EXISTS api_keys (
    api_key TEXT PRIMARY KEY,
    secret TEXT NOT NULL,
    rate_limit_per_sec REAL NOT NULL DEFAULT 10,
    burst_capacity INTEGER NOT NULL DEFAULT 20
);

-- Seed default device-group credentials for development and testing ONLY.
-- Note: Production environments (NODE_ENV=production) strictly purge and reject
-- these credentials upon startup via validateAndInitSecurityCredentials().
INSERT OR IGNORE INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
VALUES ('voltgrid-dev-key', 'voltgrid-dev-secret', 20, 40);

-- Request nonces / signatures table to prevent replay attacks within the 5-minute window
CREATE TABLE IF NOT EXISTS used_nonces (
    signature TEXT PRIMARY KEY,
    expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_used_nonces_expires ON used_nonces (expires_at);

-- Distributed token bucket table (shared state across multiple API instances)
CREATE TABLE IF NOT EXISTS rate_limits (
    api_key TEXT PRIMARY KEY,
    tokens REAL NOT NULL,
    last_refill_ms INTEGER NOT NULL
);
