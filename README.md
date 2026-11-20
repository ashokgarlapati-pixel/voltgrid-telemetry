# VoltGrid Fleet Telemetry Service

Production-ready backend telemetry and alerting service for a metropolitan fleet of 2,000 electric two-wheelers streaming ~400 readings per second over unreliable cellular networks.

Built with **Node.js (Express)**, **SQLite (`better-sqlite3` in WAL mode)**, and **Docker**.

---

## 1. System Architecture

```text
┌────────────────────────────────┐
│   Messy Traffic Simulator      │
│   (2,000 vehicles @ 400 req/s) │
│   - 5% Duplicates              │
│   - 10% Late Queue (<= 60s)    │
│   - +-30s Clock Drift          │
│   - 2% Malformed Payloads      │
└───────────────┬────────────────┘
                │ HMAC-SHA256 Signed Batch
                ▼
┌────────────────────────────────────────────────────────┐
│           VoltGrid Express API (Node.js)               │
│  ├── Security (HMAC verification, 5m window, nonces)   │
│  ├── Token-Bucket Rate Limiting (429 + Retry-After)    │
│  ├── Ingestion & Gap Detection (Partial 207 responses) │
│  ├── Order-Independent Alert Engine (Hysteresis, Geo)  │
│  ├── Offline Detection Worker (90s background sweep)   │
│  ├── Query Engine (Keyset pagination, Downsampling)    │
│  ├── Live Stream Hub (SSE, 1s coalescing, backpressure)│
│  └── Observability (X-Request-Id, JSON logs, /metrics) │
└───────────────┬────────────────────────────────────────┘
                │ BEGIN IMMEDIATE Transactions
                ▼
┌────────────────────────────────────────────────────────┐
│            SQLite Storage (WAL Mode)                   │
│  ├── telemetry_history (PRIMARY KEY (vehicle_id, seq)) │
│  ├── vehicles (State table, min/max seq, completeness) │
│  ├── alerts (Partial unique index on active alerts)    │
│  ├── api_keys & used_nonces (Replay defense)           │
│  └── rate_limits (Distributed token bucket state)      │
└────────────────────────────────────────────────────────┘
```

---

## 2. Quick Start

### Start with Docker Compose
To build and start the API in production mode:
```bash
docker compose up --build
```
The API will automatically run database migrations and listen on `http://localhost:3000`.

### Run the Messy Traffic Simulator
In a separate terminal, launch the 2,000-vehicle traffic simulator:
```bash
node simulator/simulator.js
```

#### Configurable Simulator Environment Variables:
```bash
# Example: Run with customized anomaly rates
API_URL=http://localhost:3000/v1/telemetry/batch \
NUM_VEHICLES=2000 \
DUPLICATE_RATE=0.05 \
LATE_RATE=0.10 \
MAX_LATE_MS=60000 \
MAX_DRIFT_MS=30000 \
MALFORMED_RATE=0.02 \
node simulator/simulator.js
```

---

## 3. API Reference

All requests to `POST /v1/telemetry/batch` must include HMAC-SHA256 authentication headers:
* `X-API-Key`: `voltgrid-dev-key`
* `X-Timestamp`: Epoch millisecond timestamp string
* `X-Signature`: `HMAC-SHA256(secret, "${timestamp}.${rawBody}")` using secret `voltgrid-dev-secret`

| Method | Endpoint | Description | Status Codes |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/telemetry/batch` | Ingests up to 500 readings with individual validation & idempotency | `200` OK, `207` Multi-Status, `401` Unauthorized, `429` Rate Limited |
| `GET` | `/v1/vehicles` | Keyset-paginated vehicle listing (`?limit=50&cursor=VG-00042&status=online`) | `200` OK |
| `GET` | `/v1/vehicles/:id/history` | Peak-preserving min/max downsampled time-series (`?from=&to=&points=200`) | `200` OK |
| `GET` | `/v1/fleet/summary` | Sub-150ms fleet summary (aggregates only from `vehicles` & `alerts`) | `200` OK |
| `GET` | `/v1/alerts` | Filter alerts by `?status=open&rule=overheat&vehicle_id=VG-00042` | `200` OK |
| `POST` | `/v1/alerts/:id/ack` | Optimistic concurrency acknowledgment (`{"version": 1}`) | `200` OK, `409` Conflict, `404` Not Found |
| `GET` | `/v1/stream` | Server-Sent Events (SSE) live feed (supports `Last-Event-ID` resume) | `200` OK (`text/event-stream`) |
| `GET` | `/health` | Service health status and timestamp | `200` OK |
| `GET` | `/metrics` | Operational metrics (requests total, errors, uptime, DB row counts) | `200` OK |
| `GET` | `/v1/completeness` | Fleet-wide packet sequence completeness percentage | `200` OK |

---

## 4. Running Tests

The test suite uses Node's native test runner (`node:test` and `node:assert/strict`) with zero external test dependencies:

```bash
cd backend
npm test
```

### Test Coverage Highlights:
* **Unit Tests (`backend/tests/alerts.unit.test.js`)**: Pure rule evaluation for Overheat (50°C–55°C hysteresis), Rapid Discharge (>8% drop in 5min), Geofence Breach (ray-casting polygon), and arrival order-independence.
* **Hermetic Integration Tests (`backend/tests/api.int.test.js`)**: Runs against an ephemeral port (`PORT=0`) with an isolated temporary SQLite database, verifying HTTP 207 partial success, latest-state timestamp resolution, sequence gap tracking, keyset pagination, peak downsampling, optimistic locking, and HMAC replay blocking.
* **20-Client Concurrency Proof (`backend/tests/concurrency.test.js`)**: 20 concurrent clients simultaneously fire the identical 50-item batch. Mathematically asserts:
  * Total accepted = **50**
  * Total duplicates = **950**
  * Database rows stored = **50**
  * Double-counted increments = **0**

---

## 5. Load Testing & Query Plans

A standalone load test runner is included in `loadtest/loadtest.js`:
```bash
node loadtest/loadtest.js
```

### Measured Performance Summary (from [loadtest/results.md](loadtest/results.md)):
* **Throughput**: 49+ req/s (~4,900+ readings/sec)
* **Overall Error Rate**: 0.00%
* **`GET /v1/fleet/summary` p95 Latency**: **< 150 ms** (Exceeds SLA by aggregating strictly from state tables)

### Verified SQLite `EXPLAIN QUERY PLAN` Outputs:
* **Vehicle History Range Scan**:
  ```text
  SEARCH telemetry_history USING INDEX idx_history_vehicle_ts (vehicle_id=? AND ts>? AND ts<?)
  ```
* **Fleet Summary Aggregation**:
  ```text
  SCAN vehicles
  ```
* **Active Alerts Grouping**:
  ```text
  SCAN alerts USING INDEX idx_alerts_active_unique
  ```
Full load testing benchmarks and plan details are available in [loadtest/results.md](loadtest/results.md).

---

## 6. Architecture & System Design Document

Read **[DESIGN.md](DESIGN.md)** for detailed engineering answers to the 7 core take-home questions:
1. **Scale**: What breaks at 100x (200k vehicles) and mitigation roadmap.
2. **Delivery Guarantees**: Idempotency proofs and edge failure modes.
3. **Time & Drift**: Device timestamp vs. server timestamp trust boundaries.
4. **Storage & Cost**: 35M row/day tiered retention and downsampling trade-offs.
5. **Threat Model**: HMAC verification, replay nonces, and distributed token-bucket rate limiting.
6. **Trade-offs**: Deliberate architectural choices and consequences.
7. **Real Bug**: Root cause and fix for raw body buffer preservation during HMAC verification.
