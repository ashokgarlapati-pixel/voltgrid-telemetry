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
| `POST` | `/v1/telemetry/batch` | Ingests up to 500 readings with individual validation & idempotency | `200` OK, `207` Multi-Status, `400` Bad Request, `401` Unauthorized, `429` Rate Limited |
| `GET` | `/v1/vehicles` | Keyset-paginated vehicle listing (`?limit=50&cursor=VG-00042&status=online`) | `200` OK |
| `GET` | `/v1/vehicles/:id/history` | Peak-preserving min/max downsampled time-series (`?from=&to=&points=200`) | `200` OK |
| `GET` | `/v1/fleet/summary` | Sub-150ms fleet summary (aggregates only from `vehicles` & `alerts`) | `200` OK |
| `GET` | `/v1/alerts` | Filter alerts by `?status=open&rule=overheat&vehicle_id=VG-00042` | `200` OK |
| `POST` | `/v1/alerts/:id/ack` | Optimistic concurrency acknowledgment (`{"version": 1}`) | `200` OK, `409` Conflict, `404` Not Found |
| `GET` | `/v1/stream` | Server-Sent Events (SSE) live feed (supports `Last-Event-ID` resume) | `200` OK (`text/event-stream`) |
| `GET` | `/health` | Service health status and timestamp | `200` OK |
| `GET` | `/metrics` | Operational metrics (requests total, errors, uptime, DB row counts) | `200` OK |
| `GET` | `/v1/completeness` | Fleet-wide or single-vehicle sequence completeness & gap detection (`?vehicle_id=VG-00042`) | `200` OK, `404` Not Found |

### Telemetry Field Validation Specification
Every reading in `POST /v1/telemetry/batch` is individually validated using `Number.isFinite()` and strict bounds:
* `vehicle_id`: Non-empty string (`trimmed length >= 1`).
* `seq`: Non-negative integer (`Number.isInteger(seq) && seq >= 0`).
* `ts`: Valid ISO-8601 timestamp string (`!isNaN(Date.parse(ts))`).
* `soc_pct`: Finite percentage `[0.0, 100.0]`.
* `battery_temp_c`: Finite temperature `[-40.0°C, 120.0°C]` (automotive thermal envelope).
* `speed_kmh`: Finite speed `[0.0, 200.0] km/h`.
* `lat` / `lng`: Finite coordinates `[-90.0, +90.0]` and `[-180.0, +180.0]`.
* `odometer_km`: Finite non-negative number (`>= 0.0`).
* `charging`: Strict boolean (`typeof charging === 'boolean'`).
* `fault_codes`: Array containing only strings (`code => typeof code === 'string'`).

### State Ordering & Tie-Breaking Specification
* **Highest Device Timestamp**: Current vehicle state in `vehicles` always reflects the reading with the highest device `ts`, NOT server arrival time.
* **Deterministic Tie-Breaking**: If two distinct readings from a vehicle have the exact same millisecond timestamp (`ts1 == ts2`), the higher sequence counter (`seq1 > seq2`) deterministically wins the state upsert. Server arrival order never alters state.

### Sequence Gap Detection & Limitation
* **Observed Interval Completeness**: Completeness is measured as:
  $$\text{Completeness} = \frac{\text{received\_count}}{\text{max\_seq} - \text{min\_seq} + 1}$$
* **Limitation Note**: Sequence gap tracking establishes completeness strictly within the observed interval $[\text{min\_seq}, \text{max\_seq}]$. Sequences dropped prior to the first observed reading cannot be determined without an external provisioning baseline (we do not assume sequence 0 or 1 without justification). Single-vehicle gaps are queryable via `GET /v1/completeness?vehicle_id=VG-xxx`.

### Security & Abuse Protection Specification (Part A2)

#### 1. HMAC-SHA256 Request Authentication
Every request to `POST /v1/telemetry/batch` requires three cryptographic headers:
* `X-API-Key`: Public client identifier (e.g. `voltgrid-dev-key`).
* `X-Timestamp`: Client submission timestamp in epoch milliseconds (e.g. `1763630000000`) or ISO-8601 string.
* `X-Signature`: Hex-encoded HMAC-SHA256 digest computed as:
  $$\text{X-Signature} = \text{HMAC-SHA256}(\text{secret}, \text{X-Timestamp} \mathbin{\Vert} \text{"."} \mathbin{\Vert} \text{rawBody})$$
Signatures are validated using constant-time comparison (`crypto.timingSafeEqual`) to prevent timing side-channel attacks. Secrets are loaded from the `api_keys` table and never exposed in responses or memory dumps.

#### 2. 5-Minute Request Timestamp Window
* Requests are checked against the server's current wall clock: $|\text{server\_time} - \text{X-Timestamp}| \le 300{,}000\text{ ms}$ (5 minutes).
* Stale requests older than 5 minutes or future-dated requests exceeding 5 minutes are rejected with `HTTP 401 Unauthorized`.

#### 3. Database-Level Replay Protection
* The `used_nonces` table stores all processed signatures (`PRIMARY KEY`) alongside their expiration time (`expires_at = now + 5 min`).
* If a signature is reused within the validity window, SQLite's primary key constraint rejects the insert, returning `HTTP 401: "Replayed request signature"`.
* Expired nonces are purged periodically via throttled 60-second background maintenance, avoiding expensive `DELETE` queries on every incoming request.

#### 4. Shared SQLite Token-Bucket Rate Limiter
* Rate limits are enforced on a per-API-key basis via the persistent `rate_limits` table (`tokens REAL`, `last_refill_ms INTEGER`).
* Tokens refill dynamically according to elapsed time:
  $$\text{tokens}_{\text{new}} = \min\left(\text{capacity}, \text{tokens}_{\text{current}} + \Delta t \times \text{refill\_rate}\right)$$
* Default configuration: Burst Capacity = 40 tokens, Refill Rate = 20 tokens/sec.
* Rate-limited requests return `HTTP 429 Too Many Requests` with a calculated `Retry-After: <seconds>` header:
  $$\text{Retry-After} = \max\left(1, \left\lceil \frac{1 - \text{tokens}_{\text{new}}}{\text{refill\_rate}} \right\rceil\right)$$
* Requests rejected with `HTTP 429` do NOT consume tokens.

#### 5. Two-Instance Multi-Process Architecture
```text
                 Load Balancer
                       │
              ─────────┴─────────
              │                 │
           API 1             API 2
         (Port 3000)       (Port 3001)
              │                 │
              ─────────┬─────────
                       │
             Shared SQLite Database
             (/data/voltgrid.db)
```
* Both `api` and `api2` containers share the exact same SQLite database file through a shared mounted Docker volume (`sqlite_data:/data`).
* SQLite's `BEGIN IMMEDIATE` write transactions and file-locking primitives guarantee that atomic token subtractions and signature recordings remain fully serialized across both processes with zero race conditions, lost updates, or over-consumption.
* **Storage Note**: This architecture requires local shared storage on the same host operating system. Network filesystems (NFS/SMB) are not supported due to unsafe remote POSIX lock semantics.

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
