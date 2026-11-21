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
│   - 10% Late Queue (<= 10m)    │
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
MAX_LATE_MS=600000 \
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
| `GET` | `/v1/vehicles` | Keyset-paginated vehicle listing (`?limit=50&cursor=VG-00042&status=online&min_lat=13.0&max_lat=13.2&min_lng=80.1&max_lng=80.3`) | `200` OK, `400` Bad Request |
| `GET` | `/v1/vehicles/:id/history` | Peak-preserving min/max downsampled time-series (`?from=&to=&points=200`) | `200` OK, `400` Bad Request, `404` Not Found |
| `GET` | `/v1/fleet/summary` | Sub-150ms fleet summary (aggregates strictly from `vehicles` & `alerts`) | `200` OK |
| `GET` | `/v1/alerts` | Filter alerts by `?status=open&rule=overheat&vehicle_id=VG-00042` | `200` OK |
| `POST` | `/v1/alerts/:id/ack` | Optimistic concurrency acknowledgment (`{"version": 1}`) | `200` OK, `409` Conflict, `404` Not Found |
| `GET` | `/v1/stream` | Server-Sent Events (SSE) live feed (supports `Last-Event-ID` resume) | `200` OK (`text/event-stream`) |
| `GET` | `/health` / `/health/live` | Liveness health check confirming application process is running | `200` OK |
| `GET` | `/health/ready` | Readiness check probing active SQLite database connectivity | `200` OK, `503` Service Unavailable |
| `GET` | `/metrics` | Process-local operational metrics (latency percentiles, request counts, errors, DB rows) | `200` OK |
| `GET` | `/v1/completeness` | Fleet-wide or single-vehicle sequence completeness & gap detection (`?vehicle_id=VG-00042`) | `200` OK, `404` Not Found |

### Query API Specifications (Part A4)

#### 1. Keyset Cursor Pagination & Filtering (`GET /v1/vehicles`)
* **Zero-Offset Pagination**: Uses keyset cursor pagination (`WHERE vehicle_id > @cursor ORDER BY vehicle_id ASC LIMIT @limit + 1`). Eliminates the $O(N)$ performance degradation and page drift inherent in SQL `OFFSET`.
* **Covering Index**: Supported by composite index `idx_vehicles_status_id (status, vehicle_id)` to execute queries via `SEARCH vehicles USING INDEX` with zero temporary B-tree sorting.
* **Pagination Metadata**: Returns `next_cursor` (last `vehicle_id` of the current page) and boolean `has_more`.
* **Status Filtering**: Accepts `?status=online|offline|maintenance`. Rejects invalid statuses with `400 Bad Request`.
* **Geographic Bounding Box**: Accepts `min_lat`, `max_lat`, `min_lng`, `max_lng`. Requires all 4 parameters, valid coordinate ranges ($[-90, 90]$ and $[-180, 180]$), and validates that minimums do not exceed maximums (`min <= max`).

#### 2. History & Peak-Preserving Downsampling (`GET /v1/vehicles/:id/history`)
* **Time Range**: Accepts ISO-8601 timestamps `from` and `to`. Validates formats and ensures `from < to`. Defaults to preceding 24 hours.
* **Vehicle Existence**: Returns `404 Not Found` if the requested vehicle does not exist in `vehicles` or `telemetry_history`.
* **Downsampling Algorithm**: Divides the query window into equal buckets based on `points` parameter ($2 \le \text{points} \le 5000$, default 200). Retains the minimum and maximum values for critical physical metrics (battery temperature peaks/troughs, minimum SOC, maximum speed) within each bucket.
* **Chronological Guarantees**: Always retains the initial and terminal points of the range. Returns points sorted in strict chronological order.
* **High Performance**: Evaluated against 17,280 readings (24 hours of 5-second telemetry) in under 150 ms (sub-second target).

#### 3. Fleet Aggregation SLA (`GET /v1/fleet/summary`)
* **High-Speed Summary**: Aggregates fleet counts by status (`online`, `offline`, `maintenance`), fleet average SOC (`avg_soc_pct`), observed sequence completeness (`data_completeness_pct`), and open/acknowledged alerts grouped by rule.
* **Sub-150ms SLA Guarantee**: Queries strictly from the `vehicles` state table (1,000–2,000 rows) and `alerts` table (`WHERE status != 'resolved'`). Never executes table scans on `telemetry_history`. Internal database query executes in $< 2\text{ ms}$ (end-to-end HTTP $p95 < 10\text{ ms}$ against 5M records, well within the 150 ms SLA target).

#### 4. Optimistic Concurrency Control (`POST /v1/alerts/:id/ack`)
* **Atomic OCC Update**: `UPDATE alerts SET status = 'acknowledged', updated_at = datetime('now'), version = version + 1 WHERE id = @id AND version = @version AND status = 'open'`.
* **Conflict Resolution**:
  * If version matches and alert is `open`: Returns `200 OK` with incremented `version` and new `acknowledged` status.
  * If version does not match, or alert is already acknowledged/resolved: Returns `409 Conflict`.
  * If alert does not exist: Returns `404 Not Found`.
* **Zero Race Conditions**: Tested under 10 concurrent requests; exactly 1 request succeeds and 9 receive `409 Conflict`.

### Telemetry Field Validation Specification
Every reading in `POST /v1/telemetry/batch` is individually validated using `Number.isFinite()` and strict bounds:
* `vehicle_id`: Non-empty string (`trimmed length >= 1`).
* `seq`: Non-negative integer (`Number.isInteger(seq) && seq >= 0`).
* `ts`: Valid ISO-8601 timestamp string (`!isNaN(Date.parse(ts))`) within $[T_{\text{server}} - 10\text{m}, T_{\text{server}} + 30\text{s}]$. Readings older than 10 minutes or more than 30 seconds ahead of server clock are rejected to prevent future timestamp state corruption.
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
* **Limitation Note**: Sequence gap tracking establishes completeness strictly within the observed interval $[\text{min\_seq}, \text{max\_seq}]$. Sequences dropped prior to the first observed reading cannot be determined without an external provisioning baseline. Single-vehicle gaps are queryable via `GET /v1/completeness?vehicle_id=VG-xxx`.

---

## 4. Alert Engine & Lifecycle Specification (Part A3)

The alert engine evaluates four distinct operational rules with deterministic, order-independent logic:

### 1. Overheat (`overheat`)
* **Trigger Condition**: Three (3) consecutive valid readings with `battery_temp_c > 55.0°C`.
* **Resolution Condition**: Three (3) consecutive valid readings with `battery_temp_c <= 50.0°C`.
* **Hysteresis Band**: Between `50.0°C` and `55.0°C`, the previous alert state is preserved without change.
* **De-duplication**: Repeated readings `> 55.0°C` do not create duplicate alerts while an alert is open or acknowledged.

### 2. Rapid Discharge (`rapid_discharge`)
* **Trigger Condition**: While not charging (`charging === false`), `soc_pct` drops by `> 8.0` percentage points compared to any non-charging reading within the preceding 5 minutes (300 seconds) in event-time.
* **Resolution Condition**: The vehicle starts charging (`charging === true`), the 5-minute drop rate returns to `<= 8.0` percentage points, or the 5-minute window expires without sustained drop.
* **Event-Time Calculation**: Evaluated strictly using device timestamps (`ts`), never server receipt timestamps.

### 3. Geofence Breach (`geofence_breach`)
* **Operating Polygon Boundary**: Chennai metropolitan operating area:
  ```json
  [
    { "lat": 13.0000, "lng": 80.1800 },
    { "lat": 13.1800, "lng": 80.1800 },
    { "lat": 13.1800, "lng": 80.3200 },
    { "lat": 13.0000, "lng": 80.3200 }
  ]
  ```
* **Trigger Condition**: Latest device reading is outside the operating polygon (determined by ray-casting).
* **Resolution Condition**: Latest device reading re-enters the polygon.

### 4. Offline Detection Worker (`offline`)
* **Trigger Condition**: Online vehicle has not reported a reading within `90` seconds (`OFFLINE_THRESHOLD_SEC`).
* **Detection Clock**: Evaluated using server receive time (`vehicles.updated_at`), avoiding dependence on skewed client clocks.
* **De-duplication**: Periodic sweeps (default every 10s via `OFFLINE_CHECK_INTERVAL_MS`) do not duplicate active alerts.
* **Atomic Recovery**: When a silent vehicle resumes sending telemetry, its vehicle status returns to `online`, and its active `offline` alert transitions to `resolved` with `version = version + 1` in the same transaction.

### Alert Lifecycle States & Optimistic Concurrency
Supported states: `open` $\rightarrow$ `acknowledged` $\rightarrow$ `resolved`.
* **Database Constraint**: Enforced by SQLite partial unique index:
  ```sql
  CREATE UNIQUE INDEX idx_alerts_active_unique ON alerts (vehicle_id, rule) WHERE status != 'resolved';
  ```
  Guarantees at most ONE active (`open` or `acknowledged`) alert per `(vehicle_id, rule)`.
* **Acknowledgment (`POST /v1/alerts/:id/ack`)**: Requires `{"version": <int>}`. Atomically transitions `open` $\rightarrow$ `acknowledged` and increments `version`. Stale versions return `HTTP 409 Conflict`.
* **Safe Streaming**: All database alert changes are recorded inside `BEGIN IMMEDIATE` transactions. Events are buffered and broadcast to the SSE stream (`publishAlertChange`) strictly **after** transaction commit, preventing phantom stream events on rollbacks.

### Historical Retrieval & Order Independence
* **Latest 100 Query**: `getRecentHistoryStmt` uses a subquery to retrieve the latest 100 readings in ascending chronological order:
  ```sql
  SELECT * FROM (
    SELECT * FROM telemetry_history WHERE vehicle_id = ? ORDER BY ts DESC, seq DESC LIMIT 100
  ) ORDER BY ts ASC, seq ASC;
  ```
* **Covering Index**: Supported by composite index `idx_history_vehicle_ts_seq (vehicle_id, ts DESC, seq DESC)` eliminating temporary B-tree sorting.
* **Timeline State Reconstruction**: If readings arrive in reverse or shuffled order, chronological replay reconstructs the exact alert lifecycle, ensuring identical alert status, rule, details, and active counts regardless of packet arrival order.

---

## 5. Security & Abuse Protection Specification (Part A2)

### 1. HMAC-SHA256 Request Authentication
Every request to `POST /v1/telemetry/batch` requires three cryptographic headers:
* `X-API-Key`: Public client identifier (e.g. `voltgrid-dev-key`).
* `X-Timestamp`: Client submission timestamp in epoch milliseconds or ISO-8601 string.
* `X-Signature`: Hex-encoded HMAC-SHA256 digest computed as:
  $$\text{X-Signature} = \text{HMAC-SHA256}(\text{secret}, \text{X-Timestamp} \mathbin{\Vert} \text{"."} \mathbin{\Vert} \text{rawBody})$$
Signatures are validated using constant-time comparison (`crypto.timingSafeEqual`) to prevent timing side-channel attacks. Secrets are loaded from `api_keys` and never exposed in responses or memory dumps.

### 2. 5-Minute Request Timestamp Window
* Requests are checked against the server's current wall clock: $|\text{server\_time} - \text{X-Timestamp}| \le 300{,}000\text{ ms}$ (5 minutes).
* Stale requests older than 5 minutes or future-dated requests exceeding 5 minutes are rejected with `HTTP 401 Unauthorized`.

### 3. Database-Level Replay Protection
* The `used_nonces` table stores all processed signatures (`PRIMARY KEY`) alongside their expiration time (`expires_at = now + 5 min`).
* If a signature is reused within the validity window, SQLite's primary key constraint rejects the insert, returning `HTTP 401: "Replayed request signature"`.
* Expired nonces are purged periodically via throttled 60-second background maintenance.

### 4. Shared SQLite Token-Bucket Rate Limiter
* Rate limits are enforced on a per-API-key basis via the persistent `rate_limits` table (`tokens REAL`, `last_refill_ms INTEGER`).
* Tokens refill dynamically according to elapsed time:
  $$\text{tokens}_{\text{new}} = \min\left(\text{capacity}, \text{tokens}_{\text{current}} + \Delta t \times \text{refill\_rate}\right)$$
* Default configuration: Burst Capacity = 40 tokens, Refill Rate = 20 tokens/sec.
* Rate-limited requests return `HTTP 429 Too Many Requests` with a calculated `Retry-After: <seconds>` header. Requests rejected with `HTTP 429` do NOT consume tokens.

### 5. Two-Instance Multi-Process Architecture
Both `api` and `api2` containers share the exact same SQLite database file through a shared mounted Docker volume (`sqlite_data:/data`). SQLite's `BEGIN IMMEDIATE` write transactions and file-locking primitives guarantee that atomic token subtractions and signature recordings remain fully serialized across both processes with zero race conditions, lost updates, or over-consumption.

---

## 6. Live Streaming & Server-Sent Events Specification (Part A5)

The live streaming endpoint (`GET /v1/stream`) provides real-time telemetry updates and alert events to operator dashboards and dispatchers over Server-Sent Events (SSE).

### 1. HTTP Endpoint & Transport Headers
* **Endpoint**: `GET /v1/stream`
* **Transport Headers**:
  ```http
  Content-Type: text/event-stream
  Cache-Control: no-cache, no-transform
  Connection: keep-alive
  X-Accel-Buffering: no
  ```
* **Connection Lifecycle**: Emits `: connected\n\n` on initial connect. Periodically emits `: heartbeat\n\n` comments every 15 seconds to keep intermediary proxy and NAT connections active.

### 2. Event Structure & Wire Contract
Events adhere to standard SSE wire framing (`id: <id>\nevent: <type>\ndata: <json>\n\n`):

* **Vehicle Update (`vehicle_update`)**:
  ```json
  {
    "event_id": 412,
    "event_type": "vehicle_update",
    "vehicle_id": "VG-00104",
    "state": {
      "soc_pct": 74.5,
      "battery_temp_c": 38.2,
      "speed_kmh": 32.0,
      "lat": 13.0827,
      "lng": 80.2707,
      "odometer_km": 1420.5,
      "charging": false,
      "status": "online",
      "last_seq": 8840,
      "last_ts": "2026-11-20T12:00:00.000Z"
    },
    "ts": "2026-11-20T12:00:00.000Z"
  }
  ```

* **Alert Lifecycle Change (`alert_change`)**:
  ```json
  {
    "event_id": 413,
    "event_type": "alert_change",
    "alert_id": 28,
    "vehicle_id": "VG-00104",
    "rule": "overheat",
    "status": "open",
    "version": 1,
    "details": { "trigger_temp": 56.4, "consecutive_hot": 3 },
    "ts": "2026-11-20T12:00:01.000Z"
  }
  ```

### 3. Persistent Event Log & Reconnection (`Last-Event-ID`)
* **SQLite Event Outbox**: Events are persisted into `stream_events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type, data, created_at)` in WAL mode. Monotonically increasing event IDs survive process restarts and power loss.
* **Resumption**: Accepts the `Last-Event-ID` request header (or `?lastEventId=` query parameter). Queries `SELECT * FROM stream_events WHERE id > @lastId ORDER BY id ASC` to replay missed events.
* **Retention Pruning**: Retains the latest 10,000 historical events. If a client connects with an ID older than the retention window, a `: gap_detected: ...` notification comment is sent and all available history is replayed.

### 4. Replay-to-Live Handoff
* Prevents the classic race condition where live events arriving during historical database replay are missed or delivered out-of-order.
* Connecting clients with `Last-Event-ID` enter a temporary `replaying` state where live events are placed in an internal per-client `replayQueue`.
* Once database replay completes, queued live events with `id > lastDeliveredId` are flushed in strict monotonic order before live broadcasting resumes, guaranteeing **zero missed events and zero duplicates**.

### 5. Vehicle Update Coalescing
* Ingesting ~400 readings/sec would overwhelm client event loops if dispatched individually.
* Pending vehicle state updates are collapsed in memory by `vehicle_id` (`pendingVehicles = new Map()`), retaining only the freshest state per vehicle.
* Flushed at a configurable 1-second interval (`STREAM_COALESCE_INTERVAL_MS = 1000`). Memory is bounded strictly to $O(\text{fleet\_size})$.
* **Critical Rule**: Alert lifecycle events (`open`, `acknowledged`, `resolved`) **never coalesce**—they are dispatched immediately to all connected clients.

### 6. Client Back-Pressure & Slow-Client Protection
* Monitors the return value of Node.js `res.write()`.
* If a client's kernel TCP socket buffer fills up (`canAcceptMore === false`), writing is paused and frames are queued in a bounded per-client buffer (`MAX_CLIENT_BUFFER = 100`).
* When the socket drains (`res.on('drain')`), pending frames are iteratively drained.
* If a slow client exceeds 100 queued frames, the server cleanly disconnects the client with `: disconnected: client backpressure buffer overflow\n\n`, allowing it to reconnect with `Last-Event-ID`. Fast clients and ingestion throughput remain completely uninhibited.

---

## 7. Database Model, Indexing & EXPLAIN QUERY PLAN

### 1. Migrations & Version Tracking
Database migrations are strictly versioned and executed in alphabetical order within transactional blocks. Applied migrations are recorded in `schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`:
* **Fresh Boot**: Sequentially applies `001_init.sql`, `002_alerts.sql`, `003_security.sql`, and `004_stream.sql`.
* **Idempotency**: Running migrations repeatedly skips already recorded versions, preventing redundant DDL or table locking.
* **Failure Safety**: If a migration script errors, the entire script is rolled back via `ROLLBACK`, preventing corrupted partial schema states.

### 2. EXPLAIN QUERY PLAN Verification (Top 2 Critical Queries)

#### Query 1: Vehicle Telemetry History Downsampling Query
```sql
EXPLAIN QUERY PLAN 
SELECT seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging 
FROM telemetry_history 
WHERE vehicle_id = 'VG-00001' AND ts >= '2026-11-20T00:00:00Z' AND ts <= '2026-11-21T00:00:00Z' 
ORDER BY ts ASC, seq ASC;
```
**Actual SQLite Query Plan Output:**
```text
SEARCH telemetry_history USING INDEX idx_history_vehicle_ts_seq (vehicle_id=? AND ts>? AND ts<?)
```
* **Index Used**: `idx_history_vehicle_ts_seq (vehicle_id, ts DESC, seq DESC)`.
* **Analysis**: Zero full table scan. SQLite executes a range seek on `vehicle_id` and temporal slice `ts`, reading only the required rows for the requested vehicle window.

#### Query 2: Keyset Paginated Vehicle Listing with Status Filter
```sql
EXPLAIN QUERY PLAN 
SELECT vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at 
FROM vehicles 
WHERE status = 'online' AND vehicle_id > 'VG-00100' 
ORDER BY vehicle_id ASC LIMIT 51;
```
**Actual SQLite Query Plan Output:**
```text
SEARCH vehicles USING INDEX idx_vehicles_status_id (status=? AND vehicle_id>?)
```
* **Index Used**: `idx_vehicles_status_id (status, vehicle_id)`.
* **Analysis**: Zero table scan and zero temporary B-tree sorting. SQLite utilizes the composite index to filter by `status` and seek directly to `vehicle_id > cursor`, returning rows in index order.

---

## 8. Repeatable Multi-Endpoint Load Testing

The service includes a native load testing tool (`scripts/loadtest.js`) simulating realistic mixed operational workloads:
* 50% Telemetry batch ingestion with HMAC signatures, duplicate sequences, and malformed readings.
* 25% Vehicle fleet listing (`GET /v1/vehicles`) with keyset pagination.
* 15% Vehicle time-series history (`GET /v1/vehicles/:id/history`).
* 10% Fleet health summary (`GET /v1/fleet/summary`).

### Run Load Test
```bash
cd backend
npm run loadtest
```

Configurable environment options:
```bash
VUS=10 DURATION_SEC=10 WARMUP_SEC=2 BATCH_SIZE=50 node scripts/loadtest.js
```

### Measured Multi-Endpoint Load Test Results (10 VUs, 10s Execution)

Workload mix: 50% Batch Ingestion (HMAC signed), 25% Vehicles Keyset Listing, 15% Time-Series History, 10% Fleet Summary.

| Metric | Measured Value |
| :--- | :--- |
| **Total Duration** | 10.20 seconds |
| **Total Requests** | 100 |
| **Successful Requests** | 100 (100.00%) |
| **Failed Requests** | 0 (0.00%) |
| **Overall Throughput** | 9.80 req/sec |
| **Telemetry Ingestion Rate** | 2,250 readings (221 readings/sec) |

#### Latency Percentiles by Endpoint:

| Endpoint | Request Count | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `POST /v1/telemetry/batch` | 45 | 895.85 | 1692.81 | 1693.20 | 1693.20 |
| `GET /v1/vehicles` | 30 | 817.36 | 1156.94 | 1157.42 | 1157.42 |
| `GET /v1/vehicles/:id/history` | 15 | 895.88 | 1692.88 | 1692.88 | 1692.88 |
| `GET /v1/fleet/summary` | 10 | 839.88 | 1693.16 | 1693.16 | 1693.16 |
| **Aggregated (All Endpoints)** | **100** | **840.36** | **1692.56** | **1693.16** | **1693.20** |

> **Note on Mixed-Workload Results**: In this multi-endpoint test, 50% of traffic consisted of concurrent batch ingestion writes running inside serialized SQLite write transactions (`BEGIN IMMEDIATE`). Read endpoints (including Fleet Summary at 1,693 ms p95) queued behind active disk commits. To measure Fleet Summary query efficiency under high volume independently of write lock contention, see the dedicated 5M benchmark below.

### 5 Million Readings Fleet Summary Performance Benchmark (Part A4 SLA)

A reproducible benchmark runner (`scripts/benchmark_5m.js`) evaluates `GET /v1/fleet/summary` against 5,000,000 telemetry readings across 1,000 vehicles, demonstrating compliance with the **sub-150ms p95 latency SLA**:

```bash
cd backend
npm run benchmark:5m
```

#### Measured Benchmark Results (10 Concurrent VUs, 500 Requests, 50 Warm-Up):
* **Environment**: 13th Gen Intel(R) Core(TM) i5-1335U (12 cores, 15.7 GB RAM), Windows_NT 10.0.26200 x64, Node.js v24.14.1
* **Database Driver**: `node:sqlite (built-in fallback)` with WAL mode, `synchronous = NORMAL`, `busy_timeout = 5000ms`, `cache_size = -64000`

| Metric | Initial Benchmark Run | Latest Verified Benchmark Run | Requirement / SLA | Status |
| :--- | :---: | :---: | :---: | :---: |
| **Telemetry History Records** | **5,000,000** | **5,000,000** | 5,000,000 | Populated |
| **Fleet Vehicles** | **1,000** | **1,000** | Multi-vehicle | Populated |
| **Database File Size** | **1,302.05 MiB (1,365.30 MB)** | **1,302.05 MiB (1,365.30 MB)** | ~1.3–1.4 GB | Verified |
| **Concurrency** | **10 VUs** | **10 VUs** | Concurrent | Completed |
| **Total Measured Requests** | **500** (+ 50 warm-up) | **500** (+ 50 warm-up) | 500 | Completed |
| **Successful Requests** | **500 (100.0%)** | **500 (100.0%)** | 100% | Completed |
| **Error Rate** | **0.00%** | **0.00%** | < 0.1% | **PASSED** |
| **Throughput** | **1,768 req/s** | **1,646 req/s** | - | Completed |
| **Min Latency** | **3.70 ms** | **4.26 ms** | - | Completed |
| **Average Latency** | **5.61 ms** | **6.03 ms** | - | Completed |
| **p50 Latency** | **5.56 ms** | **5.99 ms** | - | Completed |
| **p90 Latency** | **6.81 ms** | **7.26 ms** | - | Completed |
| **p95 Latency** | **7.23 ms** | **8.64 ms** | **< 150.0 ms** | **PASSED (Target Satisfied)** |
| **p99 Latency** | **9.89 ms** | **10.77 ms** | - | Completed |
| **Max Latency** | **10.37 ms** | **11.29 ms** | - | Completed |

* **Query Plan Optimization**: Aggregates directly from `vehicles` (1,000 rows) and `alerts` (filtered via `idx_alerts_active_rule`). `telemetry_history` (5,000,000 rows) is never scanned, ensuring $O(V)$ scaling instead of $O(N)$.
* **Idempotency**: Repeated execution checks existing row counts before inserting, preventing duplicate rows or artificial inflation.

---

## 9. Observability, Structured Logging & Operational Health

### 1. Request ID Correlation & Propagation
* Every incoming request receives an `X-Request-Id` header (retaining an incoming client trace ID or generating a fresh UUID v4).
* The ID is reflected in the HTTP response headers and propagated to all downstream log entries.

### 2. Structured JSON Logging
* All requests emit a single-line JSON log to standard output on completion:
  ```json
  {"ts":"2026-11-21T04:45:01.965Z","level":"info","request_id":"38c65a5b-847b-4747-8f01-de1cb7bbfc69","method":"GET","path":"/health","status":200,"duration_ms":3.78}
  ```
* **Security & Credential Redaction**: Authentication tokens, HMAC secrets, and `X-Signature` headers are never logged.

### 3. Production Health Probes
* **Liveness Probes (`GET /health` and `GET /health/live`)**: Return `200 OK` confirming the Node.js process is responsive.
* **Readiness Probe (`GET /health/ready`)**: Actively tests SQLite connectivity via a lightweight `SELECT 1` probe. Returns `200 OK` (`{"status":"ready","database":"connected"}`) when ready, or `503 Service Unavailable` if database I/O fails.

### 4. Process-Local Metrics (`GET /metrics`)
Returns process health, request counts, error rates, latency percentiles, domain counters, and live SQLite row aggregates:
* `process`: uptime, heap memory usage, Node version, scope.
* `requests`: total requests, 5xx errors, status code breakdown, grouped endpoint counts.
* `latency_ms`: min, avg, p50, p95, p99, and max.
* `telemetry`: accepted, duplicate, and rejected readings.
* `security`: auth failure count, rate-limit rejections count.
* `stream`: active connected SSE client sockets.
* `database`: live row counts from `vehicles`, `telemetry_history`, and active `alerts`.

---

## 10. Running Tests

The test suite uses Node's native test runner (`node:test` and `node:assert/strict`) with zero external test dependencies:

```bash
cd backend
# Run all 128 tests across 47 suites
npm test

# Run tests with built-in code coverage report (91.90% line coverage)
npm run test:coverage

# Run isolated unit tests
npm run test:unit

# Run high-concurrency and race-condition tests
npm run test:concurrency
```

### Test Coverage Highlights (128 Tests Across 47 Suites, 91.90% Coverage):
* **A1 Ingestion Comprehensive Suite (`backend/tests/a1_ingestion_comprehensive.test.js`)**: Exhaustive validation of single and maximum batches (500 items), batch size violations (501 items), individual missing field rejection (11 required fields), invalid vehicle IDs (empty, whitespace, non-string), unparseable timestamps, invalid numeric ranges (NaN, Infinity, out of bounds), unexpected types, duplicate handling within same batch, repeated request idempotency, out-of-order sequence insertion, and latest-state timestamp resolution.
* **A2 Security & Abuse Protection Suite (`backend/tests/a2_security_comprehensive.test.js`)**: Valid HMAC signatures, tampered signatures, wrong secret credentials, missing auth headers, unknown API keys, timestamp drift (>5m past and future), malformed timestamp strings, signature replay attacks via `used_nonces`, recovery of valid requests after auth failure, rate limiting with `Retry-After` headers, token replenishment over time, and zero secret leakage.
* **A3 Alerts Comprehensive Suite (`backend/tests/a3_alerts_comprehensive.test.js`)**: Exact threshold boundary conditions (55.0°C does not trigger, strictly >55.0°C required), temperature fluctuation hysteresis, exact 50.0°C resolution, rapid discharge exact 8.0% drop boundary (does not trigger, strictly >8.0% required), charging state resolution, 5-minute window expiration, geofence polygon boundary ray-casting, exit/re-enter transitions, offline worker idempotency (no duplicate active alerts), and online recovery on telemetry resumption.
* **A4 Queries Comprehensive Suite (`backend/tests/a4_queries_comprehensive.test.js`)**: Empty query result sets, combined status + bounding box + pagination cursor filters, keyset cursor stability under concurrent insertions with zero duplicate vehicles across pages, empty vehicle history windows, points budget downsampling enforcement, fleet summary immediate aggregation updates after ingestion, and 2-client concurrent OCC alert ack races.
* **Advanced Concurrency & Race Condition Suite (`backend/tests/concurrency_advanced.test.js`)**: 10 concurrent clients submitting different readings for the same vehicle (zero deadlocks, all stored in history, latest device timestamp preserved in live state), concurrent ingestion alongside active vehicle and summary read queries without `SQLITE_BUSY`, and simultaneous alert generation across 5 vehicles.
* **Observability & Probes Tests (`backend/tests/observability.test.js`)**: Tests `X-Request-Id` UUID generation and propagation, single-line structured JSON logs, sensitive credential redaction, `/health` and `/health/live` liveness probes, `/health/ready` database failure fallback to 503, and `/metrics` real-time counter incrementing.
* **Load Test Smoke Tests (`backend/tests/loadtest.test.js`)**: Smoke tests `scripts/loadtest.js` in automated test pipelines, asserting summary calculation, throughput, and percentile reporting.
* **Database Migration Tests (`backend/tests/migrations.test.js`)**: Verifies fresh database initialization, `schema_migrations` tracking, table constraints, index existence, and idempotent re-execution.
* **Live SSE Stream Tests (`backend/tests/stream.int.test.js`)**: Real HTTP SSE testing for connection handshake, canonical event formatting, monotonic `Last-Event-ID` replay, restart resilience, atomic replay-to-live handoff race conditions, slow-client backpressure disconnection, 1s vehicle coalescing, and 400 req/s high-throughput benchmark.
* **Query API Tests (`backend/tests/queries.int.test.js`)**: Keyset cursor pagination, bounding box boundary/inversion validation, 17,280-reading 24h history downsampling benchmark, fleet summary p95 SLA latency benchmark, and 10-client concurrent OCC alert ack race test.
* **Alert Integration Tests (`backend/tests/alerts.int.test.js`)**: Real HTTP batch ingestion testing chronological vs reverse vs random arrival convergence, >100 readings history scan, hysteresis persistence, offline worker recovery on telemetry resumption, alert ack optimistic locking, and rapid discharge window expiration.
* **Alert Unit Tests (`backend/tests/alerts.unit.test.js`)**: Pure rule evaluation for Overheat (50°C–55°C hysteresis), Rapid Discharge (>8% drop in 5min), Geofence Breach (ray-casting polygon), and offline clock injection.
* **Hermetic Integration Tests (`backend/tests/api.int.test.js`)**: Runs against an ephemeral port (`PORT=0`) with an isolated temporary SQLite database, verifying HTTP 207 partial success, latest-state timestamp resolution, sequence gap tracking, keyset pagination, peak downsampling, optimistic locking, and HMAC replay blocking.
* **20-Client Concurrency Proof (`backend/tests/concurrency.test.js`)**: 20 concurrent clients simultaneously fire the identical 50-item batch. Mathematically asserts 50 accepted, 950 duplicates, 50 rows stored, 0 double counting.
* **Two-Instance Shared DB Tests (`backend/tests/security.two_instance.test.js`)**: Verifies shared rate limiting, replay blocking, and concurrency across two distinct processes sharing the same SQLite database.

---

## 11. Architecture & System Design Document

Read **[DESIGN.md](DESIGN.md)** for detailed engineering answers to scale, delivery guarantees, clock drift, storage retention, threat modeling, and trade-offs.
