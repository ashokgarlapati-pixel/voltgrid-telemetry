# VoltGrid Fleet Telemetry Service — System Design & Reasoning (Part C)

---

## 1. Scale: 100x Fleet Growth (200,000 Vehicles / ~40,000 Readings/sec)

### What Breaks First
The **SQLite single-writer lock** on `BEGIN IMMEDIATE` transactions in `backend/src/ingestion.js` will break first. SQLite WAL mode permits concurrent readers, but allows strictly one writer process at any instant. At 2,000 vehicles (~400 readings/sec sent in batches of 100–500), write lock hold times average 5–15 ms, comfortably fitting within `PRAGMA busy_timeout = 5000`. At 40,000 readings/sec (~400 write transactions/sec), write lock contention will trigger serialized queue pileups, exceeding `busy_timeout` and throwing `SQLITE_BUSY: database is locked`.

### The Next Three Bottlenecks in Exact Order

1. **Node.js Single-Threaded CPU Saturation (HMAC + JSON Parsing)**:
   Processing 400 requests/sec with 100–500 readings per batch requires parsing ~20 MB/sec of JSON and computing 400 cryptographic HMAC-SHA256 hashes (`crypto.createHmac`) per second. Because Node.js handles HTTP parsing on a single event-loop thread, CPU utilization will hit 100%, causing event-loop lag and request timeouts.
   * *Mitigation*: Run stateless API instances behind Nginx/ALB using Node's cluster module or Kubernetes pods. Offload HMAC verification and JSON validation to an API Gateway (e.g., Envoy or Kong).

2. **Disk I/O & B-Tree Write Amplification on `telemetry_history`**:
   At 40,000 readings/sec, the service ingests **3.45 billion rows/day** (~760 GB/day). Maintaining the composite B-Tree indexes `PRIMARY KEY (vehicle_id, seq)` and `idx_history_vehicle_ts (vehicle_id, ts)` causes massive write amplification, thrashing random I/O and exhausting NVMe SSD write endurance within months.
   * *Mitigation*: Introduce an append-only distributed log (Kafka or Redpanda) as a buffer. Ingest telemetry into a columnar time-series database (ClickHouse or TimescaleDB) partitioned daily by `(ts, vehicle_id)`. Maintain live vehicle states and alerts in PostgreSQL or Redis.

3. **In-Memory SSE Fan-out & `offlineWorker` Periodic Scans**:
   In `backend/src/offlineWorker.js`, the 10-second sweep (`SELECT vehicle_id, updated_at FROM vehicles WHERE status = 'online' AND updated_at <= ?`) scans 200,000 rows, taking 50–100 ms every cycle. Concurrently, `backend/src/stream.js` must coalesce and fan out 200,000 updates per second across thousands of connected dashboard clients, exhausting memory and socket buffers.
   * *Mitigation*: Replace table scans with a Redis Sorted Set (`ZREVRANGEBYSCORE vehicles:heartbeats`) to pop timed-out vehicles in $O(\log N)$ time. Distribute live SSE streams across a Redis Pub/Sub cluster with dedicated WebSocket gateway workers.

---

## 2. Delivery Guarantees

Our ingestion pipeline implements **at-least-once delivery with idempotent processing**, achieving an **effectively-once persistent outcome**.

### Concrete Mechanism
Idempotency is enforced at the database storage layer:
1. `telemetry_history` defines `PRIMARY KEY (vehicle_id, seq)`.
2. In `backend/src/ingestion.js`, `executeBatchTransaction` uses `INSERT OR IGNORE INTO telemetry_history`.
3. If an incoming reading has already been recorded, SQLite returns `changes === 0`. The engine increments the `duplicates` counter and skips the `vehicles` state table upsert entirely.
4. Concurrency test `backend/tests/concurrency.test.js` proves this: 20 concurrent clients firing the exact same 50-item batch simultaneously results in **exactly 50 accepted rows, exactly 950 duplicate rows, and zero double-counting** in `vehicles.received_count`.

### Where It Can Still Go Wrong
* **Firmware Sequence Reset**: If vehicle firmware crashes, reboots, and resets its non-volatile `seq` counter back to 0, all new legitimate readings with `seq` lower than previously seen values will be ignored by `INSERT OR IGNORE` until `seq` exceeds the previous lifetime maximum.
* **Network Partition Post-Commit**: If SQLite commits a batch transaction but a cellular drop occurs before the HTTP `200` response reaches the vehicle, the vehicle retries. Although storage remains idempotent, the vehicle client cannot confirm delivery until the connection re-establishes.

---

## 3. Time & Clock Drift

### Which Timestamp We Trust and Why
* **Device Timestamp (`ts`)**: We trust `ts` for **vehicle operational state**, **time-series history ordering**, and **telemetry-driven alert rules** (`overheat`, `rapid_discharge`, `geofence_breach`). In `backend/src/ingestion.js`:
  ```sql
  last_ts = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.last_ts ELSE vehicles.last_ts END
  ```
  This ensures out-of-order cellular packets buffered in tunnels never overwrite newer state with stale telemetry.
* **Server Timestamp (`received_at` / `updated_at`)**: We trust server time for **offline detection** (`backend/src/offlineWorker.js`) and **HMAC replay window validation** (`backend/src/security.js`). A dead scooter cannot report its own absence; silence must be measured relative to the server clock.

### Handling 25 Seconds Fast vs. 1 Day Off
* **25 Seconds Fast**: Accepted cleanly. It easily falls within our 5-minute security window ($|T_{\text{server}} - T_{\text{req}}| \le 300\text{s}$). State ordering is preserved via device `ts`.
* **1 Day Off**: Rejected immediately with `HTTP 401 Unauthorized` by `hmacAuth` in `security.js` (`Math.abs(Date.now() - reqTimeMs) > 300000`). If unauthenticated telemetry were accepted, a vehicle clock 1 day in the future would poison `vehicles.last_ts`, causing all subsequent legitimate readings for the next 24 hours to be treated as "late" and never update live state.

---

## 4. Storage & Cost (~35 Million Rows / Day)

### Storage Calculation
Each `telemetry_history` row averages ~120 bytes of raw data plus ~100 bytes for two B-Tree indexes (`PRIMARY KEY` + `idx_history_vehicle_ts`), totaling ~220 bytes per row:
$$\text{Daily Storage} = 35{,}000{,}000 \times 220\text{ bytes} \approx 7.7\text{ GB/day} \quad (\approx 230\text{ GB/month})$$

### Concrete Tiered Retention Strategy
1. **Hot Tier (0–7 Days)**: Retain 100% of raw 5-second readings in partitioned tables for immediate debugging and alert investigations (~54 GB).
2. **Warm Tier (8–90 Days)**: Run automated nightly rollups downsampling raw rows into 1-minute buckets (storing `min/max/avg` for `soc_pct`, `battery_temp_c`, `speed_kmh`). Reduces row count by 12x (~15 GB total).
3. **Cold Tier (91–365 Days)**: Roll up into 1-hour summaries. Archive compressed raw Parquet files to AWS S3 Glacier ($0.0036/GB/month). Hard delete raw database records older than 7 days via partition dropping (`DROP TABLE`).

### What an Operator Can No Longer See After Downsampling Runs
* Exact sub-minute voltage dips or transient battery overheat spikes lasting < 60 seconds.
* Individual lost sequence numbers (`seq` gaps) to trace localized cellular dropouts.
* Exact 3-consecutive-reading streaks required to reconstruct historical alert triggers retroactively.

---

## 5. Threat Model: Device API Abuse

| Threat | Attack Vector | Exact Implementation Defense |
| :--- | :--- | :--- |
| **1. Unauthorized Request Tampering** | Rogue actor intercepts or crafts payload to falsify battery levels, simulate false crashes, or inject fake diagnostics. | `hmacAuth` in `security.js`. Requires valid `X-Signature` computed via `HMAC-SHA256(secret, "${timestamp}.${rawBody}")` and verified using `crypto.timingSafeEqual` over raw UTF-8 bytes. Secret is never leaked in memory or responses. |
| **2. Stale Request Injection** | Attacker captures packets and delays transmission to corrupt telemetry ordering. | `hmacAuth` 5-minute validity window (`Math.abs(serverNow - reqTimeMs) <= 300000ms`). Requests older than 5 minutes or > 5 minutes in future fail with `HTTP 401`. |
| **3. Replay Attacks** | Eavesdropper captures valid signed batch and resends it repeatedly to manipulate vehicle position or flood the database. | `used_nonces` table in `security.js`. Stores signatures (`PRIMARY KEY`) within validity window. Duplicate signature insertion fails (`info.changes === 0`) and returns `HTTP 401: "Replayed request signature"`. Expired nonces are purged via throttled 60s background sweeps. |
| **4. Distributed DoS & Resource Exhaustion** | Abusive clients spam endpoints across multiple IPs to exhaust CPU or SQLite write locks. | Shared SQLite token bucket in `security.js` (`rate_limits` table). Enforces per-key burst capacity (40) and refill rate (20 req/s), emitting `HTTP 429` with `Retry-After: <sec>`. Rate-limited requests do not consume tokens. |

### Multi-Instance Coordination & SQLite Storage Limitations
* **Two-Instance Transaction Coordination**: When two API instances run behind a load balancer on the same host, both mount the shared volume containing `voltgrid.db`. Both `used_nonces` and `rate_limits` updates execute inside `BEGIN IMMEDIATE` transactions. SQLite's OS-level file lock (`WAL` mode + `busy_timeout = 5000ms`) serializes write reservations across process boundaries. Neither instance can over-consume tokens or accept a duplicate replay.
* **Remaining Limitations with SQLite Deployment**: Shared SQLite requires shared local disk on a single host. It cannot run across independent machines or over network filesystems (NFS/SMB) due to unreliable byte-range file locking and cache coherence bugs. Scaling to distributed, multi-server infrastructure requires migrating shared rate-limiting to Redis and telemetry storage to clustered PostgreSQL/TimescaleDB.

---

## 6. Trade-offs: What We Chose NOT to Build

1. **Distributed Message Queue & Clustered Database (Kafka + PostgreSQL)**:
   * *Trade-off*: Chose single-process Node.js with SQLite WAL mode.
   * *Consequence*: Zero operational complexity (boots with a single `docker compose up`), but limits write throughput to a single disk node (~1,500 req/s ceiling).
2. **Durable SSE Event Log Across Restarts**:
   * *Trade-off*: Chose an in-memory 1,000-event ring buffer in `backend/src/stream.js` for `Last-Event-ID` resume.
   * *Consequence*: Lightweight and zero-dependency, but if the API container restarts, reconnecting clients cannot replay events generated prior to the restart.
3. **Multi-Tenancy & Row-Level Security (RLS)**:
   * *Trade-off*: Stored all 2,000 vehicles in a unified schema without tenant isolation keys.
   * *Consequence*: Simpler queries and indexes, but cannot isolate fleet data between multiple commercial operators on the same deployment.

---

## 7. A Real Bug Encountered & Fixed

### The Bug: Express `express.json()` Mutating Raw Payload for HMAC Verification
During Phase 4 development, legitimate signed simulator batches were intermittently rejected with `HTTP 401: "Invalid HMAC signature"`. 

### Root Cause
`hmacAuth` verified the signature by taking `JSON.stringify(req.body)`. Express's standard `express.json()` parser converts incoming JSON bytes into JavaScript objects, altering key ordering, whitespace, and floating-point representations (e.g. `13.0800` becoming `13.08`). The re-serialized string did not match the exact byte stream transmitted by the simulator, causing cryptographic hash mismatches.

### How We Proved It Was Fixed
In `backend/src/server.js`, we updated `express.json()` to capture the raw UTF-8 buffer directly during stream consumption:
```javascript
app.use(express.json({
  limit: '10mb',
  verify: (req, res, buf) => { req.rawBody = buf.toString('utf8'); }
}));
```
In `backend/src/security.js`, HMAC computation was switched to `req.rawBody`. We validated the fix in `backend/tests/api.int.test.js` with test suite assertions verifying that payloads with varying whitespace and floating-point formats authenticate with 100% reliability.
