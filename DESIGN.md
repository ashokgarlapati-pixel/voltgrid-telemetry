# VoltGrid Fleet Telemetry Service — System Design & Architecture

## 1. Scale: Scaling to 200,000 Vehicles (~40,000 readings/sec)

### The First Failure Point
The single-writer bottleneck in SQLite's WAL mode breaks first. While WAL allows concurrent readers, write transactions (`BEGIN IMMEDIATE`) are strictly serialized. At our current 2,000-vehicle fleet (~400 readings/sec in batches), lock hold times average 5–15 ms, easily staying within `busy_timeout = 5000`. At 40,000 readings/sec (~400 batches/sec), writes queue up faster than SQLite can commit them to disk, triggering `SQLITE_BUSY: database is locked`.

### Subsequent Bottlenecks & Fixes
1. **Node.js Event Loop Saturation**: Parsing ~20 MB/sec of JSON and computing 400 HMAC-SHA256 hashes per second will peg a single core at 100% CPU.
   * *Fix*: Put stateless Node instances behind an ALB/Nginx using Node cluster mode, and offload HMAC verification to an API Gateway.
2. **Disk I/O Write Amplification**: Ingesting ~3.45 billion rows/day across composite indexes `(vehicle_id, seq)` and `(vehicle_id, ts)` will exhaust NVMe write endurance.
   * *Fix*: Stream telemetry into Apache Kafka, then batch-load into a columnar time-series database (ClickHouse) partitioned daily by date.
3. **In-Memory SSE Fan-out & Heartbeat Scans**: Scanning 200k vehicles every 10s takes ~100 ms, while pushing 40k events/sec to thousands of browser clients will choke Node memory.
   * *Fix*: Use a Redis Sorted Set (`ZREVRANGEBYSCORE`) for $O(\log N)$ offline sweeps, and fan out SSE via Redis Pub/Sub.

---

## 2. Ingestion Delivery & Idempotency

We use an **at-least-once delivery model with idempotent writes** to achieve effectively-once state:
1. `telemetry_history` has a compound primary key: `PRIMARY KEY (vehicle_id, seq)`.
2. In `ingestion.js`, batches run inside an immediate transaction using `INSERT OR IGNORE`. If a reading already exists, SQLite ignores it (`changes === 0`), increments our duplicate counter, and skips updating the vehicle's live state.
3. Our 20-client concurrency test proves this: sending the same 50-item batch simultaneously across 20 connections results in exactly 50 accepted rows, 950 duplicate drops, and zero data corruption.

### Where Edge Cases Still Bite
* **Firmware Sequence Reset**: If an IoT device resets its hardware `seq` to 0 after a firmware flash, `INSERT OR IGNORE` will drop legitimate readings until `seq` exceeds the vehicle's historical maximum.
* **Network Partition on Commit**: If SQLite commits a batch but the cellular connection drops before the HTTP 200 response reaches the vehicle, the vehicle retries. The retry is safely deduplicated, but client confirmation is delayed.

---

## 3. Clock Drift, Timestamps & Ordering

### What We Trust
* **Device Time (`ts`)**: We trust device timestamp for vehicle telemetry history, downsampling, and alert evaluations (`overheat`, `rapid_discharge`, `geofence`). In `ingestion.js`, we update the `vehicles` state table using:
  `last_ts = CASE WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) THEN excluded.last_ts ELSE vehicles.last_ts END`
  This guarantees that delayed readings coming out of cell tunnels never overwrite newer state.
* **Server Time (`updated_at`)**: We trust server time for offline detection (`offlineWorker.js`) and HMAC replay window checks (`security.js`). A disconnected vehicle cannot report its own death; silence must be measured by the server's clock.

### Separation of Timestamps & Tolerance Windows

It is critical to distinguish between two completely independent timestamp validations operating at different layers of the request lifecycle:

1. **HTTP Transport Authentication Timestamp (`X-Timestamp`)**:
   * **Validated By**: `backend/src/security.js` (`hmacAuth` middleware).
   * **Tolerance Window**: $|T_{\text{server}} - T_{\text{req}}| \le 300{,}000\text{ ms}$ ($\pm 5$ minutes against server wall clock).
   * **Purpose**: Bounds the freshness of HTTP batch submissions and defines the TTL for nonce de-duplication in `used_nonces`.
   * **Rejection Behavior**: An expired, future-skewed (>5 min), or unparseable `X-Timestamp` halts request processing at the authentication boundary, returning **`HTTP 401 Unauthorized`** (`"Request timestamp is outside the allowed 5-minute window"`).

2. **Telemetry Reading Event Timestamp (`reading.ts`)**:
   * **Validated By**: `backend/src/ingestion.js` (`validateReading` in the ingestion pipeline).
   * **Tolerance Window**: $[T_{\text{server}} - 10\text{ minutes}, T_{\text{server}} + 30\text{ seconds}]$.
     - **Lateness (Up to 10 minutes)**: Accepted to accommodate cellular queuing and tunnel transit latency. Older than 10 minutes is rejected (`"ts cannot be older than 10 minutes"`).
     - **Clock Drift (Up to 30 seconds fast)**: Accepted to handle vehicle RTC hardware oscillator variance. More than 30 seconds fast is rejected (`"ts cannot be more than 30 seconds in the future"`), preventing future-timestamp state poisoning from locking `vehicles.last_ts` and corrupting live vehicle updates.
   * **Rejection Behavior**: Out-of-window telemetry timestamps are rejected at the reading level within the ingestion pipeline, returning **`HTTP 207 Multi-Status`** with `status: 'rejected'` (or `HTTP 400` if the entire batch structure is invalid). They are **never** reported as authentication failures (`HTTP 401`).

### Clarifying the "One-Day Clock Error" Misconception

An earlier draft suggested that a telemetry reading with a 1-day clock error returns `HTTP 401 Unauthorized`. That is technically incorrect:

* **Authentication does not inspect payload timestamps**: The authentication middleware (`hmacAuth`) inspects only HTTP transport headers (`X-API-Key`, `X-Timestamp`, `X-Signature`). If `X-Timestamp` falls within the allowed 5-minute window and `X-Signature` correctly matches the HMAC digest computed over that timestamp and that specific raw payload body, authentication succeeds (`next()`) and the request proceeds downstream to the ingestion handler.
* **Ingestion rejects stale or future readings independently**: If the batch payload contains a reading with a 1-day clock error, `validateReading` rejects that individual reading. The final API response sent by the route handler is `HTTP 207 Multi-Status`, **not** `HTTP 401`.

#### Concrete Example:
A vehicle client transmits an HTTP request at current server time $T_{\text{server}} = \text{12:00:00}$:
* **Headers**:
  * `X-Timestamp: 12:00:00` (matches current server wall clock).
  * `X-Signature`: A valid hex digest computed strictly over this specific batch payload: $\text{HMAC-SHA256}(\text{secret}, \text{"12:00:00."} \mathbin{\Vert} \text{rawBody})$. (Note: The signature binds cryptographically to this exact body; any alteration to the body invalidates the signature).
* **Payload**:
  ```json
  [
    {
      "vehicle_id": "VG-00042",
      "seq": 105,
      "ts": "2026-11-20T12:00:00.000Z",
      "soc_pct": 78.5,
      "battery_temp_c": 32.0,
      "speed_kmh": 40.0,
      "lat": 13.0827,
      "lng": 80.2707,
      "odometer_km": 150.0,
      "charging": false,
      "fault_codes": []
    }
  ]
  ```
  *(Note that `ts` is dated 1 day in the past due to an un-synchronized vehicle RTC clock).*

* **Lifecycle & Result**:
  1. **Transport Layer (`security.js`)**: $|T_{\text{server}} - \text{X-Timestamp}| = 0 \le 300\text{s}$, and the provided `X-Signature` matches the computed digest over the payload. Authentication succeeds and delegates to `next()` without sending a response. The request proceeds to the ingestion route handler.
  2. **Ingestion Layer (`ingestion.js`)**: The request payload is processed. `reading.ts` is 24 hours old ($> 10\text{ minutes}$). `validateReading` rejects this reading with `"ts cannot be older than 10 minutes"`.
  3. **Final API Response**: The route handler finishes processing and sends the final response as **`HTTP 207 Multi-Status`** (NOT `HTTP 401`):
     ```json
     {
       "total": 1,
       "accepted": 0,
       "duplicates": 0,
       "rejected": 1,
       "summary": { "accepted": 0, "duplicates": 0, "rejected": 1 },
       "results": [
         {
           "index": 0,
           "status": "rejected",
           "error": "ts cannot be older than 10 minutes",
           "vehicle_id": "VG-00042",
           "seq": 105
         }
       ]
     }
     ```
  Thus, a valid HTTP authentication timestamp never renders an excessively old or future-dated telemetry reading valid, and telemetry timestamp rejection remains strictly decoupled from transport authentication.

---

## 4. Alert Engine & Order Independence

### Rule Logic
* **Overheat**: Triggers after 3 consecutive readings `> 55.0°C`. Resolves after 3 consecutive readings `<= 50.0°C`. The 50–55°C band uses hysteresis to prevent alert flapping.
* **Rapid Discharge**: Triggers if SOC drops $> 8.0\%$ across non-charging readings within a rolling 5-minute device-time window. Resolves when charging starts, discharge slows, or the 5m window expires.
* **Geofence Breach**: Ray-casting algorithm checks coordinates against the Chennai operating polygon. Re-entering clears the alert.
* **Offline Detection**: A background worker checks online vehicles whose server `updated_at` is older than 90s. When new telemetry arrives, the vehicle resets to `online` and resolves the offline alert in the same database transaction.

### Replay & Order Independence
When packets arrive out-of-order, we pull the vehicle's recent history ordered chronologically:
```sql
SELECT * FROM (
  SELECT * FROM telemetry_history WHERE vehicle_id = ? ORDER BY ts DESC, seq DESC LIMIT 100
) ORDER BY ts ASC, seq ASC;
```
Re-evaluating the rule over this chronological slice reconstructs the exact state transitions. SSE alert events are emitted only **after** the database transaction successfully commits.

---

## 5. Storage Economics (~35M Rows/Day)

### Implementation Reality vs. Production Architecture
The prototype stores all telemetry as raw records in SQLite `telemetry_history`. It does **not** implement automated background rollups, warm-tier aggregation tables, or cold-tier Parquet archival (only `stream_events` implements retention pruning, capped at 10,000 events). The tiered model below outlines the proposed production lifecycle.

### Ingestion Volume & Row Footprint
* **Workload**: 2,000 vehicles transmitting every 5s = 400 readings/s ($34{,}560{,}000\text{ readings/day} \approx 34.56\text{M}$, rounded to ~35M).
* **Theoretical Row Footprint (~220 B/row)**: ~120 B raw row payload (`vehicle_id`, `seq`, `ts`, 7 metric floats, bool, status, timestamps) + ~100 B index overhead (`PRIMARY KEY (vehicle_id, seq)` ~40 B + `idx_history_vehicle_ts` ~60 B).
* **Empirical Measurement (~273 B/reading)**: The dedicated 5M-reading benchmark database file is exactly $1{,}365{,}295{,}104\text{ bytes}$ ($1{,}302.05\text{ MiB}$ binary, or $1{,}365.30\text{ MB}$ decimal). Dividing total database size by 5,000,000 readings yields $273.06\text{ bytes/reading}$. Note that this includes the entire database (schema, `telemetry_history`, `vehicles`, `alerts`, and all associated B-tree pages/indexes); it is an empirical system footprint, not an isolated row-payload measurement.
* **Storage Factors & Overhead**:
  * *B-Tree Utilization*: Storage overhead depends on schema, index count, page size (4,096 B), and insertion patterns (append-only vs random updates). The 273.06 B/reading measurement reflects empirical overhead for this workload, not a universal fill-factor guarantee.
  * *WAL Overhead*: The SQLite `-wal` file size is workload- and checkpoint-dependent (governed by checkpoint policy, write velocity, and active readers), acting as additional temporary disk usage.

### Daily & 7-Day Hot-Tier Storage Comparison
* **Theoretical Storage (at 220 B/row)**:
  * *Daily*: $34.56\text{M} \times 220\text{ B} \approx 7.60\text{ GB/day}$ ($7.08\text{ GiB}$; rounded 35M: $7.70\text{ GB/day}$).
  * *7-Day*: $7 \times 7.60\text{ GB} \approx 53.22\text{ GB}$ ($49.57\text{ GiB}$; rounded 35M: $53.90\text{ GB} \approx 54\text{ GB}$).
* **Empirical Storage (at 273.06 B/reading, benchmark-derived)**:
  * *Daily*: $34.56\text{M} \times 273.06\text{ B} \approx 9.44\text{ GB/day}$ ($8.79\text{ GiB}$; rounded 35M: $9.56\text{ GB/day}$).
  * *7-Day*: $7 \times 9.44\text{ GB} \approx 66.06\text{ GB}$ ($61.52\text{ GiB}$; rounded 35M: $66.90\text{ GB} \approx 67\text{ GB}$).

### Tiered Retention Strategy (Architectural Proposal)
1. **Hot Tier (0–7 Days)**: Full raw 5s readings in SQLite/NVMe for live triage (53–54 GB theoretical raw; 66–67 GB empirical database allocation).
2. **Warm Tier (8–90 Days — Illustrative Design Target)**: Proposed nightly rollups downsampling into 1-minute `min/max/avg` buckets:
   * *Rollup Volume*: $2{,}000\text{ vehicles} \times 1{,}440\text{ min/day} = 2{,}880{,}000\text{ rows/day}$. Over 83 days (days 8–90), this yields $239{,}040{,}000\text{ rows}$ ($259.2\text{M rows}$ for 90 days).
   * *Uncompressed Reality*: An uncompressed SQLite rollup table (~160 B/row with indexes) requires **~38.2–41.5 GB**, not 15 GB.
   * *Derivation of the ~15 GB Figure*: The original ~15 GB estimate is an **unverified design target** assuming either:
     - **Columnar/compressed storage** (e.g. Parquet or zstd with $\ge 2.5:1$ compression ratio, yielding ~60 B/row: $239\text{M} \times 60\text{ B} \approx 14.3\text{ GB} \approx 15\text{ GB}$), or
     - A smaller baseline fleet of **1,000 vehicles** ($1{,}000 \times 1{,}440 \times 90 \times 116\text{ B} \approx 15.0\text{ GB}$).
3. **Cold Tier (91–365 Days)**: Hourly rollups exported to compressed Parquet files on S3 Glacier ($0.0036/GB/month). Drop raw partitions.
*Trade-off*: Downsampling loses sub-minute transient thermal spikes and fine-grained sequence gap detection.

---

## 6. Threat Model & Abuse Protection

* **Request Tampering**: All batch ingestion requires `X-API-Key`, `X-Timestamp`, and `X-Signature`. We verify HMAC-SHA256 signatures over `${timestamp}.${rawBody}` using `crypto.timingSafeEqual`.
* **Replay Attacks**: Signatures are cached in `used_nonces` table with a 5-minute expiry. Duplicate signatures fail immediately. Expired nonces are purged via throttled cleanups.
* **Distributed DoS / Spam**: We maintain an atomic token-bucket rate limiter in SQLite (`rate_limits` table: capacity 40, refill 20 req/s). Exceeded quotas return `HTTP 429` with `Retry-After`.
* **Multi-Instance Coordination**: When two Docker containers share the same SQLite file, `BEGIN IMMEDIATE` transactions rely on OS file locking in WAL mode to serialize token consumption without Redis.

---

## 7. Real Production Bug Hit During Development

### Express `express.json()` Breaking HMAC Signatures
* **Symptom**: Validly signed batches intermittently returned `HTTP 401 Unauthorized: Invalid HMAC signature`.
* **Cause**: We initially re-serialized `JSON.stringify(req.body)` to verify the HMAC. Express's JSON parser strips arbitrary whitespace and re-formats floating-point numbers (e.g. `13.0800` becomes `13.08`), changing the byte stream and invalidating cryptographic hashes.
* **Fix**: In `server.js`, we used Express's `verify` callback to capture the raw UTF-8 payload buffer directly into `req.rawBody`:
  ```javascript
  app.use(express.json({ limit: '10mb', verify: (req, res, buf) => { req.rawBody = buf.toString('utf8'); } }));
  ```
  `security.js` now verifies against `req.rawBody`, matching the device's exact transmitted bytes.

---

## 8. Query Engine & Optimization

* **Keyset Pagination (`GET /v1/vehicles`)**: Avoided `OFFSET` because it requires scanning and dropping $N$ rows and drifts under streaming writes. Instead, we use `WHERE vehicle_id > @cursor ORDER BY vehicle_id ASC LIMIT @limit + 1`, backed by index `idx_vehicles_status_id (status, vehicle_id)`. `EXPLAIN QUERY PLAN` confirms zero temporary B-tree sorting.
* **Peak-Preserving Downsampling**: Naive interval sampling skips sharp temperature spikes. We split the time window into $\lfloor (P - 2) / 4 \rfloor$ buckets and extract minimum and maximum values for temperature, minimum SOC, and maximum speed, preserving peaks under 140 ms for 17,280 readings.
* **Fleet Summary SLA**: Derived strictly from `vehicles` (1,000–2,000 rows) and active `alerts` (`WHERE status != 'resolved'`). Never scans `telemetry_history`. SQLite query executes in < 1 ms (end-to-end HTTP $p95$ under 10 ms vs 150 ms SLA limit).
* **Optimistic Locking on Alert Ack**: Dispatchers include `version` in `POST /v1/alerts/:id/ack`. The query runs `WHERE id = @id AND version = @version AND status = 'open'`. If another dispatcher already acknowledged it, SQLite updates 0 rows and returns `409 Conflict`.

---

## 9. Live Stream (SSE) & Back-Pressure

* **Durable Event Outbox**: Rather than volatile in-memory queues that vanish on container reboot, events are written to `stream_events (id INTEGER PRIMARY KEY AUTOINCREMENT)`. Clients reconnecting with `Last-Event-ID` get exact monotonic replay across server restarts.
* **Replay-to-Live Handoff**: When a reconnecting client is catching up on historical events, incoming live events are queued in memory. Once database replay catches up, the queue is drained and deduplicated before switching to live broadcast.
* **1-Second Vehicle Coalescing**: Fanning out 400 events/sec floods client UI threads. We coalesce vehicle updates into an in-memory map for 1 second, flushing only the latest state per vehicle. Alert events bypass this and fire instantly.
* **Back-Pressure Disconnect**: If a client's TCP socket saturates (`res.write()` returns `false`), frames buffer in memory up to 100 items. If the client stays stalled beyond 100 frames, we close the socket to protect server heap.

---

## 10. Migrations, Query Plans & Observability

* **Migration Tracking**: `schema_migrations` tracks versions (`001_init.sql` to `004_stream.sql`). Each unapplied migration runs in an atomic `BEGIN IMMEDIATE` block. Running the app repeatedly skips applied scripts with zero locks.
* **Top Query Plans**: Both history downsampling (`idx_history_vehicle_ts_seq`) and keyset vehicle listing (`idx_vehicles_status_id`) run via composite index seeks with zero full table scans.
* **Structured Logs & Probes**: Requests generate or propagate `X-Request-Id` and log single-line JSON with duration and status, keeping credentials redacted. `/health/live` checks process responsiveness, while `/health/ready` runs `SELECT 1` on SQLite, returning 503 if disk I/O fails. `/metrics` exposes process latency percentiles (p50/p95/p99) and domain counters without external agents.
