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
* **Transport Request Timestamp (`X-Timestamp`)**: Checked against server clock ($|T_{\text{server}} - T_{\text{req}}| \le 300\text{s}$, $\pm 5$ minutes). Rejects expired or replayed requests with `HTTP 401 Unauthorized`.
* **Telemetry Reading Timestamp (`reading.ts`)**: Checked against server clock window $[T_{\text{server}} - 10\text{ minutes}, T_{\text{server}} + 30\text{ seconds}]$.
  - **Lateness (Up to 10 minutes)**: Accepted (handles cellular queuing and tunnel delays). Older than 10 minutes: Rejected (`HTTP 207/400`, `"ts cannot be older than 10 minutes"`).
  - **Drift (Up to 30 seconds fast)**: Accepted (handles vehicle RTC clock drift). More than 30 seconds fast: Rejected (`"ts cannot be more than 30 seconds in the future"`), preventing poison readings from locking `vehicles.last_ts` and permanently corrupting live state updates.

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

Each row in `telemetry_history` is ~120 bytes plus ~100 bytes of index overhead (~220 bytes total).
At 400 readings/sec, that is $35{,}000{,}000 \times 220\text{ B} \approx 7.7\text{ GB/day}$ (~230 GB/month).

### Tiered Retention Strategy
1. **Hot Tier (0–7 Days)**: Full raw 5-second readings in SQLite/NVMe for live operations and triage (~54 GB).
2. **Warm Tier (8–90 Days)**: Automated nightly rollups downsampling into 1-minute `min/max/avg` buckets for SOC, battery temperature, and speed (~15 GB).
3. **Cold Tier (91–365 Days)**: Hourly rollups exported to compressed Parquet files on S3 Glacier ($0.0036/GB/month). Drop old raw partitions.
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
* **Fleet Summary SLA**: Derived strictly from `vehicles` (2,000 rows) and active `alerts` (`WHERE status != 'resolved'`). Never scans `telemetry_history`. $p95$ latency is under 2 ms.
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
