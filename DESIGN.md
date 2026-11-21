# VoltGrid Fleet Telemetry Service — System Design & Architecture

## 1. Scale: Scaling to 200,000 Vehicles (Theoretical ~40,000 readings/sec)

### The First Failure Point
The single-writer bottleneck in SQLite's WAL mode breaks first. While WAL allows concurrent readers, write transactions (`BEGIN IMMEDIATE`) are strictly serialized. At our 2,000-vehicle baseline fleet (~400 readings/s theoretical volume), lock hold times average 5–15 ms (well within `busy_timeout = 5000ms`). Scaling to 200,000 vehicles is a theoretical 100x projection (40,000 readings/s, not a measured benchmark). Write request rates follow $\text{Batches/s} = \text{Readings/s} / \text{Avg Batch Size}$. The figure of ~400 batches/s holds *only* if the average batch is 100 readings (as in our mixed load test). At the maximum permitted batch size of 500 (`server.js`), write load is 80 batches/s; at 50 readings/batch (`scripts/loadtest.js`), it is 800 batches/s; and unbatched it is 40,000 req/s. Under any batch size, serialized SQLite WAL writes queue up faster than disk commits, triggering `SQLITE_BUSY`. Furthermore, SQLite WAL mode requires same-host shared-memory coordination via its memory-mapped WAL index (`.shm`), making it unsuitable for multi-machine scale-out over network filesystems (NFS/EFS).

### Subsequent Bottlenecks & Fixes
1. **Node.js Event Loop Saturation**: Parsing ~20 MB/s of JSON and computing 400 HMAC-SHA256 hashes/s saturates a single CPU core.
   * *Fix*: Offload HMAC verification to an API Gateway and deploy stateless Node instances in cluster mode behind an ALB/Nginx.
2. **Disk I/O Write Amplification**: Ingesting ~3.45 billion rows/day across composite indexes `(vehicle_id, seq)` and `(vehicle_id, ts)` exhausts NVMe write endurance.
   * *Fix*: Stream telemetry into Apache Kafka, then batch-load into a columnar time-series database (ClickHouse) partitioned daily.
3. **In-Memory SSE Fan-out & Heartbeat Scans**: Scanning 200k vehicles every 10s takes ~100 ms; pushing 40k events/s exhausts Node heap memory.
   * *Fix*: Use a Redis Sorted Set (`ZREVRANGEBYSCORE`) for $O(\log N)$ offline sweeps, and fan out SSE via Redis Pub/Sub.

---

## 2. Ingestion Delivery & Idempotency

We use an **at-least-once delivery model with idempotent writes** to achieve effectively-once state:
1. `telemetry_history` enforces database-level idempotency via compound `PRIMARY KEY (vehicle_id, seq)`.
2. In `ingestion.js`, batches execute inside an immediate transaction using `INSERT OR IGNORE`. Existing readings are dropped (`changes === 0`), duplicate counters increment, and vehicle state upserts are safely bypassed.
3. Our 20-client concurrency test validates this: submitting the same 50-item batch across 20 parallel connections yields exactly 50 accepted rows, 950 duplicate drops, and zero data corruption.

### Edge Failure Modes
* **Firmware Sequence Reset**: If an IoT device resets hardware `seq` to 0 post-flash, `INSERT OR IGNORE` drops legitimate readings until `seq` exceeds the historical maximum.
* **Network Partition on Commit**: If SQLite commits a batch but cellular connection drops before HTTP 200 reaches the vehicle, the client retries. Deduplication prevents duplicates, but client confirmation is delayed.

---

## 3. Clock Drift, Timestamps & Ordering

### What We Trust
* **Device Time (`ts`)**: Trusted for telemetry history, downsampling, and alert evaluations. In `ingestion.js`, we update the `vehicles` state table using:
  `last_ts = CASE WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) THEN excluded.last_ts ELSE vehicles.last_ts END`
  This guarantees that delayed readings emerging from cellular dead-zones never overwrite newer state.
* **Server Time (`updated_at` / `received_at`)**: Trusted for offline detection sweeps (`offlineWorker.js`) and HMAC replay window checks (`security.js`). A disconnected vehicle cannot report its own death; silence must be measured by the server clock.

### Decoupled Timestamp Validations
We enforce two completely independent timestamp validations operating at different lifecycle layers:
1. **HTTP Transport Authentication (`X-Timestamp`)**: Evaluated in `security.js` against a $\pm 5$-minute tolerance window ($|T_{\text{server}} - T_{\text{req}}| \le 300\text{s}$) to prevent replay attacks. Expired or future-skewed timestamps halt processing at the auth boundary, returning **`HTTP 401 Unauthorized`**.
2. **Telemetry Reading Timestamp (`reading.ts`)**: Evaluated per reading inside the ingestion pipeline (`ingestion.js`) against $[T_{\text{server}} - 10\text{m}, T_{\text{server}} + 30\text{s}]$. Stale readings (>10 min lateness) or future-skewed readings (>30s clock drift) are individually rejected, returning **`HTTP 207 Multi-Status`** with `status: 'rejected'` (never HTTP 401).

*The "1-Day Clock Error" Distinction*: If a vehicle with a valid HMAC signature and fresh `X-Timestamp` submits a reading dated 1 day in the past or future, transport authentication succeeds and hands off to ingestion (`next()`). The ingestion engine then rejects the stale reading with `HTTP 207 Multi-Status`—**never** `HTTP 401`.

---

## 4. Alert Engine & Order Independence

### Rule Logic
* **Overheat**: Triggers after 3 consecutive readings `> 55.0°C`. Resolves after 3 consecutive readings `<= 50.0°C` (50–55°C hysteresis prevents flapping).
* **Rapid Discharge**: Triggers if SOC drops $> 8.0\%$ across non-charging readings within a rolling 5-minute window. Resolves when charging starts, discharge slows, or the 5m window expires.
* **Geofence Breach**: Ray-casting point-in-polygon algorithm checks coordinates against the Chennai operating perimeter. Re-entry clears the alert.
* **Offline Detection**: A background worker checks online vehicles whose server `updated_at` is older than 90s. When new telemetry arrives, the vehicle resets to `online` and resolves the offline alert in the same database transaction.

### Replay & Order Independence
When packets arrive out-of-order, we pull recent history ordered chronologically:
```sql
SELECT * FROM (SELECT * FROM telemetry_history WHERE vehicle_id = ? ORDER BY ts DESC, seq DESC LIMIT 100) ORDER BY ts ASC, seq ASC;
```
Replaying over this chronological slice reconstructs identical state transitions regardless of arrival order. Alert changes are committed inside `BEGIN IMMEDIATE` transactions; SSE events (`publishAlertChange`) are emitted strictly **post-commit**, preventing phantom events on rollback.

---

## 5. Storage Economics (~35M Rows/Day)

### Implementation Reality vs. Production Architecture
The prototype stores all telemetry as raw records in SQLite `telemetry_history`. It does **not** implement automated background rollups, warm-tier aggregation tables, or cold-tier Parquet archival (only `stream_events` implements retention pruning, capped at 10,000 events). The tiered model below outlines the proposed production lifecycle.

### Ingestion Volume & Sizing (2,000 Vehicles @ 400 readings/s = 34.56M rows/day)
* **Theoretical Sizing (~220 B/row)**: ~120 B payload + ~100 B index overhead across `PRIMARY KEY (vehicle_id, seq)` and `(vehicle_id, ts)`.
  * *Daily*: $34.56\text{M} \times 220\text{ B} \approx 7.60\text{ GB/day}$ ($7.08\text{ GiB}$; rounded 35M: $7.70\text{ GB/day}$).
  * *7-Day Hot Tier*: $7 \times 7.60\text{ GB} \approx 53.22\text{ GB}$ ($49.57\text{ GiB}$; rounded 35M: $53.90\text{ GB} \approx \mathbf{54\text{ GB}}$).
* **Empirical Measurement (~273 B/reading)**: The dedicated 5M benchmark database is exactly $1{,}365{,}295{,}104\text{ bytes}$ ($1{,}302.05\text{ MiB}$ binary / $1{,}365.30\text{ MB}$ decimal), yielding $273.06\text{ bytes/reading}$ across the full database file (`telemetry_history`, `vehicles`, `alerts`, indexes, and B-tree page allocations).
  * *Daily*: $34.56\text{M} \times 273.06\text{ B} \approx 9.44\text{ GB/day}$ ($8.79\text{ GiB}$; rounded 35M: $9.56\text{ GB/day}$).
  * *7-Day Hot Tier*: $7 \times 9.44\text{ GB} \approx 66.06\text{ GB}$ ($61.52\text{ GiB}$; rounded 35M: $66.90\text{ GB} \approx \mathbf{67\text{ GB}}$).
* **Storage Realities**: Actual SQLite page overhead depends on schema, page size (4KB), index count, and insertion patterns (empirical measurement, not a universal B-tree fill-factor guarantee). SQLite `-wal` size is workload- and checkpoint-dependent temporary disk usage.

### Tiered Retention Strategy (Architectural Proposal)
1. **Hot Tier (0–7 Days)**: Full raw 5s readings in SQLite/NVMe for live triage (53–54 GB theoretical raw; 66–67 GB empirical database allocation).
2. **Warm Tier (8–90 Days — Illustrative Design Target)**: Proposed nightly rollups downsampling into 1-minute `min/max/avg` buckets ($2{,}000\text{ vehicles} \times 1{,}440\text{ min/day} \times 83\text{ days} = 239{,}040{,}000\text{ rows}$). Uncompressed SQLite storage (~160 B/row with indexes) requires **~38.2–41.5 GB**, not 15 GB. The ~15 GB figure is an **unverified design target** assuming either $\ge 2.5:1$ columnar/compressed storage (~60 B/row: $239\text{M} \times 60\text{ B} \approx 14.3\text{ GB} \approx 15\text{ GB}$) or a 1,000-vehicle baseline fleet ($1{,}000 \times 1{,}440 \times 90 \times 116\text{ B} \approx 15.0\text{ GB}$).
3. **Cold Tier (91–365 Days)**: Hourly rollups exported to compressed Parquet files on S3 Glacier ($0.0036/GB/month). Drop raw partitions.
*Trade-off*: Downsampling loses sub-minute transient thermal spikes and fine-grained sequence gap detection.

---

## 6. Threat Model & Abuse Protection

* **Request Tampering**: All batch ingestion requires `X-API-Key`, `X-Timestamp`, and `X-Signature`. We verify HMAC-SHA256 signatures over `${timestamp}.${rawBody}` using `crypto.timingSafeEqual`.
* **Replay Attacks**: Signatures are cached in `used_nonces` table with a 5-minute expiry. Duplicate signatures fail immediately. Expired nonces are purged via throttled cleanups.
* **Distributed DoS / Spam**: We maintain an atomic token-bucket rate limiter in SQLite (`rate_limits` table: capacity 40, refill 20 req/s). Exceeded quotas return `HTTP 429` with `Retry-After`.
* **Multi-Instance Scope & Volume Driver Boundaries**: In `docker-compose.yml`, `api` and `api2` share `/data/voltgrid.db` via named volume `sqlite_data` using Docker's default `local` driver on the same host. `BEGIN IMMEDIATE` starts a write transaction and obtains the required write lock, allowing SQLite's transaction and locking mechanisms to coordinate competing writers and protect token buckets, nonces, and idempotent ingestions without Redis. In WAL mode, the memory-mapped WAL index (`.shm` file) supports same-host WAL coordination and reader/writer synchronization, rather than independently serializing application state. This safe multi-process sharing applies specifically to this same-host local volume setup and must not be generalized to remote or network volume drivers. WAL mode requires same-host shared-memory coordination and is unsuitable across machines over network filesystems (NFS/EFS). Conversely, file-based rollback journals (`DELETE`, `TRUNCATE`, `PERSIST`) write journals to disk and rely on filesystem locks rather than shared memory, while `MEMORY` mode stores its rollback journal entirely in memory without a journal file; however, network filesystem locking flaws (`lockd`) and cache incoherency still pose high data-corruption risks across configurations. Multi-host scaling requires PostgreSQL, Redis, and Kafka/ClickHouse.

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
