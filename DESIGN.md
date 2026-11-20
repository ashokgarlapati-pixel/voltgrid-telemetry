# VoltGrid Fleet Telemetry Service — System Design & Reasoning

---

## 1. Scale: 100x Fleet Growth (200,000 Vehicles / ~40,000 Readings/sec)

### What Breaks First
The **SQLite single-writer lock** on `BEGIN IMMEDIATE` transactions in `ingestion.js` will break first. SQLite WAL mode permits concurrent readers, but allows strictly one writer process at any instant. At 2,000 vehicles (~400 readings/sec sent in batches of 100–500), write lock hold times average 5–15 ms, comfortably fitting within `PRAGMA busy_timeout = 5000`. At 40,000 readings/sec (~400 write transactions/sec), write lock contention will trigger serialized queue pileups, exceeding `busy_timeout` and throwing `SQLITE_BUSY: database is locked`.

### Next Three Bottlenecks in Exact Order
1. **Node.js Single-Threaded CPU Saturation (HMAC + JSON Parsing)**: Processing 400 requests/sec with 100–500 readings per batch requires parsing ~20 MB/sec of JSON and computing 400 HMAC-SHA256 hashes per second on a single event-loop thread.
   * *Mitigation*: Run stateless API instances behind Nginx/ALB using Node cluster mode or Kubernetes pods. Offload HMAC verification to an API Gateway.
2. **Disk I/O & B-Tree Write Amplification on `telemetry_history`**: Ingesting 3.45 billion rows/day (~760 GB/day) across composite indexes `(vehicle_id, seq)` and `(vehicle_id, ts)` causes massive random I/O write amplification, exhausting NVMe write endurance.
   * *Mitigation*: Introduce an append-only distributed log (Kafka) and columnar time-series storage (ClickHouse) partitioned daily by `(ts, vehicle_id)`.
3. **In-Memory SSE Fan-out & `offlineWorker` Periodic Scans**: Scanning 200,000 rows every 10s takes 50–100 ms, while `stream.js` must fan out 200,000 updates/sec across thousands of clients.
   * *Mitigation*: Replace table scans with a Redis Sorted Set (`ZREVRANGEBYSCORE vehicles:heartbeats`) to pop timed-out vehicles in $O(\log N)$ time. Distribute SSE via Redis Pub/Sub.

---

## 2. Delivery Guarantees & Idempotency

Our ingestion pipeline implements **at-least-once delivery with idempotent processing**, achieving an **effectively-once persistent outcome**:
1. `telemetry_history` enforces `PRIMARY KEY (vehicle_id, seq)`.
2. In `ingestion.js`, `executeBatchTransaction` uses `INSERT OR IGNORE INTO telemetry_history`.
3. If an incoming reading has already been recorded, SQLite returns `changes === 0`. The engine increments `duplicates` and skips the `vehicles` state upsert.
4. Concurrency test `backend/tests/concurrency.test.js` proves this: 20 concurrent clients firing the exact same 50-item batch simultaneously results in **exactly 50 accepted rows, 950 duplicate rows, and zero double-counting**.

### Where It Can Still Go Wrong
* **Firmware Sequence Reset**: If vehicle firmware reboots and resets non-volatile `seq` to 0, legitimate readings with `seq` below previous values are ignored by `INSERT OR IGNORE` until `seq` exceeds the previous lifetime maximum.
* **Network Partition Post-Commit**: If SQLite commits a batch transaction but a cellular drop occurs before HTTP `200` reaches the vehicle, the vehicle retries. Although storage remains idempotent, the vehicle client cannot confirm delivery until connection re-establishes.

---

## 3. Time, Drift, & Device Ordering

### Which Timestamp We Trust
* **Device Timestamp (`ts`)**: We trust `ts` for **vehicle operational state**, **time-series history ordering**, and **alert rules** (`overheat`, `rapid_discharge`, `geofence_breach`). In `ingestion.js`:
  ```sql
  last_ts = CASE WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq)
            THEN excluded.last_ts ELSE vehicles.last_ts END
  ```
  Out-of-order cellular packets buffered in tunnels never overwrite newer state with stale telemetry.
* **Server Timestamp (`received_at` / `updated_at`)**: We trust server time for **offline detection** (`offlineWorker.js`) and **HMAC replay window validation** (`security.js`). A dead scooter cannot report its own absence; silence must be measured relative to the server clock.

### Handling 25 Seconds Fast vs. 1 Day Off
* **25 Seconds Fast**: Accepted cleanly. Falls within our 5-minute security window ($|T_{\text{server}} - T_{\text{req}}| \le 300\text{s}$). State ordering is preserved via device `ts`.
* **1 Day Off**: Rejected immediately with `HTTP 401 Unauthorized` by `hmacAuth` in `security.js`. Prevents poisoning `vehicles.last_ts`, which would cause legitimate future readings to be discarded as late.

---

## 4. Alert Engine Architecture & Order Independence (Part A3)

### Alert Rules & State Evaluation
* **Overheat**: 3 consecutive readings `> 55.0°C` trigger; 3 consecutive readings `<= 50.0°C` resolve. Readings in the `50.0°C`–`55.0°C` band preserve the previous state (hysteresis).
* **Rapid Discharge**: Evaluates drop in `soc_pct` $> 8.0\%$ across non-charging readings within preceding 5 minutes in device-time. Resolves when charging begins, drop rate returns $\le 8.0\%$, or the 5-minute window expires.
* **Geofence Breach**: Ray-casting checks whether coordinates fall inside the Chennai operating polygon. Re-entering resolves the alert.
* **Offline Detection**: Periodic worker checks online vehicles silent $> 90\text{s}$ using server `updated_at`. When new telemetry arrives, the vehicle status resets to `online` and the active offline alert resolves atomically in the same transaction.

### Historical State Management & Subquery Correction
To evaluate alerts without unbounded table scans, `getRecentHistoryStmt` retrieves the latest 100 readings chronologically:
```sql
SELECT * FROM (
  SELECT * FROM telemetry_history WHERE vehicle_id = ? ORDER BY ts DESC, seq DESC LIMIT 100
) ORDER BY ts ASC, seq ASC;
```
Supported by composite covering index `idx_history_vehicle_ts_seq (vehicle_id, ts DESC, seq DESC)`. For high-frequency vehicles where 100 readings span $< 5$ minutes, the query expands to the complete 5-minute event window. Active alerts are persisted in SQLite with partial unique index:
```sql
CREATE UNIQUE INDEX idx_alerts_active_unique ON alerts (vehicle_id, rule) WHERE status != 'resolved';
```
When historical readings arrive in reverse or shuffled order, chronological replay reconstructs the exact alert lifecycle, ensuring identical final alert status, details, and active counts regardless of arrival order.

### Transaction Safety & Deferred Event Publishing
All alert state mutations occur inside `BEGIN IMMEDIATE` transactions. Changed alert records are collected and published to the SSE stream (`publishAlertChange`) strictly **after** transaction commit, ensuring zero phantom stream events on rollbacks.

---

## 5. Storage & Tiered Retention (~35 Million Rows / Day)

Each `telemetry_history` row averages ~120 bytes data + ~100 bytes index overhead = ~220 bytes:
$$\text{Daily Storage} = 35{,}000{,}000 \times 220\text{ bytes} \approx 7.7\text{ GB/day} \quad (\approx 230\text{ GB/month})$$

### Retention Tiers
1. **Hot Tier (0–7 Days)**: 100% raw 5-second readings in partitioned tables for immediate debugging (~54 GB).
2. **Warm Tier (8–90 Days)**: Automated nightly rollups downsampling raw rows into 1-minute buckets (`min/max/avg` for `soc_pct`, `battery_temp_c`, `speed_kmh`). 12x reduction (~15 GB).
3. **Cold Tier (91–365 Days)**: Roll up into 1-hour summaries; archive compressed Parquet files to AWS S3 Glacier ($0.0036/GB/month). Hard delete raw rows via partition dropping.

Operator visibility lost after downsampling: sub-minute transient temperature spikes, exact sequence gaps, and consecutive 3-reading streaks.

---

## 6. Threat Model & Abuse Protection

| Threat | Attack Vector | Exact Implementation Defense |
| :--- | :--- | :--- |
| **1. Request Tampering** | Rogue actor crafts payload to falsify battery or coordinates. | `hmacAuth` in `security.js`. Requires valid `X-Signature` computed via `HMAC-SHA256(secret, "${timestamp}.${rawBody}")` verified via `crypto.timingSafeEqual`. |
| **2. Stale Request Injection** | Attacker captures packets and delays transmission. | `hmacAuth` 5-minute validity window ($|T_{\text{server}} - T_{\text{req}}| \le 300\text{s}$). Rejects with `HTTP 401`. |
| **3. Replay Attacks** | Eavesdropper resends captured batch to manipulate state. | `used_nonces` table in `security.js`. Duplicate signature insertion fails (`PRIMARY KEY`), returning `HTTP 401`. Expired nonces purged via throttled sweeps. |
| **4. Distributed DoS** | Abusive clients spam endpoints to exhaust CPU/disk. | Shared SQLite token bucket in `security.js` (`rate_limits` table). Burst capacity = 40, refill = 20 req/s. Returns `HTTP 429` with `Retry-After: <sec>`. |

### Multi-Instance Coordination & Limitations
When two API instances share `voltgrid.db` across mounted volumes, `BEGIN IMMEDIATE` transactions and OS file locks in WAL mode serialize token subtractions and signature checks across processes. Shared SQLite requires local disk on a single host; network filesystems (NFS/SMB) are unsafe due to broken POSIX file-locking semantics.

---

## 7. Trade-offs: Deliberate Non-Goals

1. **Distributed MQ & Clustered DB (Kafka + Postgres)**: Chose single-process Node.js with SQLite WAL. Zero operational complexity, but write throughput ceiling is ~1,500 req/s.
2. **Durable SSE Event Log Across Restarts**: Chose an in-memory 1,000-event ring buffer in `stream.js`. Lightweight, but reconnecting clients cannot replay events prior to container restart.
3. **Multi-Tenancy & Row-Level Security**: Stored all vehicles in unified schema without tenant isolation keys, simplifying queries at the cost of commercial tenant separation.

---

## 8. Real Bug Encountered & Fixed

### Express `express.json()` Mutating Raw Payload for HMAC Verification
* **Bug**: Legitimate signed batches were intermittently rejected with `HTTP 401: "Invalid HMAC signature"`.
* **Root Cause**: `hmacAuth` verified the signature using `JSON.stringify(req.body)`. `express.json()` re-serializes JSON with different whitespace and float precision (e.g. `13.0800` becoming `13.08`), breaking cryptographic matching.
* **Fix**: In `server.js`, captured the raw UTF-8 buffer directly during stream consumption:
  ```javascript
  app.use(express.json({ limit: '10mb', verify: (req, res, buf) => { req.rawBody = buf.toString('utf8'); } }));
  ```
  In `security.js`, HMAC computation verified against `req.rawBody`. Verified with tests covering diverse float and whitespace formats.
