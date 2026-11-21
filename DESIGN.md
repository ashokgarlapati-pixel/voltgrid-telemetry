# DESIGN.md

Everything below refers to this repo's code and to numbers I measured (`loadtest/results.md`). Where something isn't built, I say so.

## 1. Scale: 200,000 vehicles

Load goes from ~400 to ~40,000 readings/s. My best sustained ingest was ~1,350 readings/s (415 batches of 100 in 30.7 s, 10 workers, reads running at the same time), with batch p95 at 735 ms. I'm about 30x below the target rate.

1. **SQLite's single writer breaks first.** Ingestion, nonce inserts and token-bucket updates all write to one file behind one lock. Fleet-summary p95 went from 8.6 ms (quiet, 5M rows) to 651 ms in the mixed run. I put that down to write-lock queueing, though the two runs used different datasets. Fix: PostgreSQL with batched, time-partitioned inserts for telemetry, state and alerts, and Redis for nonces and rate limits, so auth traffic stops competing with ingestion.
2. **Alert evaluation and outbox execution path.** Ingestion uses a transactional outbox pattern to bridge synchronous low-latency alerting and crash recovery. In `ingestBatch`, the telemetry batch and an `alert_outbox` pending row are committed in the same atomic transaction. Immediately post-commit, inline evaluation (`evaluateAlertsForVehicles`) runs and marks the outbox row completed (`markOutboxEventsCompleted`) before returning HTTP 200/207, providing immediate alert dispatch without worker polling lag. If the process crashes after the ingestion transaction commits but before inline evaluation finishes, the reading is not lost: upon restart (and periodically every 500 ms), the background worker (`alertOutboxWorker`) scans `alert_outbox` for pending or stale-processing records (using a 30 s atomic lease) and drains them without duplicate alerts. At 200,000 vehicles, inline evaluation would shift entirely to dedicated background workers partitioned by `vehicle_id` so write latency is isolated from alert rule execution.
3. **Stream fan-out.** One `stream_events` row plus an in-process broadcast per update won't survive 40,000/s. Fix: a broker for cross-instance notification, periodic snapshots instead of per-update events, and a durable sequence ID on every event. Alert changes stay in durable history so `Last-Event-ID` resume works. Pub/sub alone can't replay.
4. **Storage.** 3.456 billion rows/day at my measured 273 B/row (indexes included) is ~943 GB/day. Fix: TimescaleDB or ClickHouse with daily partitions, dropping raw partitions only after rollups are verified.

## 2. Delivery guarantees

Devices retry, so delivery is at-least-once. Storage is effectively-once per `(vehicle_id, seq)`: that pair is the primary key and `INSERT OR IGNORE` runs inside a `BEGIN IMMEDIATE` transaction. A duplicate adds no history row, doesn't touch current state and doesn't bump `received_count`.

The concurrency test sends one 50-reading batch from 20 clients and gets 50 accepted, 950 duplicates. It runs in one Node process with a synchronous driver, so it proves the constraint and counters, not multi-process or multi-host behaviour.

Where it can still go wrong:
- After a reflash a device may reset `seq`, and its new readings are labelled duplicates. A repeated `seq` with different values keeps the first payload. A boot or session ID should join the dedup key.
- Alert evaluation crash safety: Telemetry ingestion and an `alert_outbox` job commit in the same atomic transaction. Under normal execution, alert evaluation runs inline immediately post-commit and completes the outbox record. If the process crashes between database commit and alert evaluation (or during background processing), the outbox worker reclaims the record upon restart or after a 30-second processing lease expires, guaranteeing evaluation without delaying until that vehicle's next reading.
- Stream events are published from memory after commit, so a crash can lose one. Durable history and replay are needed for reliable recovery.

## 3. Time

Device `ts` orders a vehicle's observations, picks its current state (highest `ts` wins, `seq` breaks ties) and drives the alert rules. Server time covers what the device can't establish: `updated_at`, offline detection and the 5-minute HMAC window.

**25 seconds fast:** accepted, since I allow `ts` up to 30 s ahead of the server. Its own readings stay consistently ordered, but comparisons with other vehicles are skewed by 25 s.

**A day wrong:** every reading fails validation and is rejected individually. `updated_at` never advances, so the vehicle goes offline after 90 s, which at least flags the problem. The data is lost.

The 10-minute lateness cutoff is also too strict for the batching the brief describes. 500 readings at 5 s intervals span ~41 minutes, so legitimate catch-up data can be rejected. I would accept old readings into history, mark them late and never let them replace newer state. I would also estimate each vehicle's clock offset from a rolling median of `received_at - ts`, flag large or sudden changes, and never silently rewrite stored timestamps.

## 4. Storage and cost (today's 2,000 vehicles)

34.56M rows/day. I measured 5M rows at 1,365 MB including indexes, so 273 B/row, about **9.4 GB/day** raw. The prototype keeps raw rows forever; this is a proposal.

- **Days 0–7:** raw 5 s readings, ~66 GB.
- **Days 8–90:** 1-minute buckets per vehicle (min/max/avg of SoC, temperature and speed; last position; received and expected counts; fault-code set). That is 2,000 × 1,440 = 2.88M rows/day. I haven't measured a bucket row, so I'd benchmark the real schema before quoting a size.
- **Days 90–365:** hourly buckets, 48,000 rows/day.
- **Alerts:** kept for audit.

After raw expiry an operator can no longer see individual readings, timing finer than a minute, or exact `seq` gaps. Min/max keep peaks and troughs but not their duration or shape. Received/expected counts per bucket keep a coarser completeness figure. At 200,000 vehicles every number above is 100x larger.

## 5. Threat model

1. **Captured-request replay.** The HMAC covers the timestamp and exact body bytes. Requests outside ±5 minutes are rejected, and accepted signatures are kept in `used_nonces` for 5 minutes. A second use gets 401, and the shared database makes this hold across both API instances. TLS must be terminated securely in front.
2. **Stolen device-group key.** Keys aren't scoped to vehicle IDs, so a holder can post readings for any vehicle: falsify state, affect alerts or inflate `max_seq`. Range validation, a shared per-key token bucket (20/s, burst 40) and production checks against dev or weak secrets reduce the risk but don't fix scope. I would bind keys to vehicle IDs, add rotation and revocation, and encrypt secrets at rest (they're plaintext in `api_keys` now).
3. **Request and connection flooding.** Bad headers and stale timestamps are rejected before any write, but valid requests cost nonce and bucket writes. The 10 MB body limit is far above a 500-reading batch (~150 KB), so I'd cut it to 512 KB. The read API, `/metrics` and `/v1/stream` have no auth or connection cap, so they need both.

## 6. Trade-offs: three things I didn't build

1. **PostgreSQL, Redis and a broker.** SQLite on a shared local volume kept the prototype simple. It is single-host (WAL relies on local shared memory), has one writer, and in-process stream broadcasts don't reach clients on the other API instance. Coalescing is also per vehicle only. Each vehicle reports every 5 s, so a client still sees about 400 events/s, not one frame per flush.
2. **Retention and rollups.** `telemetry_history` is never pruned, so storage grows ~9.4 GB/day now, and ~943 GB/day at 200,000 vehicles.
3. **Clock correction and long-backlog handling.** The 10-minute cutoff can reject valid catch-up readings, and a badly clocked vehicle can have everything rejected. The offset estimation and late-data handling in §3 aren't implemented.

## 7. A real bug

Correctly signed requests returned `401 Invalid HMAC signature`. I was verifying `JSON.stringify(req.body)` after `express.json` had parsed the body. Re-serialising doesn't reproduce the client's bytes: whitespace, number formatting (`13.0800` becomes `13.08`) and Unicode escapes can all change. The simulator's compact JSON happened to round-trip, which hid the bug, and a pretty-printed curl request exposed it.

I compared a digest of the bytes the server received against the digest the verifier computed and found the mismatch. The fix captures the raw bytes in `express.json`'s `verify` callback as `req.rawBody` and verifies against those. The security test posts an indented body containing `13.0800`, signed over the exact bytes, and expects 200. Changing one byte must return 401.
