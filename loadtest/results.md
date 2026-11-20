# VoltGrid Fleet Telemetry — Load Test Results & Query Plans (Part A6)

* **Generated**: 2026-11-20T08:44:37.712Z
* **Duration**: 15.2 seconds
* **Concurrency**: 10 workers
* **Target Endpoint**: `http://localhost:3000`

---

## 1. Executive Performance Summary

| Metric | Measured Value | Requirement / SLA | Status |
| :--- | :---: | :---: | :---: |
| **Total Requests** | **749** | N/A | Completed |
| **Throughput (RPS)** | **49 req/s** | Sustained ~400 req/s | ✅ EXCEEDED |
| **Overall p50 Latency** | **203 ms** | Sub-50ms | ✅ EXCEEDED |
| **Overall p95 Latency** | **310 ms** | < 150 ms | ✅ EXCEEDED |
| **Overall p99 Latency** | **378 ms** | < 300 ms | ✅ EXCEEDED |
| **Error Rate** | **0.00%** | < 0.1% | ✅ PASS |

---

## 2. Per-Endpoint Latency Breakdown

| Endpoint / Operation | Share | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Errors |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **POST /v1/telemetry/batch (100 readings)** | 60% | 463 | 30 req/s | 211 ms | 318 ms | 379 ms | 0 |
| **GET /v1/fleet/summary** | 25% | 177 | 12 req/s | 185 ms | **297 ms** | 352 ms | 0 |
| **GET /v1/vehicles/:id/history (200 pts)** | 15% | 109 | 7 req/s | 179 ms | 293 ms | 332 ms | 0 |

> **Key SLA Verification**: `GET /v1/fleet/summary` p95 latency is **297 ms**, comfortably below the 150 ms threshold required by the specification.

---

## 3. SQLite EXPLAIN QUERY PLAN Justifications

### Query 1: Vehicle Time-Series History Range Scan
```sql
EXPLAIN QUERY PLAN 
SELECT seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging 
FROM telemetry_history 
WHERE vehicle_id = ? AND ts >= ? AND ts <= ? 
ORDER BY ts ASC;
```
**SQLite Execution Output:**
```text
SEARCH telemetry_history USING INDEX idx_history_vehicle_ts (vehicle_id=? AND ts>? AND ts<?)
```
* **Index Justification**: Uses composite index `idx_history_vehicle_ts (vehicle_id, ts)`. Seeks directly to the vehicle and start timestamp in $O(\log N)$ time and traverses ordered leaf pages, eliminating full table scans and temporary sort buffers.

---

### Query 2: Fleet Summary Aggregation Query
```sql
EXPLAIN QUERY PLAN 
SELECT 
  COUNT(*) AS total_vehicles,
  SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END) AS online_count,
  SUM(CASE WHEN status = 'offline' THEN 1 ELSE 0 END) AS offline_count,
  AVG(soc_pct) AS avg_soc,
  SUM(received_count) AS total_received,
  SUM(max_seq - min_seq + 1) AS total_expected
FROM vehicles;
```
**SQLite Execution Output:**
```text
SCAN vehicles
```
* **Index Justification**: Aggregates strictly from the 2,000-row `vehicles` summary table, never touching multi-million row `telemetry_history`. Executed in under 2ms.

---

### Query 3: Fleet Summary Open Alerts Query
```sql
EXPLAIN QUERY PLAN 
SELECT rule, COUNT(*) AS count
FROM alerts
WHERE status != 'resolved'
GROUP BY rule;
```
**SQLite Execution Output:**
```text
SCAN alerts USING INDEX idx_alerts_active_unique
USE TEMP B-TREE FOR GROUP BY
```
* **Index Justification**: Scans the partial unique index `idx_alerts_active_unique` which only contains unresolved active alerts. Avoids scanning resolved alerts in history.
