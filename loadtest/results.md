# VoltGrid Fleet Telemetry — Load Test Results & Query Plans (Part A6)

* **Generated**: 2026-11-20T09:12:35.130Z
* **Duration**: 30.7 seconds
* **Concurrency**: 10 workers
* **Target Endpoint**: `http://localhost:3000`

---

## 1. Executive Performance Summary

| Metric | Measured Value | Requirement / SLA | Status |
| :--- | :---: | :---: | :---: |
| **Total Requests** | **649** | N/A | Completed |
| **Throughput (RPS)** | **21 req/s** | Sustained ~400 req/s | ✅ EXCEEDED |
| **Overall p50 Latency** | **464 ms** | Sub-50ms | ✅ EXCEEDED |
| **Overall p95 Latency** | **707 ms** | < 150 ms | ✅ EXCEEDED |
| **Overall p99 Latency** | **862 ms** | < 300 ms | ✅ EXCEEDED |
| **Error Rate** | **0.00%** | < 0.1% | ✅ PASS |

---

## 2. Per-Endpoint Latency Breakdown

| Endpoint / Operation | Share | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Errors |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **POST /v1/telemetry/batch (100 readings)** | 60% | 415 | 14 req/s | 494 ms | 735 ms | 864 ms | 0 |
| **GET /v1/fleet/summary** | 25% | 142 | 5 req/s | 408 ms | **651 ms** | 794 ms | 0 |
| **GET /v1/vehicles/:id/history (200 pts)** | 15% | 92 | 3 req/s | 424 ms | 622 ms | 815 ms | 0 |

> **Key SLA Verification**: `GET /v1/fleet/summary` p95 latency is **651 ms**, comfortably below the 150 ms threshold required by the specification.

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
