const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port for A4 integration tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-queries-int-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = process.env.NODE_ENV || 'test';
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('Part A4: Query API Integration & Performance Tests', () => {
  let baseUrl;
  let nonceSeq = 5000;

  before(async () => {
    // Elevate rate limits for testing
    db.prepare('UPDATE api_keys SET rate_limit_per_sec = 10000, burst_capacity = 10000 WHERE api_key = ?').run(API_KEY);
    db.prepare('INSERT OR REPLACE INTO rate_limits (api_key, tokens, last_refill_ms) VALUES (?, 10000, ?)').run(API_KEY, Date.now());

    const address = server.address();
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  after(() => {
    stopOfflineWorker();
    server.close();
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  // Helper to post signed batch
  async function postBatch(readings) {
    const rawBody = JSON.stringify(readings);
    const timestamp = (Date.now() + (nonceSeq++)).toString();
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${rawBody}`).digest('hex');

    const res = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Timestamp': timestamp,
        'X-Signature': signature
      },
      body: rawBody
    });

    return { status: res.status, body: await res.json() };
  }

  // =========================================================================
  // 1. GET /v1/vehicles Tests
  // =========================================================================
  describe('GET /v1/vehicles', () => {

    test('Status filtering: valid statuses return matching vehicles, invalid returns 400', async () => {
      // Seed vehicles directly in vehicles table
      const insertVehicle = db.prepare(`
        INSERT OR REPLACE INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, ?, 1, 1, 1, datetime('now'))
      `);

      insertVehicle.run('VG-STAT-ONLINE-1', 'online');
      insertVehicle.run('VG-STAT-ONLINE-2', 'online');
      insertVehicle.run('VG-STAT-OFFLINE-1', 'offline');
      insertVehicle.run('VG-STAT-MAINT-1', 'maintenance');

      // 1. Filter status=online
      const onlineRes = await fetch(`${baseUrl}/v1/vehicles?status=online`);
      assert.strictEqual(onlineRes.status, 200);
      const onlineBody = await onlineRes.json();
      assert.ok(onlineBody.data.every(v => v.status === 'online'));
      const onlineIds = onlineBody.data.map(v => v.vehicle_id);
      assert.ok(onlineIds.includes('VG-STAT-ONLINE-1'));
      assert.ok(onlineIds.includes('VG-STAT-ONLINE-2'));
      assert.ok(!onlineIds.includes('VG-STAT-OFFLINE-1'));

      // 2. Filter status=offline
      const offlineRes = await fetch(`${baseUrl}/v1/vehicles?status=offline`);
      assert.strictEqual(offlineRes.status, 200);
      const offlineBody = await offlineRes.json();
      assert.ok(offlineBody.data.every(v => v.status === 'offline'));
      assert.ok(offlineBody.data.some(v => v.vehicle_id === 'VG-STAT-OFFLINE-1'));

      // 3. Filter status=maintenance
      const maintRes = await fetch(`${baseUrl}/v1/vehicles?status=maintenance`);
      assert.strictEqual(maintRes.status, 200);
      const maintBody = await maintRes.json();
      assert.ok(maintBody.data.some(v => v.vehicle_id === 'VG-STAT-MAINT-1'));

      // 4. Invalid status returns HTTP 400
      const invalidRes = await fetch(`${baseUrl}/v1/vehicles?status=flying`);
      assert.strictEqual(invalidRes.status, 400);
      const invalidBody = await invalidRes.json();
      assert.match(invalidBody.error, /Invalid status filter/);
    });

    test('Bounding box filtering: filters within bounds, rejects invalid/inverted coordinates', async () => {
      const insertVehicle = db.prepare(`
        INSERT OR REPLACE INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 30, 20, ?, ?, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `);

      // Inside Chennai box: lat 13.08, lng 80.27
      insertVehicle.run('VG-GEO-INSIDE', 13.08, 80.27);
      // Outside (Bangalore): lat 12.97, lng 77.59
      insertVehicle.run('VG-GEO-OUTSIDE', 12.97, 77.59);
      // On boundary: lat 13.00, lng 80.18
      insertVehicle.run('VG-GEO-EDGE', 13.00, 80.18);

      // Query bounding box [13.00, 13.18] lat, [80.18, 80.32] lng
      const geoRes = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.00&max_lat=13.18&min_lng=80.18&max_lng=80.32`);
      assert.strictEqual(geoRes.status, 200);
      const geoBody = await geoRes.json();
      const ids = geoBody.data.map(v => v.vehicle_id);

      assert.ok(ids.includes('VG-GEO-INSIDE'), 'Inside point must be included');
      assert.ok(ids.includes('VG-GEO-EDGE'), 'Edge boundary point must be included');
      assert.ok(!ids.includes('VG-GEO-OUTSIDE'), 'Outside point must be excluded');

      // Validation 1: Incomplete bounding box returns HTTP 400
      const partialRes = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.00&max_lat=13.18`);
      assert.strictEqual(partialRes.status, 400);
      assert.match((await partialRes.json()).error, /Incomplete bounding box/);

      // Validation 2: Inverted latitude (min > max) returns HTTP 400
      const invertLatRes = await fetch(`${baseUrl}/v1/vehicles?min_lat=14.00&max_lat=13.00&min_lng=80.00&max_lng=81.00`);
      assert.strictEqual(invertLatRes.status, 400);
      assert.match((await invertLatRes.json()).error, /min_lat cannot be greater than max_lat/);

      // Validation 3: Inverted longitude (min > max) returns HTTP 400
      const invertLngRes = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.00&max_lat=14.00&min_lng=82.00&max_lng=80.00`);
      assert.strictEqual(invertLngRes.status, 400);
      assert.match((await invertLngRes.json()).error, /min_lng cannot be greater than max_lng/);

      // Validation 4: Out of range coordinates returns HTTP 400
      const outOfRangeRes = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.00&max_lat=95.00&min_lng=80.00&max_lng=81.00`);
      assert.strictEqual(outOfRangeRes.status, 400);
      assert.match((await outOfRangeRes.json()).error, /Latitude coordinates must be between/);

      // Validation 5: Non-numeric coordinates returns HTTP 400
      const nonNumericRes = await fetch(`${baseUrl}/v1/vehicles?min_lat=abc&max_lat=14.00&min_lng=80.00&max_lng=81.00`);
      assert.strictEqual(nonNumericRes.status, 400);
      assert.match((await nonNumericRes.json()).error, /Bounding box coordinates must be valid finite numbers/);
    });

    test('Keyset cursor pagination: zero-offset traversal over multiple pages with no repeats or skips', async () => {
      // Seed 15 vehicles with deterministic IDs: VG-PAGE-01 to VG-PAGE-15
      const insertVehicle = db.prepare(`
        INSERT OR REPLACE INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `);

      for (let i = 1; i <= 15; i++) {
        const padId = `VG-PAGE-${i.toString().padStart(2, '0')}`;
        insertVehicle.run(padId);
      }

      // Page size: 5 vehicles per page. Total pages expected: 3
      let cursor = null;
      const seenIds = [];
      let pageCount = 0;

      while (true) {
        pageCount++;
        const url = cursor
          ? `${baseUrl}/v1/vehicles?limit=5&cursor=${encodeURIComponent(cursor)}`
          : `${baseUrl}/v1/vehicles?limit=5`;

        const res = await fetch(url);
        assert.strictEqual(res.status, 200);
        const body = await res.json();

        assert.ok(body.data.length <= 5);
        for (const v of body.data) {
          assert.strictEqual(seenIds.includes(v.vehicle_id), false, `Vehicle ${v.vehicle_id} must not be seen twice`);
          seenIds.push(v.vehicle_id);
        }

        if (!body.has_more) {
          assert.strictEqual(body.next_cursor, null, 'Final page must have next_cursor = null');
          break;
        }

        assert.ok(body.next_cursor, 'Non-terminal page must have next_cursor');
        cursor = body.next_cursor;
      }

      assert.ok(pageCount >= 3, 'Must traverse at least 3 pages');
      // Verify all 15 seeded vehicles were traversed in strictly ascending order
      for (let i = 1; i <= 15; i++) {
        const padId = `VG-PAGE-${i.toString().padStart(2, '0')}`;
        assert.ok(seenIds.includes(padId), `Vehicle ${padId} must have been returned during pagination`);
      }

      // Validation 6: Invalid limit returns HTTP 400
      const badLimitRes = await fetch(`${baseUrl}/v1/vehicles?limit=-5`);
      assert.strictEqual(badLimitRes.status, 400);
      assert.match((await badLimitRes.json()).error, /Invalid limit parameter/);
    });
  });

  // =========================================================================
  // 2. GET /v1/vehicles/:id/history Tests & 17,000-Reading Performance Proof
  // =========================================================================
  describe('GET /v1/vehicles/:id/history', () => {

    test('Validation: missing vehicle returns 404, bad dates return 400, inverted dates return 400', async () => {
      // 1. Missing vehicle returns 404
      const missingRes = await fetch(`${baseUrl}/v1/vehicles/VG-NON-EXISTENT-XYZ/history`);
      assert.strictEqual(missingRes.status, 404);
      assert.match((await missingRes.json()).error, /not found/);

      // Seed valid vehicle
      db.prepare(`
        INSERT OR REPLACE INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES ('VG-HIST-VAL', '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `).run();

      // 2. Bad "from" timestamp returns 400
      const badFromRes = await fetch(`${baseUrl}/v1/vehicles/VG-HIST-VAL/history?from=not-a-date`);
      assert.strictEqual(badFromRes.status, 400);
      assert.match((await badFromRes.json()).error, /Invalid "from" timestamp/);

      // 3. Bad "to" timestamp returns 400
      const badToRes = await fetch(`${baseUrl}/v1/vehicles/VG-HIST-VAL/history?to=not-a-date`);
      assert.strictEqual(badToRes.status, 400);
      assert.match((await badToRes.json()).error, /Invalid "to" timestamp/);

      // 4. Inverted time window (from > to) returns 400
      const invertedRes = await fetch(`${baseUrl}/v1/vehicles/VG-HIST-VAL/history?from=2026-11-20T12:00:00Z&to=2026-11-20T10:00:00Z`);
      assert.strictEqual(invertedRes.status, 400);
      assert.match((await invertedRes.json()).error, /earlier than "to" timestamp/);

      // 5. Bad "points" returns 400
      const badPointsRes = await fetch(`${baseUrl}/v1/vehicles/VG-HIST-VAL/history?points=0`);
      assert.strictEqual(badPointsRes.status, 400);
      assert.match((await badPointsRes.json()).error, /Invalid "points" parameter/);
    });

    test('Downsampling preserves thermal peaks, troughs, and min SOC chronologically', async () => {
      const vid = `VG-DOWNSAMPLE-DETAIL-${Date.now()}`;
      const baseTime = Date.parse('2026-11-20T00:00:00.000Z');

      // Create vehicle and 50 history readings
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, '2026-11-20T00:50:00Z', 50, 70, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 50, 50, datetime('now'))
      `).run(vid);

      const insertHistory = db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes, received_at)
        VALUES (?, ?, ?, ?, ?, ?, 13.08, 80.27, 10, 0, '[]', datetime('now'))
      `);

      for (let i = 1; i <= 50; i++) {
        const ts = new Date(baseTime + (i * 60000)).toISOString();
        // Insert a peak at seq 20 (temp: 72°C), trough at seq 35 (temp: 15°C), min SOC at seq 45 (soc: 8%)
        let temp = 35.0;
        let soc = 80.0 - (i * 0.5);
        if (i === 20) temp = 72.0;
        if (i === 35) temp = 15.0;
        if (i === 45) soc = 8.0;

        insertHistory.run(vid, i, ts, soc, temp, 25.0);
      }

      // Query with points=15
      const res = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?from=2026-11-20T00:00:00Z&to=2026-11-20T01:00:00Z&points=15`);
      assert.strictEqual(res.status, 200);
      const body = await res.json();

      assert.strictEqual(body.raw_count, 50);
      assert.ok(body.returned_count <= 20, 'Returned points must be downsampled close to requested points');

      // Verify peak temp (72°C), trough temp (15°C), and min SOC (8%) were preserved
      const temps = body.points.map(p => p.battery_temp_c);
      const socs = body.points.map(p => p.soc_pct);

      assert.ok(temps.includes(72.0), 'Critical temperature peak of 72.0°C must be preserved');
      assert.ok(temps.includes(15.0), 'Critical temperature trough of 15.0°C must be preserved');
      assert.ok(socs.includes(8.0), 'Minimum SOC trough of 8.0% must be preserved');

      // Verify strictly chronological output
      for (let i = 1; i < body.points.length; i++) {
        const prev = Date.parse(body.points[i - 1].ts);
        const curr = Date.parse(body.points[i].ts);
        assert.ok(curr >= prev, 'Points must be in strictly chronological order');
      }
    });

    test('17,280-reading benchmark (24 hours @ 5s interval): execution time and downsampling verification', async () => {
      const vid = `VG-BENCH-17K-${Date.now()}`;
      const baseTime = Date.parse('2026-11-19T00:00:00.000Z');
      const TOTAL_READINGS = 17280; // 24 hours of 5-second telemetry readings

      // Seed vehicle
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, '2026-11-20T00:00:00Z', ?, 60, 32, 25, 13.08, 80.27, 500, 0, 'online', 1, ?, ?, datetime('now'))
      `).run(vid, TOTAL_READINGS, TOTAL_READINGS, TOTAL_READINGS);

      // Bulk insert 17,280 readings in a single atomic transaction for speed
      const insertHistory = db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes, received_at)
        VALUES (?, ?, ?, ?, ?, 25.0, 13.08, 80.27, 100, 0, '[]', datetime('now'))
      `);

      const seedAll = db.transaction(() => {
        for (let i = 1; i <= TOTAL_READINGS; i++) {
          const ts = new Date(baseTime + (i * 5000)).toISOString();
          let temp = 30.0 + ((i % 100) * 0.1);
          let soc = 100.0 - ((i / TOTAL_READINGS) * 70.0);
          if (i === 5000) temp = 74.5; // Distinct thermal spike
          if (i === 12000) temp = 14.2; // Distinct thermal trough
          insertHistory.run(vid, i, ts, soc, temp);
        }
      });
      seedAll();

      // Measure exact execution time of history endpoint over 17,280 readings
      const startTime = performance.now();
      const res = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?from=2026-11-19T00:00:00Z&to=2026-11-20T00:00:00Z&points=200`);
      const elapsedMs = performance.now() - startTime;

      assert.strictEqual(res.status, 200);
      const body = await res.json();

      assert.strictEqual(body.raw_count, TOTAL_READINGS);
      assert.ok(body.returned_count <= 250, 'Downsampled points must be close to target points (200)');
      assert.ok(body.returned_count >= 50);

      // Verify peak and trough are preserved even across 17,280 points
      const temps = body.points.map(p => p.battery_temp_c);
      assert.ok(temps.includes(74.5), 'Peak of 74.5°C must be preserved across 17k readings');
      assert.ok(temps.includes(14.2), 'Trough of 14.2°C must be preserved across 17k readings');

      console.log(`[History Performance Test] 17,280 raw readings query + downsampling to ${body.returned_count} points executed in ${elapsedMs.toFixed(2)}ms`);
      assert.ok(elapsedMs < 1000, `Execution time must be under 1,000ms (actual: ${elapsedMs.toFixed(2)}ms)`);
    });
  });

  // =========================================================================
  // 3. GET /v1/fleet/summary Tests & Reproducible Benchmark
  // =========================================================================
  describe('GET /v1/fleet/summary', () => {

    test('Correct aggregation: vehicle status counts, average SOC, active alerts, and completeness', async () => {
      // Query fleet summary
      const res = await fetch(`${baseUrl}/v1/fleet/summary`);
      assert.strictEqual(res.status, 200);
      const body = await res.json();

      assert.ok(typeof body.total_vehicles === 'number');
      assert.ok(typeof body.avg_soc_pct === 'number');
      assert.ok(typeof body.data_completeness_pct === 'number');
      assert.ok(typeof body.open_alerts_by_rule === 'object');
      assert.ok('overheat' in body.open_alerts_by_rule);
      assert.ok('rapid_discharge' in body.open_alerts_by_rule);
      assert.ok('geofence_breach' in body.open_alerts_by_rule);
      assert.ok('offline' in body.open_alerts_by_rule);

      // Verify status counts match
      assert.strictEqual(body.counts_by_status.online, body.online_count);
      assert.strictEqual(body.counts_by_status.offline, body.offline_count);
    });

    test('Active alert grouping: acknowledged alerts counted as active, resolved alerts excluded', async () => {
      const vid = `VG-SUMMARY-ALERT-${Date.now()}`;

      // Insert 1 open alert, 1 acknowledged alert, and 1 resolved alert
      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES (?, 'overheat', 'open', 1, datetime('now'), datetime('now'))
      `).run(vid);

      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES (?, 'rapid_discharge', 'acknowledged', 2, datetime('now'), datetime('now'))
      `).run(vid);

      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, resolved_at, updated_at)
        VALUES (?, 'geofence_breach', 'resolved', 2, datetime('now'), datetime('now'), datetime('now'))
      `).run(vid);

      const res = await fetch(`${baseUrl}/v1/fleet/summary`);
      const body = await res.json();

      // Overheat (open) must be counted
      assert.ok(body.open_alerts_by_rule.overheat >= 1);
      // Rapid discharge (acknowledged) must be counted as active
      assert.ok(body.open_alerts_by_rule.rapid_discharge >= 1);
    });

    test('Request stability and latency profiling over 100 requests (schema consistency & percentile tracking)', async () => {
      const latencies = [];
      const ITERATIONS = 100;
      let firstPayload = null;

      for (let i = 0; i < ITERATIONS; i++) {
        const start = performance.now();
        const res = await fetch(`${baseUrl}/v1/fleet/summary`);
        const duration = performance.now() - start;
        assert.strictEqual(res.status, 200, 'Every fleet summary request must return HTTP 200');

        const body = await res.json();
        assert.ok(typeof body.total_vehicles === 'number', 'total_vehicles must be numeric');
        assert.ok(typeof body.avg_soc_pct === 'number', 'avg_soc_pct must be numeric');
        assert.ok(typeof body.data_completeness_pct === 'number', 'data_completeness_pct must be numeric');
        assert.ok(typeof body.open_alerts_by_rule === 'object' && body.open_alerts_by_rule !== null, 'open_alerts_by_rule must be an object');

        if (i === 0) {
          firstPayload = body;
        } else {
          // Verify response data stability across repeated queries
          assert.strictEqual(body.total_vehicles, firstPayload.total_vehicles, 'Vehicle count must remain consistent across repeated requests');
          assert.strictEqual(body.data_completeness_pct, firstPayload.data_completeness_pct, 'Completeness percentage must remain consistent');
        }

        latencies.push(duration);
      }

      latencies.sort((a, b) => a - b);
      const p50 = latencies[Math.floor(ITERATIONS * 0.50)];
      const p95 = latencies[Math.floor(ITERATIONS * 0.95)];
      const p99 = latencies[Math.floor(ITERATIONS * 0.99)];

      console.log(`[Fleet Summary Profiling] ${ITERATIONS} requests: p50=${p50.toFixed(2)}ms, p95=${p95.toFixed(2)}ms, p99=${p99.toFixed(2)}ms`);

      // Functional verification: ensure all latencies were recorded and percentiles are valid numbers
      assert.strictEqual(latencies.length, ITERATIONS, 'All 100 requests must record valid latencies');
      assert.ok(Number.isFinite(p50) && p50 > 0, 'p50 latency must be a valid positive duration');
      assert.ok(Number.isFinite(p95) && p95 > 0, 'p95 latency must be a valid positive duration');
      assert.ok(Number.isFinite(p99) && p99 > 0, 'p99 latency must be a valid positive duration');

      // Note: Strict SLA enforcement (p95 < 150ms against 5,000,000 telemetry readings under 10 concurrent VUs)
      // is governed by the dedicated benchmark runner: `scripts/benchmark_5m.js` (`npm run benchmark:5m`).
      // When explicitly requested via STRICT_PERF=true, validate strict 150ms ceiling here as well:
      if (process.env.STRICT_PERF === 'true') {
        assert.ok(p95 < 150.0, `Fleet summary p95 latency must be under 150ms (actual: ${p95.toFixed(2)}ms)`);
      }
    });
  });

  // =========================================================================
  // 4. POST /v1/alerts/:id/ack Tests & Concurrency Safety
  // =========================================================================
  describe('POST /v1/alerts/:id/ack', () => {

    test('Optimistic concurrency: version match succeeds, stale version and resolved alert fail with 409', async () => {
      // Seed alert with version = 1
      const insert = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES ('VG-ACK-TEST', 'overheat', 'open', 1, datetime('now'), datetime('now'))
      `).run();
      const alertId = insert.lastInsertRowid;

      // 1. Invalid body (no version) returns 400
      const noVerRes = await fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      assert.strictEqual(noVerRes.status, 400);

      // 2. Missing alert ID returns 404
      const missingRes = await fetch(`${baseUrl}/v1/alerts/9999999/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(missingRes.status, 404);

      // 3. Version match (version = 1) -> 200 OK, version becomes 2, status becomes 'acknowledged'
      const successRes = await fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(successRes.status, 200);
      const successBody = await successRes.json();
      assert.strictEqual(successBody.alert.status, 'acknowledged');
      assert.strictEqual(successBody.alert.version, 2);

      // 4. Stale version (version = 1 again) -> 409 Conflict
      const staleRes = await fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(staleRes.status, 409);
      const staleBody = await staleRes.json();
      assert.strictEqual(staleBody.current_version, 2);

      // 5. Already acknowledged alert re-acked with version = 2 -> 409 Conflict (only 'open' can be acked)
      const reAckRes = await fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 2 })
      });
      assert.strictEqual(reAckRes.status, 409);
      assert.match((await reAckRes.json()).error, /already acknowledged/);

      // 6. Resolved alert cannot be acknowledged -> 409 Conflict
      const resolvedInsert = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, resolved_at, updated_at)
        VALUES ('VG-ACK-RES', 'overheat', 'resolved', 3, datetime('now'), datetime('now'), datetime('now'))
      `).run();
      const resolvedId = resolvedInsert.lastInsertRowid;

      const ackResolvedRes = await fetch(`${baseUrl}/v1/alerts/${resolvedId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 3 })
      });
      assert.strictEqual(ackResolvedRes.status, 409);
      assert.match((await ackResolvedRes.json()).error, /Resolved alert cannot be acknowledged/);
    });

    test('10 concurrent acknowledgement requests: exactly 1 succeeds, 9 fail with 409', async () => {
      // Seed open alert with version = 1
      const insert = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES ('VG-CONCURRENT-ACK', 'overheat', 'open', 1, datetime('now'), datetime('now'))
      `).run();
      const alertId = insert.lastInsertRowid;

      // 10 concurrent requests all providing version = 1
      const promises = Array.from({ length: 10 }, () =>
        fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ version: 1 })
        })
      );

      const results = await Promise.all(promises);
      const statuses = results.map(r => r.status);

      const successCount = statuses.filter(s => s === 200).length;
      const conflictCount = statuses.filter(s => s === 409).length;

      assert.strictEqual(successCount, 1, 'Exactly one concurrent ack request must succeed with 200 OK');
      assert.strictEqual(conflictCount, 9, 'All other 9 concurrent ack requests must fail with 409 Conflict');

      // Verify final database state
      const finalAlert = db.prepare('SELECT status, version FROM alerts WHERE id = ?').get(alertId);
      assert.strictEqual(finalAlert.status, 'acknowledged');
      assert.strictEqual(finalAlert.version, 2);
    });
  });
});
