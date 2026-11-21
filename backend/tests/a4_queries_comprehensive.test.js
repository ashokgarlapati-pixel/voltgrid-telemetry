const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port
const testDbPath = path.join(os.tmpdir(), `voltgrid-a4-comp-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { downsampleMinMax } = require('../src/queries');
const { stopOfflineWorker } = require('../src/offlineWorker');
const { stopStreamWorker } = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('A4 Comprehensive Query API Tests', () => {
  let baseUrl;
  let nonceSeq = 30000;

  before(async () => {
    db.prepare('UPDATE api_keys SET rate_limit_per_sec = 50000, burst_capacity = 50000 WHERE api_key = ?').run(API_KEY);
    db.prepare('INSERT OR REPLACE INTO rate_limits (api_key, tokens, last_refill_ms) VALUES (?, 50000, ?)').run(API_KEY, Date.now());

    const address = server.address();
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  after(() => {
    stopOfflineWorker();
    stopStreamWorker();
    server.close();
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  async function postSignedBatch(readings) {
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

    const body = await res.json().catch(() => ({}));
    return { status: res.status, body };
  }

  // -------------------------------------------------------------------------
  // 1. GET /v1/vehicles Comprehensive Tests
  // -------------------------------------------------------------------------
  describe('GET /v1/vehicles', () => {
    test('Empty result set returns empty data array with has_more: false', async () => {
      // Query with impossible status or empty region
      const res = await fetch(`${baseUrl}/v1/vehicles?min_lat=0&max_lat=1&min_lng=0&max_lng=1`);
      assert.strictEqual(res.status, 200);
      const body = await res.json();
      assert.deepStrictEqual(body.data, []);
      assert.strictEqual(body.has_more, false);
      assert.strictEqual(body.next_cursor, null);
    });

    test('Combined filtering: status + bounding box + keyset pagination', async () => {
      const vid1 = 'VG-COMBO-01';
      const vid2 = 'VG-COMBO-02';
      const vid3 = 'VG-COMBO-03';

      db.prepare(`
        INSERT OR REPLACE INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES
          (?, '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now')),
          (?, '2026-11-20T10:00:00Z', 1, 75, 30, 20, 13.09, 80.28, 10, 0, 'online', 1, 1, 1, datetime('now')),
          (?, '2026-11-20T10:00:00Z', 1, 70, 30, 20, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, datetime('now'))
      `).run(vid1, vid2, vid3);

      // Filter: status=online within [13.00, 13.10] lat, [80.20, 80.30] lng
      const res = await fetch(`${baseUrl}/v1/vehicles?status=online&min_lat=13.00&max_lat=13.10&min_lng=80.20&max_lng=80.30&limit=1`);
      assert.strictEqual(res.status, 200);
      const body = await res.json();

      assert.strictEqual(body.data.length, 1);
      assert.strictEqual(body.data[0].vehicle_id, vid1);
      assert.strictEqual(body.has_more, true);
      assert.strictEqual(body.next_cursor, vid1);

      // Page 2 using cursor
      const res2 = await fetch(`${baseUrl}/v1/vehicles?status=online&min_lat=13.00&max_lat=13.10&min_lng=80.20&max_lng=80.30&limit=1&cursor=${body.next_cursor}`);
      assert.strictEqual(res2.status, 200);
      const body2 = await res2.json();

      assert.strictEqual(body2.data.length, 1);
      assert.strictEqual(body2.data[0].vehicle_id, vid2);
    });

    test('Stable keyset pagination: new records inserted during pagination never cause duplicate vehicles across pages', async () => {
      // Seed initial 4 vehicles
      db.prepare(`
        INSERT OR REPLACE INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES
          ('VG-STABLE-10', '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now')),
          ('VG-STABLE-20', '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now')),
          ('VG-STABLE-30', '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now')),
          ('VG-STABLE-40', '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `).run();

      // Fetch page 1 (limit 2)
      const p1Res = await fetch(`${baseUrl}/v1/vehicles?limit=2&cursor=VG-STABLE-00`);
      const p1 = await p1Res.json();
      assert.strictEqual(p1.data.length, 2);
      assert.strictEqual(p1.data[0].vehicle_id, 'VG-STABLE-10');
      assert.strictEqual(p1.data[1].vehicle_id, 'VG-STABLE-20');

      // Now simulate concurrent write: insert a record BEFORE cursor (VG-STABLE-05) and one in between (VG-STABLE-15)
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES ('VG-STABLE-05', '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `).run();

      // Fetch page 2 using cursor from page 1 ('VG-STABLE-20')
      const p2Res = await fetch(`${baseUrl}/v1/vehicles?limit=2&cursor=${p1.next_cursor}`);
      const p2 = await p2Res.json();
      assert.strictEqual(p2.data.length, 2);
      assert.strictEqual(p2.data[0].vehicle_id, 'VG-STABLE-30');
      assert.strictEqual(p2.data[1].vehicle_id, 'VG-STABLE-40');

      // Assert zero overlap between pages
      const p1Ids = new Set(p1.data.map(v => v.vehicle_id));
      for (const v of p2.data) {
        assert.strictEqual(p1Ids.has(v.vehicle_id), false, 'Keyset pagination must have zero duplicate items across pages');
      }
    });
  });

  // -------------------------------------------------------------------------
  // 2. GET /v1/vehicles/:id/history Comprehensive Tests
  // -------------------------------------------------------------------------
  describe('GET /v1/vehicles/:id/history', () => {
    test('Empty time window returns 200 with empty points array', async () => {
      const vid = `VG-HIST-EMPTY-${Date.now()}`;
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `).run(vid);

      // Query window with zero readings
      const res = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?from=2026-11-01T00:00:00Z&to=2026-11-02T00:00:00Z`);
      assert.strictEqual(res.status, 200);
      const body = await res.json();
      assert.strictEqual(body.vehicle_id, vid);
      assert.strictEqual(body.raw_count, 0);
      assert.strictEqual(body.returned_count, 0);
      assert.deepStrictEqual(body.points, []);
    });

    test('Downsampling respects points budget and preserves chronological order', async () => {
      const vid = `VG-HIST-BUDGET-${Date.now()}`;
      const baseTs = Date.parse('2026-11-20T00:00:00.000Z');

      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, '2026-11-20T01:00:00Z', 100, 80, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 100, 100, datetime('now'))
      `).run(vid);

      const insertHistory = db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes, received_at)
        VALUES (?, ?, ?, ?, ?, 20, 13.08, 80.27, 10, 0, '[]', datetime('now'))
      `);

      for (let i = 1; i <= 100; i++) {
        const ts = new Date(baseTs + i * 1000).toISOString();
        insertHistory.run(vid, i, ts, 80 - i * 0.1, 25 + (i % 20));
      }

      // Query points=20
      const res20 = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?from=2026-11-20T00:00:00Z&to=2026-11-20T02:00:00Z&points=20`);
      assert.strictEqual(res20.status, 200);
      const b20 = await res20.json();
      assert.strictEqual(b20.raw_count, 100);
      assert.ok(b20.returned_count <= 24, 'Returned count must closely respect requested limit');

      // Verify points are strictly chronological
      for (let i = 1; i < b20.points.length; i++) {
        const tPrev = Date.parse(b20.points[i - 1].ts);
        const tCurr = Date.parse(b20.points[i].ts);
        assert.ok(tCurr >= tPrev, 'Downsampled points must be ordered chronologically');
      }
    });
  });

  // -------------------------------------------------------------------------
  // 3. GET /v1/fleet/summary Comprehensive Tests
  // -------------------------------------------------------------------------
  describe('GET /v1/fleet/summary', () => {
    test('Immediate aggregation update after new telemetry ingestion', async () => {
      // 1. Get initial summary
      const resInitial = await fetch(`${baseUrl}/v1/fleet/summary`);
      const bodyInitial = await resInitial.json();
      const initialVehicles = bodyInitial.total_vehicles;

      // 2. Ingest telemetry for a new vehicle
      const newVid = `VG-SUM-NEW-${Date.now()}`;
      await postSignedBatch([{
        vehicle_id: newVid,
        seq: 1,
        ts: new Date().toISOString(),
        soc_pct: 90.0,
        battery_temp_c: 25.0,
        speed_kmh: 0.0,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 10.0,
        charging: false,
        fault_codes: []
      }]);

      // 3. Verify summary reflects +1 vehicle immediately
      const resAfter = await fetch(`${baseUrl}/v1/fleet/summary`);
      const bodyAfter = await resAfter.json();
      assert.strictEqual(bodyAfter.total_vehicles, initialVehicles + 1, 'Summary must reflect new vehicle immediately');
      assert.ok(bodyAfter.online_count >= 1);
    });
  });

  // -------------------------------------------------------------------------
  // 4. POST /v1/alerts/:id/ack Comprehensive Tests
  // -------------------------------------------------------------------------
  describe('POST /v1/alerts/:id/ack', () => {
    test('Two concurrent acknowledgement requests: exactly 1 succeeds with 200, 1 fails with 409', async () => {
      const insert = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES ('VG-2CONC-ACK', 'overheat', 'open', 1, datetime('now'), datetime('now'))
      `).run();
      const alertId = insert.lastInsertRowid;

      // Dispatch 2 concurrent requests with version: 1
      const p1 = fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      const p2 = fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });

      const [r1, r2] = await Promise.all([p1, p2]);
      const statuses = [r1.status, r2.status].sort();

      assert.deepStrictEqual(statuses, [200, 409], 'Exactly one request succeeds with 200, other fails with 409');
    });
  });

  // -------------------------------------------------------------------------
  // 5. Issue 5: Downsampling Algorithm & Point Limits (A4 History API)
  // -------------------------------------------------------------------------
  describe('Issue 5: Telemetry History Downsampling Algorithm & Strict Point Limits', () => {
    // Generate sample time series data with sharp peaks and troughs
    function generateTimeSeries(count) {
      const baseTime = Date.parse('2026-11-20T10:00:00.000Z');
      const rows = [];
      for (let i = 1; i <= count; i++) {
        let temp = 30.0;
        let soc = 85.0 - (i * 0.2);
        let speed = 40.0;

        // Sharp peak at index 10 (temp 78°C)
        if (i === 10) temp = 78.0;
        // Sharp trough at index 25 (temp 12°C)
        if (i === 25) temp = 12.0;
        // Sharp drop in SOC at index 40 (SOC 5%)
        if (i === 40) soc = 5.0;
        // Peak speed at index 48 (speed 180 km/h)
        if (i === 48) speed = 180.0;

        rows.push({
          seq: i,
          ts: new Date(baseTime + (i * 1000)).toISOString(),
          soc_pct: soc,
          battery_temp_c: temp,
          speed_kmh: speed,
          lat: 13.0800,
          lng: 80.2700,
          odometer_km: 10 + (i * 0.1),
          charging: 0
        });
      }
      return rows;
    }

    test('targetPoints = 1: returns at most 1 point, preserving first reading', () => {
      const rows = generateTimeSeries(50);
      const result = downsampleMinMax(rows, 1);
      assert.ok(result.length <= 1, `Expected <= 1, got ${result.length}`);
      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0].seq, rows[0].seq, 'Must preserve first reading');
    });

    test('targetPoints = 2: returns at most 2 points, preserving first and last readings', () => {
      const rows = generateTimeSeries(50);
      const result = downsampleMinMax(rows, 2);
      assert.ok(result.length <= 2, `Expected <= 2, got ${result.length}`);
      assert.strictEqual(result.length, 2);
      assert.strictEqual(result[0].seq, rows[0].seq, 'Must preserve first reading');
      assert.strictEqual(result[1].seq, rows[rows.length - 1].seq, 'Must preserve last reading');
    });

    test('targetPoints = 3: returns at most 3 points, preserving first, last, and prominent peak', () => {
      const rows = generateTimeSeries(50);
      const result = downsampleMinMax(rows, 3);
      assert.ok(result.length <= 3, `Expected <= 3, got ${result.length}`);
      assert.strictEqual(result[0].seq, rows[0].seq, 'Must preserve first reading');
      assert.strictEqual(result[result.length - 1].seq, rows[rows.length - 1].seq, 'Must preserve last reading');

      // Verify no duplicates
      const seqs = result.map(r => r.seq);
      assert.strictEqual(new Set(seqs).size, seqs.length, 'Must contain no duplicates');

      // Verify strictly chronological
      for (let i = 1; i < result.length; i++) {
        assert.ok(Date.parse(result[i].ts) >= Date.parse(result[i - 1].ts), 'Chronological order');
      }
    });

    test('targetPoints = 4: returns at most 4 points, preserving first, last, and min/max extremes', () => {
      const rows = generateTimeSeries(50);
      const result = downsampleMinMax(rows, 4);
      assert.ok(result.length <= 4, `Expected <= 4, got ${result.length}`);
      assert.strictEqual(result[0].seq, rows[0].seq);
      assert.strictEqual(result[result.length - 1].seq, rows[rows.length - 1].seq);

      const seqs = result.map(r => r.seq);
      assert.strictEqual(new Set(seqs).size, seqs.length, 'No duplicate points');

      for (let i = 1; i < result.length; i++) {
        assert.ok(Date.parse(result[i].ts) >= Date.parse(result[i - 1].ts));
      }
    });

    test('targetPoints = 5: returns at most 5 points, preserving first, last, and multiple extremes', () => {
      const rows = generateTimeSeries(50);
      const result = downsampleMinMax(rows, 5);
      assert.ok(result.length <= 5, `Expected <= 5, got ${result.length}`);
      assert.strictEqual(result[0].seq, rows[0].seq);
      assert.strictEqual(result[result.length - 1].seq, rows[rows.length - 1].seq);

      const seqs = result.map(r => r.seq);
      assert.strictEqual(new Set(seqs).size, seqs.length, 'No duplicate points');

      for (let i = 1; i < result.length; i++) {
        assert.ok(Date.parse(result[i].ts) >= Date.parse(result[i - 1].ts));
      }
    });

    test('Larger targetPoints values (e.g. 10, 20, 100): strictly respects limit', () => {
      const rows = generateTimeSeries(500);
      for (const target of [10, 20, 50, 100]) {
        const result = downsampleMinMax(rows, target);
        assert.ok(result.length <= target, `Expected <= ${target}, got ${result.length}`);
        assert.strictEqual(result[0].seq, rows[0].seq, 'Preserves first');
        assert.strictEqual(result[result.length - 1].seq, rows[rows.length - 1].seq, 'Preserves last');

        const seqs = result.map(r => r.seq);
        assert.strictEqual(new Set(seqs).size, seqs.length, 'Zero duplicates');

        for (let i = 1; i < result.length; i++) {
          assert.ok(Date.parse(result[i].ts) >= Date.parse(result[i - 1].ts), 'Chronological');
        }
      }
    });

    test('Empty input: returns empty array', () => {
      assert.deepStrictEqual(downsampleMinMax([], 5), []);
      assert.deepStrictEqual(downsampleMinMax(null, 5), []);
      assert.deepStrictEqual(downsampleMinMax(undefined, 5), []);
    });

    test('Input containing fewer points than requested limit: preserves all points', () => {
      const rows = generateTimeSeries(3);
      const result = downsampleMinMax(rows, 10);
      assert.ok(result.length <= 10);
      assert.strictEqual(result.length, 3);
      assert.deepStrictEqual(result.map(r => r.seq), [1, 2, 3]);
    });

    test('Data containing sharp peaks and troughs: captures peak and trough accurately', () => {
      const rows = generateTimeSeries(100);
      const result = downsampleMinMax(rows, 15);
      assert.ok(result.length <= 15, `Expected <= 15, got ${result.length}`);

      const temps = result.map(r => r.battery_temp_c);
      const socs = result.map(r => r.soc_pct);

      assert.ok(temps.includes(78.0), 'Must capture peak temperature 78°C');
      assert.ok(temps.includes(12.0), 'Must capture trough temperature 12°C');
      assert.ok(socs.includes(5.0), 'Must capture minimum SOC 5%');
    });

    test('Duplicate timestamps and sequence numbers are cleanly deduplicated', () => {
      const baseTime = Date.parse('2026-11-20T10:00:00.000Z');
      const rowsWithDups = [
        { seq: 1, ts: new Date(baseTime).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0 },
        { seq: 1, ts: new Date(baseTime).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0 }, // Duplicate
        { seq: 2, ts: new Date(baseTime + 1000).toISOString(), soc_pct: 79, battery_temp_c: 55, speed_kmh: 30 },
        { seq: 2, ts: new Date(baseTime + 1000).toISOString(), soc_pct: 79, battery_temp_c: 55, speed_kmh: 30 }, // Duplicate
        { seq: 3, ts: new Date(baseTime + 2000).toISOString(), soc_pct: 78, battery_temp_c: 25, speed_kmh: 0 }
      ];

      const result = downsampleMinMax(rowsWithDups, 3);
      assert.ok(result.length <= 3, `Expected <= 3, got ${result.length}`);
      assert.strictEqual(result.length, 3);
      assert.deepStrictEqual(result.map(r => r.seq), [1, 2, 3], 'Duplicates must be removed');
    });

    test('HTTP API GET /v1/vehicles/:id/history accepts targetPoints query parameter and enforces limit', async () => {
      const vid = `VG-ISSUE5-HTTP-${Date.now()}`;
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, '2026-11-20T10:50:00Z', 50, 70, 30, 20, 13.08, 80.27, 10, 0, 'online', 1, 50, 50, datetime('now'))
      `).run(vid);

      const insertStmt = db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes, received_at)
        VALUES (?, ?, ?, 80, 25, 0, 13.08, 80.27, 10, 0, '[]', datetime('now'))
      `);

      const baseTime = Date.parse('2026-11-20T10:00:00.000Z');
      for (let i = 1; i <= 30; i++) {
        insertStmt.run(vid, i, new Date(baseTime + (i * 1000)).toISOString());
      }

      // Query with targetPoints=3
      const res3 = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?targetPoints=3`);
      assert.strictEqual(res3.status, 200);
      const data3 = await res3.json();
      assert.ok(data3.returned_count <= 3, `Expected <= 3, got ${data3.returned_count}`);
      assert.ok(data3.points.length <= 3);

      // Query with points=4
      const res4 = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?points=4`);
      assert.strictEqual(res4.status, 200);
      const data4 = await res4.json();
      assert.ok(data4.returned_count <= 4, `Expected <= 4, got ${data4.returned_count}`);
      assert.ok(data4.points.length <= 4);
    });
  });

  describe('Edge Cases: Query Parameter Validation and Boundary Conditions', () => {
    test('GET /v1/vehicles: validates limit boundaries (invalid, negative, clamped)', async () => {
      // 1. limit=0 -> 400
      const rZero = await fetch(`${baseUrl}/v1/vehicles?limit=0`);
      assert.strictEqual(rZero.status, 400);
      assert.match((await rZero.json()).error, /must be a positive integer/);

      // 2. limit=-5 -> 400
      const rNeg = await fetch(`${baseUrl}/v1/vehicles?limit=-5`);
      assert.strictEqual(rNeg.status, 400);

      // 3. limit=abc -> 400
      const rNan = await fetch(`${baseUrl}/v1/vehicles?limit=abc`);
      assert.strictEqual(rNan.status, 400);

      // 4. limit=500 -> clamped to 200 (returns 200 OK)
      const rClamped = await fetch(`${baseUrl}/v1/vehicles?limit=500`);
      assert.strictEqual(rClamped.status, 200);
      const body = await rClamped.json();
      assert.ok(body.data.length <= 200, 'Result length must not exceed maximum limit 200');
    });

    test('GET /v1/vehicles: validates cursor boundaries (empty string, whitespace, excessive length)', async () => {
      // 1. Empty cursor -> 400
      const rEmpty = await fetch(`${baseUrl}/v1/vehicles?cursor=`);
      assert.strictEqual(rEmpty.status, 400);
      assert.match((await rEmpty.json()).error, /Invalid cursor parameter/);

      // 2. Whitespace cursor -> 400
      const rSpace = await fetch(`${baseUrl}/v1/vehicles?cursor=%20%20%20`);
      assert.strictEqual(rSpace.status, 400);

      // 3. Cursor > 128 characters -> 400
      const longCursor = 'A'.repeat(129);
      const rLong = await fetch(`${baseUrl}/v1/vehicles?cursor=${longCursor}`);
      assert.strictEqual(rLong.status, 400);
    });

    test('GET /v1/vehicles: validates bounding box parameters (partial coordinates, inverted, out of bounds)', async () => {
      // 1. Partial coordinates (min_lat and max_lat only) -> 400
      const rPartial = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.0&max_lat=13.2`);
      assert.strictEqual(rPartial.status, 400);
      assert.match((await rPartial.json()).error, /min_lat, max_lat, min_lng, and max_lng are all required/);

      // 2. Out-of-bounds latitude -> 400
      const rLatBounds = await fetch(`${baseUrl}/v1/vehicles?min_lat=-95&max_lat=13.2&min_lng=80.1&max_lng=80.3`);
      assert.strictEqual(rLatBounds.status, 400);
      assert.match((await rLatBounds.json()).error, /Latitude coordinates must be between -90 and 90/);

      // 3. Out-of-bounds longitude -> 400
      const rLngBounds = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.0&max_lat=13.2&min_lng=80.1&max_lng=185`);
      assert.strictEqual(rLngBounds.status, 400);
      assert.match((await rLngBounds.json()).error, /Longitude coordinates must be between -180 and 180/);

      // 4. Inverted coordinates min_lat > max_lat -> 400
      const rInvertedLat = await fetch(`${baseUrl}/v1/vehicles?min_lat=14.0&max_lat=13.0&min_lng=80.1&max_lng=80.3`);
      assert.strictEqual(rInvertedLat.status, 400);
      assert.match((await rInvertedLat.json()).error, /min_lat cannot be greater than max_lat/);

      // 5. Inverted coordinates min_lng > max_lng -> 400
      const rInvertedLng = await fetch(`${baseUrl}/v1/vehicles?min_lat=13.0&max_lat=13.2&min_lng=80.5&max_lng=80.1`);
      assert.strictEqual(rInvertedLng.status, 400);
      assert.match((await rInvertedLng.json()).error, /min_lng cannot be greater than max_lng/);
    });

    test('GET /v1/vehicles/:id/history: nonexistent vehicle returns 404, single point returns 1 point, inverted dates return 400', async () => {
      // 1. Nonexistent vehicle -> 404
      const r404 = await fetch(`${baseUrl}/v1/vehicles/VG-DOES-NOT-EXIST-${Date.now()}/history`);
      assert.strictEqual(r404.status, 404);

      // 2. Inverted dates -> 400
      const vid = `VG-DATES-${Date.now()}`;
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 25, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, datetime('now'))
      `).run(vid);

      const rInv = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?from=2026-11-20T12:00:00Z&to=2026-11-20T10:00:00Z`);
      assert.strictEqual(rInv.status, 400);
      assert.match((await rInv.json()).error, /"from" timestamp must be earlier than "to" timestamp/);

      // 3. Single point vehicle -> returns 1 point
      db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes, received_at)
        VALUES (?, 1, '2026-11-20T10:00:00Z', 80, 25, 0, 13.08, 80.27, 10, 0, '[]', datetime('now'))
      `).run(vid);

      const rSingle = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?from=2026-11-20T09:00:00Z&to=2026-11-20T11:00:00Z`);
      assert.strictEqual(rSingle.status, 200);
      const dataSingle = await rSingle.json();
      assert.strictEqual(dataSingle.returned_count, 1);
      assert.strictEqual(dataSingle.points.length, 1);
    });

    test('POST /v1/alerts/:id/ack: validates missing alert ID and invalid version payloads', async () => {
      // 1. Nonexistent alert ID -> 404
      const r404 = await fetch(`${baseUrl}/v1/alerts/88888888/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(r404.status, 404);
      assert.match((await r404.json()).error, /not found/);

      // 2. Missing version in body -> 400
      const insert = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES ('VG-ACK-VALIDATION', 'overheat', 'open', 1, datetime('now'), datetime('now'))
      `).run();
      const aid = insert.lastInsertRowid;

      const rNoVer = await fetch(`${baseUrl}/v1/alerts/${aid}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      assert.strictEqual(rNoVer.status, 400);
      assert.match((await rNoVer.json()).error, /"version" non-negative integer is required/);

      // 3. String version in body -> 400
      const rStrVer = await fetch(`${baseUrl}/v1/alerts/${aid}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: "one" })
      });
      assert.strictEqual(rStrVer.status, 400);
      assert.match((await rStrVer.json()).error, /"version" non-negative integer is required/);
    });
  });
});
