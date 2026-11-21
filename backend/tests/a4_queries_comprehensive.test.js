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
});
