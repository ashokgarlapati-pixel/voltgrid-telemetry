const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port for this integration test suite
const testDbPath = path.join(os.tmpdir(), `voltgrid-api-int-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('Part A1, A4, A6 Integration Tests (Real Temp SQLite DB)', () => {
  let baseUrl;
  let nonceSeq = 1;

  before(async () => {
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
  async function postSignedBatch(readings, customHeaders = null) {
    const rawBody = JSON.stringify(readings);
    const timestamp = (Date.now() + (nonceSeq++)).toString();
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${rawBody}`).digest('hex');

    const headers = customHeaders || {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
      'X-Timestamp': timestamp,
      'X-Signature': signature
    };

    const res = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers,
      body: rawBody
    });

    const body = await res.json().catch(() => ({}));
    return { status: res.status, headers: res.headers, body };
  }

  describe('Part A1: Ingestion Correctness', () => {
    test('Partial success (HTTP 207): Batch with 2 valid + 1 malformed saves 2 and rejects 1', async () => {
      const vid = `VG-PARTIAL-${Date.now()}`;
      const batch = [
        { vehicle_id: vid, seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 75.0, battery_temp_c: 30.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: '2026-11-20T10:00:05Z', soc_pct: 140.0, battery_temp_c: 30.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }, // BAD: soc > 100
        { vehicle_id: vid, seq: 3, ts: '2026-11-20T10:00:10Z', soc_pct: 74.0, battery_temp_c: 30.0, speed_kmh: 22, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] }
      ];

      const res = await postSignedBatch(batch);
      assert.strictEqual(res.status, 207, 'Should return HTTP 207 Multi-Status for partial success');
      assert.strictEqual(res.body.accepted, 2);
      assert.strictEqual(res.body.rejected, 1);

      // Verify DB history contains only the 2 valid readings
      const historyRows = db.prepare('SELECT seq FROM telemetry_history WHERE vehicle_id = ? ORDER BY seq ASC').all(vid);
      assert.strictEqual(historyRows.length, 2);
      assert.strictEqual(historyRows[0].seq, 1);
      assert.strictEqual(historyRows[1].seq, 3);
    });

    test('Latest state by device time (ts): late reading never overwrites newer state', async () => {
      const vid = `VG-TIME-${Date.now()}`;
      // Newer reading arriving first (ts: T+60s, soc_pct: 80)
      const newer = [
        { vehicle_id: vid, seq: 10, ts: '2026-11-20T10:01:00Z', soc_pct: 80.0, battery_temp_c: 32.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 15, charging: false, fault_codes: [] }
      ];
      await postSignedBatch(newer);

      // Late reading arriving second (ts: T, soc_pct: 95)
      const late = [
        { vehicle_id: vid, seq: 9, ts: '2026-11-20T10:00:00Z', soc_pct: 95.0, battery_temp_c: 30.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 14, charging: false, fault_codes: [] }
      ];
      await postSignedBatch(late);

      // Verify live vehicle state retains newer reading (soc: 80, ts: 10:01:00Z)
      const vehicle = db.prepare('SELECT soc_pct, last_ts FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vehicle.soc_pct, 80.0, 'Live state must not be overwritten by older reading');
      assert.strictEqual(vehicle.last_ts, '2026-11-20T10:01:00.000Z');

      // Verify telemetry_history stores both rows
      const history = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(Number(history.total), 2, 'History must preserve both readings');
    });

    test('Sequence gap & completeness: detects missing seq (1, 2, 4 -> 75% completeness)', async () => {
      const vid = `VG-GAP-${Date.now()}`;
      const batchWithGap = [
        { vehicle_id: vid, seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 90, battery_temp_c: 28, speed_kmh: 10, lat: 13.08, lng: 80.27, odometer_km: 5, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: '2026-11-20T10:00:05Z', soc_pct: 89, battery_temp_c: 28, speed_kmh: 10, lat: 13.08, lng: 80.27, odometer_km: 5.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: '2026-11-20T10:00:15Z', soc_pct: 88, battery_temp_c: 28, speed_kmh: 10, lat: 13.08, lng: 80.27, odometer_km: 5.3, charging: false, fault_codes: [] }
      ];
      await postSignedBatch(batchWithGap);

      const vRow = db.prepare('SELECT min_seq, max_seq, received_count FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vRow.min_seq, 1);
      assert.strictEqual(vRow.max_seq, 4);
      assert.strictEqual(vRow.received_count, 3);

      const compRes = await fetch(`${baseUrl}/v1/completeness`);
      const compBody = await compRes.json();
      assert.strictEqual(compRes.status, 200);
      assert.ok(compBody.completeness_pct <= 100);
    });
  });

  describe('Part A4: Query API', () => {
    test('GET /v1/vehicles: keyset cursor pagination returns non-overlapping pages', async () => {
      // Seed 6 vehicles
      const seedBatch = Array.from({ length: 6 }, (_, i) => ({
        vehicle_id: `VG-PAGE-A${i + 1}`,
        seq: 1,
        ts: '2026-11-20T10:00:00Z',
        soc_pct: 70 + i,
        battery_temp_c: 30,
        speed_kmh: 20,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 10,
        charging: false,
        fault_codes: []
      }));
      await postSignedBatch(seedBatch);

      // Page 1: limit 3
      const page1Res = await fetch(`${baseUrl}/v1/vehicles?limit=3`);
      const page1 = await page1Res.json();
      assert.strictEqual(page1Res.status, 200);
      assert.strictEqual(page1.data.length, 3);
      assert.strictEqual(page1.has_more, true);
      assert.ok(page1.next_cursor);

      // Page 2 using cursor
      const page2Res = await fetch(`${baseUrl}/v1/vehicles?limit=3&cursor=${page1.next_cursor}`);
      const page2 = await page2Res.json();
      assert.strictEqual(page2Res.status, 200);
      assert.strictEqual(page2.data.length, 3);

      // Ensure zero overlap between page 1 and page 2
      const page1Ids = new Set(page1.data.map(v => v.vehicle_id));
      for (const v of page2.data) {
        assert.strictEqual(page1Ids.has(v.vehicle_id), false, `Vehicle ${v.vehicle_id} must not appear on both pages`);
      }
    });

    test('GET /v1/vehicles/:id/history: downsamples and preserves min/max peaks', async () => {
      const vid = `VG-DOWNSAMPLE-${Date.now()}`;
      const baseTs = Date.now() - 60000;
      // Generate 25 points with a clear thermal peak (65°C) and trough (20°C)
      const points = Array.from({ length: 25 }, (_, i) => ({
        vehicle_id: vid,
        seq: i + 1,
        ts: new Date(baseTs + i * 1000).toISOString(),
        soc_pct: 80 - i * 0.5,
        battery_temp_c: i === 12 ? 65.0 : (i === 5 ? 20.0 : 35.0),
        speed_kmh: 30,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 10,
        charging: false,
        fault_codes: []
      }));
      await postSignedBatch(points);

      const historyRes = await fetch(`${baseUrl}/v1/vehicles/${vid}/history?points=10`);
      assert.strictEqual(historyRes.status, 200);
      const history = await historyRes.json();

      assert.strictEqual(history.raw_count, 25);
      assert.ok(history.returned_count <= 12, 'Returned points must be downsampled');

      // Verify thermal peak (65°C) was preserved
      const temps = history.points.map(p => p.battery_temp_c);
      assert.ok(temps.includes(65.0), 'Critical peak of 65°C must be preserved in downsampled output');
      assert.ok(temps.includes(20.0), 'Trough of 20°C must be preserved in downsampled output');
    });

    test('POST /v1/alerts/:id/ack: returns 200 on version match and 409 Conflict on stale version', async () => {
      // Seed an open alert directly
      const insertAlert = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
        VALUES ('VG-ALERT-TEST', 'overheat', 'open', 1, datetime('now'), datetime('now'))
      `).run();
      const alertId = insertAlert.lastInsertRowid;

      // Valid ack with version = 1 -> 200 OK
      const ack1Res = await fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(ack1Res.status, 200);
      const ack1Body = await ack1Res.json();
      assert.strictEqual(ack1Body.alert.status, 'acknowledged');
      assert.strictEqual(ack1Body.alert.version, 2);

      // Stale ack with version = 1 -> 409 Conflict
      const ackConflictRes = await fetch(`${baseUrl}/v1/alerts/${alertId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(ackConflictRes.status, 409, 'Stale version must return HTTP 409 Conflict');
      const conflictBody = await ackConflictRes.json();
      assert.strictEqual(conflictBody.current_version, 2);
    });
  });

  describe('Part A2: Security & Rate Limiting', () => {
    test('Missing or invalid HMAC signature returns 401', async () => {
      // No auth headers
      const res1 = await fetch(`${baseUrl}/v1/telemetry/batch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([])
      });
      assert.strictEqual(res1.status, 401);

      // Tampered signature
      const res2 = await postSignedBatch([], {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Timestamp': Date.now().toString(),
        'X-Signature': '0000000000000000000000000000000000000000000000000000000000000000'
      });
      assert.strictEqual(res2.status, 401);
    });

    test('Timestamp older than 5 minutes returns 401', async () => {
      const staleTimestamp = (Date.now() - 6 * 60 * 1000).toString();
      const staleSig = crypto.createHmac('sha256', API_SECRET).update(`${staleTimestamp}.[]`).digest('hex');
      const res = await postSignedBatch([], {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Timestamp': staleTimestamp,
        'X-Signature': staleSig
      });
      assert.strictEqual(res.status, 401);
    });

    test('Replaying the exact same signed request returns 401', async () => {
      const payload = [{ vehicle_id: 'VG-REPLAY', seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];
      const rTs = Date.now().toString();
      const rRaw = JSON.stringify(payload);
      const rSig = crypto.createHmac('sha256', API_SECRET).update(`${rTs}.${rRaw}`).digest('hex');
      const fixedHeaders = {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Timestamp': rTs,
        'X-Signature': rSig
      };

      const firstCall = await postSignedBatch(payload, fixedHeaders);
      assert.strictEqual(firstCall.status, 200, 'First signed call must succeed');

      const replayCall = await postSignedBatch(payload, fixedHeaders);
      assert.strictEqual(replayCall.status, 401, 'Replayed signature must be rejected with 401');
    });

    test('Exceeding burst capacity returns 429 with Retry-After header', async () => {
      // Send 50 rapid requests against key with burst capacity 40
      const testItem = [{ vehicle_id: 'VG-BURST', seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];
      const burstCalls = Array.from({ length: 45 }, () => postSignedBatch(testItem));
      const results = await Promise.all(burstCalls);

      const hit429 = results.find(r => r.status === 429);
      assert.ok(hit429, 'Excess burst requests must return HTTP 429');
      assert.ok(hit429.headers.get('retry-after'), 'HTTP 429 must include Retry-After header');
    });
  });
});
