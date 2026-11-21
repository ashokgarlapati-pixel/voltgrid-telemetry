const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port for this integration test suite
const testDbPath = path.join(os.tmpdir(), `voltgrid-api-int-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = process.env.NODE_ENV || 'test';
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';
const testStartMs = Date.now() - 60000;
function makeTestTs(offsetSec = 0) { return new Date(testStartMs + (offsetSec * 1000)).toISOString(); }

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
        { vehicle_id: vid, seq: 1, ts: makeTestTs(0), soc_pct: 75.0, battery_temp_c: 30.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeTestTs(5), soc_pct: 140.0, battery_temp_c: 30.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }, // BAD: soc > 100
        { vehicle_id: vid, seq: 3, ts: makeTestTs(10), soc_pct: 74.0, battery_temp_c: 30.0, speed_kmh: 22, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] }
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
        { vehicle_id: vid, seq: 10, ts: makeTestTs(60), soc_pct: 80.0, battery_temp_c: 32.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 15, charging: false, fault_codes: [] }
      ];
      await postSignedBatch(newer);

      // Late reading arriving second (ts: T, soc_pct: 95)
      const late = [
        { vehicle_id: vid, seq: 9, ts: makeTestTs(0), soc_pct: 95.0, battery_temp_c: 30.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 14, charging: false, fault_codes: [] }
      ];
      await postSignedBatch(late);

      // Verify live vehicle state retains newer reading (soc: 80, ts: 10:01:00Z)
      const vehicle = db.prepare('SELECT soc_pct, last_ts FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vehicle.soc_pct, 80.0, 'Live state must not be overwritten by older reading');
      assert.strictEqual(vehicle.last_ts, makeTestTs(60));

      // Verify telemetry_history stores both rows
      const history = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(Number(history.total), 2, 'History must preserve both readings');
    });

    test('Sequence gap & completeness: detects missing seq (1, 2, 4 -> 75% completeness)', async () => {
      const vid = `VG-GAP-${Date.now()}`;
      const batchWithGap = [
        { vehicle_id: vid, seq: 1, ts: makeTestTs(0), soc_pct: 90, battery_temp_c: 28, speed_kmh: 10, lat: 13.08, lng: 80.27, odometer_km: 5, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeTestTs(5), soc_pct: 89, battery_temp_c: 28, speed_kmh: 10, lat: 13.08, lng: 80.27, odometer_km: 5.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: makeTestTs(15), soc_pct: 88, battery_temp_c: 28, speed_kmh: 10, lat: 13.08, lng: 80.27, odometer_km: 5.3, charging: false, fault_codes: [] }
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

      // Verify detailed vehicle-level gap identification via GET /v1/completeness?vehicle_id=
      const vCompRes = await fetch(`${baseUrl}/v1/completeness?vehicle_id=${vid}`);
      assert.strictEqual(vCompRes.status, 200);
      const vComp = await vCompRes.json();
      assert.strictEqual(vComp.min_seq, 1);
      assert.strictEqual(vComp.max_seq, 4);
      assert.strictEqual(vComp.received_count, 3);
      assert.strictEqual(vComp.expected_count, 4);
      assert.strictEqual(vComp.missing_count, 1);
      assert.deepStrictEqual(vComp.missing_sequences, [3]);
      assert.strictEqual(vComp.completeness_pct, 75.0);
    });

    test('Deterministic tie-breaking: identical timestamp resolves by higher seq regardless of arrival order', async () => {
      const vid = `VG-TIE-${Date.now()}`;
      const sharedTs = makeTestTs(0);

      // Reading with lower seq (seq: 5, soc: 90)
      const rLowerSeq = { vehicle_id: vid, seq: 5, ts: sharedTs, soc_pct: 90.0, battery_temp_c: 30, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] };
      // Reading with higher seq (seq: 6, soc: 85)
      const rHigherSeq = { vehicle_id: vid, seq: 6, ts: sharedTs, soc_pct: 85.0, battery_temp_c: 30, speed_kmh: 25, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] };

      // Ingest lower seq first, then higher seq
      await postSignedBatch([rLowerSeq]);
      let vState = db.prepare('SELECT soc_pct, last_seq FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vState.last_seq, 5);
      assert.strictEqual(vState.soc_pct, 90.0);

      await postSignedBatch([rHigherSeq]);
      vState = db.prepare('SELECT soc_pct, last_seq FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vState.last_seq, 6, 'Higher seq must win tie when device timestamps are equal');
      assert.strictEqual(vState.soc_pct, 85.0);

      // Now test inverted arrival on a second vehicle: higher seq arrives first, then lower seq arrives
      const vid2 = `VG-TIE2-${Date.now()}`;
      const rLower2 = { ...rLowerSeq, vehicle_id: vid2 };
      const rHigher2 = { ...rHigherSeq, vehicle_id: vid2 };

      await postSignedBatch([rHigher2]);
      await postSignedBatch([rLower2]);

      const vState2 = db.prepare('SELECT soc_pct, last_seq FROM vehicles WHERE vehicle_id = ?').get(vid2);
      assert.strictEqual(vState2.last_seq, 6, 'Higher seq state must be preserved when lower seq arrives later');
      assert.strictEqual(vState2.soc_pct, 85.0);
    });

    test('Batch size validation: enforces bounds [1, 500] and rejects malformed batch structure', async () => {
      // Empty batch -> 400
      const emptyRes = await postSignedBatch([]);
      assert.strictEqual(emptyRes.status, 400, 'Empty batch must return HTTP 400');
      assert.match(emptyRes.body.error, /Batch size must be between 1 and 500/);

      // Single reading batch -> 200
      const singleVid = `VG-SINGLE-${Date.now()}`;
      const singleRes = await postSignedBatch([
        { vehicle_id: singleVid, seq: 1, ts: makeTestTs(0), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(singleRes.status, 200, 'Batch of 1 reading must return HTTP 200');
      assert.strictEqual(singleRes.body.accepted, 1);

      // Exactly 500 readings batch -> 200
      const prefix500 = `VG-500-${Date.now()}`;
      const batch500 = Array.from({ length: 500 }, (_, i) => ({
        vehicle_id: `${prefix500}-${i}`,
        seq: 1,
        ts: makeTestTs(0),
        soc_pct: 80,
        battery_temp_c: 25,
        speed_kmh: 0,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 1,
        charging: false,
        fault_codes: []
      }));
      const res500 = await postSignedBatch(batch500);
      assert.strictEqual(res500.status, 200, 'Batch of 500 readings must return HTTP 200');
      assert.strictEqual(res500.body.accepted, 500);

      // 501 readings batch -> 400
      const batch501 = Array.from({ length: 501 }, (_, i) => ({
        vehicle_id: `VG-501-${i}`,
        seq: 1,
        ts: makeTestTs(0),
        soc_pct: 80,
        battery_temp_c: 25,
        speed_kmh: 0,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 1,
        charging: false,
        fault_codes: []
      }));
      const res501 = await postSignedBatch(batch501);
      assert.strictEqual(res501.status, 400, 'Batch of 501 readings must return HTTP 400');
      assert.match(res501.body.error, /Batch size must be between 1 and 500/);
    });

    test('Field validation edge cases: rejects Infinity, NaN, negative odometer, invalid coordinates, bad fault_codes', async () => {
      const vid = `VG-EDGE-${Date.now()}`;
      const mixedBatch = [
        // 0: Valid reading
        { vehicle_id: vid, seq: 1, ts: makeTestTs(0), soc_pct: 90.0, battery_temp_c: 28.0, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        // 1: Null item
        null,
        // 2: Missing vehicle_id
        { seq: 2, ts: makeTestTs(5), soc_pct: 90.0, battery_temp_c: 28.0, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        // 3: Invalid timestamp
        { vehicle_id: vid, seq: 3, ts: 'not-a-timestamp', soc_pct: 90.0, battery_temp_c: 28.0, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        // 4: soc_pct is Infinity
        { vehicle_id: vid, seq: 4, ts: makeTestTs(10), soc_pct: Infinity, battery_temp_c: 28.0, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        // 5: Latitude out of range (> 90)
        { vehicle_id: vid, seq: 5, ts: makeTestTs(15), soc_pct: 90.0, battery_temp_c: 28.0, speed_kmh: 15, lat: 95.0, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        // 6: Negative odometer
        { vehicle_id: vid, seq: 6, ts: makeTestTs(20), soc_pct: 90.0, battery_temp_c: 28.0, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: -5, charging: false, fault_codes: [] },
        // 7: fault_codes contains non-string elements
        { vehicle_id: vid, seq: 7, ts: makeTestTs(25), soc_pct: 90.0, battery_temp_c: 28.0, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [123, null] },
        // 8: Valid reading
        { vehicle_id: vid, seq: 8, ts: makeTestTs(30), soc_pct: 88.0, battery_temp_c: 29.0, speed_kmh: 18, lat: 13.09, lng: 80.28, odometer_km: 10.5, charging: false, fault_codes: ['P0A80'] }
      ];

      const res = await postSignedBatch(mixedBatch);
      assert.strictEqual(res.status, 207, 'Mixed valid and invalid readings must return HTTP 207');
      assert.strictEqual(res.body.accepted, 2);
      assert.strictEqual(res.body.rejected, 7);
      assert.strictEqual(res.body.results.length, 9);

      // Verify original index preservation
      assert.strictEqual(res.body.results[0].status, 'accepted');
      assert.strictEqual(res.body.results[0].index, 0);

      assert.strictEqual(res.body.results[1].status, 'rejected');
      assert.strictEqual(res.body.results[1].index, 1);

      assert.strictEqual(res.body.results[7].status, 'rejected');
      assert.strictEqual(res.body.results[7].index, 7);
      assert.strictEqual(res.body.results[7].error, 'fault_codes must be an array of strings');

      assert.strictEqual(res.body.results[8].status, 'accepted');
      assert.strictEqual(res.body.results[8].index, 8);
    });
  });

  describe('Part A4: Query API', () => {
    test('GET /v1/vehicles: keyset cursor pagination returns non-overlapping pages', async () => {
      // Seed 6 vehicles
      const seedBatch = Array.from({ length: 6 }, (_, i) => ({
        vehicle_id: `VG-PAGE-A${i + 1}`,
        seq: 1,
        ts: makeTestTs(0),
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
      const payload = [{ vehicle_id: 'VG-REPLAY', seq: 1, ts: makeTestTs(0), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];
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
      // Send rapid requests exceeding burst capacity 40 (60 requests guarantees saturation even with refill)
      const testItem = [{ vehicle_id: 'VG-BURST', seq: 1, ts: makeTestTs(0), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];
      const burstCalls = Array.from({ length: 60 }, () => postSignedBatch(testItem));
      const results = await Promise.all(burstCalls);

      const hit429 = results.find(r => r.status === 429);
      assert.ok(hit429, 'Excess burst requests must return HTTP 429');
      assert.ok(hit429.headers.get('retry-after'), 'HTTP 429 must include Retry-After header');
    });
  });
});
