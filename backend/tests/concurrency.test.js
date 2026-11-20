const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port for concurrency suite
const testDbPath = path.join(os.tmpdir(), `voltgrid-concurrency-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('Part A1 Concurrency & Idempotency Tests (20 Concurrent Clients)', () => {
  let baseUrl;
  let nonceCounter = 1;

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

  // Helper to post signed batch with unique nonce
  async function postSignedBatch(readings) {
    const rawBody = JSON.stringify(readings);
    const timestamp = (Date.now() + (nonceCounter++)).toString();
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${rawBody}`).digest('hex');

    const headers = {
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
    return { status: res.status, body };
  }

  test('20 simultaneous clients firing identical 50-item batch: exactly 50 accepted, 950 duplicates, 0 double-counts', async () => {
    const prefix = `CONC-20-${Date.now()}`;
    const sharedBatch = Array.from({ length: 50 }, (_, i) => ({
      vehicle_id: `${prefix}-V${String(i + 1).padStart(3, '0')}`,
      seq: 1,
      ts: '2026-11-20T12:00:00.000Z',
      soc_pct: 75.0,
      battery_temp_c: 32.0,
      speed_kmh: 40.0,
      lat: 13.0827,
      lng: 80.2707,
      odometer_km: 100.0,
      charging: false,
      fault_codes: []
    }));

    // Fire 20 concurrent client requests simultaneously with unique valid HMAC signatures
    const concurrentClients = 20;
    const promises = Array.from({ length: concurrentClients }, () => postSignedBatch(sharedBatch));
    const responses = await Promise.all(promises);

    let totalAccepted = 0;
    let totalDuplicates = 0;

    for (const res of responses) {
      assert.strictEqual(res.status, 200, 'Every concurrent client request must return HTTP 200');
      totalAccepted += res.body.accepted;
      totalDuplicates += res.body.duplicates;
    }

    // Exactly 50 readings accepted across all clients; remaining 950 marked duplicate
    assert.strictEqual(totalAccepted, 50, 'Total accepted must be exactly 50');
    assert.strictEqual(totalDuplicates, 950, 'Total duplicates must be exactly 950 (19 * 50)');

    // Database verification: Exactly 50 rows in telemetry_history
    const countRow = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id LIKE ?').get(`${prefix}%`);
    assert.strictEqual(Number(countRow.total), 50, 'Database must contain exactly 50 rows in telemetry_history');

    // Database verification: vehicles received_count sum is exactly 50 (zero double counting)
    const sumRow = db.prepare('SELECT SUM(received_count) AS total_recv FROM vehicles WHERE vehicle_id LIKE ?').get(`${prefix}%`);
    assert.strictEqual(Number(sumRow.total_recv), 50, 'vehicles.received_count sum must be exactly 50');
  });

  test('Concurrent out-of-order batches: final state strictly reflects highest device timestamp', async () => {
    const vid = `CONC-OOO-${Date.now()}`;
    const baseTime = Date.parse('2026-11-20T12:00:00.000Z');

    // 5 readings with different timestamps and different battery SoCs
    const r1 = { vehicle_id: vid, seq: 1, ts: new Date(baseTime).toISOString(), soc_pct: 95.0, battery_temp_c: 30, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] };
    const r2 = { vehicle_id: vid, seq: 2, ts: new Date(baseTime + 10000).toISOString(), soc_pct: 90.0, battery_temp_c: 30, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] };
    const r3 = { vehicle_id: vid, seq: 3, ts: new Date(baseTime + 20000).toISOString(), soc_pct: 85.0, battery_temp_c: 30, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] };
    const r4 = { vehicle_id: vid, seq: 4, ts: new Date(baseTime + 30000).toISOString(), soc_pct: 80.0, battery_temp_c: 30, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] };
    const r5 = { vehicle_id: vid, seq: 5, ts: new Date(baseTime + 40000).toISOString(), soc_pct: 75.0, battery_temp_c: 30, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] };

    // Fire in completely reversed / jumbled concurrent order: [r5, r2, r4, r1, r3]
    await Promise.all([
      postSignedBatch([r5]),
      postSignedBatch([r2]),
      postSignedBatch([r4]),
      postSignedBatch([r1]),
      postSignedBatch([r3])
    ]);

    // Live vehicle state MUST have soc_pct: 75.0 (from r5, highest timestamp)
    const vehicle = db.prepare('SELECT soc_pct, last_ts, min_seq, max_seq, received_count FROM vehicles WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(vehicle.soc_pct, 75.0, 'Live vehicle state must reflect reading with highest device timestamp');
    assert.strictEqual(vehicle.last_ts, new Date(baseTime + 40000).toISOString());
    assert.strictEqual(vehicle.min_seq, 1);
    assert.strictEqual(vehicle.max_seq, 5);
    assert.strictEqual(vehicle.received_count, 5);
  });
});
