const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port
const testDbPath = path.join(os.tmpdir(), `voltgrid-conc-adv-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');
const { stopStreamWorker } = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('Advanced Concurrency and Cross-Functional Integration Tests', () => {
  let baseUrl;
  let nonceSeq = 40000;

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

  test('Concurrent writes to the same vehicle by multiple clients: zero deadlocks and correct timestamp resolution', async () => {
    const vid = `VG-SAME-VEHICLE-${Date.now()}`;
    const baseTime = Date.parse('2026-11-20T10:00:00.000Z');

    // 10 concurrent requests each writing a distinct sequence and timestamp for the same vehicle
    const promises = Array.from({ length: 10 }, (_, i) => {
      const reading = {
        vehicle_id: vid,
        seq: i + 1,
        ts: new Date(baseTime + (i * 1000)).toISOString(),
        soc_pct: 90.0 - (i * 2), // seq 10 has soc_pct 72.0
        battery_temp_c: 28.0 + (i * 0.5),
        speed_kmh: 20.0 + i,
        lat: 13.0827,
        lng: 80.2707,
        odometer_km: 100.0 + (i * 0.1),
        charging: false,
        fault_codes: []
      };
      return postSignedBatch([reading]);
    });

    const responses = await Promise.all(promises);
    for (const res of responses) {
      assert.strictEqual(res.status, 200, 'Every concurrent batch write must succeed');
      assert.strictEqual(res.body.accepted, 1);
    }

    // Verify all 10 rows exist in telemetry_history
    const historyRows = db.prepare('SELECT seq FROM telemetry_history WHERE vehicle_id = ? ORDER BY seq ASC').all(vid);
    assert.strictEqual(historyRows.length, 10);
    assert.deepStrictEqual(historyRows.map(r => r.seq), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Verify vehicles table live state reflects highest timestamp (seq 10, ts = baseTime + 9000, soc = 72.0)
    const vRow = db.prepare('SELECT soc_pct, last_seq, min_seq, max_seq, received_count FROM vehicles WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(vRow.last_seq, 10);
    assert.strictEqual(vRow.soc_pct, 72.0, 'Live state must reflect highest timestamp');
    assert.strictEqual(vRow.min_seq, 1);
    assert.strictEqual(vRow.max_seq, 10);
    assert.strictEqual(vRow.received_count, 10);
  });

  test('Concurrent ingestion and vehicle queries: queries succeed under continuous write load with zero locks', async () => {
    const prefix = `VG-CONC-RW-${Date.now()}`;

    // 5 write batches running concurrently
    const writeBatches = Array.from({ length: 5 }, (_, batchIdx) => {
      const readings = Array.from({ length: 10 }, (_, i) => ({
        vehicle_id: `${prefix}-B${batchIdx}-V${i}`,
        seq: 1,
        ts: '2026-11-20T10:00:00Z',
        soc_pct: 80,
        battery_temp_c: 30,
        speed_kmh: 20,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 50,
        charging: false,
        fault_codes: []
      }));
      return postSignedBatch(readings);
    });

    // 5 concurrent read queries running at the exact same time
    const readQueries = [
      fetch(`${baseUrl}/v1/vehicles?limit=50`),
      fetch(`${baseUrl}/v1/fleet/summary`),
      fetch(`${baseUrl}/v1/vehicles?status=online`),
      fetch(`${baseUrl}/v1/fleet/summary`),
      fetch(`${baseUrl}/v1/completeness`)
    ];

    const [writeResults, readResults] = await Promise.all([
      Promise.all(writeBatches),
      Promise.all(readQueries)
    ]);

    for (const w of writeResults) {
      assert.strictEqual(w.status, 200, 'Ingestion must succeed concurrently');
    }

    for (const r of readResults) {
      assert.strictEqual(r.status, 200, 'Read queries must succeed concurrently during writes without SQLITE_BUSY');
    }
  });

  test('Concurrent alert creation across multiple vehicles: no race conditions or duplicate alerts', async () => {
    const prefix = `VG-CONC-ALERT-${Date.now()}`;

    // 5 vehicles simultaneously trigger overheat alerts (3 readings > 55°C each)
    const alertBatches = Array.from({ length: 5 }, (_, vIdx) => {
      const vid = `${prefix}-V${vIdx}`;
      return postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: '2026-11-20T10:00:05Z', soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: '2026-11-20T10:00:10Z', soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
      ]);
    });

    const responses = await Promise.all(alertBatches);
    for (const res of responses) {
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.body.accepted, 3);
    }

    // Verify exactly 1 alert per vehicle (total 5 alerts)
    for (let vIdx = 0; vIdx < 5; vIdx++) {
      const vid = `${prefix}-V${vIdx}`;
      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' AND status = 'open'").all(vid);
      assert.strictEqual(alerts.length, 1, `Vehicle ${vid} must have exactly 1 active overheat alert`);
    }
  });
});
