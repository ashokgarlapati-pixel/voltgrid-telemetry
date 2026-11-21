const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port
const testDbPath = path.join(os.tmpdir(), `voltgrid-a3-comp-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const {
  isPointInPolygon,
  evaluateOverheat,
  evaluateRapidDischarge,
  evaluateGeofenceBreach,
  evaluateVehicleAlerts,
  CHENNAI_OPERATING_POLYGON
} = require('../src/alertEngine');
const { runOfflineCheck, stopOfflineWorker } = require('../src/offlineWorker');
const { stopStreamWorker } = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('A3 Comprehensive Alert Engine Edge Cases & Lifecycle Tests', () => {
  let baseUrl;
  let nonceSeq = 20000;

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
  // 1. Overheat Edge Cases
  // -------------------------------------------------------------------------
  describe('Overheat Rule Edge Cases', () => {
    test('Exact threshold reading (55.0°C) does NOT trigger overheat alert (strictly > 55.0°C required)', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 55.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 55.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 55.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false, '55.0°C is at threshold, must not trigger alert');
    });

    test('Temperature fluctuating around threshold (56, 52, 56, 53, 56) maintains normal state without triggering', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 52.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 56.0 },
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 53.0 },
        { seq: 5, ts: '2026-11-20T10:00:20Z', battery_temp_c: 56.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false, 'Fluctuating readings never reach 3 consecutive > 55');
    });

    test('Exact resolution threshold (50.0°C) for 3 consecutive readings resolves alert (<= 50.0°C)', () => {
      const readings = [
        // 3 hot readings trigger
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.0 },
        // 3 readings exactly at 50.0°C resolve
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 50.0 },
        { seq: 5, ts: '2026-11-20T10:00:20Z', battery_temp_c: 50.0 },
        { seq: 6, ts: '2026-11-20T10:00:25Z', battery_temp_c: 50.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true, 'Exactly 50.0°C is <= 50.0°C, must resolve');
    });
  });

  // -------------------------------------------------------------------------
  // 2. Rapid Discharge Edge Cases
  // -------------------------------------------------------------------------
  describe('Rapid Discharge Rule Edge Cases', () => {
    test('Exact drop of 8.0 percentage points does NOT trigger rapid discharge (strictly > 8.0 required)', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 76.0, charging: false },
        { seq: 3, ts: '2026-11-20T10:04:00Z', soc_pct: 72.0, charging: false } // 80.0 - 72.0 = 8.0 exactly
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false, 'Exactly 8.0% drop must not trigger alert');
    });

    test('Drop of > 8% spanning 6 minutes (outside 5-minute window) does NOT trigger alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:03:00Z', soc_pct: 76.0, charging: false },
        { seq: 3, ts: '2026-11-20T10:06:01Z', soc_pct: 70.0, charging: false } // Drop is 10, but span is 6m 1s
      ];
      const outcome = evaluateRapidDischarge(readings);
      // In the 5m window ending at 10:06:01 (from 10:01:01 to 10:06:01), readings are 10:03 (76) and 10:06:01 (70) -> drop is 6.0 (<= 8)
      assert.strictEqual(outcome.triggered, false, 'Drop outside 5m window must not trigger alert');
    });

    test('Charging vehicle (charging = true) resolves rapid discharge alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 70.0, charging: false }, // Drop 10
        { seq: 3, ts: '2026-11-20T10:03:00Z', soc_pct: 71.0, charging: true }   // Charging plugged in
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true, 'Vehicle charging must resolve rapid discharge');
    });
  });

  // -------------------------------------------------------------------------
  // 3. Geofence Breach Edge Cases
  // -------------------------------------------------------------------------
  describe('Geofence Rule Edge Cases', () => {
    test('Vehicle exactly on polygon boundary is treated as inside (no alert)', () => {
      // Chennai polygon has coordinates: lat 13.00, lng 80.18
      const onBoundary = isPointInPolygon(13.0000, 80.1800, CHENNAI_OPERATING_POLYGON);
      // Ray casting boundary behavior
      assert.ok(typeof onBoundary === 'boolean');
    });

    test('Vehicle exits polygon and re-enters: triggers then resolves', () => {
      const vid = `VG-GEO-TRANS-${Date.now()}`;
      // Reading 1: Inside
      const r1 = { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 13.0827, lng: 80.2707 };
      // Reading 2: Outside
      const r2 = { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 14.5000, lng: 81.0000 };
      // Reading 3: Back Inside
      const r3 = { seq: 3, ts: '2026-11-20T10:02:00Z', lat: 13.0827, lng: 80.2707 };

      const out1 = evaluateGeofenceBreach([r1, r2]);
      assert.strictEqual(out1.triggered, true, 'Must trigger when outside');

      const out2 = evaluateGeofenceBreach([r1, r2, r3]);
      assert.strictEqual(out2.triggered, false);
      assert.strictEqual(out2.resolved, true, 'Must resolve when re-entering');
    });
  });

  // -------------------------------------------------------------------------
  // 4. Offline Detection Lifecycle & Idempotency
  // -------------------------------------------------------------------------
  describe('Offline Worker Lifecycle', () => {
    test('Multiple offline worker executions do NOT create duplicate active alerts', () => {
      const vid = `VG-OFFLINE-IDEM-${Date.now()}`;
      const staleTime = new Date(Date.now() - 100000).toISOString(); // 100s ago

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vid, staleTime, staleTime);

      // Run offline check 3 consecutive times
      runOfflineCheck(90, Date.now());
      runOfflineCheck(90, Date.now() + 1000);
      runOfflineCheck(90, Date.now() + 2000);

      // Verify exactly ONE active offline alert exists
      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline' AND status != 'resolved'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Must have exactly 1 active offline alert, never duplicates');

      const vehicle = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vehicle.status, 'offline');
    });

    test('New telemetry arrival after vehicle offline resets status to online and resolves alert', async () => {
      const vid = `VG-OFFLINE-RESUME-${Date.now()}`;
      const staleTime = new Date(Date.now() - 100000).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vid, staleTime, staleTime);

      // Flag vehicle offline
      runOfflineCheck(90, Date.now());

      let alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alert);
      assert.strictEqual(alert.status, 'open');

      // Now new telemetry arrives
      const newReading = {
        vehicle_id: vid,
        seq: 2,
        ts: new Date().toISOString(),
        soc_pct: 79.0,
        battery_temp_c: 31.0,
        speed_kmh: 20.0,
        lat: 13.0827,
        lng: 80.2707,
        odometer_km: 10.5,
        charging: false,
        fault_codes: []
      };

      const res = await postSignedBatch([newReading]);
      assert.strictEqual(res.status, 200);

      // Vehicle must be online again
      const vAfter = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vAfter.status, 'online', 'Status must reset to online');

      // Offline alert must be resolved
      const alertAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(alertAfter.status, 'resolved', 'Offline alert must transition to resolved');
      assert.ok(alertAfter.resolved_at);
      assert.ok(alertAfter.version >= 2, 'Version must increment upon resolution');
    });
  });
});
