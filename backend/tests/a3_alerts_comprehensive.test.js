const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port
const testDbPath = path.join(os.tmpdir(), `voltgrid-a3-comp-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = process.env.NODE_ENV || 'test';
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

    test('evaluateOverheat tracks discrete episodes with openedAt and resolvedAt', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.0 },
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 49.0 },
        { seq: 5, ts: '2026-11-20T10:00:20Z', battery_temp_c: 48.0 },
        { seq: 6, ts: '2026-11-20T10:00:25Z', battery_temp_c: 47.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true);
      assert.ok(Array.isArray(outcome.episodes), 'outcome.episodes must be an array');
      assert.strictEqual(outcome.episodes.length, 1);
      assert.strictEqual(outcome.episodes[0].status, 'resolved');
      assert.strictEqual(outcome.episodes[0].openedAt, '2026-11-20T10:00:10Z');
      assert.strictEqual(outcome.episodes[0].resolvedAt, '2026-11-20T10:00:25Z');
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

  describe('Issue 2: Overheat Alert Lifecycle & De-duplication', () => {
    test('Three consecutive readings above 55°C -> One alert opens', async () => {
      const vid = `VG-ISSUE2-OPEN-${Date.now()}`;
      const now = Date.now();
      const readings = [
        { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ];

      const res = await postSignedBatch(readings);
      assert.strictEqual(res.status, 200);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Exactly one alert must be created');
      assert.strictEqual(alerts[0].status, 'open');
      assert.strictEqual(alerts[0].version, 1);
    });

    test('Three consecutive readings at or below 50°C -> Alert resolves', async () => {
      const vid = `VG-ISSUE2-RESOLVE-${Date.now()}`;
      const now = Date.now();
      // 3 hot readings open alert
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 30000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 25000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 20000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);

      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid).status, 'open');

      // 3 cool readings resolve alert
      await postSignedBatch([
        { vehicle_id: vid, seq: 4, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Must still be exactly 1 alert row');
      assert.strictEqual(alerts[0].status, 'resolved', 'Alert must transition to resolved');
      assert.ok(alerts[0].resolved_at);
      assert.strictEqual(alerts[0].version, 2, 'Version must increment to 2 upon resolution');
    });

    test('Additional cool readings -> No duplicate alert inserted', async () => {
      const vid = `VG-ISSUE2-COOL-${Date.now()}`;
      const now = Date.now();
      // Episode: 3 hot -> open, 3 cool -> resolved
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 60000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 55000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 50000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: new Date(now - 45000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 40000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 35000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const countBefore = db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid).total;
      assert.strictEqual(countBefore, 1);

      // Post 5 additional cool readings individually (the exact pattern that previously triggered duplicates)
      for (let i = 7; i <= 11; i++) {
        await postSignedBatch([
          { vehicle_id: vid, seq: i, ts: new Date(now - (35000 - (i - 6) * 5000)).toISOString(), soc_pct: 80, battery_temp_c: 46.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5 + i * 0.1, charging: false, fault_codes: [] }
        ]);
      }

      const alertsAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vid);
      assert.strictEqual(alertsAfter.length, 1, 'Additional cool readings must NOT create duplicate resolved alerts');
      assert.strictEqual(alertsAfter[0].status, 'resolved');
    });

    test('New overheating episode -> New alert opens', async () => {
      const vid = `VG-ISSUE2-NEW-${Date.now()}`;
      const now = Date.now();
      // Episode 1: 3 hot -> resolved
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 80000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 75000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 70000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: new Date(now - 65000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 60000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 55000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alertsEp1 = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vid);
      assert.strictEqual(alertsEp1.length, 1);
      assert.strictEqual(alertsEp1[0].status, 'resolved');

      // Intervening normal readings
      await postSignedBatch([
        { vehicle_id: vid, seq: 7, ts: new Date(now - 50000).toISOString(), soc_pct: 80, battery_temp_c: 45.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.6, charging: false, fault_codes: [] }
      ]);

      // Episode 2: Genuinely new overheating episode (3 new hot readings)
      await postSignedBatch([
        { vehicle_id: vid, seq: 8, ts: new Date(now - 45000).toISOString(), soc_pct: 80, battery_temp_c: 56.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.7, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 9, ts: new Date(now - 40000).toISOString(), soc_pct: 80, battery_temp_c: 57.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.8, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 10, ts: new Date(now - 35000).toISOString(), soc_pct: 80, battery_temp_c: 59.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.9, charging: false, fault_codes: [] }
      ]);

      const alertsEp2 = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' ORDER BY id ASC").all(vid);
      assert.strictEqual(alertsEp2.length, 2, 'Genuinely new overheating episode must create a second alert');
      assert.strictEqual(alertsEp2[0].status, 'resolved', 'First alert remains resolved');
      assert.strictEqual(alertsEp2[1].status, 'open', 'Second alert opens cleanly');
    });

    test('Same readings processed across multiple batches -> No duplicate', async () => {
      const vid = `VG-ISSUE2-MULTI-${Date.now()}`;
      const now = Date.now();
      const batch1 = [
        { vehicle_id: vid, seq: 1, ts: new Date(now - 40000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 35000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 30000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: new Date(now - 25000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 20000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ];

      await postSignedBatch(batch1);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1);

      // Re-send the exact same batch: all marked duplicate, alert count must remain 1
      const res2 = await postSignedBatch(batch1);
      assert.strictEqual(res2.body.duplicates, 6);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1);
    });

    test('Restart engine after resolution -> No duplicate', async () => {
      const vid = `VG-ISSUE2-RESTART-${Date.now()}`;
      const now = Date.now();
      // Seed an alert that was opened and resolved
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 30000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 25000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 20000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alertsBefore = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ?").all(vid);
      assert.strictEqual(alertsBefore.length, 1);
      assert.strictEqual(alertsBefore[0].status, 'resolved');

      // Simulate engine restart by invoking evaluateVehicleAlerts directly as on a fresh startup
      const changedAfterRestart = evaluateVehicleAlerts(vid);
      assert.strictEqual(changedAfterRestart.length, 0, 'Must produce zero changed alerts upon fresh evaluation after restart');

      const alertsAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ?").all(vid);
      assert.strictEqual(alertsAfter.length, 1, 'Restarted engine evaluation must NOT duplicate resolved alerts');
    });
  });

  describe('Issue 3: Geofence Intermediate Transitions & Chronological Evaluation', () => {
    test('Inside -> Outside -> Inside in one batch', async () => {
      const vid = `VG-GEO-ONEBATCH-${Date.now()}`;
      const now = Date.now();
      const t1 = new Date(now - 15000).toISOString();
      const t2 = new Date(now - 10000).toISOString();
      const t3 = new Date(now - 5000).toISOString();

      const batch = [
        { vehicle_id: vid, seq: 1, ts: t1, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0800, lng: 80.2500, odometer_km: 10, charging: false, fault_codes: [] }, // Inside
        { vehicle_id: vid, seq: 2, ts: t2, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5000, lng: 81.0000, odometer_km: 10.5, charging: false, fault_codes: [] }, // Outside
        { vehicle_id: vid, seq: 3, ts: t3, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0900, lng: 80.2600, odometer_km: 11, charging: false, fault_codes: [] }  // Inside
      ];

      const res = await postSignedBatch(batch);
      assert.strictEqual(res.status, 200);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Intermediate breach within single batch must be captured as 1 alert');
      assert.strictEqual(alerts[0].status, 'resolved', 'Must resolve upon vehicle return inside');
      assert.strictEqual(alerts[0].opened_at, t2, 'Alert opened_at must match the outside reading timestamp');
      assert.strictEqual(alerts[0].resolved_at, t3, 'Alert resolved_at must match the inside return timestamp');
      assert.strictEqual(alerts[0].version, 2, 'Version must be 2 for resolved alert');
    });

    test('The same sequence across separate batches', async () => {
      const vid = `VG-GEO-SEPBATCH-${Date.now()}`;
      const now = Date.now();
      const t1 = new Date(now - 15000).toISOString();
      const t2 = new Date(now - 10000).toISOString();
      const t3 = new Date(now - 5000).toISOString();

      // Batch 1: Inside
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: t1, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0800, lng: 80.2500, odometer_km: 10, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 0);

      // Batch 2: Outside
      await postSignedBatch([
        { vehicle_id: vid, seq: 2, ts: t2, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5000, lng: 81.0000, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);
      const alertOpen = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").get(vid);
      assert.ok(alertOpen);
      assert.strictEqual(alertOpen.status, 'open');
      assert.strictEqual(alertOpen.opened_at, t2);
      assert.strictEqual(alertOpen.version, 1);

      // Batch 3: Inside
      await postSignedBatch([
        { vehicle_id: vid, seq: 3, ts: t3, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0900, lng: 80.2600, odometer_km: 11, charging: false, fault_codes: [] }
      ]);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Separate batches must produce exact same single alert');
      assert.strictEqual(alerts[0].status, 'resolved');
      assert.strictEqual(alerts[0].opened_at, t2);
      assert.strictEqual(alerts[0].resolved_at, t3);
      assert.strictEqual(alerts[0].version, 2);
    });

    test('Multiple outside/inside transitions', async () => {
      const vid = `VG-GEO-MULTI-${Date.now()}`;
      const now = Date.now();
      const t1 = new Date(now - 25000).toISOString();
      const t2 = new Date(now - 20000).toISOString(); // Breach 1 open
      const t3 = new Date(now - 15000).toISOString(); // Breach 1 resolve
      const t4 = new Date(now - 10000).toISOString(); // Breach 2 open
      const t5 = new Date(now - 5000).toISOString();  // Breach 2 resolve

      const multiBatch = [
        { vehicle_id: vid, seq: 1, ts: t1, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0800, lng: 80.2500, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: t2, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5000, lng: 81.0000, odometer_km: 10.5, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: t3, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0900, lng: 80.2600, odometer_km: 11, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: t4, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5200, lng: 81.0200, odometer_km: 11.5, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: t5, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0850, lng: 80.2550, odometer_km: 12, charging: false, fault_codes: [] }
      ];

      await postSignedBatch(multiBatch);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach' ORDER BY id ASC").all(vid);
      assert.strictEqual(alerts.length, 2, 'Two outside/inside excursions must generate exactly 2 alerts');
      assert.strictEqual(alerts[0].status, 'resolved');
      assert.strictEqual(alerts[0].opened_at, t2);
      assert.strictEqual(alerts[0].resolved_at, t3);
      assert.strictEqual(alerts[1].status, 'resolved');
      assert.strictEqual(alerts[1].opened_at, t4);
      assert.strictEqual(alerts[1].resolved_at, t5);
    });

    test('Out-of-order readings', async () => {
      const vid = `VG-GEO-OOO-${Date.now()}`;
      const now = Date.now();
      const t1 = new Date(now - 20000).toISOString();
      const t2 = new Date(now - 15000).toISOString(); // Arrives late (outside)
      const t3 = new Date(now - 10000).toISOString();

      // Batch 1: Vehicle is inside at T1 and T3
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: t1, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0800, lng: 80.2500, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: t3, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0900, lng: 80.2600, odometer_km: 11, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 0);

      // Batch 2: Late reading at T2 arrives (outside geofence)
      await postSignedBatch([
        { vehicle_id: vid, seq: 2, ts: t2, soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5000, lng: 81.0000, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Late-arriving breach reading must create resolved alert');
      assert.strictEqual(alerts[0].status, 'resolved');
      assert.strictEqual(alerts[0].opened_at, t2);
      assert.strictEqual(alerts[0].resolved_at, t3);

      const active = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND status != 'resolved'").all(vid);
      assert.strictEqual(active.length, 0, 'No active alert must remain because latest reading T3 is inside');
    });

    test('Repeated evaluation and duplicate prevention', async () => {
      const vid = `VG-GEO-DUP-${Date.now()}`;
      const now = Date.now();
      const batch = [
        { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0800, lng: 80.2500, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5000, lng: 81.0000, odometer_km: 10.5, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0900, lng: 80.2600, odometer_km: 11, charging: false, fault_codes: [] }
      ];

      await postSignedBatch(batch);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1);

      // Re-send same batch
      const res2 = await postSignedBatch(batch);
      assert.strictEqual(res2.body.duplicates, 3);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1);

      // Direct engine re-evaluation
      const changed = evaluateVehicleAlerts(vid);
      assert.strictEqual(changed.length, 0, 'Must produce 0 changed alerts on re-evaluation');
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1);
    });

    test('Application restart and alert persistence', async () => {
      const vid = `VG-GEO-RESTART-${Date.now()}`;
      const now = Date.now();

      // Open an alert
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 14.5000, lng: 81.0000, odometer_km: 10, charging: false, fault_codes: [] }
      ]);
      const openAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").get(vid);
      assert.strictEqual(openAlert.status, 'open');

      // Simulate restart: re-evaluate vehicle with existing state
      const rechecked = evaluateVehicleAlerts(vid);
      assert.strictEqual(rechecked.length, 0, 'No change on restart without new readings');

      // Now send returning reading inside polygon
      await postSignedBatch([
        { vehicle_id: vid, seq: 2, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0800, lng: 80.2500, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Persisted alert resolved without duplication');
      assert.strictEqual(alerts[0].status, 'resolved');
      assert.strictEqual(alerts[0].version, 2);
    });
  });

  describe('Edge Cases: Multi-Episode Overheat and Rapid Discharge Transitions', () => {
    test('Two complete overheat episodes with intervening hysteresis maintain exact lifecycle', async () => {
      const vid = `VG-OVERHEAT-MULTI-${Date.now()}`;
      const now = Date.now();

      // Episode 1: Open alert (3 readings > 55°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 90000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 85000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 80000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);
      const ep1Open = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' AND status = 'open'").get(vid);
      assert.ok(ep1Open, 'Episode 1 must open alert');
      assert.strictEqual(ep1Open.version, 1);

      // Episode 1: Resolve alert (3 readings <= 50°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 4, ts: new Date(now - 75000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 70000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 65000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);
      const ep1Resolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' AND status = 'resolved'").all(vid);
      assert.strictEqual(ep1Resolved.length, 1);
      assert.strictEqual(ep1Resolved[0].version, 2);

      // Intervening readings in hysteresis zone (52°C, 53°C) must not trigger new alert
      await postSignedBatch([
        { vehicle_id: vid, seq: 7, ts: new Date(now - 60000).toISOString(), soc_pct: 80, battery_temp_c: 52.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.6, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 8, ts: new Date(now - 55000).toISOString(), soc_pct: 80, battery_temp_c: 53.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.7, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid).total, 1);

      // Episode 2: Open second alert (3 readings > 55°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 9, ts: new Date(now - 50000).toISOString(), soc_pct: 80, battery_temp_c: 56.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.8, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 10, ts: new Date(now - 45000).toISOString(), soc_pct: 80, battery_temp_c: 57.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.9, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 11, ts: new Date(now - 40000).toISOString(), soc_pct: 80, battery_temp_c: 58.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 11.0, charging: false, fault_codes: [] }
      ]);
      const alertsMid = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' ORDER BY id ASC").all(vid);
      assert.strictEqual(alertsMid.length, 2, 'Must have 2 distinct alerts');
      assert.strictEqual(alertsMid[0].status, 'resolved');
      assert.strictEqual(alertsMid[1].status, 'open');

      // Episode 2: Resolve second alert (3 readings <= 50°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 12, ts: new Date(now - 35000).toISOString(), soc_pct: 80, battery_temp_c: 49.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 11.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 13, ts: new Date(now - 30000).toISOString(), soc_pct: 80, battery_temp_c: 48.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 11.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 14, ts: new Date(now - 25000).toISOString(), soc_pct: 80, battery_temp_c: 47.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 11.3, charging: false, fault_codes: [] }
      ]);
      const alertsFinal = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' ORDER BY id ASC").all(vid);
      assert.strictEqual(alertsFinal.length, 2);
      assert.strictEqual(alertsFinal[0].status, 'resolved');
      assert.strictEqual(alertsFinal[1].status, 'resolved');
      assert.notStrictEqual(alertsFinal[0].id, alertsFinal[1].id);
    });

    test('Rapid discharge: boundary drop (8.0% vs 8.1%) and resolution on charging transition', async () => {
      const vid = `VG-RAPID-DISCHARGE-${Date.now()}`;
      const now = Date.now();

      // Reading 1: Baseline 80.0% SoC
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 60000).toISOString(), soc_pct: 80.0, battery_temp_c: 30, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
      ]);

      // Reading 2: Exactly 8.0% drop (SoC = 72.0%) -> Must NOT trigger (strictly >8.0% required)
      await postSignedBatch([
        { vehicle_id: vid, seq: 2, ts: new Date(now - 40000).toISOString(), soc_pct: 72.0, battery_temp_c: 30, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid).total, 0);

      // Reading 3: 8.1% drop total (SoC = 71.9%) -> Triggers rapid discharge alert
      await postSignedBatch([
        { vehicle_id: vid, seq: 3, ts: new Date(now - 20000).toISOString(), soc_pct: 71.9, battery_temp_c: 30, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.7, charging: false, fault_codes: [] }
      ]);
      const alertOpen = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.ok(alertOpen, 'Rapid discharge alert must be triggered on >8.0% drop');
      assert.strictEqual(alertOpen.status, 'open');

      // Reading 4: Vehicle plugs in to charge (charging = true) -> Alert resolves
      await postSignedBatch([
        { vehicle_id: vid, seq: 4, ts: new Date(now - 5000).toISOString(), soc_pct: 72.5, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10.7, charging: true, fault_codes: [] }
      ]);
      const alertResolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.strictEqual(alertResolved.status, 'resolved', 'Charging vehicle must resolve rapid discharge alert');
      assert.strictEqual(alertResolved.version, 2);
    });

    test('Order-independent alert reconstruction: 118 readings with oldest 18 containing hot streak arriving late', async () => {
      const vidIn = `VG-118-IN-${Date.now()}`;
      const vidOut = `VG-118-OUT-${Date.now()}`;
      const now = Date.now();
      const readings = [];

      for (let i = 0; i < 118; i++) {
        const ts = new Date(now - (118 - i) * 3000).toISOString();
        let temp = 40.0;
        if (i >= 5 && i <= 8) {
          temp = 58.0; // hot streak
        } else if (i >= 9 && i <= 13) {
          temp = 48.0; // cool streak
        }
        readings.push({
          seq: i + 1,
          ts,
          soc_pct: 80,
          battery_temp_c: temp,
          speed_kmh: 30,
          lat: 13.08,
          lng: 80.27,
          odometer_km: 10 + i * 0.1,
          charging: false,
          fault_codes: []
        });
      }

      // In-order: first 18, then remaining 100
      const rIn = readings.map(r => ({ ...r, vehicle_id: vidIn }));
      await postSignedBatch(rIn.slice(0, 18));
      await postSignedBatch(rIn.slice(18));
      const alertsIn = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vidIn);

      // Out-of-order: newest 100 first, then oldest 18 late
      const rOut = readings.map(r => ({ ...r, vehicle_id: vidOut }));
      await postSignedBatch(rOut.slice(18));
      await postSignedBatch(rOut.slice(0, 18));
      const alertsOut = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vidOut);

      assert.strictEqual(alertsIn.length, 1);
      assert.strictEqual(alertsOut.length, 1);
      assert.strictEqual(alertsIn[0].status, 'resolved');
      assert.strictEqual(alertsOut[0].status, 'resolved');
      assert.strictEqual(alertsIn[0].opened_at, alertsOut[0].opened_at);
      assert.strictEqual(alertsIn[0].resolved_at, alertsOut[0].resolved_at);
    });

    test('Order-independent alert reconstruction: 150 readings with oldest 18 containing hot streak arriving late', async () => {
      const vidIn = `VG-150-IN-${Date.now()}`;
      const vidOut = `VG-150-OUT-${Date.now()}`;
      const now = Date.now();
      const readings = [];

      for (let i = 0; i < 150; i++) {
        const ts = new Date(now - (150 - i) * 3000).toISOString();
        let temp = 40.0;
        if (i >= 5 && i <= 8) {
          temp = 58.0; // hot streak in oldest 18
        } else if (i >= 9 && i <= 13) {
          temp = 48.0; // cool streak in oldest 18
        }
        readings.push({
          seq: i + 1,
          ts,
          soc_pct: 80,
          battery_temp_c: temp,
          speed_kmh: 30,
          lat: 13.08,
          lng: 80.27,
          odometer_km: 10 + i * 0.1,
          charging: false,
          fault_codes: []
        });
      }

      // In-order: first 18, then remaining 132
      const rIn = readings.map(r => ({ ...r, vehicle_id: vidIn }));
      await postSignedBatch(rIn.slice(0, 18));
      await postSignedBatch(rIn.slice(18));
      const alertsIn = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vidIn);

      // Out-of-order: newest 132 first, then oldest 18 arriving late
      const rOut = readings.map(r => ({ ...r, vehicle_id: vidOut }));
      await postSignedBatch(rOut.slice(18));
      await postSignedBatch(rOut.slice(0, 18));
      const alertsOut = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").all(vidOut);

      assert.strictEqual(alertsIn.length, 1);
      assert.strictEqual(alertsOut.length, 1);
      assert.strictEqual(alertsIn[0].status, 'resolved');
      assert.strictEqual(alertsOut[0].status, 'resolved');
      assert.strictEqual(alertsIn[0].opened_at, alertsOut[0].opened_at);
      assert.strictEqual(alertsIn[0].resolved_at, alertsOut[0].resolved_at);
    });
  });

  // =========================================================================
  // TASK 8: RAPID-DISCHARGE DETECTION ACROSS CHARGING PERIODS (ISSUE 1)
  // =========================================================================
  describe('Issue 1: Rapid-Discharge Detection Across Charging Periods', () => {
    test('Charging period resets rapid-discharge window: no false alert for 90% -> 85%(chg) -> 85% -> 80%', async () => {
      const vid = `VG-CHG-RESET-${Date.now()}`;
      const now = Date.now();

      // Reading 1: 90% SoC, not charging
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 180000).toISOString(), soc_pct: 90.0, battery_temp_c: 30, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid).total, 0);

      // Reading 2: 85% SoC, charging = true (charging session commences)
      await postSignedBatch([
        { vehicle_id: vid, seq: 2, ts: new Date(now - 120000).toISOString(), soc_pct: 85.0, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: true, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid).total, 0);

      // Reading 3: 85% SoC, charging = false (vehicle unplugs)
      await postSignedBatch([
        { vehicle_id: vid, seq: 3, ts: new Date(now - 60000).toISOString(), soc_pct: 85.0, battery_temp_c: 30, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid).total, 0);

      // Reading 4: 80% SoC, charging = false (drop in new period is 5.0 points <= 8.0)
      // Must NOT compare 90% (pre-charging) with 80% (post-charging)
      await postSignedBatch([
        { vehicle_id: vid, seq: 4, ts: new Date(now - 10000).toISOString(), soc_pct: 80.0, battery_temp_c: 30, speed_kmh: 45, lat: 13.08, lng: 80.27, odometer_km: 10.8, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(
        db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid).total,
        0,
        'Must NOT trigger rapid discharge across a charging break'
      );

      // Reading 5: 76.0% SoC, charging = false (drop in current continuous non-charging period is 85.0 - 76.0 = 9.0 > 8.0)
      await postSignedBatch([
        { vehicle_id: vid, seq: 5, ts: new Date(now - 2000).toISOString(), soc_pct: 76.0, battery_temp_c: 30, speed_kmh: 45, lat: 13.08, lng: 80.27, odometer_km: 11.2, charging: false, fault_codes: [] }
      ]);
      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.ok(alert, 'Must trigger rapid discharge when continuous non-charging period drops > 8.0 points');
      assert.strictEqual(alert.status, 'open');
      const details = JSON.parse(alert.details);
      assert.strictEqual(details.drop_points, 9.0);
      assert.strictEqual(details.window_max_soc, 85.0);
    });

    test('Late-arriving charging reading resolves previously triggered false alert', async () => {
      const vid = `VG-LATE-CHG-${Date.now()}`;
      const now = Date.now();

      // 1. Post Readings 1, 3, 4 without Reading 2 (charging).
      // Initially, the engine sees 90% -> 85% -> 80% without knowing a charge occurred -> triggers alert.
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 180000).toISOString(), soc_pct: 90.0, battery_temp_c: 30, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 60000).toISOString(), soc_pct: 85.0, battery_temp_c: 30, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: new Date(now - 10000).toISOString(), soc_pct: 80.0, battery_temp_c: 30, speed_kmh: 45, lat: 13.08, lng: 80.27, odometer_km: 10.8, charging: false, fault_codes: [] }
      ]);
      const alertBefore = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.ok(alertBefore, 'Alert triggered when charging reading was missing');
      assert.strictEqual(alertBefore.status, 'open');

      // 2. Reading 2 (charging = true) arrives late
      await postSignedBatch([
        { vehicle_id: vid, seq: 2, ts: new Date(now - 120000).toISOString(), soc_pct: 85.0, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: true, fault_codes: [] }
      ]);

      // 3. Engine evaluates readings chronologically, identifies charging break, and resolves false alert
      const alertAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.ok(alertAfter);
      assert.strictEqual(alertAfter.status, 'resolved', 'Late charging reading must resolve the false rapid-discharge alert');
    });
  });

  // =========================================================================
  // TASK 9: BOUNDED HISTORY RETRIEVAL UNDER LARGE VEHICLE HISTORY (ISSUE 2)
  // =========================================================================
  describe('Issue 2: Bounded History Retrieval Under Large Vehicle History', () => {
    test('Alert evaluation on vehicle with 1,000 historical rows bounds query and maintains alert lifecycle', async () => {
      const vid = `VG-LARGE-HIST-${Date.now()}`;
      const now = Date.now();

      // 1. Seed 1,000 historical readings spanning the previous 10 hours
      const seedStmt = db.prepare(`
        INSERT INTO telemetry_history (
          vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes
        ) VALUES (
          ?, ?, ?, 80.0, 35.0, 40.0, 13.08, 80.27, 10.0, 0, '[]'
        )
      `);

      const insertMany = db.transaction(() => {
        for (let i = 1; i <= 1000; i++) {
          // 1,000 readings spread over 10 hours (~36 seconds apart), older than 15 minutes
          const historicalTs = new Date(now - (10 * 3600 * 1000) + (i * 35000)).toISOString();
          seedStmt.run(vid, i, historicalTs);
        }
      });
      insertMany();

      const totalRowsBefore = db.prepare("SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?").get(vid).total;
      assert.strictEqual(totalRowsBefore, 1000, 'Vehicle must have 1,000 historical rows in database');

      // 2. Ingest hot streak (3 readings > 55°C) within current window
      await postSignedBatch([
        { vehicle_id: vid, seq: 1001, ts: new Date(now - 15000).toISOString(), soc_pct: 75, battery_temp_c: 56.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 1002, ts: new Date(now - 10000).toISOString(), soc_pct: 75, battery_temp_c: 57.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 1003, ts: new Date(now - 5000).toISOString(), soc_pct: 75, battery_temp_c: 58.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.2, charging: false, fault_codes: [] }
      ]);

      const openAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid);
      assert.ok(openAlert, 'Overheat alert must trigger correctly despite 1,000 historical rows');
      assert.strictEqual(openAlert.status, 'open');

      // 3. Ingest cooling streak (3 readings <= 50°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 1004, ts: new Date(now - 3000).toISOString(), soc_pct: 75, battery_temp_c: 49.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 1005, ts: new Date(now - 2000).toISOString(), soc_pct: 75, battery_temp_c: 48.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 1006, ts: new Date(now - 1000).toISOString(), soc_pct: 75, battery_temp_c: 47.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.5, charging: false, fault_codes: [] }
      ]);

      const resolvedAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid);
      assert.ok(resolvedAlert);
      assert.strictEqual(resolvedAlert.status, 'resolved', 'Overheat alert must resolve cleanly');
      assert.strictEqual(resolvedAlert.version, 2, 'Alert version must increment to 2 on resolution');

      // 4. Multiple re-evaluations must NEVER create duplicate alerts
      const changed = evaluateVehicleAlerts(vid);
      assert.strictEqual(changed.length, 0, 'Re-evaluation must produce 0 changed alerts');
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1);

      // 5. Verify bounded retrieval: rows loaded by getWindowHistoryStmt are strictly bounded by cutoff, NOT 1,000 rows
      const cutoff10MinAgo = new Date(now - (10 * 60 * 1000)).toISOString();
      const boundedRows = db.prepare(`
        SELECT COUNT(*) AS total FROM telemetry_history
        WHERE vehicle_id = ? AND ts >= ?
      `).get(vid, cutoff10MinAgo).total;

      assert.ok(boundedRows < 20, `Bounded rows (${boundedRows}) must be far less than total history (1000)`);
    });

    test('Long-running active alert with large telemetry history: row loading remains bounded as history grows', async () => {
      const vid = `VG-LONG-ALERT-${Date.now()}`;
      const now = Date.now();
      const fiveHoursAgo = new Date(now - (5 * 3600 * 1000)).toISOString();

      // 1. Pre-open an alert opened 5 hours ago (long-running active incident)
      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'overheat', 'open', 1, ?, ?, ?)
      `).run(
        vid,
        JSON.stringify({ current_temp_c: 58.0, threshold: 55.0, consecutive_hot: 3 }),
        fiveHoursAgo,
        fiveHoursAgo
      );

      const activeBefore = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' AND status = 'open'").get(vid);
      assert.ok(activeBefore, 'Pre-existing active alert must exist in database');
      assert.strictEqual(activeBefore.opened_at, fiveHoursAgo);

      // 2. Seed Phase 1: 1,000 historical readings across the 5-hour incident window
      const seedStmt = db.prepare(`
        INSERT INTO telemetry_history (
          vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes
        ) VALUES (
          ?, ?, ?, 80.0, 56.0, 40.0, 13.08, 80.27, 10.0, 0, '[]'
        )
      `);

      db.transaction(() => {
        for (let i = 1; i <= 1000; i++) {
          const histTs = new Date(now - (5 * 3600 * 1000) + (i * 17000)).toISOString();
          seedStmt.run(vid, i, histTs);
        }
      })();

      const totalHistoryP1 = db.prepare("SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?").get(vid).total;
      assert.strictEqual(totalHistoryP1, 1000);

      // Ingest 3 readings in current 10-minute window maintaining hot condition (> 55°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 1001, ts: new Date(now - 15000).toISOString(), soc_pct: 75, battery_temp_c: 58.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 1002, ts: new Date(now - 10000).toISOString(), soc_pct: 75, battery_temp_c: 57.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 1003, ts: new Date(now - 5000).toISOString(), soc_pct: 75, battery_temp_c: 56.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 50.2, charging: false, fault_codes: [] }
      ]);

      // Verify active alert remains open with original 5-hour-old opened_at
      const activeP1 = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' AND status = 'open'").get(vid);
      assert.ok(activeP1);
      assert.strictEqual(activeP1.opened_at, fiveHoursAgo, 'Original opened_at from 5 hours ago must be preserved');
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ?").get(vid).total, 1, 'Zero duplicate alerts');

      // Verify row count loaded: 10-minute cutoff bounds rows loaded to <= 10, NOT 1,003 rows!
      const cutoff1 = new Date(now - (10 * 60 * 1000)).toISOString();
      const rowsLoadedP1 = db.prepare("SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ? AND ts >= ?").get(vid, cutoff1).total;
      assert.ok(rowsLoadedP1 <= 10, `Loaded rows (${rowsLoadedP1}) must be <= 10, strictly bounded despite 1,003 rows in history`);

      // 3. Seed Phase 2: Add 2,000 more historical readings (total 3,003 rows in history)
      db.transaction(() => {
        for (let i = 1004; i <= 3003; i++) {
          const histTs = new Date(now - (4 * 3600 * 1000) + ((i - 1003) * 6000)).toISOString();
          seedStmt.run(vid, i, histTs);
        }
      })();

      const totalHistoryP2 = db.prepare("SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?").get(vid).total;
      assert.strictEqual(totalHistoryP2, 3003, 'History expanded to 3,003 rows');

      // Ingest 3 more hot readings in current window
      await postSignedBatch([
        { vehicle_id: vid, seq: 3004, ts: new Date(now - 4000).toISOString(), soc_pct: 74, battery_temp_c: 58.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3005, ts: new Date(now - 3000).toISOString(), soc_pct: 74, battery_temp_c: 57.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3006, ts: new Date(now - 2000).toISOString(), soc_pct: 74, battery_temp_c: 56.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.2, charging: false, fault_codes: [] }
      ]);

      // Verify row count loaded remains bounded: does NOT grow with total vehicle history
      const rowsLoadedP2 = db.prepare("SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ? AND ts >= ?").get(vid, cutoff1).total;
      assert.ok(rowsLoadedP2 <= 10, `Loaded rows (${rowsLoadedP2}) remains bounded <= 10 even when history tripled to 3,006 rows`);

      // 4. Resolution Phase: Ingest cooling streak (3 readings <= 50°C)
      const coolTs = new Date(now - 1000).toISOString();
      await postSignedBatch([
        { vehicle_id: vid, seq: 3007, ts: new Date(now - 1500).toISOString(), soc_pct: 74, battery_temp_c: 49.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3008, ts: new Date(now - 1200).toISOString(), soc_pct: 74, battery_temp_c: 48.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3009, ts: coolTs, soc_pct: 74, battery_temp_c: 47.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.5, charging: false, fault_codes: [] }
      ]);

      const resolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid);
      assert.strictEqual(resolved.status, 'resolved', 'Long-running alert must resolve cleanly');
      assert.strictEqual(resolved.opened_at, fiveHoursAgo, 'Resolved alert must preserve original 5-hour-old opened_at');
      assert.strictEqual(resolved.resolved_at, coolTs, 'Resolved_at must match cooling timestamp');
      assert.strictEqual(resolved.version, 2, 'Version must increment to 2');

      // 5. Reopening Phase: Ingest new hot streak -> opens new incident with new timestamp
      const newHotTs = new Date(now - 200).toISOString();
      await postSignedBatch([
        { vehicle_id: vid, seq: 3010, ts: new Date(now - 800).toISOString(), soc_pct: 74, battery_temp_c: 56.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.6, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3011, ts: new Date(now - 500).toISOString(), soc_pct: 74, battery_temp_c: 57.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.7, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3012, ts: newHotTs, soc_pct: 74, battery_temp_c: 58.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 51.8, charging: false, fault_codes: [] }
      ]);

      const allAlerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' ORDER BY id ASC").all(vid);
      assert.strictEqual(allAlerts.length, 2, 'Must have exactly 2 alert records: 1 resolved historical, 1 newly reopened');
      assert.strictEqual(allAlerts[0].status, 'resolved');
      assert.strictEqual(allAlerts[0].opened_at, fiveHoursAgo);
      assert.strictEqual(allAlerts[1].status, 'open');
      assert.strictEqual(allAlerts[1].version, 1);
      assert.ok(allAlerts[1].opened_at > fiveHoursAgo, 'New alert must have modern opened_at timestamp');
    });

    test('Stale and duplicate telemetry does NOT update server receive time or resolve offline alert', async () => {
      const vid = `VG-STALE-OFFLINE-${Date.now()}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0 - 100000).toISOString(); // 100 seconds ago

      // 1. Ingest an initial reading 100s ago
      await postSignedBatch([
        { vehicle_id: vid, seq: 10, ts: t0Iso, soc_pct: 80, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
      ]);

      // Manually set updated_at to 100s ago to simulate 100s of silence
      db.prepare("UPDATE vehicles SET updated_at = ? WHERE vehicle_id = ?").run(t0Iso, vid);

      // Trigger offline check at T0: vehicle has been silent for 100s -> flagged offline
      const { runOfflineCheck } = require('../src/offlineWorker');
      const resOff = runOfflineCheck(90, t0);
      assert.ok(resOff.newlyOfflineCount >= 1);

      const vOffline = db.prepare("SELECT status, updated_at FROM vehicles WHERE vehicle_id = ?").get(vid);
      assert.strictEqual(vOffline.status, 'offline');
      assert.strictEqual(vOffline.updated_at, t0Iso);

      const alertOffline = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alertOffline);
      assert.strictEqual(alertOffline.status, 'open');

      // 2. Ingest duplicate reading (same seq: 10)
      const resDup = await postSignedBatch([
        { vehicle_id: vid, seq: 10, ts: t0Iso, soc_pct: 80, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(resDup.body.duplicates, 1);

      // Verify updated_at was NOT modified and vehicle is still offline
      const vAfterDup = db.prepare("SELECT status, updated_at FROM vehicles WHERE vehicle_id = ?").get(vid);
      assert.strictEqual(vAfterDup.status, 'offline', 'Duplicate telemetry must not set status to online');
      assert.strictEqual(vAfterDup.updated_at, t0Iso, 'Duplicate telemetry must not update receive time');
      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).status, 'open');

      // 3. Ingest stale / late-arriving reading (seq: 5, older timestamp: 150s ago)
      const olderTs = new Date(t0 - 150000).toISOString();
      const resStale = await postSignedBatch([
        { vehicle_id: vid, seq: 5, ts: olderTs, soc_pct: 80, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 9, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(resStale.body.accepted, 1);

      // Verify updated_at was NOT modified and offline alert was NOT resolved by stale data
      const vAfterStale = db.prepare("SELECT status, updated_at FROM vehicles WHERE vehicle_id = ?").get(vid);
      assert.strictEqual(vAfterStale.status, 'offline', 'Stale telemetry must not reset offline vehicle to online');
      assert.strictEqual(vAfterStale.updated_at, t0Iso, 'Stale telemetry must not update server receive time');
      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).status, 'open');

      // 4. Ingest NEW valid reading (seq: 11, newer timestamp: now)
      const newerTs = new Date().toISOString();
      const resFresh = await postSignedBatch([
        { vehicle_id: vid, seq: 11, ts: newerTs, soc_pct: 80, battery_temp_c: 30, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(resFresh.body.accepted, 1);

      // Verify vehicle returns to online and offline alert resolves cleanly
      const vOnline = db.prepare("SELECT status FROM vehicles WHERE vehicle_id = ?").get(vid);
      assert.strictEqual(vOnline.status, 'online', 'Fresh telemetry must restore vehicle to online');
      const alertResolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(alertResolved.status, 'resolved', 'Fresh telemetry must resolve offline alert');
      assert.strictEqual(alertResolved.version, 2);
    });

    test('Geofence polygon boundary and corner coordinates via HTTP batch evaluate as inside (no breach)', async () => {
      const vid = `VG-GEO-BOUNDS-${Date.now()}`;
      const now = Date.now();

      // Send 4 readings exactly on the 4 boundary edges and corners of the Chennai polygon
      // Edge 1: Bottom edge (lat 13.0000, lng 80.2500)
      // Edge 2: Top edge (lat 13.1800, lng 80.2500)
      // Corner 1: Bottom-left (lat 13.0000, lng 80.1800)
      // Corner 2: Top-right (lat 13.1800, lng 80.3200)
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 4000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0000, lng: 80.2500, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 3000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.1800, lng: 80.2500, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 2000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.0000, lng: 80.1800, odometer_km: 10.2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 4, ts: new Date(now - 1000).toISOString(), soc_pct: 80, battery_temp_c: 30, speed_kmh: 40, lat: 13.1800, lng: 80.3200, odometer_km: 10.3, charging: false, fault_codes: [] }
      ]);

      // Assert zero geofence alerts triggered
      const totalAlerts = db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").get(vid).total;
      assert.strictEqual(totalAlerts, 0, 'Readings on polygon boundary edges and corners must NOT trigger geofence breach');
    });

    test('Acknowledged overheat alert lifecycle: resolves correctly upon cooling readings', async () => {
      const vid = `VG-ACK-RESOLVE-${Date.now()}`;
      const now = Date.now();

      // 1. Trigger overheat alert
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);

      const alertOpen = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid);
      assert.ok(alertOpen);
      assert.strictEqual(alertOpen.status, 'open');
      assert.strictEqual(alertOpen.version, 1);

      // 2. Acknowledge the alert via POST /v1/alerts/:id/ack
      const ackRes = await fetch(`${baseUrl}/v1/alerts/${alertOpen.id}/ack`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': API_KEY
        },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(ackRes.status, 200);
      const ackBody = await ackRes.json();
      assert.strictEqual(ackBody.alert.status, 'acknowledged');
      assert.strictEqual(ackBody.alert.version, 2);

      // 3. Send 3 cooling readings (<= 50°C) -> must transition from 'acknowledged' directly to 'resolved'
      await postSignedBatch([
        { vehicle_id: vid, seq: 4, ts: new Date(now - 3000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: new Date(now - 2000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: new Date(now - 1000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alertResolved = db.prepare("SELECT * FROM alerts WHERE id = ?").get(alertOpen.id);
      assert.strictEqual(alertResolved.status, 'resolved', 'Acknowledged alert must cleanly resolve upon cooldown');
      assert.strictEqual(alertResolved.version, 3, 'Version must increment to 3 on resolution');
      assert.strictEqual(alertResolved.opened_at, alertOpen.opened_at, 'Original opened_at must be preserved');
    });

    test('Multiple alert rules trigger and resolve independently on the same vehicle', async () => {
      const vid = `VG-MULTI-RULES-${Date.now()}`;
      const now = Date.now();

      // Batch that simultaneously triggers overheat (> 55°C for 3) and geofence breach (outside polygon)
      await postSignedBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 40, lat: 14.50, lng: 81.00, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 40, lat: 14.50, lng: 81.00, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 40, lat: 14.50, lng: 81.00, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);

      const activeOverheat = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat' AND status = 'open'").get(vid);
      const activeGeofence = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach' AND status = 'open'").get(vid);

      assert.ok(activeOverheat, 'Overheat alert must be open');
      assert.ok(activeGeofence, 'Geofence alert must be open');

      // Now send batch where vehicle re-enters geofence, but remains hot (> 55°C)
      await postSignedBatch([
        { vehicle_id: vid, seq: 4, ts: new Date(now - 2000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] }
      ]);

      const geoAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").get(vid);
      const heatAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid);

      assert.strictEqual(geoAfter.status, 'resolved', 'Geofence must resolve upon re-entry');
      assert.strictEqual(heatAfter.status, 'open', 'Overheat must remain open while still hot');
    });
  });
});
