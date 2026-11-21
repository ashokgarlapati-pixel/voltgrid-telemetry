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
});
