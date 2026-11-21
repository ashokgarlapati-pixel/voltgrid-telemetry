const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary database for alert engine unit tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-unit-alerts-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = process.env.NODE_ENV || 'test';
process.env.DB_PATH = testDbPath;

const db = require('../src/db');
const {
  isPointInPolygon,
  evaluateOverheat,
  evaluateRapidDischarge,
  evaluateGeofenceBreach,
  evaluateVehicleAlerts,
  CHENNAI_OPERATING_POLYGON
} = require('../src/alertEngine');
const { runOfflineCheck } = require('../src/offlineWorker');

describe('Part A3 Alert Engine Unit Tests', () => {

  after(() => {
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  describe('Rule 1: Overheat with Hysteresis & 3-Streak Logic', () => {
    test('2 consecutive readings > 55°C does NOT trigger overheat alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.5 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false, 'Should not trigger with only 2 hot readings');
      assert.strictEqual(outcome.resolved, false);
    });

    test('3 consecutive readings > 55°C triggers overheat alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.5 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.2 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, true, 'Should trigger with 3 consecutive hot readings');
      assert.strictEqual(outcome.resolved, false);
      assert.strictEqual(outcome.details.current_temp_c, 58.2);
    });

    test('Hysteresis: temperatures between 50°C and 55°C maintain current triggered state', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.5 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.2 },
        // Hysteresis reading: 53°C should maintain triggered state
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 53.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, true, 'Hysteresis temperature (53°C) must maintain triggered state');
      assert.strictEqual(outcome.resolved, false);
    });

    test('Hysteresis: temperatures between 50°C and 55°C after normal readings stay normal', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 45.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 52.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 53.5 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false, 'Hysteresis temperature after normal must not trigger');
      assert.strictEqual(outcome.resolved, false);
    });

    test('3 consecutive readings <= 50°C resolves overheat alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.5 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.2 },
        // 3 consecutive cool readings <= 50°C
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 49.0 },
        { seq: 5, ts: '2026-11-20T10:00:20Z', battery_temp_c: 48.5 },
        { seq: 6, ts: '2026-11-20T10:00:25Z', battery_temp_c: 47.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true, 'Should resolve after 3 consecutive readings <= 50°C');
    });

    test('4th reading > 55°C maintains triggered state without duplicate trigger', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.5 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.2 },
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 59.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, true);
      assert.strictEqual(outcome.resolved, false);
    });
  });

  describe('Rule 2: Rapid Discharge (>8% drop in 5 min while discharging)', () => {
    test('>8% SoC drop within 5 min while charging = false triggers alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 75.0, charging: false },
        { seq: 3, ts: '2026-11-20T10:04:00Z', soc_pct: 70.0, charging: false } // Drop: 10 points (> 8)
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, true, 'Should trigger when SoC drops > 8% while discharging');
      assert.strictEqual(outcome.resolved, false);
      assert.strictEqual(outcome.details.drop_points, 10.0);
    });

    test('Same drop (>8%) while charging = true does NOT trigger alert (resolves instead)', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: true },
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 75.0, charging: true },
        { seq: 3, ts: '2026-11-20T10:04:00Z', soc_pct: 70.0, charging: true }
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false, 'Charging vehicle must never trigger rapid discharge');
      assert.strictEqual(outcome.resolved, true, 'Charging resolves rapid discharge');
    });

    test('Drop returning <= 8% resolves rapid discharge alert', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:04:00Z', soc_pct: 70.0, charging: false }, // Drop 10
        // Later readings where 5-min window has <= 8 drop
        { seq: 3, ts: '2026-11-20T10:10:00Z', soc_pct: 69.0, charging: false },
        { seq: 4, ts: '2026-11-20T10:12:00Z', soc_pct: 68.0, charging: false } // Drop across 10:10-10:12 is 1 point
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true, 'Should resolve when 5-min drop returns <= 8 points');
    });

    test('Insufficient history for new vehicle with 1 reading does not trigger or resolve', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false }
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, false);
    });

    test('5-minute window expiration with active alert resolves cleanly', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        // Reading 10 minutes later (only 1 reading in the new 5-minute window)
        { seq: 2, ts: '2026-11-20T10:10:00Z', soc_pct: 79.5, charging: false }
      ];
      const outcome = evaluateRapidDischarge(readings, { id: 1, status: 'open' });
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true, 'Window expiration must resolve active alert');
    });

    test('Discharge within one continuous non-charging period (>8% drop triggers)', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 85.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 76.5, charging: false },
        { seq: 3, ts: '2026-11-20T10:04:00Z', soc_pct: 76.0, charging: false } // Drop: 9.0 points (> 8.0)
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, true, 'Discharge > 8% in continuous non-charging period must trigger');
      assert.strictEqual(outcome.resolved, false);
      assert.strictEqual(outcome.details.drop_points, 9.0);
    });

    test('Charging occurring between two non-charging periods: no false rapid-discharge alert', () => {
      // Example: 90% non-charging -> 85% charging -> 85% non-charging -> 80% non-charging
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 90.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:01:00Z', soc_pct: 85.0, charging: true },  // Charging break!
        { seq: 3, ts: '2026-11-20T10:02:00Z', soc_pct: 85.0, charging: false },
        { seq: 4, ts: '2026-11-20T10:03:00Z', soc_pct: 80.0, charging: false }  // Drop in new period is only 5.0 (85 - 80)
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false, 'Must NOT compare 90% with 80% across charging break');
      assert.strictEqual(outcome.resolved, true, 'Must evaluate to resolved (drop is 5.0 <= 8.0)');
      assert.strictEqual(outcome.details.drop_points, 5.0);
    });

    test('Charging reading followed by a new non-charging period: drop only evaluated in new period', () => {
      // In the new period, drop exceeds 8% (85% -> 75% = 10% drop)
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 95.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:01:00Z', soc_pct: 90.0, charging: true },  // Charging break!
        { seq: 3, ts: '2026-11-20T10:02:00Z', soc_pct: 85.0, charging: false }, // New period start
        { seq: 4, ts: '2026-11-20T10:04:00Z', soc_pct: 75.0, charging: false }  // Drop in new period is 10.0 (> 8.0)
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, true, 'Drop > 8% within new continuous period must trigger alert');
      assert.strictEqual(outcome.details.window_max_soc, 85.0, 'Window max must be from new period (85.0), not pre-charging (95.0)');
      assert.strictEqual(outcome.details.drop_points, 10.0);
    });

    test('Out-of-order arrival of readings across charging transitions', () => {
      // Shuffled readings: Reading 2 (charging) arrives out of order
      const shuffled = [
        { seq: 4, ts: '2026-11-20T10:03:00Z', soc_pct: 80.0, charging: false },
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 90.0, charging: false },
        { seq: 3, ts: '2026-11-20T10:02:00Z', soc_pct: 85.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:01:00Z', soc_pct: 85.0, charging: true }   // Intermediate charging reading
      ];
      const outcome = evaluateRapidDischarge(shuffled);
      assert.strictEqual(outcome.triggered, false, 'Chronological sort must identify intermediate charging reading');
      assert.strictEqual(outcome.details.drop_points, 5.0, 'Drop must be calculated strictly from post-charging period');
    });

    test('Duplicate readings in continuous non-charging period do not trigger false alerts', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false },
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 80.0, charging: false }, // Duplicate
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 78.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:02:00Z', soc_pct: 78.0, charging: false }  // Duplicate (drop is 2.0 <= 8.0)
      ];
      const outcome = evaluateRapidDischarge(readings);
      assert.strictEqual(outcome.triggered, false, 'Duplicates must not trigger rapid discharge');
      assert.strictEqual(outcome.details.drop_points, 2.0);
    });
  });

  describe('Rule 3: Geofence Breach (Operating Polygon)', () => {
    test('Point inside polygon evaluates as inside', () => {
      // Center of Chennai operating polygon (13.09, 80.25)
      const inside = isPointInPolygon(13.0900, 80.2500, CHENNAI_OPERATING_POLYGON);
      assert.strictEqual(inside, true, 'Chennai city center must be inside polygon');
    });

    test('Point outside polygon triggers geofence_breach', () => {
      // Far outside polygon (e.g. lat 14.50, lng 81.00)
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 14.5000, lng: 81.0000 }
      ];
      const outcome = evaluateGeofenceBreach(readings);
      assert.strictEqual(outcome.triggered, true, 'Outside coordinates must trigger geofence breach');
      assert.strictEqual(outcome.resolved, false);
    });

    test('Point re-entering polygon resolves geofence_breach', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 14.5000, lng: 81.0000 }, // Outside
        { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 13.0900, lng: 80.2500 }  // Back inside
      ];
      const outcome = evaluateGeofenceBreach(readings);
      assert.strictEqual(outcome.triggered, false);
      assert.strictEqual(outcome.resolved, true, 'Re-entering polygon must resolve breach');
    });
  });

  describe('Rule 4: Offline Detection Logic (Clock Injection)', () => {
    test('runOfflineCheck with injectable clock accurately flags vehicles', () => {
      const vid = `VG-UNIT-OFFLINE-${Date.now()}`;
      const now = Date.now();

      // Seed vehicle online at T0
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (
          ?, '2026-11-20T10:00:00Z', 1, 80, 35, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?
        )
      `).run(vid, new Date(now).toISOString());

      // At T0 + 30s: vehicle is not stale (< 90s)
      const res30s = runOfflineCheck(90, now + 30000);
      const v30s = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(v30s.status, 'online');

      // At T0 + 95s: vehicle is stale (> 90s)
      const res95s = runOfflineCheck(90, now + 95000);
      const v95s = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(v95s.status, 'offline');

      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alert);
      assert.strictEqual(alert.status, 'open');
    });
  });

  describe('Order-Independence Verification', () => {
    test('Shuffled arrival order produces identical alert state to chronological order', () => {
      const chronological = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.0 }
      ];

      // Replay in reverse arrival order
      const reversed = [...chronological].reverse();

      // Ensure chronological sort produces identical result
      const sortedReversed = [...reversed].sort((a, b) => Date.parse(a.ts) - Date.parse(b.ts));
      const resChronological = evaluateOverheat(chronological);
      const resReversedSorted = evaluateOverheat(sortedReversed);

      assert.strictEqual(resChronological.triggered, true);
      assert.strictEqual(resReversedSorted.triggered, true);
      assert.strictEqual(resChronological.details.current_temp_c, resReversedSorted.details.current_temp_c);
    });
  });
});
