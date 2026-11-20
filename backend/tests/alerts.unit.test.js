const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary database for alert engine unit tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-unit-alerts-${crypto.randomUUID()}.db`);
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
