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

    test('Normal reading between hot readings breaks consecutive sequence (no alert)', () => {
      // Streak interrupted: 56°C, 57°C, 45°C (normal breaks streak!), 56°C, 57°C -> never reaches 3 consecutive
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 45.0 }, // Normal reading breaks streak!
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 56.5 },
        { seq: 5, ts: '2026-11-20T10:00:20Z', battery_temp_c: 57.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.triggered, false, 'Normal reading must reset hot streak counter');
      assert.strictEqual(outcome.resolved, false);
    });

    test('Readings between 50°C and 55°C must NOT resolve an active alert (hysteresis)', () => {
      const readings = [
        // Already active alert triggered in past
        { seq: 4, ts: '2026-11-20T10:00:15Z', battery_temp_c: 52.0 },
        { seq: 5, ts: '2026-11-20T10:00:20Z', battery_temp_c: 51.5 },
        { seq: 6, ts: '2026-11-20T10:00:25Z', battery_temp_c: 53.0 }
      ];
      const activeAlert = { id: 1, opened_at: '2026-11-20T10:00:00Z', status: 'open' };
      const outcome = evaluateOverheat(readings, activeAlert);
      assert.strictEqual(outcome.triggered, true, 'Alert must remain triggered in hysteresis band');
      assert.strictEqual(outcome.resolved, false, 'Hysteresis temperatures must NOT resolve active alert');
    });

    test('New overheating episode after resolution creates a new distinct alert', () => {
      const readings = [
        // Incident 1: 3 hot -> triggers
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.0 },
        // Cooldown: 3 cool -> resolves
        { seq: 4, ts: '2026-11-20T10:01:00Z', battery_temp_c: 49.0 },
        { seq: 5, ts: '2026-11-20T10:01:05Z', battery_temp_c: 48.0 },
        { seq: 6, ts: '2026-11-20T10:01:10Z', battery_temp_c: 47.0 },
        // Incident 2: 3 hot -> new episode
        { seq: 7, ts: '2026-11-20T10:05:00Z', battery_temp_c: 56.5 },
        { seq: 8, ts: '2026-11-20T10:05:05Z', battery_temp_c: 57.5 },
        { seq: 9, ts: '2026-11-20T10:05:10Z', battery_temp_c: 59.0 }
      ];
      const outcome = evaluateOverheat(readings);
      assert.strictEqual(outcome.episodes.length, 2, 'Must record 2 discrete episodes');
      assert.strictEqual(outcome.episodes[0].status, 'resolved');
      assert.strictEqual(outcome.episodes[0].openedAt, '2026-11-20T10:00:10Z');
      assert.strictEqual(outcome.episodes[0].resolvedAt, '2026-11-20T10:01:10Z');
      assert.strictEqual(outcome.episodes[1].status, 'open');
      assert.strictEqual(outcome.episodes[1].openedAt, '2026-11-20T10:05:10Z');
    });

    test('Acknowledged alerts retain correct lifecycle behavior during resolution', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 49.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 48.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 47.0 }
      ];
      const acknowledgedAlert = { id: 42, status: 'acknowledged', opened_at: '2026-11-20T09:50:00Z' };
      const outcome = evaluateOverheat(readings, acknowledgedAlert);
      assert.strictEqual(outcome.resolved, true, 'Cool readings must resolve acknowledged alert');
      assert.strictEqual(outcome.openedAt, '2026-11-20T09:50:00Z', 'Original opened_at must be preserved');
      assert.strictEqual(outcome.resolvedAt, '2026-11-20T10:00:10Z');
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

    test('Each of the four polygon edges evaluates as inside (non-breaching)', () => {
      // Bottom edge: lat 13.0000, lng 80.2500
      assert.strictEqual(isPointInPolygon(13.0000, 80.2500), true, 'Bottom edge must be inside');
      // Top edge: lat 13.1800, lng 80.2500
      assert.strictEqual(isPointInPolygon(13.1800, 80.2500), true, 'Top edge must be inside');
      // Left edge: lat 13.0900, lng 80.1800
      assert.strictEqual(isPointInPolygon(13.0900, 80.1800), true, 'Left edge must be inside');
      // Right edge: lat 13.0900, lng 80.3200
      assert.strictEqual(isPointInPolygon(13.0900, 80.3200), true, 'Right edge must be inside');
    });

    test('All four polygon corners evaluate as inside (non-breaching)', () => {
      // Bottom-left corner (13.0000, 80.1800)
      assert.strictEqual(isPointInPolygon(13.0000, 80.1800), true, 'Bottom-left corner must be inside');
      // Top-left corner (13.1800, 80.1800)
      assert.strictEqual(isPointInPolygon(13.1800, 80.1800), true, 'Top-left corner must be inside');
      // Top-right corner (13.1800, 80.3200)
      assert.strictEqual(isPointInPolygon(13.1800, 80.3200), true, 'Top-right corner must be inside');
      // Bottom-right corner (13.0000, 80.3200)
      assert.strictEqual(isPointInPolygon(13.0000, 80.3200), true, 'Bottom-right corner must be inside');
    });

    test('Point just inside boundary evaluates as inside', () => {
      // Just inside bottom-left
      assert.strictEqual(isPointInPolygon(13.0001, 80.1801), true, 'Point just inside boundary must evaluate as inside');
    });

    test('Point just outside boundary triggers geofence breach', () => {
      // Just outside bottom-left
      assert.strictEqual(isPointInPolygon(12.9999, 80.1799), false, 'Point just outside boundary must evaluate as outside');
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 12.9999, lng: 80.1799 }
      ];
      const outcome = evaluateGeofenceBreach(readings);
      assert.strictEqual(outcome.triggered, true, 'Point just outside boundary must trigger geofence breach');
    });

    test('Vehicle remaining outside across multiple readings maintains single open episode', () => {
      const readings = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 14.5000, lng: 81.0000 },
        { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 14.5100, lng: 81.0100 },
        { seq: 3, ts: '2026-11-20T10:02:00Z', lat: 14.5200, lng: 81.0200 }
      ];
      const outcome = evaluateGeofenceBreach(readings);
      assert.strictEqual(outcome.triggered, true);
      assert.strictEqual(outcome.episodes.length, 1, 'Continuous breach must produce exactly 1 episode');
      assert.strictEqual(outcome.episodes[0].status, 'open');
      assert.strictEqual(outcome.episodes[0].openedAt, '2026-11-20T10:00:00Z');
    });

    test('Multiple separate breach episodes tracked chronologically', () => {
      const readings = [
        // Breach 1: 10:00 -> 10:01
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 14.5000, lng: 81.0000 }, // outside
        { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 13.0900, lng: 80.2500 }, // inside (resolves 1)
        // Breach 2: 10:05 -> 10:06
        { seq: 3, ts: '2026-11-20T10:05:00Z', lat: 14.6000, lng: 81.1000 }, // outside (opens 2)
        { seq: 4, ts: '2026-11-20T10:06:00Z', lat: 13.0900, lng: 80.2500 }  // inside (resolves 2)
      ];
      const outcome = evaluateGeofenceBreach(readings);
      assert.strictEqual(outcome.episodes.length, 2, 'Must record 2 discrete resolved episodes');
      assert.strictEqual(outcome.episodes[0].status, 'resolved');
      assert.strictEqual(outcome.episodes[0].openedAt, '2026-11-20T10:00:00Z');
      assert.strictEqual(outcome.episodes[0].resolvedAt, '2026-11-20T10:01:00Z');
      assert.strictEqual(outcome.episodes[1].status, 'resolved');
      assert.strictEqual(outcome.episodes[1].openedAt, '2026-11-20T10:05:00Z');
      assert.strictEqual(outcome.episodes[1].resolvedAt, '2026-11-20T10:06:00Z');
    });

    test('Out-of-order and duplicate coordinates produce correct geofence state', () => {
      const chronological = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 13.0900, lng: 80.2500 }, // inside
        { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 14.5000, lng: 81.0000 }, // outside (triggers)
        { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 14.5000, lng: 81.0000 }  // duplicate
      ];
      // Reverse arrival
      const reversed = [...chronological].reverse();
      const outChron = evaluateGeofenceBreach(chronological);
      const outRev = evaluateGeofenceBreach(reversed);

      assert.strictEqual(outChron.triggered, true);
      assert.strictEqual(outRev.triggered, true);
      assert.strictEqual(outChron.openedAt, outRev.openedAt);
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

    test('No alert before 90 seconds, alert triggered at exact 90-second boundary', () => {
      const vid = `VG-OFFLINE-BOUNDARY-${Date.now()}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vid, t0Iso, t0Iso);

      // At 89.9 seconds: silence < 90s -> no offline alert
      const res89 = runOfflineCheck(90, t0 + 89900);
      assert.strictEqual(res89.newlyOfflineCount, 0);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).total, 0);

      // At 90.0 seconds: silence >= 90s -> triggers offline alert
      const res90 = runOfflineCheck(90, t0 + 90000);
      assert.strictEqual(res90.newlyOfflineCount, 1);
      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alert);
      assert.strictEqual(alert.status, 'open');
    });

    test('Repeated scheduler execution does NOT duplicate active offline alerts', () => {
      const vid = `VG-OFFLINE-REP-${Date.now()}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vid, t0Iso, t0Iso);

      // 1st run at 100s: triggers alert
      runOfflineCheck(90, t0 + 100000);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).total, 1);

      // 2nd, 3rd, 4th runs at 110s, 120s, 130s: must NOT create duplicate alerts
      runOfflineCheck(90, t0 + 110000);
      runOfflineCheck(90, t0 + 120000);
      runOfflineCheck(90, t0 + 130000);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).total, 1);
    });

    test('Multiple vehicles becoming offline independently are handled concurrently', () => {
      const v1 = `VG-MULTI-OFF-1-${Date.now()}`;
      const v2 = `VG-MULTI-OFF-2-${Date.now()}`;
      const t0 = Date.now();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES 
          (?, '2026-11-20T10:00:00Z', 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?),
          (?, '2026-11-20T10:00:00Z', 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(v1, new Date(t0).toISOString(), v2, new Date(t0).toISOString());

      const res = runOfflineCheck(90, t0 + 95000);
      assert.strictEqual(res.newlyOfflineCount, 2, 'Both silent vehicles must transition to offline');

      const a1 = db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(v1);
      const a2 = db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(v2);
      assert.strictEqual(a1.status, 'open');
      assert.strictEqual(a2.status, 'open');
    });

    test('Server restart recovery: vehicles already offline without alert are detected and flagged', () => {
      const vid = `VG-RESTART-OFF-${Date.now()}`;
      const t0 = Date.now();

      // Seed a vehicle that was marked offline before restart, but has no active alert
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, new Date(t0 - 120000).toISOString());

      // On startup / check: vehicle without active alert must be detected and alert opened
      const res = runOfflineCheck(90, t0);
      assert.ok(res.newlyOfflineCount >= 1);
      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alert, 'Alert must be opened for offline vehicle missing alert after restart');
      assert.strictEqual(alert.status, 'open');

      // Subsequent check must not duplicate it
      const res2 = runOfflineCheck(90, t0 + 10000);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).total, 1);
    });

    test('Concurrent scheduler executions run safely via transaction locks', () => {
      const vid = `VG-CONCURR-OFF-${Date.now()}`;
      const t0 = Date.now();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, '2026-11-20T10:00:00Z', 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vid, new Date(t0).toISOString());

      // Execute 3 concurrent checks simultaneously at T0 + 100s
      const r1 = runOfflineCheck(90, t0 + 100000);
      const r2 = runOfflineCheck(90, t0 + 100000);
      const r3 = runOfflineCheck(90, t0 + 100000);

      // Exactly 1 newly offline alert created across all concurrent invocations
      assert.strictEqual(r1.newlyOfflineCount + r2.newlyOfflineCount + r3.newlyOfflineCount, 1);
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).total, 1);
    });
  });

  describe('Order-Independence Verification', () => {
    test('Overheat: Chronological, reverse, and randomized arrival orders produce identical results', () => {
      const chronological = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', battery_temp_c: 56.0 },
        { seq: 2, ts: '2026-11-20T10:00:05Z', battery_temp_c: 57.0 },
        { seq: 3, ts: '2026-11-20T10:00:10Z', battery_temp_c: 58.0 },
        { seq: 4, ts: '2026-11-20T10:01:00Z', battery_temp_c: 49.0 },
        { seq: 5, ts: '2026-11-20T10:01:05Z', battery_temp_c: 48.0 },
        { seq: 6, ts: '2026-11-20T10:01:10Z', battery_temp_c: 47.0 }
      ];

      const reversed = [...chronological].reverse();
      const randomized = [chronological[2], chronological[0], chronological[5], chronological[1], chronological[4], chronological[3]];

      const resChron = evaluateOverheat(chronological);
      const resRev = evaluateOverheat(reversed);
      const resRand = evaluateOverheat(randomized);

      assert.deepStrictEqual(resChron, resRev, 'Chronological and reverse arrival must produce identical overheat outcome');
      assert.deepStrictEqual(resChron, resRand, 'Chronological and randomized arrival must produce identical overheat outcome');
    });

    test('Rapid Discharge: Chronological, reverse, and randomized arrival orders produce identical results', () => {
      const chronological = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', soc_pct: 90.0, charging: false },
        { seq: 2, ts: '2026-11-20T10:01:00Z', soc_pct: 85.0, charging: true },
        { seq: 3, ts: '2026-11-20T10:02:00Z', soc_pct: 85.0, charging: false },
        { seq: 4, ts: '2026-11-20T10:03:00Z', soc_pct: 75.0, charging: false } // Drop: 10 in new continuous period
      ];

      const reversed = [...chronological].reverse();
      const randomized = [chronological[3], chronological[0], chronological[2], chronological[1]];

      const resChron = evaluateRapidDischarge(chronological);
      const resRev = evaluateRapidDischarge(reversed);
      const resRand = evaluateRapidDischarge(randomized);

      assert.deepStrictEqual(resChron, resRev, 'Chronological and reverse arrival must produce identical discharge outcome');
      assert.deepStrictEqual(resChron, resRand, 'Chronological and randomized arrival must produce identical discharge outcome');
    });

    test('Geofence: Chronological, reverse, and randomized arrival orders produce identical results', () => {
      const chronological = [
        { seq: 1, ts: '2026-11-20T10:00:00Z', lat: 13.0900, lng: 80.2500 }, // inside
        { seq: 2, ts: '2026-11-20T10:01:00Z', lat: 14.5000, lng: 81.0000 }, // outside
        { seq: 3, ts: '2026-11-20T10:02:00Z', lat: 13.0900, lng: 80.2500 }  // inside
      ];

      const reversed = [...chronological].reverse();
      const randomized = [chronological[1], chronological[2], chronological[0]];

      const resChron = evaluateGeofenceBreach(chronological);
      const resRev = evaluateGeofenceBreach(reversed);
      const resRand = evaluateGeofenceBreach(randomized);

      assert.deepStrictEqual(resChron, resRev, 'Chronological and reverse arrival must produce identical geofence outcome');
      assert.deepStrictEqual(resChron, resRand, 'Chronological and randomized arrival must produce identical geofence outcome');
    });
  });
});
