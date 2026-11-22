const { test, describe, before, after, beforeEach, afterEach } = require('node:test');
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
  isPointOnSegment,
  evaluateOverheat,
  evaluateRapidDischarge,
  evaluateGeofenceBreach,
  evaluateVehicleAlerts,
  getVehicleHistoryForEvaluation,
  getWindowHistoryStmt,
  DEFAULT_MAX_HISTORY_ROWS,
  CHENNAI_OPERATING_POLYGON
} = require('../src/alertEngine');
const {
  runOfflineCheck,
  startOfflineWorker,
  stopOfflineWorker,
  isWorkerRunning
} = require('../src/offlineWorker');
const { ingestBatch } = require('../src/ingestion');

describe('Part A3 Alert Engine Unit Tests', () => {

  after(() => {
    try {
      stopOfflineWorker();
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

    test('Explicit isPointOnSegment tests for horizontal, vertical, and corner boundary segments', () => {
      const p1 = { lat: 13.0000, lng: 80.1800 }; // Bottom-left
      const p2 = { lat: 13.1800, lng: 80.1800 }; // Top-left
      const p3 = { lat: 13.1800, lng: 80.3200 }; // Top-right
      const p4 = { lat: 13.0000, lng: 80.3200 }; // Bottom-right

      // Vertical left edge
      assert.strictEqual(isPointOnSegment(13.0900, 80.1800, p1, p2), true, 'Midpoint on vertical left edge');
      // Horizontal top edge
      assert.strictEqual(isPointOnSegment(13.1800, 80.2500, p2, p3), true, 'Midpoint on horizontal top edge');
      // Vertical right edge
      assert.strictEqual(isPointOnSegment(13.0900, 80.3200, p3, p4), true, 'Midpoint on vertical right edge');
      // Horizontal bottom edge
      assert.strictEqual(isPointOnSegment(13.0000, 80.2500, p4, p1), true, 'Midpoint on horizontal bottom edge');

      // Corner vertices
      assert.strictEqual(isPointOnSegment(13.0000, 80.1800, p1, p2), true, 'Endpoint p1 on vertical left edge');
      assert.strictEqual(isPointOnSegment(13.1800, 80.1800, p2, p3), true, 'Endpoint p2 on horizontal top edge');
    });

    test('Floating-point precision edge cases evaluate as inside without moving effective boundary', () => {
      // 1e-9 precision jitter on boundary
      assert.strictEqual(isPointInPolygon(13.000000001, 80.2500), true, 'Sub-nanometer jitter on bottom edge');
      assert.strictEqual(isPointInPolygon(13.1800, 80.250000001), true, 'Sub-nanometer jitter on top edge');
      assert.strictEqual(isPointInPolygon(13.0900, 80.180000001), true, 'Sub-nanometer jitter on left edge');
      assert.strictEqual(isPointInPolygon(13.0900, 80.319999999), true, 'Sub-nanometer jitter on right edge');
    });

    test('Points slightly outside in all four cardinal directions evaluate as outside (breach)', () => {
      // North of top edge (lat 13.1801 > 13.1800)
      assert.strictEqual(isPointInPolygon(13.1801, 80.2500), false, 'Point north of top edge must be outside');
      // South of bottom edge (lat 12.9999 < 13.0000)
      assert.strictEqual(isPointInPolygon(12.9999, 80.2500), false, 'Point south of bottom edge must be outside');
      // West of left edge (lng 80.1799 < 80.1800)
      assert.strictEqual(isPointInPolygon(13.0900, 80.1799), false, 'Point west of left edge must be outside');
      // East of right edge (lng 80.3201 > 80.3200)
      assert.strictEqual(isPointInPolygon(13.0900, 80.3201), false, 'Point east of right edge must be outside');
    });

    test('Interior and exterior points far from boundary preserve existing behavior', () => {
      // Well inside operating area
      assert.strictEqual(isPointInPolygon(13.0900, 80.2500), true, 'Interior point must be inside');
      // Far outside operating area
      assert.strictEqual(isPointInPolygon(14.0000, 81.0000), false, 'Far exterior point must be outside');
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

    test('Offline vehicle with an existing active alert remains unchanged (no duplicate)', () => {
      const vid = `VG-EXISTING-OFF-${Date.now()}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0 - 100000).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, t0Iso, t0Iso);

      // Pre-existing active offline alert
      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{"silence_seconds": 100}', ?, ?)
      `).run(vid, t0Iso, t0Iso);

      const beforeAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);

      // Run offline check
      const res = runOfflineCheck(90, t0);
      assert.strictEqual(res.newlyOfflineCount, 0, 'Must not create a new alert for vehicle that already has an active alert');

      const afterAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(afterAlert.id, beforeAlert.id);
      assert.strictEqual(afterAlert.version, beforeAlert.version);
      assert.strictEqual(afterAlert.status, 'open');
    });

    test('New valid telemetry after restart restores vehicle to online and resolves offline alert', () => {
      const vid = `VG-RESTORE-OFF-${Date.now()}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0 - 120000).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, t0Iso, t0Iso);

      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{"silence_seconds": 120}', ?, ?)
      `).run(vid, t0Iso, t0Iso);

      // New fresh telemetry arrives
      const freshTs = new Date(t0).toISOString();
      db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes)
        VALUES (?, 2, ?, 80, 30, 0, 13.08, 80.27, 10.1, 0, '[]')
      `).run(vid, freshTs);

      db.prepare(`
        UPDATE vehicles
        SET status = 'online', last_ts = ?, last_seq = 2, updated_at = ?
        WHERE vehicle_id = ?
      `).run(freshTs, freshTs, vid);

      evaluateVehicleAlerts(vid, freshTs);

      const resolvedAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(resolvedAlert.status, 'resolved', 'Fresh telemetry must resolve the offline alert');
      assert.strictEqual(resolvedAlert.version, 2);
    });

    test('Duplicate and older telemetry do NOT mark vehicle online or resolve offline alert', () => {
      const vid = `VG-NO-REVIVE-${Date.now()}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0 - 150000).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 10, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 10, 10, ?)
      `).run(vid, t0Iso, t0Iso);

      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{"silence_seconds": 150}', ?, ?)
      `).run(vid, t0Iso, t0Iso);

      evaluateVehicleAlerts(vid, t0Iso);

      const vStatus = db.prepare("SELECT status FROM vehicles WHERE vehicle_id = ?").get(vid).status;
      const alertStatus = db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).status;

      assert.strictEqual(vStatus, 'offline', 'Vehicle must remain offline');
      assert.strictEqual(alertStatus, 'open', 'Offline alert must remain open');
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

  describe('Overheat Late-Arrival Verification', () => {
    test('Inserting a late-arriving reading into historical sequence triggers alert correctly without duplicates', () => {
      const vid = `VG-HEAT-LATE-${Date.now()}`;
      const t0 = Date.now();
      const t1 = new Date(t0).toISOString();
      const t2 = new Date(t0 + 5000).toISOString(); // Will be inserted late
      const t3 = new Date(t0 + 10000).toISOString();

      // Step 1: Initial readings: T1 (56°C) and T3 (57°C) -> Only 2 readings, so streak = 2, NO alert
      db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes)
        VALUES (?, 1, ?, 80, 56.0, 40, 13.08, 80.27, 10, 0, '[]'),
               (? , 3, ?, 80, 57.0, 40, 13.08, 80.27, 10.2, 0, '[]')
      `).run(vid, t1, vid, t3);

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 3, 80, 57.0, 40, 13.08, 80.27, 10.2, 0, 'online', 1, 3, 2, ?)
      `).run(vid, t3, t3);

      evaluateVehicleAlerts(vid, t3);

      const countBefore = db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid).total;
      assert.strictEqual(countBefore, 0, 'No alert before 3 consecutive hot readings');

      // Step 2: Late-arriving reading T2 (56.5°C) arrives with seq = 2
      db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes)
        VALUES (?, 2, ?, 80, 56.5, 40, 13.08, 80.27, 10.1, 0, '[]')
      `).run(vid, t2);

      // Step 3: Re-evaluate the vehicle
      evaluateVehicleAlerts(vid, t3);

      // Step 4: Final alert state must now be triggered (open, version = 1)
      const alertAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid);
      assert.ok(alertAfter, 'Alert must be opened once late reading completes 3 consecutive hot readings');
      assert.strictEqual(alertAfter.status, 'open');
      assert.strictEqual(alertAfter.version, 1);

      // Step 5: Re-evaluate again: confirm NO duplicate alert is created
      evaluateVehicleAlerts(vid, t3);
      const totalAlerts = db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE vehicle_id = ? AND rule = 'overheat'").get(vid).total;
      assert.strictEqual(totalAlerts, 1, 'Must never create duplicate alert on repeated evaluation');
    });
  });

  describe('History Retrieval Scalability & Bounded Memory', () => {
    test('2,500 telemetry rows in window are bounded to maxRows without excessive memory or latency', () => {
      const vid = `VG-SCALE-2500-${Date.now()}`;
      const t0 = Date.now();
      const insertStmt = db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes)
        VALUES (?, ?, ?, 80, 40.0, 30, 13.08, 80.27, 10, 0, '[]')
      `);

      // Insert 2,500 historical readings spanning the 10-minute window (every 240ms)
      const insertMany = db.transaction(() => {
        for (let i = 1; i <= 2500; i++) {
          const ts = new Date(t0 - 600000 + (i * 240)).toISOString();
          insertStmt.run(vid, i, ts);
        }
      });
      insertMany();

      const latestTs = new Date(t0).toISOString();
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 2500, 80, 40.0, 30, 13.08, 80.27, 10, 0, 'online', 1, 2500, 2500, ?)
      `).run(vid, latestTs, latestTs);

      // Measure rows retrieved by getWindowHistoryStmt
      const cutoffIso = new Date(t0 - 600000).toISOString();
      const rows = getWindowHistoryStmt.all(vid, cutoffIso);

      assert.ok(rows.length <= DEFAULT_MAX_HISTORY_ROWS, `Retrieved rows (${rows.length}) must not exceed limit (${DEFAULT_MAX_HISTORY_ROWS})`);
      assert.strictEqual(rows.length, 1000, 'Must retrieve exactly bounded 1,000 rows out of 2,500');

      // Verify chronological ordering
      assert.ok(Date.parse(rows[0].ts) <= Date.parse(rows[rows.length - 1].ts), 'Rows must be sorted chronologically');
      assert.strictEqual(rows[rows.length - 1].seq, 2500, 'Newest reading (seq 2500) must be included');

      // Measure evaluation duration
      const startTime = performance.now();
      const changed = evaluateVehicleAlerts(vid, latestTs);
      const durationMs = performance.now() - startTime;

      console.log(`[Scalability Measurement] Evaluated vehicle with 2,500 history rows in ${durationMs.toFixed(2)}ms (retrieved ${rows.length} rows)`);
      assert.ok(durationMs < 50, `Alert evaluation duration (${durationMs.toFixed(2)}ms) must remain well under 50ms`);
      assert.strictEqual(Array.isArray(changed), true);
    });
  });

  describe('Offline Restart Recovery Comprehensive Suite (Tests A - G)', () => {
    beforeEach(() => {
      stopOfflineWorker();
      db.exec('DELETE FROM alerts; DELETE FROM telemetry_history; DELETE FROM vehicles; DELETE FROM alert_outbox;');
    });

    afterEach(() => {
      stopOfflineWorker();
    });

    test('Test A: Normal offline detection (online vehicle becomes offline when silent past threshold)', () => {
      const vid = `VG-TEST-A-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vid, t0Iso, t0Iso);

      // Within silence threshold (< 90s, e.g. at 30s): remains online, 0 alerts
      const res30 = runOfflineCheck(90, t0 + 30000);
      assert.strictEqual(res30.newlyOfflineCount, 0);
      const v30 = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(v30.status, 'online');
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).c, 0);

      // Past silence threshold (>= 90s, e.g. at 95s): transitions to offline, alert opened
      const res95 = runOfflineCheck(90, t0 + 95000);
      assert.strictEqual(res95.newlyOfflineCount, 1);
      const v95 = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(v95.status, 'offline');
      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alert);
      assert.strictEqual(alert.status, 'open');
      assert.strictEqual(alert.version, 1);
    });

    test('Test B: Missing alert recovery (backend restart: offline vehicle without an active offline alert gets an offline alert created upon startup)', () => {
      const vid = `VG-TEST-B-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const t0 = Date.now();
      const staleIso = new Date(t0 - 150000).toISOString(); // 150 seconds ago

      // Vehicle was already marked 'offline' before crash/restart, but has NO active offline alert
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 75, 29, 0, 13.08, 80.27, 12, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, staleIso, staleIso);

      assert.strictEqual(db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid).c, 0);

      // On startup check: must identify this vehicle and create its missing offline alert
      const res = runOfflineCheck(90, t0);
      assert.ok(res.newlyOfflineCount >= 1, 'Must detect and create alert for already-offline vehicle lacking alert');

      const vAfter = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vAfter.status, 'offline', 'Status must remain offline');

      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(alert, 'Offline alert must be created');
      assert.strictEqual(alert.status, 'open');
      assert.strictEqual(alert.version, 1);
      const details = JSON.parse(alert.details);
      assert.ok(details.silence_seconds >= 150);
    });

    test('Test C: Duplicate prevention (subsequent checks or startup do not create duplicate active offline alerts)', () => {
      const vid = `VG-TEST-C-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const t0 = Date.now();
      const staleIso = new Date(t0 - 120000).toISOString();

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 1, 75, 29, 0, 13.08, 80.27, 12, 0, 'online', 1, 1, 1, ?)
      `).run(vid, staleIso, staleIso);

      // 1st run triggers initial offline alert
      const res1 = runOfflineCheck(90, t0);
      assert.strictEqual(res1.newlyOfflineCount, 1);
      const initialAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(initialAlert);

      // 2nd, 3rd, and 4th subsequent checks at later times
      const res2 = runOfflineCheck(90, t0 + 10000);
      const res3 = runOfflineCheck(90, t0 + 20000);
      const res4 = runOfflineCheck(90, t0 + 30000);

      assert.strictEqual(res2.newlyOfflineCount, 0);
      assert.strictEqual(res3.newlyOfflineCount, 0);
      assert.strictEqual(res4.newlyOfflineCount, 0);

      const allAlerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").all(vid);
      assert.strictEqual(allAlerts.length, 1, 'Must strictly have exactly 1 offline alert');
      assert.strictEqual(allAlerts[0].id, initialAlert.id);
      assert.strictEqual(allAlerts[0].version, 1);
    });

    test('Test D: Immediate startup recovery and worker lifecycle (isWorkerRunning, repeated start no-op, stop, and restart)', () => {
      const vidOnline = `VG-TEST-D-ON-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const vidOffline = `VG-TEST-D-OFF-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const vidLateStale = `VG-TEST-D-LATE-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const t0 = Date.now();
      const staleIso = new Date(t0 - 110000).toISOString();

      // Ensure worker is not running initially
      assert.strictEqual(isWorkerRunning(), false, 'isWorkerRunning must be false before startOfflineWorker');

      // Seed 1 online stale vehicle and 1 offline vehicle without alert
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES 
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?),
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, ?)
      `).run(vidOnline, staleIso, staleIso, vidOffline, staleIso, staleIso);

      // Start worker with a huge interval (60 seconds) so the timer won't fire during this test
      startOfflineWorker(60000, 90, t0);

      try {
        // Assert worker is running
        assert.strictEqual(isWorkerRunning(), true, 'Offline worker must report running while interval is active');

        // Immediately (synchronously upon return), the startup recovery check must have executed!
        const vOn = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vidOnline);
        const vOff = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vidOffline);
        assert.strictEqual(vOn.status, 'offline', 'Online stale vehicle must immediately become offline');
        assert.strictEqual(vOff.status, 'offline', 'Already offline vehicle must remain offline');

        const alertOn = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vidOnline);
        const alertOff = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vidOffline);
        assert.ok(alertOn, 'Alert for vidOnline must be created immediately on startup');
        assert.strictEqual(alertOn.status, 'open');
        assert.ok(alertOff, 'Alert for vidOffline must be created immediately on startup');
        assert.strictEqual(alertOff.status, 'open');

        // Seed another stale vehicle AFTER the worker has already started
        db.prepare(`
          INSERT INTO vehicles (
            vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
            lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
          ) VALUES (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
        `).run(vidLateStale, staleIso, staleIso);

        // Repeated start: re-invoking startOfflineWorker while running should be a safe no-op.
        // It must NOT create another interval and must NOT re-run startup recovery.
        startOfflineWorker(60000, 90, t0);
        assert.strictEqual(isWorkerRunning(), true, 'Worker must continue running');
        const vLateCheck = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vidLateStale);
        assert.strictEqual(vLateCheck.status, 'online', 'Repeated start must NOT re-run recovery check on already-running worker');

        // Clean shutdown
        stopOfflineWorker();
        assert.strictEqual(isWorkerRunning(), false, 'isWorkerRunning must be false after stopOfflineWorker');

        // Restart worker after stopping: must successfully start and run recovery check
        startOfflineWorker(60000, 90, t0);
        assert.strictEqual(isWorkerRunning(), true, 'isWorkerRunning must be true after restarting');
        const vLateAfterRestart = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vidLateStale);
        assert.strictEqual(vLateAfterRestart.status, 'offline', 'Restarted worker must execute immediate startup recovery');
      } finally {
        stopOfflineWorker();
        assert.strictEqual(isWorkerRunning(), false, 'Offline worker must stop cleanly');
      }
    });

    test('Test E: Fresh telemetry recovery (vehicle is offline with an active offline alert; new valid telemetry arrives via ingestBatch; status changes to online, alert is resolved)', () => {
      const vid = `VG-TEST-E-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0 - 150000).toISOString();

      // Seed offline vehicle with an active offline alert
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 10, 70, 30, 0, 13.08, 80.27, 20, 0, 'offline', 1, 10, 10, ?)
      `).run(vid, t0Iso, t0Iso);

      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{"silence_seconds": 150}', ?, ?)
      `).run(vid, t0Iso, t0Iso);

      const alertBefore = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(alertBefore.status, 'open');
      assert.strictEqual(alertBefore.version, 1);

      // Fresh valid advancing telemetry arrives via ingestBatch
      const freshTs = new Date(t0).toISOString();
      const batchRes = ingestBatch([{
        vehicle_id: vid,
        seq: 11,
        ts: freshTs,
        soc_pct: 69.5,
        battery_temp_c: 32.0,
        speed_kmh: 25.0,
        lat: 13.0827,
        lng: 80.2707,
        odometer_km: 20.5,
        charging: false,
        fault_codes: []
      }]);

      assert.strictEqual(batchRes.accepted, 1);

      // Status must change to online
      const vAfter = db.prepare('SELECT status, last_seq, last_ts FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vAfter.status, 'online', 'Vehicle status must be restored to online');
      assert.strictEqual(vAfter.last_seq, 11);

      // Alert must be resolved
      const alertAfter = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(alertAfter.status, 'resolved', 'Offline alert must transition to resolved');
      assert.strictEqual(alertAfter.version, 2, 'Alert version must increment upon resolution');
      assert.ok(alertAfter.resolved_at, 'resolved_at timestamp must be set');
    });

    test('Test F: Duplicate and older telemetry (vehicle is offline with active offline alert; duplicate or older telemetry is ingested via ingestBatch; alert remains active and status remains offline)', () => {
      const vid = `VG-TEST-F-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      const t0 = Date.now();
      const t0Iso = new Date(t0 - 150000).toISOString();

      // Seed vehicle offline with seq 20
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES (?, ?, 20, 70, 30, 0, 13.08, 80.27, 20, 0, 'offline', 1, 20, 20, ?)
      `).run(vid, t0Iso, t0Iso);

      // Existing telemetry row at seq 20
      db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes)
        VALUES (?, 20, ?, 70, 30, 0, 13.08, 80.27, 20, 0, '[]')
      `).run(vid, t0Iso);

      // Active offline alert
      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{"silence_seconds": 150}', ?, ?)
      `).run(vid, t0Iso, t0Iso);

      // 1. Ingest duplicate reading (seq 20, same ts)
      const dupRes = ingestBatch([{
        vehicle_id: vid,
        seq: 20,
        ts: t0Iso,
        soc_pct: 70,
        battery_temp_c: 30,
        speed_kmh: 0,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 20,
        charging: false,
        fault_codes: []
      }]);

      assert.strictEqual(dupRes.duplicates, 1, 'Must be marked duplicate');
      assert.strictEqual(dupRes.accepted, 0);

      // Vehicle must remain offline and alert open
      let vCheck = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vCheck.status, 'offline', 'Duplicate telemetry must NOT restore online status');
      let aCheck = db.prepare("SELECT status, version FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(aCheck.status, 'open', 'Duplicate telemetry must NOT resolve offline alert');
      assert.strictEqual(aCheck.version, 1);

      // 2. Ingest older telemetry (seq 15, earlier ts)
      const olderTs = new Date(t0 - 200000).toISOString();
      const oldRes = ingestBatch([{
        vehicle_id: vid,
        seq: 15,
        ts: olderTs,
        soc_pct: 71,
        battery_temp_c: 29,
        speed_kmh: 10,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 19.5,
        charging: false,
        fault_codes: []
      }]);

      assert.strictEqual(oldRes.accepted, 1, 'Older telemetry accepted into historical log');

      // Vehicle must STILL remain offline and alert open because reading did not advance latest state
      vCheck = db.prepare('SELECT status, last_seq FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vCheck.status, 'offline', 'Older telemetry must NOT restore online status');
      assert.strictEqual(vCheck.last_seq, 20, 'last_seq must not regress');
      aCheck = db.prepare("SELECT status, version FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(aCheck.status, 'open', 'Older telemetry must NOT resolve offline alert');
      assert.strictEqual(aCheck.version, 1);
    });

    test('Test G: Multiple vehicles (batch of vehicles with mixed states: online, offline with alert, offline without alert, and fresh; verify correct state transitions for all)', () => {
      const t0 = Date.now();
      const vFresh = `VG-TEST-G-FRESH-${Date.now()}`;
      const vStale = `VG-TEST-G-STALE-${Date.now()}`;
      const vWithAlert = `VG-TEST-G-WITHALERT-${Date.now()}`;
      const vNoAlert = `VG-TEST-G-NOALERT-${Date.now()}`;
      const vMaint = `VG-TEST-G-MAINT-${Date.now()}`;

      const t20sAgo = new Date(t0 - 20000).toISOString();
      const t120sAgo = new Date(t0 - 120000).toISOString();
      const t130sAgo = new Date(t0 - 130000).toISOString();
      const t140sAgo = new Date(t0 - 140000).toISOString();
      const t200sAgo = new Date(t0 - 200000).toISOString();

      // Seed the 5 vehicles
      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES 
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?),
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?),
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, ?),
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'offline', 1, 1, 1, ?),
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'maintenance', 1, 1, 1, ?)
      `).run(
        vFresh, t20sAgo, t20sAgo,
        vStale, t120sAgo, t120sAgo,
        vWithAlert, t130sAgo, t130sAgo,
        vNoAlert, t140sAgo, t140sAgo,
        vMaint, t200sAgo, t200sAgo
      );

      // Seed existing alert for vWithAlert
      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{"silence_seconds": 130}', ?, ?)
      `).run(vWithAlert, t130sAgo, t130sAgo);
      const existingAlertId = db.prepare("SELECT id FROM alerts WHERE vehicle_id = ?").get(vWithAlert).id;

      // Execute offline check at t0
      const res = runOfflineCheck(90, t0);

      // Verify transitions:
      // vFresh: remains online, no alert
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vFresh).status, 'online');
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vFresh).c, 0);

      // vStale: becomes offline, alert created
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vStale).status, 'offline');
      const alertStale = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vStale);
      assert.ok(alertStale);
      assert.strictEqual(alertStale.status, 'open');

      // vWithAlert: remains offline, alert preserved without duplication
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vWithAlert).status, 'offline');
      const alertsWithAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").all(vWithAlert);
      assert.strictEqual(alertsWithAlert.length, 1);
      assert.strictEqual(alertsWithAlert[0].id, existingAlertId);

      // vNoAlert: remains offline, missing alert recovered
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vNoAlert).status, 'offline');
      const alertNoAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vNoAlert);
      assert.ok(alertNoAlert, 'Missing offline alert must be recovered');
      assert.strictEqual(alertNoAlert.status, 'open');

      // vMaint: remains maintenance, no alert created
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vMaint).status, 'maintenance');
      assert.strictEqual(db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vMaint).c, 0);

      // Recovery via fresh telemetry for vStale:
      const freshTs = new Date(t0 + 5000).toISOString();
      const ingestRes = ingestBatch([{
        vehicle_id: vStale,
        seq: 2,
        ts: freshTs,
        soc_pct: 79,
        battery_temp_c: 31,
        speed_kmh: 15,
        lat: 13.0827,
        lng: 80.2707,
        odometer_km: 11,
        charging: false,
        fault_codes: []
      }]);
      assert.strictEqual(ingestRes.accepted, 1);
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vStale).status, 'online');
      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vStale).status, 'resolved');
    });

    test('Test H: Custom threshold parameter is recorded in alert details (e.g. 120s non-default threshold)', () => {
      const vidStale120 = `VG-CUSTOM-THRESH-120-${Date.now()}`;
      const vidFresh100 = `VG-CUSTOM-THRESH-100-${Date.now()}`;
      const t0 = Date.now();
      const t130sAgo = new Date(t0 - 130000).toISOString(); // 130s ago (> 120s threshold)
      const t100sAgo = new Date(t0 - 100000).toISOString(); // 100s ago (< 120s threshold, though > 90s)

      db.prepare(`
        INSERT INTO vehicles (
          vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
          lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
        ) VALUES 
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?),
          (?, ?, 1, 80, 30, 0, 13.08, 80.27, 10, 0, 'online', 1, 1, 1, ?)
      `).run(vidStale120, t130sAgo, t130sAgo, vidFresh100, t100sAgo, t100sAgo);

      // Run offline check with non-default threshold of 120 seconds
      const res = runOfflineCheck(120, t0);
      assert.strictEqual(res.newlyOfflineCount, 1, 'Only vehicle silent >= 120s must become offline');

      // Vehicle silent for 100s should still be online because threshold is 120s
      const vFresh = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vidFresh100);
      assert.strictEqual(vFresh.status, 'online', 'Vehicle within custom 120s threshold must remain online');

      // Vehicle silent for 130s should be offline
      const vStale = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vidStale120);
      assert.strictEqual(vStale.status, 'offline', 'Vehicle exceeding custom 120s threshold must become offline');

      // Alert details must record the custom threshold (120), not default 90!
      const alert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vidStale120);
      assert.ok(alert, 'Alert must exist');
      assert.strictEqual(alert.status, 'open');
      const details = JSON.parse(alert.details);
      assert.strictEqual(details.threshold_seconds, 120, 'Alert details must record the exact supplied threshold (120s)');
      assert.ok(details.silence_seconds >= 130, 'Alert details must record silence_seconds >= 130');
    });
  });
});
