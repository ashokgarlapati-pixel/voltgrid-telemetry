const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database
const testDbPath = path.join(os.tmpdir(), `voltgrid-sim-test-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = process.env.NODE_ENV || 'test';
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { validateReading, ingestBatch } = require('../src/ingestion');
const {
  MAX_LATE_MS,
  MAX_DRIFT_MS,
  generateNextReading,
  corruptPayload,
  vehicles
} = require('../../simulator/simulator');

describe('Simulator Configuration & Ingestion Window Compatibility Suite', () => {
  after(() => {
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  test('Simulator defaults: MAX_LATE_MS is 600000ms (10 minutes) and MAX_DRIFT_MS is 30000ms (30 seconds)', () => {
    assert.strictEqual(MAX_LATE_MS, 600000, 'MAX_LATE_MS default must be 600,000 ms (10 minutes)');
    assert.strictEqual(MAX_DRIFT_MS, 30000, 'MAX_DRIFT_MS default must be 30,000 ms (30 seconds)');
  });

  test('Clock drift: readings generated across entire fleet fall strictly within [-30s, +30s] of device time', () => {
    const now = Date.now();
    for (let i = 0; i < Math.min(200, vehicles.length); i++) {
      const v = vehicles[i];
      assert.ok(v.clock_drift_ms >= -30000 && v.clock_drift_ms <= 30000, 'Vehicle drift must be within [-30s, +30s]');

      const reading = generateNextReading(v);
      const readingMs = Date.parse(reading.ts);
      assert.ok(!isNaN(readingMs), 'Reading timestamp must be valid ISO string');

      // Check against current server time
      const diffMs = readingMs - now;
      assert.ok(diffMs >= -35000 && diffMs <= 35000, `Reading timestamp drift (${diffMs}ms) must be within ±30s (with tolerance)`);

      // All real-time generated readings must pass validation
      const err = validateReading(reading, now);
      assert.strictEqual(err, null, `Generated reading must pass validation: ${err}`);
    }
  });

  test('Delayed readings: 10-minute delayed queue readings are within supported window and accepted by ingestion', () => {
    const serverNow = Date.now();

    // Test extreme drift vehicles (-30s, 0s, +30s)
    const driftScenarios = [-30000, -15000, 0, 15000, 30000];

    for (const drift of driftScenarios) {
      const testVehicle = {
        vehicle_id: `VG-DELAY-DRIFT-${drift}`,
        seq: 1,
        clock_drift_ms: drift,
        soc_pct: 80.0,
        battery_temp_c: 30.0,
        speed_kmh: 40.0,
        lat: 13.0827,
        lng: 80.2707,
        odometer_km: 100.0,
        charging: false,
        fault_codes: []
      };

      // Test delay values across the full 10-minute span (up to 570s - 599s)
      const testDelaysSec = [30, 60, 180, 300, 540, 570];

      for (const delaySec of testDelaysSec) {
        const generationTime = serverNow - (delaySec * 1000);
        const deviceTime = generationTime + drift;
        const reading = {
          vehicle_id: testVehicle.vehicle_id,
          seq: testVehicle.seq++,
          ts: new Date(deviceTime).toISOString(),
          soc_pct: testVehicle.soc_pct,
          battery_temp_c: testVehicle.battery_temp_c,
          speed_kmh: testVehicle.speed_kmh,
          lat: testVehicle.lat,
          lng: testVehicle.lng,
          odometer_km: testVehicle.odometer_km,
          charging: testVehicle.charging,
          fault_codes: testVehicle.fault_codes
        };

        // When reading arrives at serverNow, verify its age
        const readingTimeMs = Date.parse(reading.ts);
        const ageMs = serverNow - readingTimeMs;

        // Must not be older than 10 minutes (600,000ms)
        assert.ok(ageMs <= 600000, `Reading age (${ageMs}ms) must not exceed 10 minutes`);

        // Must not be more than 30 seconds in the future
        assert.ok(readingTimeMs <= serverNow + 30000, 'Reading must not exceed 30 seconds into the future');

        // Verify ingestion accepts the delayed reading
        const validationError = validateReading(reading, serverNow);
        assert.strictEqual(validationError, null, `Delayed reading (delay=${delaySec}s, drift=${drift}ms) must be accepted`);
      }
    }
  });

  test('Batch ingestion accepts simulator batches containing real-world delays and clock drift', () => {
    const serverNow = Date.now();
    const batch = [];

    for (let i = 0; i < 50; i++) {
      const drift = (Math.random() * 60000) - 30000; // [-30s, +30s]
      const delay = Math.floor(Math.random() * 540000); // [0, 9 minutes]
      const generationTime = serverNow - delay;
      const deviceTime = generationTime + drift;

      batch.push({
        vehicle_id: `VG-SIM-BATCH-${i}`,
        seq: 1,
        ts: new Date(deviceTime).toISOString(),
        soc_pct: 75.0,
        battery_temp_c: 28.0,
        speed_kmh: 35.0,
        lat: 13.0827,
        lng: 80.2707,
        odometer_km: 50.0 + i,
        charging: false,
        fault_codes: []
      });
    }

    const result = ingestBatch(batch, new Date(serverNow).toISOString());
    assert.strictEqual(result.total, 50);
    assert.strictEqual(result.accepted, 50, 'All simulated delayed and drifted readings must be accepted');
    assert.strictEqual(result.rejected, 0);
  });
});
