#!/usr/bin/env node
'use strict';

/**
 * VoltGrid Fleet Telemetry — Replay Tool (Bonus Task)
 * 
 * Re-runs a recorded window of historical telemetry through the pure Alert Engine
 * in both chronological and shuffled arrival orders to verify deterministic,
 * order-independent alert state convergence.
 */

const assert = require('assert');
const db = require('../backend/src/db');
const {
  evaluateOverheat,
  evaluateRapidDischarge,
  evaluateGeofenceBreach
} = require('../backend/src/alertEngine');

function shuffleArray(array) {
  const arr = [...array];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function evaluateAll(readings) {
  return {
    overheat: evaluateOverheat(readings),
    rapid_discharge: evaluateRapidDischarge(readings),
    geofence_breach: evaluateGeofenceBreach(readings)
  };
}

function runReplayTool() {
  console.log('======================================================================');
  console.log('VoltGrid Fleet Telemetry — Alert Engine Replay Tool (Bonus Task)');
  console.log('======================================================================\n');

  // Pick an active vehicle with telemetry history
  const vehicle = db.prepare(`
    SELECT vehicle_id, COUNT(*) as count 
    FROM telemetry_history 
    GROUP BY vehicle_id 
    HAVING count >= 5 
    LIMIT 1
  `).get();

  let readings = [];
  let vehicleId = 'VG-REPLAY-DEMO';

  if (vehicle) {
    vehicleId = vehicle.vehicle_id;
    readings = db.prepare(`
      SELECT seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, charging 
      FROM telemetry_history 
      WHERE vehicle_id = ? 
      ORDER BY ts ASC 
      LIMIT 100
    `).all(vehicleId);
    console.log(`[Replay] Loaded ${readings.length} recorded readings for live vehicle ${vehicleId}`);
  } else {
    console.log(`[Replay] Generating synthetic recorded window for vehicle ${vehicleId}...`);
    const baseTime = Date.now() - 3600000;
    readings = [
      { seq: 1, ts: new Date(baseTime).toISOString(), soc_pct: 95, battery_temp_c: 42, speed_kmh: 30, lat: 13.0827, lng: 80.2707, charging: false },
      { seq: 2, ts: new Date(baseTime + 10000).toISOString(), soc_pct: 94, battery_temp_c: 56, speed_kmh: 32, lat: 13.0830, lng: 80.2710, charging: false },
      { seq: 3, ts: new Date(baseTime + 20000).toISOString(), soc_pct: 93, battery_temp_c: 57, speed_kmh: 35, lat: 13.0832, lng: 80.2712, charging: false },
      { seq: 4, ts: new Date(baseTime + 30000).toISOString(), soc_pct: 92, battery_temp_c: 58, speed_kmh: 34, lat: 13.0835, lng: 80.2715, charging: false },
      { seq: 5, ts: new Date(baseTime + 40000).toISOString(), soc_pct: 82, battery_temp_c: 59, speed_kmh: 30, lat: 13.0837, lng: 80.2718, charging: false },
      { seq: 6, ts: new Date(baseTime + 50000).toISOString(), soc_pct: 81, battery_temp_c: 49, speed_kmh: 20, lat: 13.0840, lng: 80.2720, charging: false },
      { seq: 7, ts: new Date(baseTime + 60000).toISOString(), soc_pct: 80, battery_temp_c: 48, speed_kmh: 10, lat: 13.0842, lng: 80.2722, charging: false },
      { seq: 8, ts: new Date(baseTime + 70000).toISOString(), soc_pct: 80, battery_temp_c: 47, speed_kmh: 0,  lat: 13.0845, lng: 80.2725, charging: false },
    ];
  }

  // 1. Run Chronological Evaluation
  console.log('[Replay] Executing Pass 1: Strict Chronological Order (ts ASC)...');
  const chronologicalOutcome = evaluateAll(readings);

  // 2. Run Shuffled Out-of-Order Evaluation
  console.log('[Replay] Executing Pass 2: Randomized Out-of-Order Arrival...');
  const shuffledReadings = shuffleArray(readings);
  const shuffledOutcome = evaluateAll(shuffledReadings);

  // 3. Compare Results
  console.log('\n---------------- Evaluation Comparison ----------------');
  const rules = ['overheat', 'rapid_discharge', 'geofence_breach'];
  let matches = 0;

  for (const rule of rules) {
    const cRes = chronologicalOutcome[rule] || { triggered: false, resolved: false };
    const sRes = shuffledOutcome[rule] || { triggered: false, resolved: false };

    const match = (cRes.triggered === sRes.triggered) && (cRes.resolved === sRes.resolved);
    if (match) matches++;

    console.log(`Rule [${rule.padEnd(16)}]: Chronological = { triggered: ${String(cRes.triggered).padEnd(5)}, resolved: ${String(cRes.resolved).padEnd(5)} } | Replayed = { triggered: ${String(sRes.triggered).padEnd(5)}, resolved: ${String(sRes.resolved).padEnd(5)} } -> ${match ? '✅ MATCH' : '❌ MISMATCH'}`);
  }

  assert.strictEqual(matches, rules.length, 'All alert rules must converge to identical states regardless of arrival order.');
  console.log('\n[Replay Proof] Passed! Shuffled traffic replay produced 100% IDENTICAL alert states.');
  console.log('======================================================================\n');
}

if (require.main === module) {
  runReplayTool();
}

module.exports = { runReplayTool };
