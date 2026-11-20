/**
 * VoltGrid Fleet Telemetry Service - Integration Test Suite for Alert Engine & Ingestion
 */

const assert = require('assert');
const { app, server } = require('../src/server');
const { runOfflineCheck } = require('../src/offlineWorker');

async function runTests() {
  console.log('='.repeat(70));
  console.log('RUNNING PHASE 2 END-TO-END INTEGRATION TESTS');
  console.log('='.repeat(70));

  const PORT = process.env.PORT || 3000;
  const baseUrl = `http://localhost:${PORT}`;

  // Helper to post batch
  async function postBatch(readings) {
    const res = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(readings)
    });
    return { status: res.status, body: await res.json() };
  }

  // Helper to query alerts
  async function getAlerts(params = '') {
    const res = await fetch(`${baseUrl}/v1/alerts${params ? '?' + params : ''}`);
    return { status: res.status, body: await res.json() };
  }

  // --- Test 1: Overheat Alert Open & Resolve ---
  console.log('\n[Integration 1] Overheat Alert lifecycle...');
  const runSuffix = Date.now();
  const testId = `TEST-HEAT-${runSuffix}`;

  // Send 3 hot readings > 55°C
  const hotBatch = [
    { vehicle_id: testId, seq: 101, ts: '2026-11-20T11:00:00Z', soc_pct: 80, battery_temp_c: 56.5, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] },
    { vehicle_id: testId, seq: 102, ts: '2026-11-20T11:00:05Z', soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] },
    { vehicle_id: testId, seq: 103, ts: '2026-11-20T11:00:10Z', soc_pct: 80, battery_temp_c: 58.2, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] }
  ];
  await postBatch(hotBatch);

  // Check that overheat alert is OPEN
  let alertsRes = await getAlerts(`vehicle_id=${testId}&rule=overheat&status=open`);
  assert.strictEqual(alertsRes.body.total, 1, 'Should have exactly 1 open overheat alert');
  assert.strictEqual(alertsRes.body.alerts[0].status, 'open');

  // De-duplication check: send 4th hot reading -> should NOT create a duplicate alert
  await postBatch([
    { vehicle_id: testId, seq: 104, ts: '2026-11-20T11:00:15Z', soc_pct: 80, battery_temp_c: 59.0, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] }
  ]);
  alertsRes = await getAlerts(`vehicle_id=${testId}&rule=overheat&status=open`);
  assert.strictEqual(alertsRes.body.total, 1, 'Still only 1 open alert (no duplicates)');

  // Send 3 cooling readings <= 50°C -> resolves overheat alert
  const coolBatch = [
    { vehicle_id: testId, seq: 105, ts: '2026-11-20T11:00:20Z', soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] },
    { vehicle_id: testId, seq: 106, ts: '2026-11-20T11:00:25Z', soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] },
    { vehicle_id: testId, seq: 107, ts: '2026-11-20T11:00:30Z', soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 40, lat: 13.0827, lng: 80.2707, odometer_km: 100, charging: false, fault_codes: [] }
  ];
  await postBatch(coolBatch);

  const resolvedRes = await getAlerts(`vehicle_id=${testId}&rule=overheat&status=resolved`);
  assert.strictEqual(resolvedRes.body.total, 1, 'Overheat alert should now be resolved');
  console.log('✓ Overheat open, de-duplication, and resolution verified.');

  // --- Test 2: Geofence Breach lifecycle ---
  console.log('\n[Integration 2] Geofence Breach lifecycle...');
  const geoId = `TEST-GEO-${runSuffix}`;

  // Send reading outside polygon
  await postBatch([
    { vehicle_id: geoId, seq: 201, ts: '2026-11-20T11:00:00Z', soc_pct: 85, battery_temp_c: 30, speed_kmh: 50, lat: 12.5000, lng: 77.0000, odometer_km: 50, charging: false, fault_codes: [] }
  ]);
  let geoAlerts = await getAlerts(`vehicle_id=${geoId}&rule=geofence_breach&status=open`);
  assert.strictEqual(geoAlerts.body.total, 1, 'Geofence breach alert should be open');

  // Return inside polygon
  await postBatch([
    { vehicle_id: geoId, seq: 202, ts: '2026-11-20T11:00:05Z', soc_pct: 85, battery_temp_c: 30, speed_kmh: 50, lat: 13.0827, lng: 80.2707, odometer_km: 51, charging: false, fault_codes: [] }
  ]);
  geoAlerts = await getAlerts(`vehicle_id=${geoId}&rule=geofence_breach&status=resolved`);
  assert.strictEqual(geoAlerts.body.total, 1, 'Geofence breach alert should be resolved');
  console.log('✓ Geofence breach open and resolution verified.');

  // --- Test 3: Offline Detection Worker & Auto-Resolution ---
  console.log('\n[Integration 3] Offline Worker & Auto-Resolution...');
  const offId = `TEST-OFF-${runSuffix}`;

  // Initial reading
  await postBatch([
    { vehicle_id: offId, seq: 301, ts: '2026-11-20T11:00:00Z', soc_pct: 90, battery_temp_c: 28, speed_kmh: 0, lat: 13.0827, lng: 80.2707, odometer_km: 10, charging: false, fault_codes: [] }
  ]);

  // Run offline check with 0 threshold (flags immediately)
  runOfflineCheck(0);

  let offAlerts = await getAlerts(`vehicle_id=${offId}&rule=offline&status=open`);
  assert.strictEqual(offAlerts.body.total, 1, 'Offline alert should be open');

  // Send a new reading -> should automatically resolve offline alert!
  await postBatch([
    { vehicle_id: offId, seq: 302, ts: '2026-11-20T11:01:40Z', soc_pct: 89, battery_temp_c: 29, speed_kmh: 15, lat: 13.0827, lng: 80.2707, odometer_km: 11, charging: false, fault_codes: [] }
  ]);
  offAlerts = await getAlerts(`vehicle_id=${offId}&rule=offline&status=resolved`);
  assert.strictEqual(offAlerts.body.total, 1, 'Offline alert should be automatically resolved on new reading');
  console.log('✓ Offline detection and automatic resolution verified.');

  // --- Test 4: Concurrency & Idempotency Test (20 concurrent clients firing identical batch) ---
  console.log('\n[Integration 4] Concurrency & Idempotency (20 simultaneous clients firing the same batch)...');
  const concId = `TEST-CONC-${runSuffix}`;
  const concurrentBatch = Array.from({ length: 25 }, (_, i) => ({
    vehicle_id: `${concId}-${i}`,
    seq: 1,
    ts: '2026-11-20T11:30:00Z',
    soc_pct: 75.0,
    battery_temp_c: 30.0,
    speed_kmh: 35.0,
    lat: 13.0827,
    lng: 80.2707,
    odometer_km: 50.0,
    charging: false,
    fault_codes: []
  }));

  // Fire 20 concurrent client requests simultaneously
  const concurrentClients = 20;
  const promises = Array.from({ length: concurrentClients }, () => postBatch(concurrentBatch));
  const results = await Promise.all(promises);

  let totalAccepted = 0;
  let totalDuplicates = 0;
  for (const r of results) {
    assert.strictEqual(r.status, 200);
    totalAccepted += r.body.accepted;
    totalDuplicates += r.body.duplicates;
  }

  // Exactly 25 readings must be accepted in total; remaining 475 must be marked duplicate
  assert.strictEqual(totalAccepted, 25, 'Exactly 25 readings should be accepted across all 20 concurrent clients');
  assert.strictEqual(totalDuplicates, 25 * (concurrentClients - 1), 'Remaining 475 readings must be identified as duplicates');

  const db = require('../src/db');
  const countRow = db.prepare('SELECT COUNT(*) AS c FROM telemetry_history WHERE vehicle_id LIKE ?').get(`${concId}%`);
  assert.strictEqual(Number(countRow.c), 25, 'Database must store exactly 25 history rows (zero duplicates)');
  console.log(`✓ 20 concurrent clients verified: ${totalAccepted} accepted, ${totalDuplicates} duplicates, 0 double-counts in DB.`);

  console.log('\n' + '='.repeat(70));
  console.log('ALL INTEGRATION TESTS PASSED PERFECTLY! 🚀');
  console.log('='.repeat(70));

  const { stopOfflineWorker } = require('../src/offlineWorker');
  stopOfflineWorker();
  server.close();
}

runTests().catch(err => {
  console.error('Integration test failed:', err);
  const { stopOfflineWorker } = require('../src/offlineWorker');
  stopOfflineWorker();
  server.close();
  process.exitCode = 1;
});
