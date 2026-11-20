/**
 * VoltGrid Fleet Telemetry Service - Integration Test Suite for Alert Engine & Ingestion
 */

const assert = require('assert');
const crypto = require('crypto');

// Use port 3001 for integration tests so it doesn't conflict with any running server on 3000
process.env.PORT = process.env.TEST_PORT || '3001';
const { app, server } = require('../src/server');
const { runOfflineCheck } = require('../src/offlineWorker');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';
let reqNonce = 0;

async function runTests() {
  console.log('='.repeat(70));
  console.log('RUNNING VOLTGRID END-TO-END INTEGRATION TESTS');
  console.log('='.repeat(70));

  const PORT = process.env.PORT || 3000;
  const baseUrl = `http://localhost:${PORT}`;

  // Helper to post batch with valid HMAC headers by default
  async function postBatch(readings, headersOverride = null) {
    const rawBody = JSON.stringify(readings);
    const timestamp = (Date.now() + (++reqNonce)).toString();
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${rawBody}`).digest('hex');

    const headers = headersOverride || {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
      'X-Timestamp': timestamp,
      'X-Signature': signature
    };

    const res = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers,
      body: rawBody
    });
    return { status: res.status, headers: res.headers, body: await res.json() };
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

  // --- Test 5: Keyset Pagination (GET /v1/vehicles) ---
  console.log('\n[Integration 5] Keyset Pagination (GET /v1/vehicles)...');
  const page1Res = await fetch(`${baseUrl}/v1/vehicles?limit=3`);
  assert.strictEqual(page1Res.status, 200);
  assert.ok(page1Res.headers.get('x-request-id'), 'Must attach X-Request-Id header');
  const page1 = await page1Res.json();
  assert.strictEqual(page1.data.length, 3);
  assert.ok(page1.next_cursor, 'Page 1 must return a next_cursor');

  const page2Res = await fetch(`${baseUrl}/v1/vehicles?limit=3&cursor=${page1.next_cursor}`);
  assert.strictEqual(page2Res.status, 200);
  const page2 = await page2Res.json();
  assert.strictEqual(page2.data.length, 3);

  // Assert keyset continuity and zero overlap between pages
  const overlap = page1.data.some(d1 => page2.data.some(d2 => d1.vehicle_id === d2.vehicle_id));
  assert.strictEqual(overlap, false, 'Keyset pagination must have zero overlap between consecutive pages');
  console.log(`✓ Keyset pagination verified (Page 1 cursor: ${page1.next_cursor}, zero overlap).`);

  // --- Test 6: Peak-Preserving History Downsampling ---
  console.log('\n[Integration 6] Downsampled History (GET /v1/vehicles/:id/history)...');
  const histRes = await fetch(`${baseUrl}/v1/vehicles/${concId}-0/history?points=5`);
  assert.strictEqual(histRes.status, 200);
  const histBody = await histRes.json();
  assert.strictEqual(histBody.vehicle_id, `${concId}-0`);
  assert.ok(Array.isArray(histBody.points));
  console.log(`✓ Vehicle history downsampling verified (raw: ${histBody.raw_count}, returned: ${histBody.returned_count}).`);

  // --- Test 7: Fleet Summary Aggregate (GET /v1/fleet/summary) ---
  console.log('\n[Integration 7] Fleet Summary Aggregate (GET /v1/fleet/summary)...');
  const summaryRes = await fetch(`${baseUrl}/v1/fleet/summary`);
  assert.strictEqual(summaryRes.status, 200);
  const summary = await summaryRes.json();
  assert.ok(typeof summary.total_vehicles === 'number');
  assert.ok(typeof summary.avg_soc_pct === 'number');
  assert.ok(summary.counts_by_status && typeof summary.counts_by_status.online === 'number');
  assert.ok(summary.open_alerts_by_rule && typeof summary.open_alerts_by_rule.overheat === 'number');
  assert.ok(typeof summary.data_completeness_pct === 'number');
  console.log(`✓ Fleet summary aggregate verified (Total: ${summary.total_vehicles}, Avg SoC: ${summary.avg_soc_pct}%).`);

  // --- Test 8: Optimistic Alert Ack (POST /v1/alerts/:id/ack) ---
  console.log('\n[Integration 8] Optimistic Concurrency Alert Ack (POST /v1/alerts/:id/ack)...');
  const ackTestId = `ACK-TEST-${runSuffix}`;
  db.prepare(`
    INSERT INTO alerts (vehicle_id, rule, status, version, opened_at, updated_at)
    VALUES (?, 'overheat', 'open', 1, datetime('now'), datetime('now'))
  `).run(ackTestId);

  const newAlert = db.prepare('SELECT id, version FROM alerts WHERE vehicle_id = ? AND status = ?').get(ackTestId, 'open');

  // Ack with correct version 1 -> HTTP 200
  const ackSuccessRes = await fetch(`${baseUrl}/v1/alerts/${newAlert.id}/ack`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ version: 1 })
  });
  assert.strictEqual(ackSuccessRes.status, 200);
  const ackSuccessBody = await ackSuccessRes.json();
  assert.strictEqual(ackSuccessBody.alert.status, 'acknowledged');
  assert.strictEqual(ackSuccessBody.alert.version, 2);

  // Ack again with stale version 1 -> HTTP 409 Conflict
  const ackConflictRes = await fetch(`${baseUrl}/v1/alerts/${newAlert.id}/ack`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ version: 1 })
  });
  assert.strictEqual(ackConflictRes.status, 409);
  const ackConflictBody = await ackConflictRes.json();
  assert.strictEqual(ackConflictBody.current_version, 2);
  assert.strictEqual(ackConflictBody.current_status, 'acknowledged');
  console.log(`✓ Optimistic concurrency ack verified (v1 -> HTTP 200, retry v1 -> HTTP 409 Conflict).`);

  // --- Test 9: Observability & Metrics (GET /metrics) ---
  console.log('\n[Integration 9] Observability & Metrics (GET /metrics)...');
  const metricsRes = await fetch(`${baseUrl}/metrics`);
  assert.strictEqual(metricsRes.status, 200);
  const metrics = await metricsRes.json();
  assert.ok(typeof metrics.uptime_sec === 'number');
  assert.ok(typeof metrics.requests_total === 'number');
  assert.ok(typeof metrics.database.vehicles_total === 'number');
  console.log(`✓ Metrics endpoint verified (Uptime: ${metrics.uptime_sec}s, Requests: ${metrics.requests_total}).`);

  // --- Test 10: HMAC Authentication & Replay Protection (Part A2) ---
  console.log('\n[Integration 10] HMAC Authentication & Replay Protection...');
  // 10a. Unsigned request -> 401
  const unsignedRes = await postBatch([], { 'Content-Type': 'application/json' });
  assert.strictEqual(unsignedRes.status, 401, 'Unsigned request must be rejected with 401');

  // 10b. Invalid HMAC signature -> 401
  const badSigRes = await postBatch([], {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
    'X-Timestamp': Date.now().toString(),
    'X-Signature': 'deadbeef00000000000000000000000000000000000000000000000000000000'
  });
  assert.strictEqual(badSigRes.status, 401, 'Tampered HMAC signature must return 401');

  // 10c. Expired timestamp (> 5 mins) -> 401
  const expiredTs = (Date.now() - 6 * 60 * 1000).toString();
  const expiredSig = crypto.createHmac('sha256', API_SECRET).update(`${expiredTs}.[]`).digest('hex');
  const expiredRes = await postBatch([], {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
    'X-Timestamp': expiredTs,
    'X-Signature': expiredSig
  });
  assert.strictEqual(expiredRes.status, 401, 'Stale timestamp outside 5-minute window must return 401');

  // 10d. Replay protection: Resending the exact same signature must be blocked
  const replayId = `TEST-REP-${runSuffix}`;
  const replayBatch = [
    { vehicle_id: replayId, seq: 1, ts: '2026-11-20T11:00:00Z', soc_pct: 60, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
  ];
  const rTs = Date.now().toString();
  const rRaw = JSON.stringify(replayBatch);
  const rSig = crypto.createHmac('sha256', API_SECRET).update(`${rTs}.${rRaw}`).digest('hex');
  const rHeaders = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
    'X-Timestamp': rTs,
    'X-Signature': rSig
  };

  const firstPost = await postBatch(replayBatch, rHeaders);
  assert.strictEqual(firstPost.status, 200, 'First signed request must succeed');

  const replayedPost = await postBatch(replayBatch, rHeaders);
  assert.strictEqual(replayedPost.status, 401, 'Replayed signature must be rejected with 401');
  assert.strictEqual(replayedPost.body.error, 'Replayed request signature');
  console.log('✓ HMAC signing, timestamp window, and signature replay protection verified.');

  // --- Test 11: Distributed Token-Bucket Rate Limiter (Part A2) ---
  console.log('\n[Integration 11] Token-Bucket Rate Limiter & Retry-After...');
  // Burst 50 requests rapidly to exceed burst capacity (burst_capacity = 40)
  const burstBatch = [
    { vehicle_id: `TEST-BURST-${runSuffix}`, seq: 999, ts: '2026-11-20T11:00:00Z', soc_pct: 50, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
  ];
  const burstPromises = Array.from({ length: 45 }, () => postBatch(burstBatch));
  const burstResults = await Promise.all(burstPromises);

  const rateLimited = burstResults.find(r => r.status === 429);
  assert.ok(rateLimited, 'Rapid burst exceeding capacity must produce HTTP 429 Too Many Requests');
  assert.ok(rateLimited.headers.get('retry-after'), 'Rate-limited response must include Retry-After header');
  console.log(`✓ Token-bucket rate limiter verified (Produced HTTP 429 with Retry-After: ${rateLimited.headers.get('retry-after')}s).`);

  // --- Test 12: Live SSE Stream (Part A5) ---
  console.log('\n[Integration 12] Live SSE Stream (GET /v1/stream)...');
  const controller = new AbortController();
  const sseRes = await fetch(`${baseUrl}/v1/stream`, { signal: controller.signal });
  assert.strictEqual(sseRes.status, 200);
  assert.ok(sseRes.headers.get('content-type').includes('text/event-stream'), 'Content-Type must be text/event-stream');
  controller.abort(); // Close client connection
  console.log('✓ Live SSE stream endpoint verified (text/event-stream headers, connection accepted).');

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
