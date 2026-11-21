const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port for alert integration tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-alerts-int-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { runOfflineCheck, stopOfflineWorker } = require('../src/offlineWorker');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';
const alertTestStartMs = Date.now() - 500000;
function makeAlertTestTs(offsetSec = 0) { return new Date(alertTestStartMs + (offsetSec * 1000)).toISOString(); }

describe('Part A3 Alert Engine Integration & Out-of-Order Tests', () => {
  let baseUrl;
  let nonceSeq = 1000;

  before(async () => {
    // Seed plenty of rate limiter tokens so the 50+ fast requests in this integration test don't 429
    db.prepare('UPDATE api_keys SET rate_limit_per_sec = 10000, burst_capacity = 10000 WHERE api_key = ?').run(API_KEY);
    db.prepare('INSERT OR REPLACE INTO rate_limits (api_key, tokens, last_refill_ms) VALUES (?, 10000, ?)').run(API_KEY, Date.now());

    const address = server.address();
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  after(() => {
    stopOfflineWorker();
    server.close();
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  // Helper to post HMAC-signed telemetry batch to real HTTP ingestion endpoint
  async function postBatch(readings) {
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

    const body = await res.json();
    return { status: res.status, body };
  }

  // =========================================================================
  // TASK 10: REAL OUT-OF-ORDER INGESTION & ORDER INDEPENDENCE
  // =========================================================================
  describe('Out-of-Order Ingestion & Order Independence', () => {

    test('Chronological vs Reverse vs Random arrival produces identical alert state', async () => {
      const vidChrono = `VG-CHRONO-${Date.now()}`;
      const vidReverse = `VG-REVERSE-${Date.now()}`;
      const vidRandom = `VG-RANDOM-${Date.now()}`;

      // Sequence: 3 hot readings (trigger overheat) followed by 3 cool readings (resolve overheat)
      const baseReadings = [
        { seq: 1, ts: makeAlertTestTs(0), soc_pct: 80, battery_temp_c: 56.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] },
        { seq: 2, ts: makeAlertTestTs(5), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100.1, charging: false, fault_codes: [] },
        { seq: 3, ts: makeAlertTestTs(10), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100.2, charging: false, fault_codes: [] },
        { seq: 4, ts: makeAlertTestTs(15), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100.3, charging: false, fault_codes: [] },
        { seq: 5, ts: makeAlertTestTs(20), soc_pct: 80, battery_temp_c: 48.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100.4, charging: false, fault_codes: [] },
        { seq: 6, ts: makeAlertTestTs(25), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100.5, charging: false, fault_codes: [] }
      ];

      // 1. Chronological Ingestion
      for (const r of baseReadings) {
        const res = await postBatch([{ ...r, vehicle_id: vidChrono }]);
        assert.strictEqual(res.status, 200);
      }

      // 2. Reverse Ingestion
      const reversed = [...baseReadings].reverse();
      for (const r of reversed) {
        const res = await postBatch([{ ...r, vehicle_id: vidReverse }]);
        assert.strictEqual(res.status, 200);
      }

      // 3. Shuffled / Random Ingestion
      const shuffled = [baseReadings[2], baseReadings[0], baseReadings[4], baseReadings[1], baseReadings[5], baseReadings[3]];
      for (const r of shuffled) {
        const res = await postBatch([{ ...r, vehicle_id: vidRandom }]);
        assert.strictEqual(res.status, 200);
      }

      // Fetch actual SQLite alert rows
      const chronoAlerts = db.prepare('SELECT rule, status, opened_at, resolved_at FROM alerts WHERE vehicle_id = ?').all(vidChrono);
      const reverseAlerts = db.prepare('SELECT rule, status, opened_at, resolved_at FROM alerts WHERE vehicle_id = ?').all(vidReverse);
      const randomAlerts = db.prepare('SELECT rule, status, opened_at, resolved_at FROM alerts WHERE vehicle_id = ?').all(vidRandom);

      // Verify identical count: all 3 vehicles had exactly 1 alert
      assert.strictEqual(chronoAlerts.length, 1);
      assert.strictEqual(reverseAlerts.length, 1);
      assert.strictEqual(randomAlerts.length, 1);

      // Verify identical rule, status, opened_at and resolved_at
      for (const list of [chronoAlerts, reverseAlerts, randomAlerts]) {
        assert.strictEqual(list[0].rule, 'overheat');
        assert.strictEqual(list[0].status, 'resolved');
        assert.strictEqual(list[0].opened_at, baseReadings[2].ts);
        assert.strictEqual(list[0].resolved_at, baseReadings[5].ts);
      }

      // Verify active alert count is 0 for all
      const activeChrono = db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND status != 'resolved'").get(vidChrono).c;
      const activeReverse = db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND status != 'resolved'").get(vidReverse).c;
      const activeRandom = db.prepare("SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ? AND status != 'resolved'").get(vidRandom).c;
      assert.strictEqual(activeChrono, 0);
      assert.strictEqual(activeReverse, 0);
      assert.strictEqual(activeRandom, 0);
    });

    test('Late arrival from the past does not overwrite active vehicle alert state', async () => {
      const vid = `VG-LATE-${Date.now()}`;

      // Vehicle is healthy inside operating polygon
      await postBatch([
        { vehicle_id: vid, seq: 10, ts: makeAlertTestTs(100), soc_pct: 80, battery_temp_c: 40, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 50, charging: false, fault_codes: [] }
      ]);

      let alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(alerts.length, 0, 'No alert should exist for normal vehicle');

      // Now a late reading arrives from 2 hours ago where vehicle was outside geofence
      await postBatch([
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(0), soc_pct: 85, battery_temp_c: 42, speed_kmh: 0, lat: 15.00, lng: 81.00, odometer_km: 10, charging: false, fault_codes: [] }
      ]);

      // Latest known state is still seq 10 inside geofence -> active geofence alert must NOT be opened!
      const activeAlerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND status != 'resolved'").all(vid);
      assert.strictEqual(activeAlerts.length, 0, 'Late reading from past must not trigger active geofence alert over newer normal state');
    });

    test('Duplicate arrival is safely ignored and never duplicates alerts', async () => {
      const vid = `VG-DUP-${Date.now()}`;
      const hotBatch = [
        { vehicle_id: vid, seq: 1, ts: makeAlertTestTs(0), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(5), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: makeAlertTestTs(10), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ];

      // First post triggers overheat alert
      const res1 = await postBatch(hotBatch);
      assert.strictEqual(res1.status, 200);
      assert.strictEqual(res1.body.accepted, 3);

      const alert1 = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(alert1.length, 1);
      assert.strictEqual(alert1[0].status, 'open');

      // Re-posting identical batch: all 3 are duplicates, zero alert modification
      const res2 = await postBatch(hotBatch);
      assert.strictEqual(res2.status, 200);
      assert.strictEqual(res2.body.duplicates, 3);

      const alert2 = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(alert2.length, 1, 'Duplicate batch must not create duplicate alerts');
      assert.strictEqual(alert2[0].version, 1, 'Version must remain unchanged on duplicate replay');
    });

    test('Overlapping batches correctly increment readings and maintain consistent alert state', async () => {
      const vid = `VG-OVERLAP-${Date.now()}`;

      // Batch 1: seq 1, 2 (both hot)
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: makeAlertTestTs(0), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(5), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare('SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ?').get(vid).c, 0, '2 readings must not trigger');

      // Batch 2: seq 2 (duplicate), seq 3 (hot - completes 3 streak)
      const resOverlap = await postBatch([
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(5), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: makeAlertTestTs(10), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(resOverlap.body.duplicates, 1);
      assert.strictEqual(resOverlap.body.accepted, 1);

      const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(alerts.length, 1);
      assert.strictEqual(alerts[0].status, 'open');
    });

    test('Identical timestamps with different sequence numbers breaks ties deterministically', async () => {
      const vid = `VG-TIE-${Date.now()}`;
      const sharedTs = makeAlertTestTs(0);

      // Readings with same timestamp but increasing seq numbers
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: sharedTs, soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: sharedTs, soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: sharedTs, soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);

      const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(alerts.length, 1);
      assert.strictEqual(alerts[0].status, 'open');
    });
  });

  // =========================================================================
  // TASK 4: MORE THAN 100 READINGS & HISTORICAL STATE PERSISTENCE
  // =========================================================================
  describe('Historical Retrieval with >100 Readings', () => {

    test('Detects alert triggered on reading 120 (verifies query reads latest 100, not oldest 100)', async () => {
      const vid = `VG-HUNDRED-${Date.now()}`;

      // Insert 115 normal readings (temp 40°C)
      const normalReadings = [];
      const baseTime = Date.now() - 250000;
      for (let i = 1; i <= 115; i++) {
        normalReadings.push({
          vehicle_id: vid,
          seq: i,
          ts: new Date(baseTime + (i * 1000)).toISOString(),
          soc_pct: 80,
          battery_temp_c: 40.0,
          speed_kmh: 30,
          lat: 13.08,
          lng: 80.27,
          odometer_km: i,
          charging: false,
          fault_codes: []
        });
      }

      // Post in chunks of 50
      await postBatch(normalReadings.slice(0, 50));
      await postBatch(normalReadings.slice(50, 100));
      await postBatch(normalReadings.slice(100, 115));

      assert.strictEqual(db.prepare('SELECT COUNT(*) AS c FROM alerts WHERE vehicle_id = ?').get(vid).c, 0);

      // Now post readings 116, 117, 118 with temp > 55°C
      const hotReadings = [
        { vehicle_id: vid, seq: 116, ts: new Date(baseTime + 116000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 116, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 117, ts: new Date(baseTime + 117000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 117, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 118, ts: new Date(baseTime + 118000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 118, charging: false, fault_codes: [] }
      ];
      await postBatch(hotReadings);

      // If the bug (ORDER BY ts ASC LIMIT 100) were present, this would be 0 because readings 116-118 would never be retrieved!
      const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(alerts.length, 1, 'Alert triggered on reading 118 must be detected');
      assert.strictEqual(alerts[0].status, 'open');
    });

    test('Active alert survives 100+ subsequent readings in hysteresis zone (52°C)', async () => {
      const vid = `VG-HYST-100-${Date.now()}`;
      const baseTime = Date.now() - 250000;

      // 1. Open alert with 3 hot readings
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(baseTime + 1000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: new Date(baseTime + 2000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 2, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: new Date(baseTime + 3000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 3, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare('SELECT status FROM alerts WHERE vehicle_id = ?').get(vid).status, 'open');

      // 2. Stream 120 readings in hysteresis range (52°C)
      const hysteresisReadings = [];
      for (let i = 4; i <= 124; i++) {
        hysteresisReadings.push({
          vehicle_id: vid,
          seq: i,
          ts: new Date(baseTime + (i * 1000)).toISOString(),
          soc_pct: 80,
          battery_temp_c: 52.0, // In 50°C-55°C hysteresis zone
          speed_kmh: 30,
          lat: 13.08,
          lng: 80.27,
          odometer_km: i,
          charging: false,
          fault_codes: []
        });
      }
      await postBatch(hysteresisReadings.slice(0, 60));
      await postBatch(hysteresisReadings.slice(60, 121));

      // The original hot readings (seq 1-3) have rolled OUT of the latest 100 readings (which are now seq 25-124).
      // The persisted alert state must remain OPEN.
      const alertAfter120 = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(alertAfter120.status, 'open', 'Alert must maintain open state despite trigger readings rolling past 100-window');

      // 3. Now 3 cool readings <= 50°C arrive
      await postBatch([
        { vehicle_id: vid, seq: 125, ts: new Date(baseTime + 125000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 125, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 126, ts: new Date(baseTime + 126000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 126, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 127, ts: new Date(baseTime + 127000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 127, charging: false, fault_codes: [] }
      ]);

      const alertResolved = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(alertResolved.status, 'resolved', 'Alert must cleanly resolve after 3 cool readings');
      assert.strictEqual(alertResolved.version, 2, 'Version must increment to 2 on resolution');
      assert.ok(alertResolved.resolved_at, 'resolved_at must be populated');
    });
  });

  // =========================================================================
  // TASK 7: OFFLINE WORKER INTEGRATION & TELEMETRY RESUMPTION
  // =========================================================================
  describe('Offline Worker & Telemetry Resumption', () => {

    test('Vehicle silent > 90s transitions to offline; new reading resolves alert atomically', async () => {
      const vid = `VG-OFFLINE-RECOVER-${Date.now()}`;
      const now = Date.now();

      // Step 1: Ingest reading at time T0
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: new Date(now).toISOString(), soc_pct: 80, battery_temp_c: 40, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] }
      ]);

      const vOnline = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vOnline.status, 'online');

      // Step 2: Check offline at T0 + 30 seconds -> vehicle is still active
      const res30s = runOfflineCheck(90, now + 30000);
      assert.strictEqual(res30s.newlyOfflineCount, 0);
      assert.strictEqual(db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid).status, 'online');

      // Step 3: Check offline at T0 + 95 seconds -> vehicle is silent beyond 90s
      const res95s = runOfflineCheck(90, now + 95000);
      assert.ok(res95s.newlyOfflineCount >= 1);

      // Verify vehicle status is offline and alert created
      const vOffline = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vOffline.status, 'offline');

      const offlineAlert = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.ok(offlineAlert, 'Offline alert must be created');
      assert.strictEqual(offlineAlert.status, 'open');
      assert.strictEqual(offlineAlert.version, 1);

      // Step 4: Repeated check at T0 + 120s does NOT duplicate offline alert
      const res120s = runOfflineCheck(90, now + 120000);
      assert.strictEqual(res120s.newlyOfflineCount, 0);
      const allOfflineAlerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").all(vid);
      assert.strictEqual(allOfflineAlerts.length, 1, 'Offline alert must never be duplicated');

      // Step 5: Vehicle resumes reporting with current timestamp
      await postBatch([
        { vehicle_id: vid, seq: 2, ts: new Date().toISOString(), soc_pct: 79, battery_temp_c: 41, speed_kmh: 15, lat: 13.08, lng: 80.27, odometer_km: 11, charging: false, fault_codes: [] }
      ]);

      // Verify vehicle status immediately restored to 'online'
      const vRestored = db.prepare('SELECT status FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vRestored.status, 'online');

      // Verify offline alert is atomically resolved with version increment
      const alertAfterResume = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid);
      assert.strictEqual(alertAfterResume.status, 'resolved', 'Offline alert must resolve when telemetry resumes');
      assert.strictEqual(alertAfterResume.version, 2, 'Version must increment to 2');
      assert.ok(alertAfterResume.resolved_at, 'resolved_at must be populated');
    });
  });

  // =========================================================================
  // TASK 8: ALERT LIFECYCLE & OPTIMISTIC CONCURRENCY ACKNOWLEDGEMENT
  // =========================================================================
  describe('Alert Lifecycle Transitions & Acknowledgment', () => {

    test('open -> acknowledged -> resolved lifecycle with optimistic concurrency', async () => {
      const vid = `VG-LIFE-${Date.now()}`;

      // 1. Trigger overheat alert
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: makeAlertTestTs(0), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(5), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 3, ts: makeAlertTestTs(10), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
      ]);

      const alertOpen = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(alertOpen.status, 'open');
      assert.strictEqual(alertOpen.version, 1);

      // 2. Acknowledge with correct version (version: 1) -> 200 OK
      const ackRes = await fetch(`${baseUrl}/v1/alerts/${alertOpen.id}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(ackRes.status, 200);
      const ackBody = await ackRes.json();
      assert.strictEqual(ackBody.alert.status, 'acknowledged');
      assert.strictEqual(ackBody.alert.version, 2);

      // 3. Stale acknowledge with old version (version: 1) -> 409 Conflict
      const staleAck = await fetch(`${baseUrl}/v1/alerts/${alertOpen.id}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: 1 })
      });
      assert.strictEqual(staleAck.status, 409);

      // 4. Cool readings resolve the acknowledged alert -> status becomes 'resolved', version: 3
      await postBatch([
        { vehicle_id: vid, seq: 4, ts: makeAlertTestTs(15), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 5, ts: makeAlertTestTs(20), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 6, ts: makeAlertTestTs(25), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
      ]);

      const alertResolved = db.prepare('SELECT * FROM alerts WHERE id = ?').get(alertOpen.id);
      assert.strictEqual(alertResolved.status, 'resolved');
      assert.strictEqual(alertResolved.version, 3);
      assert.ok(alertResolved.resolved_at);

      // 5. Subsequent overheat triggers a brand NEW alert with version 1
      await postBatch([
        { vehicle_id: vid, seq: 7, ts: makeAlertTestTs(60), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 11, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 8, ts: makeAlertTestTs(65), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 11.1, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 9, ts: makeAlertTestTs(70), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 20, lat: 13.08, lng: 80.27, odometer_km: 11.2, charging: false, fault_codes: [] }
      ]);

      const allAlerts = db.prepare('SELECT id, status, version FROM alerts WHERE vehicle_id = ? ORDER BY id ASC').all(vid);
      assert.strictEqual(allAlerts.length, 2, 'New breach after resolution creates second alert');
      assert.strictEqual(allAlerts[0].status, 'resolved');
      assert.strictEqual(allAlerts[1].status, 'open');
      assert.strictEqual(allAlerts[1].version, 1);
    });
  });

  // =========================================================================
  // TASK 5: RAPID DISCHARGE INTEGRATION VIA HTTP
  // =========================================================================
  describe('Rapid Discharge Integration & Edge Cases', () => {

    test('Discharge >8% in 5 min triggers; vehicle charging resolves immediately', async () => {
      const vid = `VG-DISCHARGE-${Date.now()}`;

      // Ingest drop of 10 points in 2 minutes while not charging
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: makeAlertTestTs(0), soc_pct: 85.0, battery_temp_c: 35, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(120), soc_pct: 74.0, battery_temp_c: 36, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 11, charging: false, fault_codes: [] }
      ]);

      const alertOpen = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.ok(alertOpen, 'Rapid discharge alert must trigger');
      assert.strictEqual(alertOpen.status, 'open');

      // Vehicle starts charging -> alert must immediately resolve
      await postBatch([
        { vehicle_id: vid, seq: 3, ts: makeAlertTestTs(180), soc_pct: 75.0, battery_temp_c: 37, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 11, charging: true, fault_codes: [] }
      ]);

      const alertResolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.strictEqual(alertResolved.status, 'resolved', 'Charging vehicle must resolve rapid discharge');
    });

    test('5-minute window expiration resolves rapid discharge alert', async () => {
      const vid = `VG-WINDOW-EXP-${Date.now()}`;

      // Trigger rapid discharge: 85% to 75% in 2 minutes
      await postBatch([
        { vehicle_id: vid, seq: 1, ts: makeAlertTestTs(0), soc_pct: 85.0, battery_temp_c: 35, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(120), soc_pct: 75.0, battery_temp_c: 36, speed_kmh: 40, lat: 13.08, lng: 80.27, odometer_km: 11, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid).status, 'open');

      // 8 minutes later, vehicle reports a reading (SoC: 74.5% - minimal drop)
      // The previous drop occurred >5 minutes ago, so the 5-minute window has expired
      await postBatch([
        { vehicle_id: vid, seq: 3, ts: makeAlertTestTs(450), soc_pct: 74.5, battery_temp_c: 35, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 11, charging: false, fault_codes: [] }
      ]);

      const alertResolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'rapid_discharge'").get(vid);
      assert.strictEqual(alertResolved.status, 'resolved', '5-minute window expiration must resolve rapid discharge alert');
    });
  });

  // =========================================================================
  // TASK 6: GEOFENCE BREACH INTEGRATION VIA HTTP
  // =========================================================================
  describe('Geofence Breach Integration', () => {

    test('Outside operating polygon triggers breach; re-entering resolves', async () => {
      const vid = `VG-GEO-${Date.now()}`;

      // Step 1: Outside polygon (lat 14.50, lng 81.00)
      const res1 = await postBatch([
        { vehicle_id: vid, seq: 1, ts: makeAlertTestTs(0), soc_pct: 80, battery_temp_c: 35, speed_kmh: 50, lat: 14.5000, lng: 81.0000, odometer_km: 100, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(res1.status, 200);

      const alertOpen = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").get(vid);
      assert.ok(alertOpen, 'Geofence breach alert must trigger when outside polygon');
      assert.strictEqual(alertOpen.status, 'open');

      // Step 2: Repeated outside reading does not duplicate alert
      const res2 = await postBatch([
        { vehicle_id: vid, seq: 2, ts: makeAlertTestTs(60), soc_pct: 80, battery_temp_c: 35, speed_kmh: 50, lat: 14.5500, lng: 81.0500, odometer_km: 101, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(res2.status, 200);

      const alerts = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").all(vid);
      assert.strictEqual(alerts.length, 1, 'Repeated outside readings must not duplicate alert');

      // Step 3: Re-enter polygon (Chennai center: lat 13.0827, lng 80.2707)
      const res3 = await postBatch([
        { vehicle_id: vid, seq: 3, ts: makeAlertTestTs(300), soc_pct: 79, battery_temp_c: 35, speed_kmh: 20, lat: 13.0827, lng: 80.2707, odometer_km: 105, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(res3.status, 200);

      const alertResolved = db.prepare("SELECT * FROM alerts WHERE vehicle_id = ? AND rule = 'geofence_breach'").get(vid);
      assert.strictEqual(alertResolved.status, 'resolved', 'Re-entering operating polygon must resolve breach alert');
      assert.strictEqual(alertResolved.version, 2);
    });
  });
});
