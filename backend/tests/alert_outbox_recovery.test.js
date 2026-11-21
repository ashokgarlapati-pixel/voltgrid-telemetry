const { test, describe, before, after } = require('node:test');
const assert = require('node:assert');
const path = require('path');
const os = require('os');
const fs = require('fs');
const crypto = require('crypto');

// Isolated temporary SQLite database for alert outbox crash recovery tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-outbox-crash-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = 'test';
process.env.DB_PATH = testDbPath;
process.env.OFFLINE_CHECK_INTERVAL_MS = '60000';
process.env.ALERT_OUTBOX_INTERVAL_MS = '60000';

const db = require('../src/db');
const { validateReading, executeBatchTransaction, ingestBatch } = require('../src/ingestion');
const {
  processPendingOutboxEvents,
  startAlertOutboxWorker,
  stopAlertOutboxWorker
} = require('../src/alertOutboxWorker');

describe('Transactional Outbox: Alert Processing Crash Recovery', () => {
  after(() => {
    stopAlertOutboxWorker();
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  test('Schema verification: alert_outbox table and indexes exist after migration', () => {
    const table = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'alert_outbox'").get();
    assert.ok(table, 'alert_outbox table must exist');

    const columns = db.prepare("PRAGMA table_info(alert_outbox)").all();
    const colNames = columns.map(c => c.name);
    assert.ok(colNames.includes('claimed_at'), 'claimed_at column must exist for processing lease');

    const indexes = db.prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'alert_outbox'").all();
    const indexNames = indexes.map(i => i.name);
    assert.ok(indexNames.includes('idx_alert_outbox_status_id'), 'idx_alert_outbox_status_id must exist');
    assert.ok(indexNames.includes('idx_alert_outbox_vehicle_status'), 'idx_alert_outbox_vehicle_status must exist');
    assert.ok(indexNames.includes('idx_alert_outbox_status_claimed'), 'idx_alert_outbox_status_claimed must exist');
  });

  test('Simulate crash after telemetry insertion but before alert evaluation: worker recovery', () => {
    const vid = `VG-CRASH-${Date.now()}`;
    const now = Date.now();
    const nowIso = new Date(now).toISOString();

    // 3 consecutive hot readings > 55°C (which trigger overheat alert)
    const readings = [
      {
        vehicle_id: vid,
        seq: 1,
        ts: new Date(now - 15000).toISOString(),
        soc_pct: 80,
        battery_temp_c: 58.0,
        speed_kmh: 30,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 10,
        charging: false,
        fault_codes: []
      },
      {
        vehicle_id: vid,
        seq: 2,
        ts: new Date(now - 10000).toISOString(),
        soc_pct: 80,
        battery_temp_c: 58.5,
        speed_kmh: 30,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 10.1,
        charging: false,
        fault_codes: []
      },
      {
        vehicle_id: vid,
        seq: 3,
        ts: new Date(now - 5000).toISOString(),
        soc_pct: 80,
        battery_temp_c: 59.0,
        speed_kmh: 30,
        lat: 13.08,
        lng: 80.27,
        odometer_km: 10.2,
        charging: false,
        fault_codes: []
      }
    ];

    // Step 1: Execute atomic batch transaction (saves telemetry + outbox event in same transaction)
    const validItems = readings.map((reading, index) => ({ reading, index }));
    const txResult = executeBatchTransaction(validItems, nowIso);
    assert.strictEqual(txResult.accepted, 3);

    // Step 2: SIMULATE CRASH!
    // The application crashes here: evaluateAlertsForVehicles is NEVER called.
    // Verify telemetry is committed to DB:
    const historyRows = db.prepare('SELECT * FROM telemetry_history WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(historyRows.length, 3, 'Telemetry must be safely stored in database');

    // Verify outbox has a pending event:
    const pendingOutbox = db.prepare("SELECT * FROM alert_outbox WHERE vehicle_id = ? AND status = 'pending'").all(vid);
    assert.strictEqual(pendingOutbox.length, 1, 'Transactional outbox must contain pending event');
    assert.strictEqual(pendingOutbox[0].status, 'pending');

    // Verify alert is NOT yet evaluated (before worker recovery):
    const alertsBeforeRecovery = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alertsBeforeRecovery.length, 0, 'No alert should exist prior to recovery');

    // Step 3: Restart worker / trigger recovery
    const recoveryResult = processPendingOutboxEvents();
    assert.strictEqual(recoveryResult.processedCount, 1, 'Worker must process the 1 pending outbox event');

    // Step 4: Verify that the alert is eventually generated!
    const alertsAfterRecovery = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alertsAfterRecovery.length, 1, 'Overheat alert must be generated by worker');
    assert.strictEqual(alertsAfterRecovery[0].rule, 'overheat');
    assert.strictEqual(alertsAfterRecovery[0].status, 'open');

    // Step 5: Verify outbox row is marked completed
    const completedOutbox = db.prepare('SELECT * FROM alert_outbox WHERE id = ?').get(pendingOutbox[0].id);
    assert.strictEqual(completedOutbox.status, 'completed');
    assert.ok(completedOutbox.processed_at, 'processed_at must be populated');

    // Step 6: Verify idempotency / retry safety: second pass produces zero duplicate alerts
    const secondPass = processPendingOutboxEvents();
    assert.strictEqual(secondPass.processedCount, 0);
    const alertsFinal = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alertsFinal.length, 1, 'Must never produce duplicate alerts on subsequent worker passes');
  });

  test('Simulate crash with out-of-order resolved alert: worker recovers and records resolved alert', () => {
    const vid = `VG-CRASH-RESOLVED-${Date.now()}`;
    const now = Date.now();
    const nowIso = new Date(now).toISOString();

    // 6 readings: 3 hot (>55°C) followed by 3 cool (<=50°C)
    const readings = [
      { vehicle_id: vid, seq: 1, ts: new Date(now - 30000).toISOString(), soc_pct: 80, battery_temp_c: 56.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 2, ts: new Date(now - 25000).toISOString(), soc_pct: 80, battery_temp_c: 57.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 3, ts: new Date(now - 20000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 4, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 49.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.3, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 5, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 48.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.4, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 6, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 47.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.5, charging: false, fault_codes: [] }
    ];

    // Simulate crash after transaction commit
    const validItems = readings.map((reading, index) => ({ reading, index }));
    executeBatchTransaction(validItems, nowIso);

    // Verify 0 alerts prior to worker run
    assert.strictEqual(db.prepare('SELECT COUNT(*) as c FROM alerts WHERE vehicle_id = ?').get(vid).c, 0);

    // Worker restart
    processPendingOutboxEvents();

    // Verify resolved alert is generated
    const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alerts.length, 1);
    assert.strictEqual(alerts[0].status, 'resolved');
    assert.ok(alerts[0].opened_at);
    assert.ok(alerts[0].resolved_at);
  });

  test('Normal ingestion completes outbox event immediately without waiting for worker poll', () => {
    const vid = `VG-NORMAL-INGEST-${Date.now()}`;
    const now = Date.now();

    const readings = [
      { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 2, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 58.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 3, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 59.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
    ];

    // Full normal ingestion (without crash)
    const result = ingestBatch(readings);
    assert.strictEqual(result.accepted, 3);

    // Alert should be created immediately
    const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alerts.length, 1);
    assert.strictEqual(alerts[0].status, 'open');

    // Outbox event should be completed immediately
    const outboxRows = db.prepare('SELECT * FROM alert_outbox WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(outboxRows.length, 1);
    assert.strictEqual(outboxRows[0].status, 'completed');
    assert.ok(outboxRows[0].processed_at);

    // Background worker finds 0 pending events
    const workerResult = processPendingOutboxEvents();
    assert.strictEqual(workerResult.processedCount, 0);
  });

  test('Retry failed events safely without creating duplicate alerts', () => {
    const vid = `VG-RETRY-${Date.now()}`;
    const now = Date.now();
    const nowIso = new Date(now).toISOString();

    const readings = [
      { vehicle_id: vid, seq: 1, ts: new Date(now - 15000).toISOString(), soc_pct: 80, battery_temp_c: 58.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 2, ts: new Date(now - 10000).toISOString(), soc_pct: 80, battery_temp_c: 58.5, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.1, charging: false, fault_codes: [] },
      { vehicle_id: vid, seq: 3, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 59.0, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 10.2, charging: false, fault_codes: [] }
    ];

    const validItems = readings.map((reading, index) => ({ reading, index }));
    executeBatchTransaction(validItems, nowIso);

    // Manually mark attempts = 1, error = 'Simulated transient network/database timeout' to simulate previous failure
    db.prepare("UPDATE alert_outbox SET attempts = 1, error = 'Simulated timeout' WHERE vehicle_id = ?").run(vid);

    // Worker retries the pending event
    const retryResult = processPendingOutboxEvents();
    assert.strictEqual(retryResult.processedCount, 1, 'Worker must retry and successfully process event');

    // Confirm alert generated
    const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alerts.length, 1);
    assert.strictEqual(alerts[0].status, 'open');

    // Worker runs again on the same vehicle: verify idempotency
    const extraPass = processPendingOutboxEvents();
    assert.strictEqual(extraPass.processedCount, 0);
    const alertsAfter = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alertsAfter.length, 1, 'Must not duplicate alert on retry');
  });

  test('Rollback safety: if batch transaction rolls back, zero outbox rows are committed', () => {
    const vid = `VG-ROLLBACK-${Date.now()}`;
    const initialOutboxCount = db.prepare("SELECT COUNT(*) as c FROM alert_outbox").get().c;

    assert.throws(() => {
      db.transaction(() => {
        db.prepare(`
          INSERT INTO alert_outbox (vehicle_id, status, created_at)
          VALUES (?, 'pending', datetime('now'))
        `).run(vid);
        throw new Error('Simulated ingestion transaction abort');
      })();
    }, /Simulated ingestion transaction abort/);

    const afterCount = db.prepare("SELECT COUNT(*) as c FROM alert_outbox").get().c;
    assert.strictEqual(afterCount, initialOutboxCount, 'Aborted transaction must not commit outbox row');
  });

  test('Crash while in processing status: worker reclaims stale processing record after lease expires', () => {
    const vid = `VG-STALE-LEASE-${Date.now()}`;
    const now = Date.now();
    const staleTime = new Date(now - 60000).toISOString(); // Claimed 60 seconds ago

    // Insert 3 hot readings in telemetry_history for vid
    for (let seq = 1; seq <= 3; seq++) {
      db.prepare(`
        INSERT INTO telemetry_history (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, fault_codes, received_at)
        VALUES (?, ?, ?, 80, 58.0, 0, 13.08, 80.27, 100, 0, '[]', ?)
      `).run(vid, seq, new Date(now - 15000 + seq * 1000).toISOString(), new Date(now).toISOString());
    }

    // Insert outbox row stuck in 'processing' status with an expired lease timestamp
    const insertResult = db.prepare(`
      INSERT INTO alert_outbox (vehicle_id, status, attempts, created_at, claimed_at)
      VALUES (?, 'processing', 1, ?, ?)
    `).run(vid, staleTime, staleTime);
    const outboxId = insertResult.lastInsertRowid;

    // Verify alert does not exist yet
    assert.strictEqual(db.prepare('SELECT COUNT(*) as c FROM alerts WHERE vehicle_id = ?').get(vid).c, 0);

    // Run recovery worker with leaseMs = 30000 (claimed 60s ago > 30s threshold -> stale)
    const recoveryResult = processPendingOutboxEvents(100, new Date(now).toISOString(), 30000);
    assert.strictEqual(recoveryResult.processedCount, 1, 'Worker must reclaim and process the stale processing record');

    // Confirm alert was successfully generated
    const alerts = db.prepare('SELECT * FROM alerts WHERE vehicle_id = ?').all(vid);
    assert.strictEqual(alerts.length, 1);
    assert.strictEqual(alerts[0].rule, 'overheat');
    assert.strictEqual(alerts[0].status, 'open');

    // Confirm outbox row is marked completed with incremented attempts
    const row = db.prepare('SELECT * FROM alert_outbox WHERE id = ?').get(outboxId);
    assert.strictEqual(row.status, 'completed');
    assert.strictEqual(row.attempts, 2, 'Attempts must be incremented upon reclaiming');
    assert.ok(row.claimed_at, 'claimed_at must be updated');
    assert.ok(row.processed_at, 'processed_at must be populated');
  });

  test('Active processing lease is protected and not reclaimed before expiration', () => {
    const vid = `VG-ACTIVE-LEASE-${Date.now()}`;
    const now = Date.now();
    const freshClaimTime = new Date(now - 5000).toISOString(); // Claimed only 5s ago (active lease)

    // Insert outbox row with fresh active lease
    const insertResult = db.prepare(`
      INSERT INTO alert_outbox (vehicle_id, status, attempts, created_at, claimed_at)
      VALUES (?, 'processing', 1, ?, ?)
    `).run(vid, freshClaimTime, freshClaimTime);
    const outboxId = insertResult.lastInsertRowid;

    // Worker pass with leaseMs = 30000 (claimed 5s ago < 30s threshold -> NOT stale)
    const result = processPendingOutboxEvents(100, new Date(now).toISOString(), 30000);
    assert.strictEqual(result.processedCount, 0, 'Worker must NOT reclaim active lease');

    const row = db.prepare('SELECT * FROM alert_outbox WHERE id = ?').get(outboxId);
    assert.strictEqual(row.status, 'processing', 'Record must remain in processing status under active lease');
    assert.strictEqual(row.attempts, 1, 'Attempts must not be incremented');
  });

  test('Atomic lease claim prevents simultaneous processing by concurrent workers', () => {
    const vid = `VG-CONCUR-CLAIM-${Date.now()}`;
    const now = Date.now();
    const nowIso = new Date(now).toISOString();

    // Insert pending outbox row
    const insertResult = db.prepare(`
      INSERT INTO alert_outbox (vehicle_id, status, created_at)
      VALUES (?, 'pending', ?)
    `).run(vid, nowIso);
    const outboxId = insertResult.lastInsertRowid;

    // Worker 1 and Worker 2 both query pending rows and see outboxId
    const staleCutoff = new Date(now - 30000).toISOString();
    const claimStmt = db.prepare(`
      UPDATE alert_outbox
      SET status = 'processing',
          claimed_at = @now,
          attempts = attempts + 1
      WHERE id = @id
        AND (
          status = 'pending'
          OR (status = 'processing' AND (claimed_at IS NULL OR claimed_at <= @stale_cutoff))
        )
    `);

    // Worker 1 claims atomically
    const claim1 = claimStmt.run({ id: outboxId, now: nowIso, stale_cutoff: staleCutoff });
    assert.strictEqual(claim1.changes, 1, 'First worker must successfully claim record');

    // Worker 2 attempts to claim the same record simultaneously
    const claim2 = claimStmt.run({ id: outboxId, now: nowIso, stale_cutoff: staleCutoff });
    assert.strictEqual(claim2.changes, 0, 'Second worker must be locked out (changes = 0)');

    // Verify row state
    const row = db.prepare('SELECT * FROM alert_outbox WHERE id = ?').get(outboxId);
    assert.strictEqual(row.status, 'processing');
    assert.strictEqual(row.attempts, 1);
  });
});
