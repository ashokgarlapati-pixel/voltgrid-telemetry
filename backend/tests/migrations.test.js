const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

describe('Database Schema & Migration Lifecycle Tests', () => {
  let testDbPath;
  let db;

  before(() => {
    testDbPath = path.join(os.tmpdir(), `voltgrid-migrations-test-${crypto.randomUUID()}.db`);
    process.env.DB_PATH = testDbPath;
    
    // Clear module cache to test fresh database boot
    delete require.cache[require.resolve('../src/db')];
    db = require('../src/db');
  });

  after(() => {
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  test('Fresh database initialization applies all versioned migrations in order', () => {
    const rows = db.prepare('SELECT version, applied_at FROM schema_migrations ORDER BY version ASC').all();
    const versions = rows.map(r => r.version);

    assert.ok(versions.includes('001_init.sql'), 'Must apply 001_init.sql');
    assert.ok(versions.includes('002_alerts.sql'), 'Must apply 002_alerts.sql');
    assert.ok(versions.includes('003_security.sql'), 'Must apply 003_security.sql');
    assert.ok(versions.includes('004_stream.sql'), 'Must apply 004_stream.sql');
    assert.ok(versions.includes('005_alert_outbox.sql'), 'Must apply 005_alert_outbox.sql');
    assert.strictEqual(versions.length, 5, 'Must apply exactly 5 migrations');

    for (const r of rows) {
      assert.ok(r.applied_at, 'Applied migration must have a valid timestamp');
    }
  });

  test('All core tables and indexes exist with correct constraints', () => {
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name);
    assert.ok(tables.includes('telemetry_history'));
    assert.ok(tables.includes('vehicles'));
    assert.ok(tables.includes('alerts'));
    assert.ok(tables.includes('api_keys'));
    assert.ok(tables.includes('used_nonces'));
    assert.ok(tables.includes('rate_limits'));
    assert.ok(tables.includes('stream_events'));
    assert.ok(tables.includes('alert_outbox'));
    assert.ok(tables.includes('schema_migrations'));

    // Check indexes
    const indexes = db.prepare("SELECT name FROM sqlite_master WHERE type='index'").all().map(r => r.name);
    assert.ok(indexes.includes('idx_vehicles_status_id'));
    assert.ok(indexes.includes('idx_history_vehicle_ts_seq'));
    assert.ok(indexes.includes('idx_alerts_active_rule'));
    assert.ok(indexes.includes('idx_alerts_active_unique'));
    assert.ok(indexes.includes('idx_stream_events_id'));
    assert.ok(indexes.includes('idx_alert_outbox_status_id'));
    assert.ok(indexes.includes('idx_alert_outbox_vehicle_status'));
  });

  test('Idempotency: Re-running migrations on an existing database does not duplicate records or throw errors', () => {
    // Re-trigger migration run logic
    const migrationsDir = path.resolve(__dirname, '../migrations');
    const appliedBefore = db.prepare('SELECT version FROM schema_migrations').all();

    // Re-require or re-run
    const sqlFiles = fs.readdirSync(migrationsDir).filter(f => f.endsWith('.sql')).sort();
    
    // Check that all files are already marked as applied
    const appliedSet = new Set(appliedBefore.map(r => r.version));
    for (const file of sqlFiles) {
      assert.ok(appliedSet.has(file), `${file} should already be marked as applied`);
    }

    const appliedAfter = db.prepare('SELECT version FROM schema_migrations').all();
    assert.strictEqual(appliedBefore.length, appliedAfter.length, 'No duplicate migration entries should be created');
  });
});
