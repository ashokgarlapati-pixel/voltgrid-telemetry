const { test, describe } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');

describe('Fleet Summary 5M Performance Benchmark Test Suite', () => {

  test('scripts/benchmark_5m.js executes end-to-end, enforces idempotency, and validates sub-150ms SLA', { timeout: 45000 }, (t, done) => {
    const testDbPath = path.join(os.tmpdir(), `voltgrid-bench-test-${crypto.randomUUID()}.db`);
    const scriptPath = path.resolve(__dirname, '../../scripts/benchmark_5m.js');

    const env = {
      ...process.env,
      BENCH_DB_PATH: testDbPath,
      PORT: '0',
      TARGET_READINGS: '10000',
      VEHICLES: '100',
      CONCURRENCY: '4',
      REQUESTS: '50',
      WARMUP: '10'
    };

    execFile('node', [scriptPath], { env }, (err, stdout, stderr) => {
      // Clean up temp DB
      try {
        if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
        if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
        if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
      } catch (_) {}

      assert.ifError(err);
      assert.ok(stdout.includes('OFFICIAL BENCHMARK RESULTS'), 'Must output official benchmark results header');
      assert.ok(stdout.includes('SLA Verification Target   : p95 < 150.0 ms'), 'Must verify sub-150ms SLA target');
      assert.ok(stdout.includes('SLA Target Status         : PASSED'), 'SLA Target must be PASSED');
      assert.ok(stdout.includes('Throughput'), 'Must report throughput');
      assert.ok(stdout.includes('p50 Latency'), 'Must report p50 latency');
      assert.ok(stdout.includes('p95 Latency'), 'Must report p95 latency');
      assert.ok(stdout.includes('p99 Latency'), 'Must report p99 latency');

      done();
    });
  });

  test('Idempotency verification: repeated execution on existing database skips regeneration and avoids duplicates', { timeout: 45000 }, (t, done) => {
    const testDbPath = path.join(os.tmpdir(), `voltgrid-bench-idempotent-${crypto.randomUUID()}.db`);
    const scriptPath = path.resolve(__dirname, '../../scripts/benchmark_5m.js');

    const env = {
      ...process.env,
      BENCH_DB_PATH: testDbPath,
      PORT: '0',
      TARGET_READINGS: '5000',
      VEHICLES: '50',
      CONCURRENCY: '2',
      REQUESTS: '20',
      WARMUP: '5'
    };

    // First run: generates 5,000 records
    execFile('node', [scriptPath], { env }, (err1, stdout1) => {
      assert.ifError(err1);
      assert.ok(stdout1.includes('Dataset generation complete'), 'First run must generate dataset');

      // Second run: should detect existing records and skip generation
      execFile('node', [scriptPath], { env }, (err2, stdout2) => {
        try {
          if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
          if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
          if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
        } catch (_) {}

        assert.ifError(err2);
        assert.ok(stdout2.includes('Skipping dataset generation to avoid duplicate data'), 'Second run must skip generation to prevent duplicate data');
        assert.ok(stdout2.includes('SLA Target Status         : PASSED'), 'Repeated run must also pass SLA');
        done();
      });
    });
  });

});
