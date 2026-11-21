const { test, describe } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');

describe('Repeatable Load Test Verification Suite', () => {
  test('Native loadtest.js runs automated workload and outputs performance percentiles', { timeout: 30000 }, (t, done) => {
    const testDbPath = path.join(os.tmpdir(), `voltgrid-loadtest-smoke-${crypto.randomUUID()}.db`);
    const scriptPath = path.resolve(__dirname, '../../scripts/loadtest.js');

    const env = {
      ...process.env,
      DB_PATH: testDbPath,
      PORT: '0',
      DURATION_SEC: '1',
      WARMUP_SEC: '0',
      VUS: '2',
      BATCH_SIZE: '10'
    };

    execFile('node', [scriptPath], { env }, (err, stdout, stderr) => {
      // Clean up temp DB
      try {
        if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
        if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
        if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
      } catch (_) {}

      assert.ifError(err);
      assert.ok(stdout.includes('LOAD TEST RESULTS SUMMARY'), 'Must print load test results summary');
      assert.ok(stdout.includes('Overall Throughput'), 'Must report overall throughput');
      assert.ok(stdout.includes('AGGREGATED ALL ENDPOINTS'), 'Must report aggregate latency percentiles');
      assert.ok(stdout.includes('p50 (ms)'), 'Must report p50 latency');
      assert.ok(stdout.includes('p95 (ms)'), 'Must report p95 latency');
      assert.ok(stdout.includes('p99 (ms)'), 'Must report p99 latency');

      done();
    });
  });
});
