const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port for observability tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-obs-test-${crypto.randomUUID()}.db`);
process.env.NODE_ENV = process.env.NODE_ENV || 'test';
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');
const { stopStreamWorker } = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('Part A6 Observability, Logging, Probes & Metrics Tests', () => {
  let baseUrl;

  before(async () => {
    const address = server.address();
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  after(() => {
    stopOfflineWorker();
    stopStreamWorker();
    server.close();
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  function signPayload(bodyStr, timestamp) {
    return crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${bodyStr}`).digest('hex');
  }

  test('Request ID Correlation: generates UUID v4 when incoming X-Request-Id is missing', async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert.strictEqual(res.status, 200);

    const reqId = res.headers.get('x-request-id');
    assert.ok(reqId, 'Must return X-Request-Id header');
    // UUID v4 regex validation
    assert.match(
      reqId,
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      'Generated request ID must be a valid UUID v4'
    );
  });

  test('Request ID Correlation: preserves and propagates incoming X-Request-Id', async () => {
    const customId = 'client-trace-id-xyz-98765';
    const res = await fetch(`${baseUrl}/health`, {
      headers: {
        'x-request-id': customId
      }
    });
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.headers.get('x-request-id'), customId, 'Must propagate custom X-Request-Id');
  });

  test('Structured JSON Logging: outputs single-line JSON log without leaking credentials', async () => {
    const originalLog = console.log;
    const capturedLogs = [];
    console.log = (...args) => {
      capturedLogs.push(args.join(' '));
      originalLog.apply(console, args);
    };

    try {
      const customTrace = `trace-log-${crypto.randomUUID()}`;
      const res = await fetch(`${baseUrl}/health/live`, {
        headers: {
          'x-request-id': customTrace,
          'authorization': 'Bearer super-secret-token',
          'x-signature': 'super-secret-hmac'
        }
      });
      assert.strictEqual(res.status, 200);

      // Give event loop a microtick to fire res.on('finish')
      await new Promise(r => setTimeout(r, 50));

      const matchingLog = capturedLogs.find(l => {
        try {
          const parsed = JSON.parse(l);
          return parsed.request_id === customTrace;
        } catch (_) {
          return false;
        }
      });

      assert.ok(matchingLog, 'Must emit structured JSON log containing request_id');
      const parsed = JSON.parse(matchingLog);
      assert.ok(parsed.ts, 'Log must contain ISO timestamp');
      assert.strictEqual(parsed.level, 'info');
      assert.strictEqual(parsed.method, 'GET');
      assert.strictEqual(parsed.path, '/health/live');
      assert.strictEqual(parsed.status, 200);
      assert.ok(typeof parsed.duration_ms === 'number', 'Log must contain duration_ms');

      // Verify no sensitive credentials leaked
      assert.strictEqual(matchingLog.includes('super-secret-token'), false, 'Must not leak Authorization header');
      assert.strictEqual(matchingLog.includes('super-secret-hmac'), false, 'Must not leak X-Signature header');
    } finally {
      console.log = originalLog;
    }
  });

  test('Liveness Probes: GET /health and GET /health/live return 200 OK', async () => {
    const resRoot = await fetch(`${baseUrl}/health`);
    assert.strictEqual(resRoot.status, 200);
    const dataRoot = await resRoot.json();
    assert.strictEqual(dataRoot.status, 'ok');
    assert.ok(typeof dataRoot.uptime_sec === 'number');

    const resLive = await fetch(`${baseUrl}/health/live`);
    assert.strictEqual(resLive.status, 200);
    const dataLive = await resLive.json();
    assert.strictEqual(dataLive.status, 'ok');
    assert.ok(typeof dataLive.uptime_sec === 'number');
  });

  test('Readiness Probe: GET /health/ready returns 200 when SQLite database is connected', async () => {
    const res = await fetch(`${baseUrl}/health/ready`);
    assert.strictEqual(res.status, 200);
    const data = await res.json();
    assert.strictEqual(data.status, 'ready');
    assert.strictEqual(data.database, 'connected');
    assert.ok(data.timestamp);
  });

  test('Readiness Probe: returns 503 Service Unavailable when DB connectivity probe fails', async () => {
    const originalPrepare = db.prepare;
    try {
      // Temporarily mock db.prepare to throw error for readiness probe
      db.prepare = (sql) => {
        if (sql.includes('SELECT 1')) {
          return {
            get: () => { throw new Error('Simulated SQLite disk I/O lock failure'); }
          };
        }
        return originalPrepare.call(db, sql);
      };

      const res = await fetch(`${baseUrl}/health/ready`);
      assert.strictEqual(res.status, 503);
      const data = await res.json();
      assert.strictEqual(data.status, 'not_ready');
      assert.ok(data.error);
    } finally {
      db.prepare = originalPrepare;
    }
  });

  test('Process Metrics: GET /metrics returns comprehensive operational metrics', async () => {
    const res = await fetch(`${baseUrl}/metrics`);
    assert.strictEqual(res.status, 200);
    const metrics = await res.json();

    // Verify schema structure
    assert.ok(metrics.process, 'Must have process metrics');
    assert.ok(typeof metrics.process.uptime_sec === 'number');
    assert.ok(typeof metrics.process.memory_usage_mb === 'number');
    assert.strictEqual(metrics.process.scope, 'process-local');

    assert.ok(metrics.requests, 'Must have requests metrics');
    assert.ok(typeof metrics.requests.total === 'number');
    assert.ok(typeof metrics.requests.errors === 'number');
    assert.ok(typeof metrics.requests.status_codes === 'object');
    assert.ok(typeof metrics.requests.by_endpoint === 'object');

    assert.ok(metrics.latency_ms, 'Must have latency metrics');
    assert.ok(typeof metrics.latency_ms.p50 === 'number');
    assert.ok(typeof metrics.latency_ms.p95 === 'number');
    assert.ok(typeof metrics.latency_ms.p99 === 'number');

    assert.ok(metrics.telemetry, 'Must have telemetry metrics');
    assert.ok(typeof metrics.telemetry.readings_accepted_total === 'number');
    assert.ok(typeof metrics.telemetry.readings_duplicates_total === 'number');
    assert.ok(typeof metrics.telemetry.readings_rejected_total === 'number');

    assert.ok(metrics.security, 'Must have security metrics');
    assert.ok(typeof metrics.security.auth_failures_total === 'number');
    assert.ok(typeof metrics.security.rate_limit_rejections_total === 'number');

    assert.ok(metrics.stream, 'Must have stream metrics');
    assert.ok(typeof metrics.stream.active_sse_connections === 'number');

    assert.ok(metrics.database, 'Must have database aggregates');
    assert.ok(typeof metrics.database.vehicles_total === 'number');
    assert.ok(typeof metrics.database.history_total === 'number');
    assert.ok(typeof metrics.database.active_alerts_total === 'number');
  });

  test('Metrics Integration: increments counters on auth failures and telemetry ingestion', async () => {
    const preRes = await (await fetch(`${baseUrl}/metrics`)).json();
    const preAuthFails = preRes.security.auth_failures_total;
    const preAccepted = preRes.telemetry.readings_accepted_total;

    // 1. Trigger an auth failure (invalid API key)
    const authFailRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': 'invalid-bogus-key',
        'x-timestamp': Date.now().toString(),
        'x-signature': '0000000000000000000000000000000000000000000000000000000000000000'
      },
      body: JSON.stringify([{ vehicle_id: 'VG-METRIC-01', seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 30, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] }])
    });
    assert.strictEqual(authFailRes.status, 401);

    // 2. Perform a valid batch ingestion
    const validBody = JSON.stringify([
      { vehicle_id: 'VG-METRIC-01', seq: 10, ts: new Date().toISOString(), soc_pct: 85, battery_temp_c: 26, speed_kmh: 40, lat: 13.082, lng: 80.271, odometer_km: 105, charging: false, fault_codes: [] },
      { vehicle_id: 'VG-METRIC-02', seq: 10, ts: new Date().toISOString(), soc_pct: 75, battery_temp_c: 28, speed_kmh: 45, lat: 13.085, lng: 80.275, odometer_km: 210, charging: false, fault_codes: [] }
    ]);
    const timestamp = Date.now().toString();
    const sig = signPayload(validBody, timestamp);

    const ingestRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY,
        'x-timestamp': timestamp,
        'x-signature': sig
      },
      body: validBody
    });
    assert.strictEqual(ingestRes.status, 200);

    // 3. Verify updated metrics
    const postRes = await (await fetch(`${baseUrl}/metrics`)).json();
    assert.strictEqual(postRes.security.auth_failures_total, preAuthFails + 1, 'Auth failure counter must increment by 1');
    assert.strictEqual(postRes.telemetry.readings_accepted_total, preAccepted + 2, 'Telemetry accepted counter must increment by 2');
  });

  test('Prometheus Exposition Format: GET /metrics with text/plain or ?format=prometheus returns standard Prometheus format', async () => {
    // 1. Request with Accept: text/plain
    const textRes = await fetch(`${baseUrl}/metrics`, {
      headers: { 'Accept': 'text/plain; version=0.0.4' }
    });
    assert.strictEqual(textRes.status, 200);
    assert.ok(textRes.headers.get('content-type')?.includes('text/plain'), 'Content-Type must be text/plain');
    const textBody = await textRes.text();

    assert.ok(textBody.includes('# HELP voltgrid_uptime_seconds'), 'Must contain uptime help comment');
    assert.ok(textBody.includes('# TYPE voltgrid_uptime_seconds gauge'), 'Must contain uptime type comment');
    assert.match(textBody, /^voltgrid_uptime_seconds \d+/m, 'Must output uptime metric value');

    assert.ok(textBody.includes('# HELP voltgrid_requests_total'), 'Must contain requests total help');
    assert.ok(textBody.includes('# TYPE voltgrid_requests_total counter'), 'Must contain requests total type');
    assert.match(textBody, /^voltgrid_requests_total \d+/m, 'Must output requests total value');

    assert.ok(textBody.includes('# HELP voltgrid_request_duration_ms'), 'Must contain duration quantile help');
    assert.ok(textBody.includes('voltgrid_request_duration_ms{quantile="0.95"}'), 'Must output p95 quantile');

    assert.ok(textBody.includes('voltgrid_readings_accepted_total'), 'Must output telemetry accepted metric');
    assert.ok(textBody.includes('voltgrid_database_vehicles_total'), 'Must output database vehicles metric');

    // 2. Request with ?format=prometheus query parameter
    const queryRes = await fetch(`${baseUrl}/metrics?format=prometheus`);
    assert.strictEqual(queryRes.status, 200);
    assert.ok(queryRes.headers.get('content-type')?.includes('text/plain'), 'Content-Type must be text/plain for ?format=prometheus');
    const queryBody = await queryRes.text();
    assert.ok(queryBody.includes('voltgrid_uptime_seconds'), 'Query param format must output Prometheus metrics');
  });
});
