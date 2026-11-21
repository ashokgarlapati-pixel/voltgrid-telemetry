const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port
const testDbPath = path.join(os.tmpdir(), `voltgrid-a2-comp-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');
const { stopStreamWorker } = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('A2 Comprehensive Authentication and Abuse Protection Tests', () => {
  let baseUrl;
  let nonceSeq = 10000;

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

  function createSignedBatchRequest(readings, overrides = {}) {
    const rawBody = JSON.stringify(readings);
    const timestamp = overrides.timestamp !== undefined ? overrides.timestamp : (Date.now() + (nonceSeq++)).toString();
    const apiKey = overrides.apiKey !== undefined ? overrides.apiKey : API_KEY;
    const secret = overrides.secret !== undefined ? overrides.secret : API_SECRET;

    const signature = overrides.signature !== undefined
      ? overrides.signature
      : (secret ? crypto.createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex') : null);

    const headers = {
      'Content-Type': 'application/json'
    };

    if (apiKey !== null) headers['X-API-Key'] = apiKey;
    if (timestamp !== null) headers['X-Timestamp'] = timestamp;
    if (signature !== null) headers['X-Signature'] = signature;

    return { rawBody, headers };
  }

  async function sendBatch(readings, overrides = {}) {
    const { rawBody, headers } = createSignedBatchRequest(readings, overrides);
    const res = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers,
      body: rawBody
    });
    const body = await res.json().catch(() => ({}));
    return { status: res.status, headers: res.headers, body };
  }

  const sampleReading = [{
    vehicle_id: 'VG-SEC-01',
    seq: 1,
    ts: new Date().toISOString(),
    soc_pct: 80,
    battery_temp_c: 25,
    speed_kmh: 0,
    lat: 13.08,
    lng: 80.27,
    odometer_km: 1,
    charging: false,
    fault_codes: []
  }];

  test('Valid HMAC signature returns HTTP 200 OK', async () => {
    const res = await sendBatch(sampleReading);
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.accepted, 1);
  });

  test('Invalid HMAC signature (tampered body or bad signature) returns HTTP 401', async () => {
    // 1. Tampered signature string
    const res1 = await sendBatch(sampleReading, {
      signature: 'deadbeef00000000000000000000000000000000000000000000000000000000'
    });
    assert.strictEqual(res1.status, 401);
    assert.match(res1.body.error, /Invalid HMAC signature/);

    // 2. Tampered secret (wrong device group secret)
    const res2 = await sendBatch(sampleReading, {
      secret: 'wrong-group-secret'
    });
    assert.strictEqual(res2.status, 401);
    assert.match(res2.body.error, /Invalid HMAC signature/);
  });

  test('Missing authentication headers return HTTP 401', async () => {
    // Missing signature
    const noSigRes = await sendBatch(sampleReading, { signature: null });
    assert.strictEqual(noSigRes.status, 401);
    assert.match(noSigRes.body.error, /Missing required authentication headers/);

    // Missing API key
    const noKeyRes = await sendBatch(sampleReading, { apiKey: null });
    assert.strictEqual(noKeyRes.status, 401);
    assert.match(noKeyRes.body.error, /Missing required authentication headers/);

    // Missing timestamp
    const noTsRes = await sendBatch(sampleReading, { timestamp: null });
    assert.strictEqual(noTsRes.status, 401);
    assert.match(noTsRes.body.error, /Missing required authentication headers/);
  });

  test('Invalid or unknown API key returns HTTP 401', async () => {
    const res = await sendBatch(sampleReading, { apiKey: 'unknown-rogue-api-key' });
    assert.strictEqual(res.status, 401);
    assert.match(res.body.error, /Invalid or unknown API key/);
  });

  test('Timestamp outside allowed 5-minute window returns HTTP 401', async () => {
    const now = Date.now();

    // 1. 6 minutes in the past
    const pastTs = (now - 6 * 60 * 1000).toString();
    const pastRes = await sendBatch(sampleReading, { timestamp: pastTs });
    assert.strictEqual(pastRes.status, 401);
    assert.match(pastRes.body.error, /Request timestamp is outside the allowed 5-minute window/);

    // 2. 6 minutes in the future
    const futureTs = (now + 6 * 60 * 1000).toString();
    const futureRes = await sendBatch(sampleReading, { timestamp: futureTs });
    assert.strictEqual(futureRes.status, 401);
    assert.match(futureRes.body.error, /Request timestamp is outside the allowed 5-minute window/);
  });

  test('Invalid timestamp formats (non-numeric, garbage) return HTTP 401', async () => {
    const res = await sendBatch(sampleReading, { timestamp: 'not-a-valid-timestamp-xyz' });
    assert.strictEqual(res.status, 401);
    assert.match(res.body.error, /Request timestamp is outside the allowed 5-minute window/);
  });

  test('Reused nonce / replay attempt returns HTTP 401', async () => {
    const fixedTs = (Date.now() + 500).toString();
    const { rawBody, headers } = createSignedBatchRequest(sampleReading, { timestamp: fixedTs });

    // First attempt succeeds
    const firstRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers,
      body: rawBody
    });
    assert.strictEqual(firstRes.status, 200);

    // Replay attempt with exact same signature header returns 401
    const replayRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers,
      body: rawBody
    });
    assert.strictEqual(replayRes.status, 401);
    const replayBody = await replayRes.json();
    assert.match(replayBody.error, /Replayed request signature/);
  });

  test('Valid request succeeds immediately after authentication failure', async () => {
    // 1. Send invalid request (bad sig)
    const failRes = await sendBatch(sampleReading, { signature: 'invalid-hex' });
    assert.strictEqual(failRes.status, 401);

    // 2. Send valid request immediately after
    const vid = `VG-POST-FAIL-${Date.now()}`;
    const validRes = await sendBatch([{ ...sampleReading[0], vehicle_id: vid }]);
    assert.strictEqual(validRes.status, 200);
    assert.strictEqual(validRes.body.accepted, 1);
  });

  test('Rate limiting: requests exceeding burst capacity return HTTP 429 with Retry-After header', async () => {
    // Register a specific test API key with small burst capacity (5 requests)
    const limitedKey = `test-limited-${Date.now()}`;
    const limitedSecret = 'limited-test-secret';

    db.prepare(`
      INSERT INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
      VALUES (?, ?, 2, 5)
    `).run(limitedKey, limitedSecret);

    db.prepare(`
      INSERT INTO rate_limits (api_key, tokens, last_refill_ms)
      VALUES (?, 5, ?)
    `).run(limitedKey, Date.now());

    // Fire 10 rapid requests: first 5 should succeed, remaining should 429
    const results = [];
    for (let i = 0; i < 10; i++) {
      const res = await sendBatch(sampleReading, { apiKey: limitedKey, secret: limitedSecret });
      results.push(res);
    }

    const statuses = results.map(r => r.status);
    const count200 = statuses.filter(s => s === 200).length;
    const count429 = statuses.filter(s => s === 429).length;

    assert.ok(count200 >= 5, 'Must allow up to burst capacity');
    assert.ok(count429 >= 1, 'Must reject requests exceeding burst capacity with 429');

    // Check Retry-After header
    const sample429 = results.find(r => r.status === 429);
    const retryAfter = sample429.headers.get('retry-after');
    assert.ok(retryAfter, 'HTTP 429 must include Retry-After header');
    assert.ok(parseInt(retryAfter, 10) >= 1, 'Retry-After must be integer >= 1');
  });

  test('Token bucket refill behavior: tokens replenish over time', async () => {
    const refillKey = `test-refill-${Date.now()}`;
    const refillSecret = 'refill-test-secret';

    // 10 tokens/sec, burst capacity 2
    db.prepare(`
      INSERT INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
      VALUES (?, ?, 10, 2)
    `).run(refillKey, refillSecret);

    db.prepare(`
      INSERT INTO rate_limits (api_key, tokens, last_refill_ms)
      VALUES (?, 2, ?)
    `).run(refillKey, Date.now());

    // Drain both tokens
    const r1 = await sendBatch(sampleReading, { apiKey: refillKey, secret: refillSecret });
    const r2 = await sendBatch(sampleReading, { apiKey: refillKey, secret: refillSecret });
    assert.strictEqual(r1.status, 200);
    assert.strictEqual(r2.status, 200);

    // Immediate 3rd request should fail with 429
    const r3 = await sendBatch(sampleReading, { apiKey: refillKey, secret: refillSecret });
    assert.strictEqual(r3.status, 429);

    // Wait 250ms (at 10 tokens/sec, 250ms restores 2.5 tokens)
    await new Promise(r => setTimeout(r, 250));

    // Next request should succeed
    const r4 = await sendBatch(sampleReading, { apiKey: refillKey, secret: refillSecret });
    assert.strictEqual(r4.status, 200, 'Replenished token must allow subsequent request');
  });

  test('Sensitive authentication details are never exposed in error responses', async () => {
    const res = await sendBatch(sampleReading, { apiKey: 'invalid-key' });
    const bodyStr = JSON.stringify(res.body);
    assert.strictEqual(bodyStr.includes(API_SECRET), false, 'API secret must never appear in response body');
    assert.strictEqual(bodyStr.includes('sqlite'), false, 'Internal database details must not leak');
  });
});
