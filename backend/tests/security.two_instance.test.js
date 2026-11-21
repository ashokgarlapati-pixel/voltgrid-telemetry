const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

// Use isolated temporary SQLite database for this 2-instance test suite
const sharedDbPath = path.join(os.tmpdir(), `voltgrid-two-inst-${crypto.randomUUID()}.db`);

describe('Part A2: Two-Instance Shared SQLite Integration Tests', () => {
  let proc1, proc2;
  let port1, port2;
  let baseUrl1, baseUrl2;
  let testDb;

  // Helper to spawn an independent server process and await its listening port
  function startServerProcess(instanceName) {
    return new Promise((resolve, reject) => {
      const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
        env: {
          ...process.env,
          PORT: '0',
          DB_PATH: sharedDbPath,
          NODE_ENV: 'test'
        },
        stdio: ['ignore', 'pipe', 'pipe']
      });

      let stdout = '';
      let resolved = false;

      proc.stdout.on('data', chunk => {
        const text = chunk.toString();
        stdout += text;
        const match = text.match(/\[VoltGrid API\] Listening on port (\d+)/);
        if (match && !resolved) {
          resolved = true;
          const port = parseInt(match[1], 10);
          resolve({ proc, port });
        }
      });

      proc.stderr.on('data', chunk => {
        // Log warnings if any
      });

      proc.on('error', err => {
        if (!resolved) reject(err);
      });

      proc.on('exit', (code, signal) => {
        if (!resolved) {
          reject(new Error(`Server process ${instanceName} exited prematurely with code ${code} signal ${signal}. Output: ${stdout}`));
        }
      });
    });
  }

  before(async () => {
    // 1. Start Instance 1
    const inst1 = await startServerProcess('API-1');
    proc1 = inst1.proc;
    port1 = inst1.port;
    baseUrl1 = `http://127.0.0.1:${port1}`;

    // 2. Start Instance 2 (shares the exact same SQLite database file)
    const inst2 = await startServerProcess('API-2');
    proc2 = inst2.proc;
    port2 = inst2.port;
    baseUrl2 = `http://127.0.0.1:${port2}`;

    // 3. Open test database handle for direct state assertions
    let Database;
    try {
      Database = require('better-sqlite3');
      testDb = new Database(sharedDbPath);
    } catch (_) {
      const { DatabaseSync } = require('node:sqlite');
      testDb = new DatabaseSync(sharedDbPath);
    }
  });

  after(() => {
    try {
      if (proc1) proc1.kill();
      if (proc2) proc2.kill();
    } catch (_) {}
    try {
      if (testDb && typeof testDb.close === 'function') testDb.close();
      if (fs.existsSync(sharedDbPath)) fs.unlinkSync(sharedDbPath);
      if (fs.existsSync(`${sharedDbPath}-wal`)) fs.unlinkSync(`${sharedDbPath}-wal`);
      if (fs.existsSync(`${sharedDbPath}-shm`)) fs.unlinkSync(`${sharedDbPath}-shm`);
    } catch (_) {}
  });

  // Helper to post signed request to a target base URL
  let nonceSeq = 1;
  async function postSigned(baseUrl, payload, overrides = {}) {
    const rawBody = JSON.stringify(payload);
    const timestamp = overrides.timestamp !== undefined ? overrides.timestamp : (Date.now() + (nonceSeq++)).toString();
    const apiKey = overrides.apiKey !== undefined ? overrides.apiKey : API_KEY;
    const secret = overrides.secret !== undefined ? overrides.secret : API_SECRET;

    let signature = overrides.signature;
    if (signature === undefined) {
      signature = crypto.createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex');
    }

    const headers = {
      'Content-Type': 'application/json'
    };
    if (apiKey !== null) headers['X-API-Key'] = apiKey;
    if (timestamp !== null) headers['X-Timestamp'] = timestamp;
    if (signature !== null) headers['X-Signature'] = signature;

    const res = await fetch(`${baseUrl}/v1/telemetry/batch`, {
      method: 'POST',
      headers,
      body: rawBody
    });

    const body = await res.json().catch(() => ({}));
    return { status: res.status, headers: res.headers, body };
  }

  test('Both API instances run on separate ports sharing the exact same DB file', () => {
    assert.ok(port1 > 0, 'Instance 1 must have an allocated port');
    assert.ok(port2 > 0, 'Instance 2 must have an allocated port');
    assert.notStrictEqual(port1, port2, 'Instances must run on separate ports');
  });

  test('TEST 1: Shared Token Bucket across 2 instances (Capacity=40, Refill=0 -> 100 requests)', async () => {
    const testKey = `test-bucket-${Date.now()}`;
    const testSecret = 'bucket-secret';

    // Insert test key with burst_capacity = 40, rate_limit_per_sec = 0 (no refill)
    testDb.exec(`
      INSERT INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
      VALUES ('${testKey}', '${testSecret}', 0, 40)
    `);

    const payload = [
      { vehicle_id: 'VG-RATE-001', seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }
    ];

    // Fire 100 simultaneous requests: 50 sent to Instance 1, 50 sent to Instance 2
    const promises = [];
    for (let i = 0; i < 50; i++) {
      promises.push(postSigned(baseUrl1, payload, { apiKey: testKey, secret: testSecret }));
      promises.push(postSigned(baseUrl2, payload, { apiKey: testKey, secret: testSecret }));
    }

    const responses = await Promise.all(promises);

    let acceptedCount = 0;
    let rateLimitedCount = 0;

    for (const res of responses) {
      if (res.status === 200) {
        acceptedCount++;
      } else if (res.status === 429) {
        rateLimitedCount++;
        const retryAfter = res.headers.get('retry-after');
        assert.ok(retryAfter, 'HTTP 429 response must include Retry-After header');
        assert.ok(parseInt(retryAfter, 10) >= 1, 'Retry-After must be positive');
      }
    }

    assert.strictEqual(acceptedCount, 40, 'Combined accepted requests across both instances must equal exactly 40');
    assert.strictEqual(rateLimitedCount, 60, 'Combined rejected requests across both instances must equal exactly 60');

    // Direct database state verification
    let tokenRow;
    if (typeof testDb.prepare === 'function') {
      tokenRow = testDb.prepare('SELECT tokens FROM rate_limits WHERE api_key = ?').get(testKey);
    }
    if (tokenRow) {
      assert.strictEqual(Number(tokenRow.tokens), 0, 'Database tokens balance must equal 0 and never drop below zero');
    }
  });

  test('TEST 2: Shared Replay Protection across 2 instances', async () => {
    const payload = [
      { vehicle_id: `VG-REPLAY-${Date.now()}`, seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }
    ];
    const timestamp = Date.now().toString();
    const rawBody = JSON.stringify(payload);
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${rawBody}`).digest('hex');

    // 1. Send signed request to Instance 1 -> accepted (HTTP 200)
    const res1 = await postSigned(baseUrl1, payload, { timestamp, signature });
    assert.strictEqual(res1.status, 200, 'Original request to Instance 1 must be accepted');

    // 2. Re-send the exact same signed request to Instance 2 -> rejected (HTTP 401 Replayed request signature)
    const res2 = await postSigned(baseUrl2, payload, { timestamp, signature });
    assert.strictEqual(res2.status, 401, 'Replay of exact signature to Instance 2 must be rejected with 401');
    assert.strictEqual(res2.body.error, 'Replayed request signature');

    // 3. Simultaneous replay attack against both instances with an identical signature
    const simPayload = [{ vehicle_id: `VG-SIM-REPLAY-${Date.now()}`, seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];
    const simTimestamp = (Date.now() + 1000).toString();
    const simSig = crypto.createHmac('sha256', API_SECRET).update(`${simTimestamp}.${JSON.stringify(simPayload)}`).digest('hex');

    const simPromises = [
      postSigned(baseUrl1, simPayload, { timestamp: simTimestamp, signature: simSig }),
      postSigned(baseUrl1, simPayload, { timestamp: simTimestamp, signature: simSig }),
      postSigned(baseUrl2, simPayload, { timestamp: simTimestamp, signature: simSig }),
      postSigned(baseUrl2, simPayload, { timestamp: simTimestamp, signature: simSig })
    ];

    const simResponses = await Promise.all(simPromises);
    const simAccepted = simResponses.filter(r => r.status === 200).length;
    const simRejected = simResponses.filter(r => r.status === 401 && r.body.error === 'Replayed request signature').length;

    assert.strictEqual(simAccepted, 1, 'Exactly 1 request of simultaneous identical replays must succeed');
    assert.strictEqual(simRejected, 3, 'Remaining 3 simultaneous replays must be rejected');
  });

  test('TEST 3: Token Replenishment over time', async () => {
    const testKey = `test-refill-${Date.now()}`;
    const testSecret = 'refill-secret';

    // Key with burst_capacity = 2, refill rate = 10 tokens/sec
    testDb.exec(`
      INSERT INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
      VALUES ('${testKey}', '${testSecret}', 10, 2)
    `);

    const payload = [{ vehicle_id: 'VG-REFILL', seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];

    // Consume both tokens
    const r1 = await postSigned(baseUrl1, payload, { apiKey: testKey, secret: testSecret });
    const r2 = await postSigned(baseUrl2, payload, { apiKey: testKey, secret: testSecret });
    assert.strictEqual(r1.status, 200);
    assert.strictEqual(r2.status, 200);

    // 3rd request without delay -> 429
    const r3 = await postSigned(baseUrl1, payload, { apiKey: testKey, secret: testSecret });
    assert.strictEqual(r3.status, 429);
    assert.ok(r3.headers.get('retry-after') >= 1);

    // Wait 250ms (at 10 tokens/sec, 250ms refills 2.5 tokens, clamped to capacity 2)
    await new Promise(resolve => setTimeout(resolve, 250));

    // Next request to Instance 2 must succeed
    const r4 = await postSigned(baseUrl2, payload, { apiKey: testKey, secret: testSecret });
    assert.strictEqual(r4.status, 200, 'Request must succeed after token replenishment');
  });

  test('TEST 4: Concurrent requests across instances (checks token non-negativity & zero lost updates)', async () => {
    const testKey = `test-race-${Date.now()}`;
    const testSecret = 'race-secret';

    testDb.exec(`
      INSERT INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
      VALUES ('${testKey}', '${testSecret}', 0, 25)
    `);

    const payload = [{ vehicle_id: 'VG-CONC-RATE', seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];

    // Send 60 concurrent requests across both instances (30 to Instance 1, 30 to Instance 2)
    const promises = [];
    for (let i = 0; i < 30; i++) {
      promises.push(postSigned(baseUrl1, payload, { apiKey: testKey, secret: testSecret }));
      promises.push(postSigned(baseUrl2, payload, { apiKey: testKey, secret: testSecret }));
    }

    const responses = await Promise.all(promises);
    const accepted = responses.filter(r => r.status === 200).length;
    const rateLimited = responses.filter(r => r.status === 429).length;

    assert.strictEqual(accepted, 25, 'Exactly 25 requests must be accepted up to burst capacity');
    assert.strictEqual(rateLimited, 35, 'Remaining 35 requests must receive HTTP 429');

    // Verify token balance in SQLite is never negative
    let tokenRow;
    if (typeof testDb.prepare === 'function') {
      tokenRow = testDb.prepare('SELECT tokens FROM rate_limits WHERE api_key = ?').get(testKey);
    }
    if (tokenRow) {
      assert.strictEqual(Number(tokenRow.tokens), 0, 'Tokens must be zero and never negative');
    }
  });

  test('TEST 5: Authentication failure modes and invalid credentials', async () => {
    const payload = [{ vehicle_id: 'VG-AUTH-TEST', seq: 1, ts: new Date().toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 1, charging: false, fault_codes: [] }];

    // 1. Unknown API key
    const resUnknown = await postSigned(baseUrl1, payload, { apiKey: 'unknown-key' });
    assert.strictEqual(resUnknown.status, 401);
    assert.strictEqual(resUnknown.body.error, 'Invalid or unknown API key');

    // 2. Incorrect HMAC signature
    const resBadSig = await postSigned(baseUrl2, payload, { signature: 'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789' });
    assert.strictEqual(resBadSig.status, 401);
    assert.strictEqual(resBadSig.body.error, 'Invalid HMAC signature');

    // 3. Expired timestamp (> 5 minutes in the past)
    const expiredTs = (Date.now() - (6 * 60 * 1000)).toString();
    const resExpired = await postSigned(baseUrl1, payload, { timestamp: expiredTs });
    assert.strictEqual(resExpired.status, 401);
    assert.match(resExpired.body.error, /outside the allowed 5-minute window/);

    // 4. Future timestamp (> 5 minutes in the future)
    const futureTs = (Date.now() + (6 * 60 * 1000)).toString();
    const resFuture = await postSigned(baseUrl2, payload, { timestamp: futureTs });
    assert.strictEqual(resFuture.status, 401);
    assert.match(resFuture.body.error, /outside the allowed 5-minute window/);

    // 5. Missing headers
    const resNoHeaders = await fetch(`${baseUrl1}/v1/telemetry/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    assert.strictEqual(resNoHeaders.status, 401);

    // 6. Malformed signature (odd length or corrupt string)
    const resMalformed = await postSigned(baseUrl2, payload, { signature: 'xyz-not-hex' });
    assert.strictEqual(resMalformed.status, 401);
    assert.strictEqual(resMalformed.body.error, 'Invalid HMAC signature');
  });

  test('TEST 6: Telemetry Ingestion Idempotency across 2 instances', async () => {
    const vehicleId = `VG-TWO-DUP-${Date.now()}`;
    const reading = {
      vehicle_id: vehicleId,
      seq: 10,
      ts: new Date().toISOString(),
      soc_pct: 75.5,
      battery_temp_c: 32.0,
      speed_kmh: 45.0,
      lat: 13.0827,
      lng: 80.2707,
      odometer_km: 120.5,
      charging: false,
      fault_codes: []
    };

    // 1. Post original reading to Instance 1 -> accepted: 1, duplicates: 0
    const res1 = await postSigned(baseUrl1, [reading]);
    assert.strictEqual(res1.status, 200, 'Original batch to Instance 1 must return HTTP 200');
    assert.strictEqual(res1.body.summary.accepted, 1, 'Instance 1 must accept the fresh reading');
    assert.strictEqual(res1.body.summary.duplicates, 0, 'Instance 1 duplicate count must be 0');

    // 2. Post identical reading (same vehicle_id and seq) to Instance 2 with fresh timestamp/signature -> accepted: 0, duplicates: 1
    const res2 = await postSigned(baseUrl2, [reading]);
    assert.strictEqual(res2.status, 200, 'Duplicate batch to Instance 2 must return HTTP 200');
    assert.strictEqual(res2.body.summary.accepted, 0, 'Instance 2 must not accept duplicate reading');
    assert.strictEqual(res2.body.summary.duplicates, 1, 'Instance 2 must report duplicate count of 1');

    // 3. Direct SQLite verification: exactly 1 row persisted in telemetry_history
    let rowCount = 0;
    if (typeof testDb.prepare === 'function') {
      const row = testDb.prepare('SELECT COUNT(*) as count FROM telemetry_history WHERE vehicle_id = ? AND seq = ?').get(vehicleId, 10);
      rowCount = Number(row.count);
    }
    assert.strictEqual(rowCount, 1, 'Shared database must contain exactly 1 row in telemetry_history for (vehicle_id, seq)');

    // 4. Concurrent duplicate submission: fire identical reading simultaneously to both instances
    const concVehicle = `VG-CONC-DUP-${Date.now()}`;
    const concReading = {
      vehicle_id: concVehicle,
      seq: 1,
      ts: new Date().toISOString(),
      soc_pct: 82.0,
      battery_temp_c: 28.5,
      speed_kmh: 15.0,
      lat: 13.0820,
      lng: 80.2700,
      odometer_km: 50.0,
      charging: false,
      fault_codes: []
    };

    const [concRes1, concRes2] = await Promise.all([
      postSigned(baseUrl1, [concReading]),
      postSigned(baseUrl2, [concReading])
    ]);

    assert.strictEqual(concRes1.status, 200);
    assert.strictEqual(concRes2.status, 200);

    const totalAccepted = concRes1.body.summary.accepted + concRes2.body.summary.accepted;
    const totalDuplicates = concRes1.body.summary.duplicates + concRes2.body.summary.duplicates;

    assert.strictEqual(totalAccepted, 1, 'Exactly one concurrent request must be accepted');
    assert.strictEqual(totalDuplicates, 1, 'Exactly one concurrent request must be marked duplicate');

    let concDbCount = 0;
    if (typeof testDb.prepare === 'function') {
      const row = testDb.prepare('SELECT COUNT(*) as count FROM telemetry_history WHERE vehicle_id = ? AND seq = ?').get(concVehicle, 1);
      concDbCount = Number(row.count);
    }
    assert.strictEqual(concDbCount, 1, 'Shared database must contain exactly 1 row for concurrent duplicate');
  });
});
