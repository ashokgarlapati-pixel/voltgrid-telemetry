const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');

// Use isolated temporary SQLite database and ephemeral port
const testDbPath = path.join(os.tmpdir(), `voltgrid-a1-comp-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');
const { stopStreamWorker } = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

describe('A1 Comprehensive Telemetry Ingestion Tests', () => {
  let baseUrl;
  let nonceSeq = 100;

  before(async () => {
    // Elevate rate limits for testing
    db.prepare('UPDATE api_keys SET rate_limit_per_sec = 50000, burst_capacity = 50000 WHERE api_key = ?').run(API_KEY);
    db.prepare('INSERT OR REPLACE INTO rate_limits (api_key, tokens, last_refill_ms) VALUES (?, 50000, ?)').run(API_KEY, Date.now());

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

  async function postSignedBatch(readings) {
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

    const body = await res.json().catch(() => ({}));
    return { status: res.status, headers: res.headers, body };
  }

  function createValidReading(vehicleId, seq = 1, ts = null) {
    const defaultTs = new Date().toISOString();
    return {
      vehicle_id: vehicleId,
      seq,
      ts: ts || defaultTs,
      soc_pct: 85.0,
      battery_temp_c: 30.0,
      speed_kmh: 35.0,
      lat: 13.0827,
      lng: 80.2707,
      odometer_km: 120.5,
      charging: false,
      fault_codes: []
    };
  }

  test('Valid single reading and batch return 200 OK with correct summary', async () => {
    const vid = `VG-VALID-${Date.now()}`;
    const reading = createValidReading(vid, 1);

    const res = await postSignedBatch([reading]);
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.total, 1);
    assert.strictEqual(res.body.accepted, 1);
    assert.strictEqual(res.body.duplicates, 0);
    assert.strictEqual(res.body.rejected, 0);
    assert.strictEqual(res.body.summary.accepted, 1);
    assert.strictEqual(res.body.results.length, 1);
    assert.strictEqual(res.body.results[0].status, 'accepted');
    assert.strictEqual(res.body.results[0].index, 0);

    // Verify stored in DB
    const historyRow = db.prepare('SELECT * FROM telemetry_history WHERE vehicle_id = ? AND seq = 1').get(vid);
    assert.ok(historyRow, 'Row must exist in telemetry_history');
    assert.strictEqual(historyRow.soc_pct, 85.0);

    const vehicleRow = db.prepare('SELECT * FROM vehicles WHERE vehicle_id = ?').get(vid);
    assert.ok(vehicleRow, 'Row must exist in vehicles');
    assert.strictEqual(vehicleRow.last_seq, 1);
    assert.strictEqual(vehicleRow.status, 'online');
  });

  test('Empty batch returns HTTP 400 Bad Request', async () => {
    const res = await postSignedBatch([]);
    assert.strictEqual(res.status, 400);
    assert.match(res.body.error, /Batch size must be between 1 and 500/);
  });

  test('Batch containing exactly maximum supported readings (500) succeeds', async () => {
    const prefix = `VG-MAX500-${Date.now()}`;
    const batch = Array.from({ length: 500 }, (_, i) => createValidReading(`${prefix}-${i}`, 1));

    const res = await postSignedBatch(batch);
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.total, 500);
    assert.strictEqual(res.body.accepted, 500);
    assert.strictEqual(res.body.rejected, 0);
  });

  test('Batch exceeding maximum size (501 readings) returns HTTP 400', async () => {
    const prefix = `VG-OVER501-${Date.now()}`;
    const batch = Array.from({ length: 501 }, (_, i) => createValidReading(`${prefix}-${i}`, 1));

    const res = await postSignedBatch(batch);
    assert.strictEqual(res.status, 400);
    assert.match(res.body.error, /Batch size must be between 1 and 500/);
  });

  test('Missing required fields: rejects each individual missing field with specific error', async () => {
    const requiredFields = [
      'vehicle_id', 'seq', 'ts', 'soc_pct', 'battery_temp_c',
      'speed_kmh', 'lat', 'lng', 'odometer_km', 'charging', 'fault_codes'
    ];

    const testReadings = requiredFields.map(field => {
      const reading = createValidReading(`VG-MISSING-${field}`, 1);
      delete reading[field];
      return reading;
    });

    const res = await postSignedBatch(testReadings);
    assert.strictEqual(res.status, 207); // Partial success / multi-status (all rejected)
    assert.strictEqual(res.body.rejected, requiredFields.length);
    assert.strictEqual(res.body.accepted, 0);

    for (let i = 0; i < requiredFields.length; i++) {
      const result = res.body.results[i];
      assert.strictEqual(result.status, 'rejected');
      assert.strictEqual(result.index, i);
      assert.ok(result.error, `Must report error for missing ${requiredFields[i]}`);
    }
  });

  test('Invalid vehicle identifiers: rejects empty string, whitespace only, and non-strings', async () => {
    const invalidIds = ['', '   ', null, undefined, 12345, {}];
    const batch = invalidIds.map((badId, idx) => {
      const r = createValidReading('VG-TEMP', idx + 1);
      r.vehicle_id = badId;
      return r;
    });

    const res = await postSignedBatch(batch);
    assert.strictEqual(res.status, 207);
    assert.strictEqual(res.body.rejected, invalidIds.length);
    for (const r of res.body.results) {
      assert.strictEqual(r.status, 'rejected');
      assert.match(r.error, /vehicle_id must be a non-empty string/);
    }
  });

  test('Invalid timestamps: rejects unparseable strings, invalid dates, and non-strings', async () => {
    const badTimestamps = ['invalid-date', '2026-99-99T99:99:99Z', '', 1732000000000, null];
    const batch = badTimestamps.map((badTs, idx) => {
      const r = createValidReading(`VG-TS-${idx}`, 1);
      r.ts = badTs;
      return r;
    });

    const res = await postSignedBatch(batch);
    assert.strictEqual(res.status, 207);
    assert.strictEqual(res.body.rejected, badTimestamps.length);
    for (const r of res.body.results) {
      assert.strictEqual(r.status, 'rejected');
      assert.match(r.error, /ts must be a valid ISO timestamp string/);
    }
  });

  test('Invalid numeric ranges: rejects NaN, Infinity, and out-of-boundary numbers', async () => {
    const invalidNumericCases = [
      { field: 'seq', val: -1, msg: /seq must be a non-negative integer/ },
      { field: 'seq', val: 1.5, msg: /seq must be a non-negative integer/ },
      { field: 'soc_pct', val: -0.1, msg: /soc_pct must be a finite number between 0 and 100/ },
      { field: 'soc_pct', val: 100.1, msg: /soc_pct must be a finite number between 0 and 100/ },
      { field: 'soc_pct', val: NaN, msg: /soc_pct must be a finite number between 0 and 100/ },
      { field: 'soc_pct', val: Infinity, msg: /soc_pct must be a finite number between 0 and 100/ },
      { field: 'battery_temp_c', val: -40.1, msg: /battery_temp_c must be a finite number between -40 and 120/ },
      { field: 'battery_temp_c', val: 120.1, msg: /battery_temp_c must be a finite number between -40 and 120/ },
      { field: 'speed_kmh', val: -0.5, msg: /speed_kmh must be a finite number between 0 and 200/ },
      { field: 'speed_kmh', val: 200.5, msg: /speed_kmh must be a finite number between 0 and 200/ },
      { field: 'lat', val: -90.1, msg: /lat must be a finite number between -90 and 90/ },
      { field: 'lat', val: 90.1, msg: /lat must be a finite number between -90 and 90/ },
      { field: 'lng', val: -180.1, msg: /lng must be a finite number between -180 and 180/ },
      { field: 'lng', val: 180.1, msg: /lng must be a finite number between -180 and 180/ },
      { field: 'odometer_km', val: -1, msg: /odometer_km must be a finite non-negative number/ },
      { field: 'charging', val: 'true', msg: /charging must be a boolean/ },
      { field: 'fault_codes', val: 'P0A80', msg: /fault_codes must be an array of strings/ },
      { field: 'fault_codes', val: ['P0A80', 123], msg: /fault_codes must be an array of strings/ }
    ];

    const batch = invalidNumericCases.map((c, idx) => {
      const r = createValidReading(`VG-NUM-${idx}`, 1);
      r[c.field] = c.val;
      return r;
    });

    const res = await postSignedBatch(batch);
    assert.strictEqual(res.status, 207);
    assert.strictEqual(res.body.rejected, invalidNumericCases.length);

    for (let i = 0; i < invalidNumericCases.length; i++) {
      const r = res.body.results[i];
      assert.strictEqual(r.status, 'rejected');
      assert.match(r.error, invalidNumericCases[i].msg);
    }
  });

  test('Null and unexpected data types: rejects non-object items in batch', async () => {
    const badItems = [null, undefined, 'some-string', 12345, true, []];
    const res = await postSignedBatch(badItems);
    assert.strictEqual(res.status, 207);
    assert.strictEqual(res.body.rejected, badItems.length);
    for (const r of res.body.results) {
      assert.strictEqual(r.status, 'rejected');
      assert.match(r.error, /Reading must be a non-null object/);
    }
  });

  test('Duplicate readings within same batch: accepts first, marks second duplicate', async () => {
    const vid = `VG-IN-BATCH-DUP-${Date.now()}`;
    const now = Date.now();
    const r1 = createValidReading(vid, 1, new Date(now - 5000).toISOString());
    const r2 = { ...createValidReading(vid, 1, new Date(now - 5000).toISOString()), soc_pct: 82.0 }; // duplicate seq 1

    const res = await postSignedBatch([r1, r2]);
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.total, 2);
    assert.strictEqual(res.body.accepted, 1);
    assert.strictEqual(res.body.duplicates, 1);
    assert.strictEqual(res.body.rejected, 0);

    assert.strictEqual(res.body.results[0].status, 'accepted');
    assert.strictEqual(res.body.results[1].status, 'duplicate');

    // DB has only 1 row
    const count = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(Number(count.total), 1);
  });

  test('Repeated requests with same readings: database idempotency', async () => {
    const vid = `VG-REPEATED-${Date.now()}`;
    const now = Date.now();
    const batch = [
      createValidReading(vid, 1, new Date(now - 5000).toISOString()),
      createValidReading(vid, 2, new Date(now - 4000).toISOString()),
      createValidReading(vid, 3, new Date(now - 3000).toISOString())
    ];

    // First call: all 3 accepted
    const firstRes = await postSignedBatch(batch);
    assert.strictEqual(firstRes.status, 200);
    assert.strictEqual(firstRes.body.accepted, 3);
    assert.strictEqual(firstRes.body.duplicates, 0);

    // Second call with same readings: all 3 marked duplicate
    const secondRes = await postSignedBatch(batch);
    assert.strictEqual(secondRes.status, 200);
    assert.strictEqual(secondRes.body.accepted, 0);
    assert.strictEqual(secondRes.body.duplicates, 3);

    // Database still has exactly 3 rows
    const count = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(Number(count.total), 3);

    const vRow = db.prepare('SELECT received_count FROM vehicles WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(vRow.received_count, 3, 'received_count must not double count duplicates');
  });

  test('Out-of-order telemetry: sequences arriving [3, 1, 2] are all stored and min/max seq correct', async () => {
    const vid = `VG-OOO-SEQ-${Date.now()}`;
    const now = Date.now();
    const r3 = createValidReading(vid, 3, new Date(now - 3000).toISOString());
    const r1 = createValidReading(vid, 1, new Date(now - 5000).toISOString());
    const r2 = createValidReading(vid, 2, new Date(now - 4000).toISOString());

    await postSignedBatch([r3]);
    await postSignedBatch([r1]);
    await postSignedBatch([r2]);

    const vRow = db.prepare('SELECT min_seq, max_seq, received_count FROM vehicles WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(vRow.min_seq, 1);
    assert.strictEqual(vRow.max_seq, 3);
    assert.strictEqual(vRow.received_count, 3);

    const historyRows = db.prepare('SELECT seq FROM telemetry_history WHERE vehicle_id = ? ORDER BY seq ASC').all(vid);
    assert.deepStrictEqual(historyRows.map(r => r.seq), [1, 2, 3]);
  });

  test('Late-arriving reading: older timestamp never overwrites newer vehicle state', async () => {
    const vid = `VG-LATE-${Date.now()}`;
    const now = Date.now();
    const newerTs = new Date(now - 1000).toISOString();
    const olderTs = new Date(now - 10000).toISOString();
    const newer = createValidReading(vid, 10, newerTs);
    newer.soc_pct = 70.0;
    newer.speed_kmh = 45.0;

    const older = createValidReading(vid, 8, olderTs);
    older.soc_pct = 95.0;
    older.speed_kmh = 10.0;

    await postSignedBatch([newer]);
    await postSignedBatch([older]);

    const vRow = db.prepare('SELECT soc_pct, speed_kmh, last_ts, last_seq FROM vehicles WHERE vehicle_id = ?').get(vid);
    assert.strictEqual(vRow.soc_pct, 70.0, 'Live state must not be overwritten by older reading');
    assert.strictEqual(vRow.speed_kmh, 45.0);
    assert.strictEqual(vRow.last_ts, newerTs);
    assert.strictEqual(vRow.last_seq, 10);
  });

  describe('Issue 1: Telemetry Timestamp Window Validation [T - 10m, T + 30s]', () => {
    test('A reading with a current timestamp is accepted', async () => {
      const vid = `VG-TS-CUR-${Date.now()}`;
      const r = createValidReading(vid, 1, new Date().toISOString());
      const res = await postSignedBatch([r]);
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.body.accepted, 1);
      assert.strictEqual(res.body.rejected, 0);
    });

    test('A reading up to 10 minutes old is accepted', async () => {
      const vid = `VG-TS-10M-${Date.now()}`;
      const r = createValidReading(vid, 1, new Date(Date.now() - 10 * 60 * 1000 + 2000).toISOString());
      const res = await postSignedBatch([r]);
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.body.accepted, 1);
      assert.strictEqual(res.body.rejected, 0);
    });

    test('A reading more than 10 minutes old (e.g. 11 minutes) is rejected', async () => {
      const vid = `VG-TS-11M-${Date.now()}`;
      const r = createValidReading(vid, 1, new Date(Date.now() - 11 * 60 * 1000).toISOString());
      const res = await postSignedBatch([r]);
      assert.strictEqual(res.status, 207);
      assert.strictEqual(res.body.accepted, 0);
      assert.strictEqual(res.body.rejected, 1);
      assert.strictEqual(res.body.results[0].status, 'rejected');
      assert.strictEqual(res.body.results[0].error, 'ts cannot be older than 10 minutes');
    });

    test('A reading up to 30 seconds in the future is accepted', async () => {
      const vid = `VG-TS-30S-${Date.now()}`;
      const r = createValidReading(vid, 1, new Date(Date.now() + 20 * 1000).toISOString());
      const res = await postSignedBatch([r]);
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.body.accepted, 1);
      assert.strictEqual(res.body.rejected, 0);
    });

    test('A reading more than 30 seconds in the future (e.g. 31 seconds) is rejected', async () => {
      const vid = `VG-TS-31S-${Date.now()}`;
      const r = createValidReading(vid, 1, new Date(Date.now() + 35 * 1000).toISOString());
      const res = await postSignedBatch([r]);
      assert.strictEqual(res.status, 207);
      assert.strictEqual(res.body.accepted, 0);
      assert.strictEqual(res.body.rejected, 1);
      assert.strictEqual(res.body.results[0].status, 'rejected');
      assert.strictEqual(res.body.results[0].error, 'ts cannot be more than 30 seconds in the future');
    });

    test('An invalid timestamp string is rejected', async () => {
      const vid = `VG-TS-INV-${Date.now()}`;
      const r = createValidReading(vid, 1, 'not-a-valid-date-string');
      const res = await postSignedBatch([r]);
      assert.strictEqual(res.status, 207);
      assert.strictEqual(res.body.accepted, 0);
      assert.strictEqual(res.body.rejected, 1);
      assert.strictEqual(res.body.results[0].status, 'rejected');
      assert.strictEqual(res.body.results[0].error, 'ts must be a valid ISO timestamp string');
    });

    test('A reading with a future timestamp (e.g. 5 hours ahead) is rejected and does not corrupt vehicle state', async () => {
      const vid = `VG-TS-CORRUPT-${Date.now()}`;
      const futureReading = createValidReading(vid, 1, new Date(Date.now() + 5 * 3600 * 1000).toISOString());
      futureReading.soc_pct = 40.0;
      const res1 = await postSignedBatch([futureReading]);
      assert.strictEqual(res1.status, 207);
      assert.strictEqual(res1.body.rejected, 1);
      assert.strictEqual(res1.body.results[0].error, 'ts cannot be more than 30 seconds in the future');

      const vBefore = db.prepare('SELECT * FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.strictEqual(vBefore, undefined, 'Vehicle state must not be created from rejected future reading');

      const legitimateTs = new Date().toISOString();
      const legitimateReading = createValidReading(vid, 2, legitimateTs);
      legitimateReading.soc_pct = 85.0;
      const res2 = await postSignedBatch([legitimateReading]);
      assert.strictEqual(res2.status, 200);
      assert.strictEqual(res2.body.accepted, 1);

      const vAfter = db.prepare('SELECT * FROM vehicles WHERE vehicle_id = ?').get(vid);
      assert.ok(vAfter, 'Vehicle state should be created by legitimate reading');
      assert.strictEqual(vAfter.last_ts, legitimateTs);
      assert.strictEqual(vAfter.soc_pct, 85.0);
    });
  });

  describe('Edge Cases: Payload Formats and Sequence Completeness', () => {
    test('Wrapped payload { readings: [...] } is accepted and ingested', async () => {
      const vid = `VG-WRAP-${Date.now()}`;
      const payload = {
        readings: [
          createValidReading(vid, 1),
          createValidReading(vid, 2)
        ]
      };

      const res = await postSignedBatch(payload);
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.body.accepted, 2);
      assert.strictEqual(res.body.rejected, 0);

      const rows = db.prepare('SELECT * FROM telemetry_history WHERE vehicle_id = ?').all(vid);
      assert.strictEqual(rows.length, 2);
    });

    test('Invalid payload types (string, number, bad object) return HTTP 400', async () => {
      // 1. Object with incorrect key
      const resObj = await postSignedBatch({ wrong_key: [createValidReading('VG-ERR', 1)] });
      assert.strictEqual(resObj.status, 400);
      assert.match(resObj.body.error, /Invalid payload: expected an array of readings or { readings: \[\.\.\.\] }/);

      // 2. Object with non-array readings property
      const resNonArray = await postSignedBatch({ readings: 'not-an-array' });
      assert.strictEqual(resNonArray.status, 400);
      assert.match(resNonArray.body.error, /Invalid payload: expected an array of readings or { readings: \[\.\.\.\] }/);

      // 3. Raw malformed JSON / non-object payload
      const resStr = await postSignedBatch('not-a-valid-array');
      assert.strictEqual(resStr.status, 400);
      assert.ok(resStr.body.error, 'Must report client error for invalid payload string');
    });

    test('Batch where 100% of readings are invalid returns HTTP 207 Multi-Status with 0 accepted', async () => {
      const invalidBatch = [
        { ...createValidReading('VG-ALL-INV-1', 1), soc_pct: 150.0 }, // invalid SoC > 100
        { ...createValidReading('VG-ALL-INV-2', 2), battery_temp_c: 'too-hot' }, // non-numeric temp
        { ...createValidReading('VG-ALL-INV-3', 3), lat: 95.0 } // invalid lat > 90
      ];

      const res = await postSignedBatch(invalidBatch);
      assert.strictEqual(res.status, 207);
      assert.strictEqual(res.body.accepted, 0);
      assert.strictEqual(res.body.rejected, 3);
      assert.strictEqual(res.body.results.length, 3);
      assert.ok(res.body.results.every(r => r.status === 'rejected'));
    });

    test('GET /v1/completeness: returns 404 for nonexistent vehicle, accurate gaps for non-contiguous sequences', async () => {
      // 1. Nonexistent vehicle returns 404
      const missingRes = await fetch(`${baseUrl}/v1/completeness?vehicle_id=VG-NONEXISTENT-${Date.now()}`);
      assert.strictEqual(missingRes.status, 404);
      const missingBody = await missingRes.json();
      assert.match(missingBody.error, /not found/);

      // 2. Ingest non-contiguous sequence [1, 3, 5]
      const vid = `VG-GAP-TEST-${Date.now()}`;
      await postSignedBatch([
        createValidReading(vid, 1),
        createValidReading(vid, 3),
        createValidReading(vid, 5)
      ]);

      const compRes = await fetch(`${baseUrl}/v1/completeness?vehicle_id=${vid}`);
      assert.strictEqual(compRes.status, 200);
      const compBody = await compRes.json();

      assert.strictEqual(compBody.vehicle_id, vid);
      assert.strictEqual(compBody.min_seq, 1);
      assert.strictEqual(compBody.max_seq, 5);
      assert.strictEqual(compBody.received_count, 3);
      assert.strictEqual(compBody.expected_count, 5);
      assert.strictEqual(compBody.missing_count, 2);
      assert.strictEqual(compBody.completeness_pct, 60.0);
      assert.deepStrictEqual(compBody.missing_sequences, [2, 4]);
    });
  });
});
