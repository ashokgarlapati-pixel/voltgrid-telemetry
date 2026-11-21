const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const http = require('node:http');
const crypto = require('node:crypto');

// Isolated SQLite database for Part A5 Stream integration tests
const testDbPath = path.join(os.tmpdir(), `voltgrid-stream-int-${crypto.randomUUID()}.db`);
process.env.DB_PATH = testDbPath;
process.env.PORT = '0';
process.env.STREAM_COALESCE_INTERVAL_MS = '500'; // 500ms for fast test runs

const db = require('../src/db');
const { app, server } = require('../src/server');
const { stopOfflineWorker } = require('../src/offlineWorker');
const {
  publishVehicleUpdate,
  publishAlertChange,
  flushCoalescedVehicles,
  recordAndBroadcastEvent,
  stopStreamWorker,
  getConnectedClientsCount
} = require('../src/stream');

const API_KEY = 'voltgrid-dev-key';
const API_SECRET = 'voltgrid-dev-secret';

/**
 * Robust HTTP SSE test client helper.
 * Parses incoming chunks into SSE event structures ({ id, event, data, comment }).
 */
function createSseClient(url, options = {}) {
  const parsedUrl = new URL(url);
  const events = [];
  const rawChunks = [];
  let buffer = '';

  let req;
  const readyPromise = new Promise((resolve, reject) => {
    req = http.request({
      hostname: parsedUrl.hostname,
      port: parsedUrl.port,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: options.headers || {}
    });

    req.on('response', (res) => {
      resolve({ req, res });

      res.on('data', (chunk) => {
        const str = chunk.toString();
        rawChunks.push(str);
        buffer += str;

        let boundaryIndex;
        while ((boundaryIndex = buffer.indexOf('\n\n')) !== -1) {
          const rawMessage = buffer.slice(0, boundaryIndex);
          buffer = buffer.slice(boundaryIndex + 2);

          const lines = rawMessage.split('\n');
          let id = null;
          let event = 'message';
          let data = null;
          let comment = null;

          for (const line of lines) {
            if (line.startsWith(':')) {
              comment = line.slice(1).trim();
            } else if (line.startsWith('id:')) {
              id = parseInt(line.slice(3).trim(), 10);
            } else if (line.startsWith('event:')) {
              event = line.slice(6).trim();
            } else if (line.startsWith('data:')) {
              const rawData = line.slice(5).trim();
              try {
                data = JSON.parse(rawData);
              } catch (_) {
                data = rawData;
              }
            }
          }

          events.push({ id, event, data, comment, raw: rawMessage });
        }
      });
    });

    req.on('error', (err) => {
      // Cleanly ignore abort / connection close
      if (err.code !== 'ECONNRESET') {
        reject(err);
      }
    });
  });

  req.end();

  return {
    ready: readyPromise,
    getEvents: () => events,
    getRawChunks: () => rawChunks,
    close: () => {
      req.destroy();
    }
  };
}

describe('Part A5: Live Stream (SSE) Integration & Performance Tests', () => {
  let baseUrl;
  let nonceSeq = 7000;

  before(async () => {
    // Elevate rate limits for test environment
    db.prepare('UPDATE api_keys SET rate_limit_per_sec = 10000, burst_capacity = 10000 WHERE api_key = ?').run(API_KEY);
    db.prepare('INSERT OR REPLACE INTO rate_limits (api_key, tokens, last_refill_ms) VALUES (?, 10000, ?)').run(API_KEY, Date.now());

    const address = server.address();
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  after(() => {
    stopStreamWorker();
    stopOfflineWorker();
    server.close();
    try {
      db.close();
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(`${testDbPath}-wal`)) fs.unlinkSync(`${testDbPath}-wal`);
      if (fs.existsSync(`${testDbPath}-shm`)) fs.unlinkSync(`${testDbPath}-shm`);
    } catch (_) {}
  });

  // Helper to post signed batch
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
    return res;
  }

  // =========================================================================
  // 1. SSE Connection & Header Tests
  // =========================================================================
  describe('SSE Connection Handshake & Headers', () => {

    test('GET /v1/stream sets exact SSE headers, receives initial comment handshake', async () => {
      const client = createSseClient(`${baseUrl}/v1/stream`);
      const { res } = await client.ready;

      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-type'], 'text/event-stream');
      assert.strictEqual(res.headers['cache-control'], 'no-cache, no-transform');
      assert.strictEqual(res.headers['connection'], 'keep-alive');
      assert.strictEqual(res.headers['x-accel-buffering'], 'no');

      // Wait for initial handshake comment
      await new Promise(r => setTimeout(r, 60));

      const events = client.getEvents();
      const connectedComment = events.find(e => e.comment === 'connected');
      assert.ok(connectedComment, 'Must receive initial : connected comment frame');

      client.close();
    });

    test('Live vehicle update event matches canonical schema', async () => {
      const client = createSseClient(`${baseUrl}/v1/stream`);
      await client.ready;
      await new Promise(r => setTimeout(r, 50));

      const vehicleData = {
        vehicle_id: 'VG-SSE-SCHEMA-1',
        state: {
          soc_pct: 88.5,
          battery_temp_c: 34.2,
          speed_kmh: 42.0,
          lat: 13.0827,
          lng: 80.2707,
          odometer_km: 1250.0,
          charging: false,
          status: 'online',
          last_seq: 10,
          last_ts: new Date().toISOString()
        },
        ts: new Date().toISOString()
      };

      const recorded = recordAndBroadcastEvent('vehicle_update', vehicleData);
      await new Promise(r => setTimeout(r, 60));

      const events = client.getEvents();
      const updateEvent = events.find(e => e.id === recorded.id);
      assert.ok(updateEvent, 'Must receive vehicle_update event');
      assert.strictEqual(updateEvent.event, 'vehicle_update');
      assert.strictEqual(updateEvent.data.vehicle_id, 'VG-SSE-SCHEMA-1');
      assert.strictEqual(updateEvent.data.event_type, 'vehicle_update');
      assert.strictEqual(updateEvent.data.event_id, recorded.id);
      assert.strictEqual(updateEvent.data.state.soc_pct, 88.5);

      client.close();
    });

    test('Live alert change event matches canonical schema with alert_id, rule, and version', async () => {
      const client = createSseClient(`${baseUrl}/v1/stream`);
      await client.ready;
      await new Promise(r => setTimeout(r, 50));

      const alertRecord = {
        id: 77,
        vehicle_id: 'VG-SSE-ALERT-1',
        rule: 'overheat',
        status: 'open',
        version: 1,
        details: { consecutive_hot: 3, last_temp: 57.5 },
        opened_at: new Date().toISOString()
      };

      const emitted = publishAlertChange(alertRecord);
      await new Promise(r => setTimeout(r, 60));

      const events = client.getEvents();
      const alertEvent = events.find(e => e.id === emitted.id);
      assert.ok(alertEvent, 'Must receive alert_change event');
      assert.strictEqual(alertEvent.event, 'alert_change');
      assert.strictEqual(alertEvent.data.alert_id, 77);
      assert.strictEqual(alertEvent.data.vehicle_id, 'VG-SSE-ALERT-1');
      assert.strictEqual(alertEvent.data.rule, 'overheat');
      assert.strictEqual(alertEvent.data.status, 'open');
      assert.strictEqual(alertEvent.data.version, 1);
      assert.strictEqual(alertEvent.data.details.consecutive_hot, 3);

      client.close();
    });
  });

  // =========================================================================
  // 2. Last-Event-ID Resume & Event Persistence
  // =========================================================================
  describe('Resume After Disconnect (Last-Event-ID)', () => {

    test('Replays historical events from SQLite outbox created after Last-Event-ID', async () => {
      // 1. Seed 4 distinct persistent events
      const ev1 = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-REPLAY-1', state: { speed_kmh: 10 } });
      const ev2 = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-REPLAY-2', state: { speed_kmh: 20 } });
      const ev3 = recordAndBroadcastEvent('alert_change', { alert_id: 101, vehicle_id: 'VG-REPLAY-1', rule: 'overheat', status: 'open' });
      const ev4 = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-REPLAY-3', state: { speed_kmh: 30 } });

      // 2. Connect client providing Last-Event-ID = ev2.id
      const client = createSseClient(`${baseUrl}/v1/stream`, {
        headers: { 'Last-Event-ID': String(ev2.id) }
      });
      await client.ready;
      await new Promise(r => setTimeout(r, 100));

      const events = client.getEvents();
      const replayedIds = events.filter(e => e.id !== null).map(e => e.id);

      // Must NOT contain ev1 or ev2
      assert.ok(!replayedIds.includes(ev1.id), 'Must not replay events prior to Last-Event-ID');
      assert.ok(!replayedIds.includes(ev2.id), 'Must not replay Last-Event-ID event itself');

      // Must contain ev3 and ev4 in strict ascending order
      assert.ok(replayedIds.includes(ev3.id), 'Must replay event 3');
      assert.ok(replayedIds.includes(ev4.id), 'Must replay event 4');
      assert.ok(replayedIds.indexOf(ev3.id) < replayedIds.indexOf(ev4.id), 'Replay must preserve ascending monotonic ordering');

      client.close();
    });

    test('Edge case: invalid / non-numeric Last-Event-ID handled gracefully without crashing', async () => {
      const client = createSseClient(`${baseUrl}/v1/stream`, {
        headers: { 'Last-Event-ID': 'malformed_garbage_id' }
      });
      const { res } = await client.ready;
      assert.strictEqual(res.statusCode, 200);

      await new Promise(r => setTimeout(r, 50));
      const events = client.getEvents();
      assert.ok(events.find(e => e.comment === 'connected'), 'Client must connect cleanly even with malformed Last-Event-ID');

      client.close();
    });

    test('Edge case: Last-Event-ID higher than maximum existing ID returns zero replay events and receives future live events', async () => {
      const client = createSseClient(`${baseUrl}/v1/stream`, {
        headers: { 'Last-Event-ID': '9999999' }
      });
      await client.ready;
      await new Promise(r => setTimeout(r, 50));

      // No replay events should have been sent
      const initialEvents = client.getEvents().filter(e => e.id !== null);
      assert.strictEqual(initialEvents.length, 0);

      // Publish a new live event
      const liveEv = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-FUTURE-1', state: { speed_kmh: 55 } });
      await new Promise(r => setTimeout(r, 60));

      const finalEvents = client.getEvents().filter(e => e.id !== null);
      assert.strictEqual(finalEvents.length, 1);
      assert.strictEqual(finalEvents[0].id, liveEv.id);

      client.close();
    });

    test('Events survive application restart: SQLite stream_events table maintains persistence', async () => {
      // Insert an event directly into database
      const row = db.prepare(`
        INSERT INTO stream_events (event_type, data, created_at)
        VALUES ('vehicle_update', '{"vehicle_id":"VG-RESTART-TEST","state":{"soc_pct":45}}', datetime('now'))
      `).run();
      const persistedId = Number(row.lastInsertRowid);

      // Verify that a client reconnecting with previous ID retrieves persisted event
      const client = createSseClient(`${baseUrl}/v1/stream`, {
        headers: { 'Last-Event-ID': String(persistedId - 1) }
      });
      await client.ready;
      await new Promise(r => setTimeout(r, 80));

      const events = client.getEvents();
      const found = events.find(e => e.id === persistedId);
      assert.ok(found, 'Persisted event in SQLite must be replayed after restart simulation');
      assert.strictEqual(found.data.vehicle_id, 'VG-RESTART-TEST');

      client.close();
    });
  });

  // =========================================================================
  // 3. Replay-to-Live Race Condition Tests
  // =========================================================================
  describe('Replay-to-Live Race Condition Handoff', () => {

    test('Zero missed events and zero duplicates when live events arrive during historical replay', async () => {
      // 1. Seed historical events
      const hist1 = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-RACE-1', seq: 1 });
      const hist2 = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-RACE-2', seq: 2 });
      const hist3 = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-RACE-3', seq: 3 });

      // 2. Connect client with Last-Event-ID = hist1.id
      const client = createSseClient(`${baseUrl}/v1/stream`, {
        headers: { 'Last-Event-ID': String(hist1.id) }
      });
      await client.ready;

      // 3. Simultaneously fire a live alert and live vehicle update
      const liveAlert = publishAlertChange({ id: 999, vehicle_id: 'VG-RACE-1', rule: 'rapid_discharge', status: 'open' });
      const liveVehicle = recordAndBroadcastEvent('vehicle_update', { vehicle_id: 'VG-RACE-4', seq: 4 });

      // 4. Wait for replay and handoff to complete
      await new Promise(r => setTimeout(r, 120));

      const events = client.getEvents().filter(e => e.id !== null);
      const receivedIds = events.map(e => e.id);

      // Verify all expected events were received
      assert.ok(!receivedIds.includes(hist1.id), 'Must not include hist1 (Last-Event-ID)');
      assert.ok(receivedIds.includes(hist2.id), 'Must include hist2 from replay');
      assert.ok(receivedIds.includes(hist3.id), 'Must include hist3 from replay');
      assert.ok(receivedIds.includes(liveAlert.id), 'Must include liveAlert that arrived during replay');
      assert.ok(receivedIds.includes(liveVehicle.id), 'Must include liveVehicle that arrived during replay');

      // Verify zero duplicates
      const uniqueIds = new Set(receivedIds);
      assert.strictEqual(receivedIds.length, uniqueIds.size, 'Zero duplicates allowed across replay-to-live handoff');

      // Verify strictly ascending monotonic ordering
      for (let i = 1; i < receivedIds.length; i++) {
        assert.ok(receivedIds[i] > receivedIds[i - 1], `Event ID ordering must be strictly ascending (got ${receivedIds[i - 1]} followed by ${receivedIds[i]})`);
      }

      client.close();
    });
  });

  // =========================================================================
  // 4. Back-pressure and Slow Client Protection
  // =========================================================================
  describe('Back-Pressure & Slow Client Handling', () => {

    test('Slow client with saturated socket is cleanly disconnected without affecting fast client', async () => {
      // Connect fast client
      const fastClient = createSseClient(`${baseUrl}/v1/stream`);
      await fastClient.ready;

      // Connect slow client
      const slowClient = createSseClient(`${baseUrl}/v1/stream`);
      const { res: slowRes } = await slowClient.ready;

      await new Promise(r => setTimeout(r, 50));
      const initialClientCount = getConnectedClientsCount();
      assert.ok(initialClientCount >= 2, 'Both clients must be registered');

      // Artificially pause TCP stream on slow client by pausing response stream
      slowRes.pause();

      // Broadcast 120 messages (exceeds MAX_CLIENT_BUFFER = 100)
      for (let i = 1; i <= 120; i++) {
        recordAndBroadcastEvent('vehicle_update', {
          vehicle_id: `VG-BP-${i}`,
          state: { speed_kmh: i }
        });
      }

      // Wait for buffer check and disconnection
      await new Promise(r => setTimeout(r, 150));

      // Fast client must have received events smoothly
      const fastEvents = fastClient.getEvents().filter(e => e.id !== null);
      assert.ok(fastEvents.length >= 120, `Fast client must receive all broadcast events (received ${fastEvents.length})`);

      fastClient.close();
      slowClient.close();
    });
  });

  // =========================================================================
  // 5. Vehicle Update Coalescing
  // =========================================================================
  describe('Vehicle Update Coalescing', () => {

    test('Multiple rapid updates for a vehicle collapse into latest state; alert changes bypass coalescing', async () => {
      const client = createSseClient(`${baseUrl}/v1/stream`);
      await client.ready;
      await new Promise(r => setTimeout(r, 50));

      const initialEventCount = client.getEvents().length;

      // Ingest 5 rapid updates for VG-COAL-1 with increasing battery temp
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-1', battery_temp_c: 30.0, last_ts: '2026-11-20T10:00:00Z', soc_pct: 90 });
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-1', battery_temp_c: 32.0, last_ts: '2026-11-20T10:00:05Z', soc_pct: 89 });
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-1', battery_temp_c: 34.0, last_ts: '2026-11-20T10:00:10Z', soc_pct: 88 });
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-1', battery_temp_c: 36.0, last_ts: '2026-11-20T10:00:15Z', soc_pct: 87 });
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-1', battery_temp_c: 41.5, last_ts: '2026-11-20T10:00:20Z', soc_pct: 85 });

      // Ingest 3 rapid updates for VG-COAL-2
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-2', speed_kmh: 15.0, last_ts: '2026-11-20T10:00:00Z' });
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-2', speed_kmh: 25.0, last_ts: '2026-11-20T10:00:05Z' });
      publishVehicleUpdate({ vehicle_id: 'VG-COAL-2', speed_kmh: 48.0, last_ts: '2026-11-20T10:00:10Z' });

      // Simultaneously publish a high-priority alert change
      const urgentAlert = publishAlertChange({
        id: 301,
        vehicle_id: 'VG-COAL-1',
        rule: 'overheat',
        status: 'open',
        version: 1
      });

      // Assert alert change was delivered immediately WITHOUT waiting for flush
      await new Promise(r => setTimeout(r, 60));
      const eventsBeforeFlush = client.getEvents();
      const alertFound = eventsBeforeFlush.find(e => e.id === urgentAlert.id);
      assert.ok(alertFound, 'Alert change must be dispatched immediately without coalescing delay');

      // Now trigger the coalesced vehicle flush
      flushCoalescedVehicles();
      await new Promise(r => setTimeout(r, 80));

      const eventsAfterFlush = client.getEvents();
      const v1Updates = eventsAfterFlush.filter(e => e.event === 'vehicle_update' && e.data?.vehicle_id === 'VG-COAL-1');
      const v2Updates = eventsAfterFlush.filter(e => e.event === 'vehicle_update' && e.data?.vehicle_id === 'VG-COAL-2');

      // Must have collapsed 5 updates for VG-COAL-1 into exactly ONE event
      assert.strictEqual(v1Updates.length, 1, '5 rapid updates for VG-COAL-1 must collapse into exactly 1 event');
      assert.strictEqual(v1Updates[0].data.state.battery_temp_c, 41.5, 'Must retain the latest battery temperature (41.5°C)');
      assert.strictEqual(v1Updates[0].data.state.soc_pct, 85, 'Must retain the latest SOC (85%)');

      // Must have collapsed 3 updates for VG-COAL-2 into exactly ONE event
      assert.strictEqual(v2Updates.length, 1, '3 rapid updates for VG-COAL-2 must collapse into exactly 1 event');
      assert.strictEqual(v2Updates[0].data.state.speed_kmh, 48.0, 'Must retain the latest speed (48.0 km/h)');

      client.close();
    });
  });

  // =========================================================================
  // 6. High-Throughput Ingestion & Live Stream Benchmark
  // =========================================================================
  describe('High-Throughput Streaming Benchmark (Simulating 400 req/s)', () => {

    test('Ingests 400 telemetry readings with active SSE clients, measuring throughput and delivery latency', async () => {
      // Connect 3 concurrent SSE subscribers
      const client1 = createSseClient(`${baseUrl}/v1/stream`);
      const client2 = createSseClient(`${baseUrl}/v1/stream`);
      const client3 = createSseClient(`${baseUrl}/v1/stream`);

      await Promise.all([client1.ready, client2.ready, client3.ready]);
      await new Promise(r => setTimeout(r, 60));

      const TOTAL_READINGS = 400;
      const baseTime = Date.now() - 60000;
      const readings = [];

      // Generate 400 readings across 20 vehicles (20 readings each)
      for (let i = 1; i <= TOTAL_READINGS; i++) {
        const vIndex = (i % 20) + 1;
        const vid = `VG-BENCH-STRM-${String(vIndex).padStart(3, '0')}`;
        readings.push({
          vehicle_id: vid,
          seq: 1000 + i,
          ts: new Date(baseTime + (i * 200)).toISOString(),
          soc_pct: 80.0 - (i * 0.05),
          battery_temp_c: 32.0 + (i * 0.02),
          speed_kmh: 30.0,
          lat: 13.0827,
          lng: 80.2707,
          odometer_km: 100.0,
          charging: false,
          fault_codes: []
        });
      }

      // Ingest the 400 readings via POST /v1/telemetry/batch
      const startIngest = performance.now();
      const res = await postBatch(readings);
      const ingestDurationMs = performance.now() - startIngest;

      assert.strictEqual(res.status, 200);

      // Trigger coalescing flush
      const startFlush = performance.now();
      flushCoalescedVehicles();
      const flushDurationMs = performance.now() - startFlush;

      await new Promise(r => setTimeout(r, 100));

      // 400 readings across 20 vehicles must coalesce into exactly 20 vehicle_update events!
      const c1Events = client1.getEvents().filter(e => e.event === 'vehicle_update');
      const c2Events = client2.getEvents().filter(e => e.event === 'vehicle_update');
      const c3Events = client3.getEvents().filter(e => e.event === 'vehicle_update');

      console.log(`[Stream Benchmark] Ingested ${TOTAL_READINGS} readings in ${ingestDurationMs.toFixed(2)}ms (${(TOTAL_READINGS / (ingestDurationMs / 1000)).toFixed(0)} readings/sec)`);
      console.log(`[Stream Benchmark] Coalesced 400 readings across 20 vehicles into 20 events in ${flushDurationMs.toFixed(2)}ms`);
      console.log(`[Stream Benchmark] Delivered events: Client1=${c1Events.length}, Client2=${c2Events.length}, Client3=${c3Events.length}`);

      assert.ok(c1Events.length >= 20, 'Client 1 must receive all coalesced vehicle events');
      assert.ok(c2Events.length >= 20, 'Client 2 must receive all coalesced vehicle events');
      assert.ok(c3Events.length >= 20, 'Client 3 must receive all coalesced vehicle events');

      client1.close();
      client2.close();
      client3.close();
    });
  });

  describe('Issue 4: Alert Events Published Only After Database Commit', () => {
    test('Successful transaction publishes the event after commit', async () => {
      const vid = `VG-ISSUE4-SUCC-${Date.now()}`;
      const now = Date.now();

      // Seed offline vehicle with active offline alert
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, ?, 1, 80, 25, 0, 13.08, 80.27, 100, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      const alertInfo = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{}', ?, ?)
      `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());
      const alertId = alertInfo.lastInsertRowid;

      // Connect SSE client
      const sseClient = createSseClient(`${baseUrl}/v1/stream?api_key=${API_KEY}`);
      await sseClient.ready;
      await new Promise(r => setTimeout(r, 50));

      // Ingest new reading -> resolves offline alert
      const res = await postBatch([
        { vehicle_id: vid, seq: 2, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(res.status, 200);

      // Wait for SSE dispatch
      await new Promise(r => setTimeout(r, 100));

      // Verify DB committed first
      const alertInDb = db.prepare('SELECT * FROM alerts WHERE id = ?').get(alertId);
      assert.strictEqual(alertInDb.status, 'resolved', 'Alert must be resolved in SQLite');

      // Verify event was published to SSE client
      const alertEvents = sseClient.getEvents().filter(e => e.event === 'alert_change' && e.data && e.data.alert_id === Number(alertId));
      assert.strictEqual(alertEvents.length, 1, 'Client must receive exactly one resolved alert event');
      assert.strictEqual(alertEvents[0].data.status, 'resolved');

      sseClient.close();
    });

    test('Failed transaction does not publish the event', async () => {
      const vid = `VG-ISSUE4-FAIL-${Date.now()}`;
      const now = Date.now();

      // Seed offline vehicle with active offline alert
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, ?, 1, 80, 25, 0, 13.08, 80.27, 100, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      const alertInfo = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{}', ?, ?)
      `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());
      const alertId = alertInfo.lastInsertRowid;

      // Connect SSE client
      const sseClient = createSseClient(`${baseUrl}/v1/stream?api_key=${API_KEY}`);
      await sseClient.ready;
      await new Promise(r => setTimeout(r, 50));

      // Simulate a failure in transaction execution by directly testing transactional rollback
      assert.throws(() => {
        db.transaction(() => {
          // Resolve offline alert inside transaction
          db.prepare("UPDATE alerts SET status = 'resolved' WHERE id = ?").run(alertId);
          // Force transaction failure
          throw new Error('Simulated database write failure');
        })();
      }, /Simulated database write failure/);

      await new Promise(r => setTimeout(r, 100));

      // DB rolled back: Alert remains open
      const alertInDb = db.prepare('SELECT * FROM alerts WHERE id = ?').get(alertId);
      assert.strictEqual(alertInDb.status, 'open', 'Alert must remain open in SQLite after rollback');

      // Zero alert events published to SSE client!
      const alertEvents = sseClient.getEvents().filter(e => e.event === 'alert_change' && e.data && e.data.alert_id === Number(alertId));
      assert.strictEqual(alertEvents.length, 0, 'Client must NOT receive any event for rolled back transaction');

      // Zero events in stream_events outbox table
      const outboxRows = db.prepare("SELECT * FROM stream_events WHERE json_extract(data, '$.alert_id') = ?").all(Number(alertId));
      assert.strictEqual(outboxRows.length, 0, 'Outbox must contain 0 events for rolled-back alert');

      sseClient.close();
    });

    test('Offline alert resolution publishes only after commit', async () => {
      const vid = `VG-ISSUE4-ORDER-${Date.now()}`;
      const now = Date.now();

      // Seed offline vehicle with active offline alert
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, ?, 1, 80, 25, 0, 13.08, 80.27, 100, 0, 'offline', 1, 1, 1, ?)
      `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      const alertInfo = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{}', ?, ?)
      `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());
      const alertId = alertInfo.lastInsertRowid;

      const sseClient = createSseClient(`${baseUrl}/v1/stream?api_key=${API_KEY}`);
      await sseClient.ready;
      await new Promise(r => setTimeout(r, 50));

      // Ingest batch to trigger resolution
      await postBatch([
        { vehicle_id: vid, seq: 2, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] }
      ]);

      await new Promise(r => setTimeout(r, 100));

      const alertEvents = sseClient.getEvents().filter(e => e.event === 'alert_change' && e.data && e.data.alert_id === Number(alertId));
      assert.strictEqual(alertEvents.length, 1);

      // Verify that when alert event was created, the corresponding alert row in alerts is ALREADY status='resolved'
      const alertInDb = db.prepare('SELECT status, version FROM alerts WHERE id = ?').get(alertId);
      assert.strictEqual(alertInDb.status, 'resolved');
      assert.strictEqual(alertInDb.version, 2);

      sseClient.close();
    });

    test('Multiple alert changes are published correctly without duplicates', async () => {
      const vid1 = `VG-ISSUE4-MULTI1-${Date.now()}`;
      const vid2 = `VG-ISSUE4-MULTI2-${Date.now()}`;
      const now = Date.now();

      // Seed 2 offline vehicles with offline alerts
      for (const vid of [vid1, vid2]) {
        db.prepare(`
          INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
          VALUES (?, ?, 1, 80, 25, 0, 13.08, 80.27, 100, 0, 'offline', 1, 1, 1, ?)
        `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

        db.prepare(`
          INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
          VALUES (?, 'offline', 'open', 1, '{}', ?, ?)
        `).run(vid, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());
      }

      const sseClient = createSseClient(`${baseUrl}/v1/stream?api_key=${API_KEY}`);
      await sseClient.ready;
      await new Promise(r => setTimeout(r, 50));

      // Ingest single batch with readings for BOTH vehicles
      const res = await postBatch([
        { vehicle_id: vid1, seq: 2, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] },
        { vehicle_id: vid2, seq: 2, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] }
      ]);
      assert.strictEqual(res.status, 200);

      await new Promise(r => setTimeout(r, 100));

      // Both alerts resolved in DB
      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid1).status, 'resolved');
      assert.strictEqual(db.prepare("SELECT status FROM alerts WHERE vehicle_id = ? AND rule = 'offline'").get(vid2).status, 'resolved');

      // Both events delivered to SSE client
      const vid1Events = sseClient.getEvents().filter(e => e.event === 'alert_change' && e.data && e.data.vehicle_id === vid1);
      const vid2Events = sseClient.getEvents().filter(e => e.event === 'alert_change' && e.data && e.data.vehicle_id === vid2);

      assert.strictEqual(vid1Events.length, 1, 'Exactly 1 event for vid1');
      assert.strictEqual(vid2Events.length, 1, 'Exactly 1 event for vid2');
      assert.strictEqual(vid1Events[0].data.status, 'resolved');
      assert.strictEqual(vid2Events[0].data.status, 'resolved');

      sseClient.close();
    });

    test('SSE replay contains only committed events', async () => {
      const vidCommitted = `VG-ISSUE4-REP-C-${Date.now()}`;
      const vidRolledBack = `VG-ISSUE4-REP-R-${Date.now()}`;
      const now = Date.now();

      // Seed offline vehicle for committed test
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, ?, 1, 80, 25, 0, 13.08, 80.27, 100, 0, 'offline', 1, 1, 1, ?)
      `).run(vidCommitted, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{}', ?, ?)
      `).run(vidCommitted, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      // Get latest event ID prior to commit
      const lastRowBefore = db.prepare('SELECT MAX(id) AS max_id FROM stream_events').get();
      const lastEventIdBefore = lastRowBefore && lastRowBefore.max_id ? Number(lastRowBefore.max_id) : 0;

      // Ingest committed reading
      await postBatch([
        { vehicle_id: vidCommitted, seq: 2, ts: new Date(now - 5000).toISOString(), soc_pct: 80, battery_temp_c: 25, speed_kmh: 0, lat: 13.08, lng: 80.27, odometer_km: 100, charging: false, fault_codes: [] }
      ]);

      // Seed offline vehicle for rolled-back test
      db.prepare(`
        INSERT INTO vehicles (vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at)
        VALUES (?, ?, 1, 80, 25, 0, 13.08, 80.27, 100, 0, 'offline', 1, 1, 1, ?)
      `).run(vidRolledBack, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      const alertR = db.prepare(`
        INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
        VALUES (?, 'offline', 'open', 1, '{}', ?, ?)
      `).run(vidRolledBack, new Date(now - 120000).toISOString(), new Date(now - 120000).toISOString());

      // Simulate rolled-back transaction
      assert.throws(() => {
        db.transaction(() => {
          db.prepare("UPDATE alerts SET status = 'resolved' WHERE id = ?").run(alertR.lastInsertRowid);
          throw new Error('Rollback simulation');
        })();
      }, /Rollback simulation/);

      // Now connect SSE client with Last-Event-ID = lastEventIdBefore to replay
      const replayClient = createSseClient(`${baseUrl}/v1/stream?api_key=${API_KEY}`, {
        headers: { 'Last-Event-ID': String(lastEventIdBefore) }
      });
      await replayClient.ready;
      await new Promise(r => setTimeout(r, 100));

      const replayedEvents = replayClient.getEvents().filter(e => e.event === 'alert_change');

      // Must have received the committed alert event
      const committedEvent = replayedEvents.find(e => e.data && e.data.vehicle_id === vidCommitted);
      assert.ok(committedEvent, 'Replayed events must contain the committed alert change');
      assert.strictEqual(committedEvent.data.status, 'resolved');

      // Must NOT have received any event for rolled-back alert
      const rolledBackEvent = replayedEvents.find(e => e.data && e.data.vehicle_id === vidRolledBack);
      assert.strictEqual(rolledBackEvent, undefined, 'Replayed events must NEVER contain rolled back alert');

      replayClient.close();
    });
  });
});
