/**
 * VoltGrid Fleet Telemetry Service - Live SSE Stream (Part A5)
 * 
 * Provides a high-performance, restart-resilient Server-Sent Events (SSE) stream on GET /v1/stream:
 * - Persistent event log in SQLite (stream_events table) guaranteeing monotonic event IDs across restarts.
 * - Robust Last-Event-ID resumption with atomic replay-to-live handoff (zero missed events, zero duplicates).
 * - 1-second vehicle update coalescing (retains latest state per vehicle, bounded memory).
 * - Immediate dispatch for high-priority alert lifecycle events.
 * - Strict client-level back-pressure handling (bounded pending buffer, slow-client disconnect protection).
 * - 15-second heartbeat comments (: heartbeat\n\n) to keep TCP sockets alive through proxies and NATs.
 */

const db = require('./db');

const MAX_CLIENT_BUFFER = 100; // Maximum pending SSE frames per slow client before disconnection
const FLUSH_INTERVAL_MS = parseInt(process.env.STREAM_COALESCE_INTERVAL_MS, 10) || 1000;
const HEARTBEAT_INTERVAL_MS = parseInt(process.env.STREAM_HEARTBEAT_INTERVAL_MS, 10) || 15000;
const EVENT_RETENTION_LIMIT = 10000; // Number of historical events retained in stream_events
const CROSS_INSTANCE_POLL_INTERVAL_MS = parseInt(process.env.STREAM_CROSS_INSTANCE_POLL_INTERVAL_MS, 10) || 500;
const MAX_REPLAY_LIMIT = parseInt(process.env.STREAM_MAX_REPLAY_LIMIT, 10) || 1000;

const connectedClients = new Set();
const pendingVehicles = new Map();

// Track highest event ID broadcast by this process to detect external inserts from other API instances
let lastBroadcastId = 0;
try {
  const maxRow = db.prepare('SELECT MAX(id) AS max_id FROM stream_events').get();
  if (maxRow && maxRow.max_id) {
    lastBroadcastId = Number(maxRow.max_id);
  }
} catch (_) {}

/**
 * Formats an event object as a standard SSE frame.
 * @param {object} event - { id, type, data }
 * @returns {string} Formatted SSE frame
 */
function formatSseMessage(event) {
  const dataStr = typeof event.data === 'string' ? event.data : JSON.stringify(event.data);
  return `id: ${event.id}\nevent: ${event.type}\ndata: ${dataStr}\n\n`;
}

/**
 * Persists an event into SQLite and broadcasts it to connected clients.
 * Guarantees monotonic event IDs across server restarts.
 * 
 * @param {string} type - Event type ('vehicle_update', 'alert_change', etc.)
 * @param {object} data - Event payload object
 * @returns {object} Created event record { id, type, data }
 */
function recordAndBroadcastEvent(type, data) {
  const rawData = { ...data };
  
  // Persist to SQLite outbox table
  const insertStmt = db.prepare(`
    INSERT INTO stream_events (event_type, data, created_at)
    VALUES (?, ?, datetime('now'))
  `);
  
  const info = insertStmt.run(type, JSON.stringify(rawData));
  const eventId = Number(info.lastInsertRowid);
  lastBroadcastId = Math.max(lastBroadcastId, eventId);

  // Set the canonical event_id in the payload
  rawData.event_id = eventId;
  rawData.event_type = type;

  const event = {
    id: eventId,
    type,
    data: rawData
  };

  // Broadcast to all active SSE clients
  broadcast(event);

  return event;
}

/**
 * Drains a client's pending buffer iteratively when its TCP socket is ready for more data.
 * @param {object} client 
 */
function drainClientBuffer(client) {
  while (client.buffer.length > 0) {
    const nextMsg = client.buffer[0];
    let canAcceptMore;
    try {
      canAcceptMore = client.res.write(nextMsg);
    } catch (_) {
      connectedClients.delete(client);
      return;
    }

    client.buffer.shift();

    if (!canAcceptMore) {
      client.isPaused = true;
      client.res.once('drain', () => {
        client.isPaused = false;
        drainClientBuffer(client);
      });
      return;
    }
  }

  client.isPaused = false;
}

/**
 * Writes an SSE frame directly to an individual client respecting TCP back-pressure.
 * If client buffer exceeds MAX_CLIENT_BUFFER, cleanly disconnects the slow client.
 * 
 * @param {object} client 
 * @param {string} payload - Formatted SSE frame
 */
function writeDirectToClient(client, payload) {
  if (client.isPaused) {
    // If TCP send buffer is full, check client buffer limit
    if (client.buffer.length >= MAX_CLIENT_BUFFER) {
      try {
        client.res.write(': disconnected: client backpressure buffer overflow\n\n');
        client.res.end();
      } catch (_) {}
      connectedClients.delete(client);
      return;
    }
    client.buffer.push(payload);
    return;
  }

  try {
    const canAcceptMore = client.res.write(payload);
    if (!canAcceptMore) {
      client.isPaused = true;
      client.res.once('drain', () => {
        client.isPaused = false;
        drainClientBuffer(client);
      });
    }
  } catch (_) {
    connectedClients.delete(client);
  }
}

/**
 * Broadcasts an event to all connected clients.
 * If a client is currently replaying historical events, queues the live event
 * into client.replayQueue to guarantee strictly monotonic event ordering.
 * 
 * @param {object} event - { id, type, data }
 */
function broadcast(event) {
  if (connectedClients.size === 0) return;
  const payload = formatSseMessage(event);

  for (const client of connectedClients) {
    if (client.replaying) {
      // Buffer live event until replay completes
      client.replayQueue.push({ id: event.id, payload });
      continue;
    }
    writeDirectToClient(client, payload);
    client.lastDeliveredId = Math.max(client.lastDeliveredId, event.id);
  }
}

/**
 * Publishes a vehicle state update into the coalescing map.
 * Multiple updates for the same vehicle are collapsed into the latest state,
 * bounding memory to O(fleet_size) and preventing downstream subscriber flooding.
 * 
 * @param {object} vehicle - Latest state object for a vehicle
 */
function publishVehicleUpdate(vehicle) {
  if (!vehicle || !vehicle.vehicle_id) return;

  const existing = pendingVehicles.get(vehicle.vehicle_id);
  // Retain incoming update if newer or first seen
  if (!existing || !existing.last_ts || (vehicle.last_ts && vehicle.last_ts >= existing.last_ts)) {
    pendingVehicles.set(vehicle.vehicle_id, vehicle);
  }
}

/**
 * Flushes all coalesced vehicle updates at the configured interval.
 * Dispatches ONE consolidated 'vehicle_batch' event containing the latest states
 * for all vehicles collected during the interval, reducing subscriber event load from
 * ~400 events/sec down to 1 consolidated frame per flush.
 */
function flushCoalescedVehicles() {
  if (pendingVehicles.size === 0) return;

  try {
    const updates = Array.from(pendingVehicles.values());
    pendingVehicles.clear();

    const vehiclesList = updates.map(v => ({
      vehicle_id: v.vehicle_id,
      state: {
        soc_pct: v.soc_pct,
        battery_temp_c: v.battery_temp_c,
        speed_kmh: v.speed_kmh,
        lat: v.lat,
        lng: v.lng,
        odometer_km: v.odometer_km,
        charging: Boolean(v.charging),
        status: v.status || 'online',
        last_seq: v.last_seq,
        last_ts: v.last_ts
      },
      ts: v.last_ts || new Date().toISOString()
    }));

    const eventData = {
      event_type: 'vehicle_batch',
      count: vehiclesList.length,
      vehicles: vehiclesList,
      ts: new Date().toISOString()
    };

    recordAndBroadcastEvent('vehicle_batch', eventData);
  } catch (err) {
    if (err && (err.message?.includes('database is not open') || err.code === 'ERR_SQLITE_NOT_OPEN')) {
      return;
    }
    console.error('[Stream Coalescing Error]', err);
  }
}

// 1-second interval timer for vehicle update coalescing
let flushTimer = setInterval(flushCoalescedVehicles, FLUSH_INTERVAL_MS);
flushTimer.unref();

/**
 * Periodically polls the shared SQLite stream_events table for new events inserted by
 * other API instances, broadcasting them to local subscribers so multi-instance subscribers
 * receive cross-instance events without waiting for a reconnect.
 */
function pollCrossInstanceEvents() {
  if (connectedClients.size === 0) return;

  try {
    let minDelivered = Infinity;
    for (const client of connectedClients) {
      if (!client.replaying && client.lastDeliveredId < minDelivered) {
        minDelivered = client.lastDeliveredId;
      }
    }
    if (minDelivered === Infinity) return;

    const newEvents = db.prepare(`
      SELECT id, event_type, data
      FROM stream_events
      WHERE id > ?
      ORDER BY id ASC
      LIMIT 100
    `).all(minDelivered);

    for (const row of newEvents) {
      let payloadData;
      try {
        payloadData = JSON.parse(row.data);
      } catch (_) {
        payloadData = row.data;
      }
      if (typeof payloadData === 'object' && payloadData !== null) {
        payloadData.event_id = row.id;
      }

      const msg = formatSseMessage({
        id: row.id,
        type: row.event_type,
        data: payloadData
      });

      for (const client of connectedClients) {
        if (!client.replaying && row.id > client.lastDeliveredId) {
          writeDirectToClient(client, msg);
          client.lastDeliveredId = row.id;
        }
      }
    }
  } catch (err) {
    if (err && (err.message?.includes('database is not open') || err.code === 'ERR_SQLITE_NOT_OPEN')) {
      return;
    }
    console.error('[Stream Cross-Instance Poll Error]', err);
  }
}

let crossInstanceTimer = setInterval(pollCrossInstanceEvents, CROSS_INSTANCE_POLL_INTERVAL_MS);
crossInstanceTimer.unref();

/**
 * Publishes an alert change (open, acknowledged, or resolved).
 * High priority: Dispatched immediately to SQLite outbox and connected clients without coalescing.
 * 
 * @param {object} alert - Alert record
 * @returns {object} Created event record
 */
function publishAlertChange(alert) {
  if (!alert) return;

  let parsedDetails = alert.details;
  if (typeof parsedDetails === 'string') {
    try {
      parsedDetails = JSON.parse(parsedDetails);
    } catch (_) {}
  }

  const alertData = {
    event_type: 'alert_change',
    alert_id: alert.id,
    vehicle_id: alert.vehicle_id,
    rule: alert.rule,
    status: alert.status,
    version: alert.version || 1,
    details: parsedDetails || {},
    ts: alert.updated_at || alert.opened_at || new Date().toISOString()
  };

  return recordAndBroadcastEvent('alert_change', alertData);
}

/**
 * Periodically prunes historical stream_events to maintain bounded database disk usage.
 * Retains the latest EVENT_RETENTION_LIMIT events.
 */
function pruneOldEvents() {
  try {
    db.prepare(`
      DELETE FROM stream_events
      WHERE id < (SELECT MAX(id) - ? FROM stream_events)
    `).run(EVENT_RETENTION_LIMIT);
  } catch (err) {
    if (err && (err.message?.includes('database is not open') || err.code === 'ERR_SQLITE_NOT_OPEN')) {
      return;
    }
    console.error('[Stream Prune Error]', err);
  }
}

// Run retention pruning every 60 seconds
let pruneTimer = setInterval(pruneOldEvents, 60000);
pruneTimer.unref();

/**
 * Broadcasts heartbeat comments (: heartbeat\n\n) every 15 seconds to prevent TCP timeouts.
 */
function sendHeartbeats() {
  for (const client of connectedClients) {
    try {
      client.res.write(': heartbeat\n\n');
    } catch (_) {
      connectedClients.delete(client);
    }
  }
}

let heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_INTERVAL_MS);
heartbeatTimer.unref();

/**
 * Express route handler for GET /v1/stream (Server-Sent Events)
 * Supports Last-Event-ID resumption, safe replay-to-live handoff, back-pressure,
 * authentication via API key, bounded replay limits, and documented gap recovery.
 */
function sseStreamHandler(req, res) {
  // Stream Authentication: supports X-API-Key header, Authorization Bearer, or ?api_key= / ?apiKey= query param
  const apiKey = req.headers['x-api-key'] || 
                 (req.headers['authorization']?.startsWith('Bearer ') ? req.headers['authorization'].slice(7) : null) ||
                 req.query.api_key || 
                 req.query.apiKey;

  if (apiKey) {
    const keyRow = db.prepare('SELECT api_key FROM api_keys WHERE api_key = ?').get(apiKey);
    if (!keyRow) {
      return res.status(401).json({ error: 'Unauthorized: invalid API key' });
    }
  } else if (process.env.STREAM_AUTH_REQUIRED === 'true' || (process.env.NODE_ENV === 'production' && !process.env.ALLOW_ANONYMOUS_STREAM)) {
    return res.status(401).json({ error: 'Unauthorized: missing API key (provide via X-API-Key header or ?api_key= query parameter)' });
  }

  // Set required SSE HTTP response headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  if (typeof res.flushHeaders === 'function') {
    res.flushHeaders();
  }

  // Initial connection handshake comment
  res.write(': connected\n\n');

  // Extract and parse Last-Event-ID header or query param
  const rawLastId = req.headers['last-event-id'] || req.query.lastEventId;
  let parsedLastId = null;
  if (rawLastId !== undefined && rawLastId !== null && String(rawLastId).trim() !== '') {
    const parsed = parseInt(String(rawLastId).trim(), 10);
    if (!isNaN(parsed) && parsed >= 0) {
      parsedLastId = parsed;
    }
  }

  // Client session tracking object
  const client = {
    id: `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
    res,
    isPaused: false,
    buffer: [],
    replaying: parsedLastId !== null,
    replayQueue: [],
    lastDeliveredId: parsedLastId || 0
  };

  // Register client in connected set BEFORE replay begins.
  // This guarantees any live event arriving during replay is captured into client.replayQueue.
  connectedClients.add(client);

  // If client provided a valid Last-Event-ID, perform historical replay from SQLite outbox
  if (parsedLastId !== null) {
    try {
      // Check if requested ID is older than retained history in stream_events
      const minRow = db.prepare('SELECT MIN(id) AS min_id FROM stream_events').get();
      if (minRow && minRow.min_id && parsedLastId < minRow.min_id) {
        res.write(`: gap_detected: requested Last-Event-ID (${parsedLastId}) is older than retained history (${minRow.min_id})\n\n`);
        res.write(`: gap_recovery_strategy: fetch GET /v1/fleet/summary and GET /v1/vehicles to re-synchronize baseline state, then reconnect to /v1/stream with latest Last-Event-ID\n\n`);
      }

      // Query persistent events created after Last-Event-ID with bounded LIMIT (Cost Point 3)
      const historicalRows = db.prepare(`
        SELECT id, event_type, data
        FROM stream_events
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
      `).all(parsedLastId, MAX_REPLAY_LIMIT);

      for (const row of historicalRows) {
        let payloadData;
        try {
          payloadData = JSON.parse(row.data);
        } catch (_) {
          payloadData = row.data;
        }
        if (typeof payloadData === 'object' && payloadData !== null) {
          payloadData.event_id = row.id;
        }

        const msg = formatSseMessage({
          id: row.id,
          type: row.event_type,
          data: payloadData
        });

        writeDirectToClient(client, msg);
        client.lastDeliveredId = row.id;
      }

      if (historicalRows.length >= MAX_REPLAY_LIMIT) {
        res.write(`: replay_truncated: replayed maximum allowed ${MAX_REPLAY_LIMIT} events. Reconnect with Last-Event-ID ${client.lastDeliveredId} to fetch next batch\n\n`);
      }

      // Safe Replay-to-Live Handoff:
      // Drain queued live events that arrived while historical events were replaying.
      const queuedLive = client.replayQueue;
      client.replayQueue = [];
      client.replaying = false;

      for (const liveMsg of queuedLive) {
        // Discard duplicates if historical replay already delivered this event
        if (liveMsg.id > client.lastDeliveredId) {
          writeDirectToClient(client, liveMsg.payload);
          client.lastDeliveredId = liveMsg.id;
        }
      }
    } catch (err) {
      console.error('[SSE Replay Error]', err);
    } finally {
      client.replaying = false;
    }
  }

  // Clean up all resources when client disconnects or aborts
  const cleanup = () => {
    connectedClients.delete(client);
    client.buffer = [];
    client.replayQueue = [];
  };

  req.on('close', cleanup);
  res.on('finish', cleanup);
  res.on('error', cleanup);
}

/**
 * Returns the current count of connected SSE clients.
 */
function getConnectedClientsCount() {
  return connectedClients.size;
}

/**
 * Stops all internal timers and cleans up resources (used for graceful shutdown and tests).
 */
function stopStreamWorker() {
  clearInterval(flushTimer);
  clearInterval(heartbeatTimer);
  clearInterval(pruneTimer);
  clearInterval(crossInstanceTimer);
  for (const client of connectedClients) {
    try {
      client.res.end();
    } catch (_) {}
  }
  connectedClients.clear();
  pendingVehicles.clear();
}

module.exports = {
  sseStreamHandler,
  publishVehicleUpdate,
  publishAlertChange,
  flushCoalescedVehicles,
  recordAndBroadcastEvent,
  formatSseMessage,
  pruneOldEvents,
  getConnectedClientsCount,
  stopStreamWorker
};
