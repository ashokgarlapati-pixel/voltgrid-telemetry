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

const connectedClients = new Set();
const pendingVehicles = new Map();

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
 * Dispatches an individual 'vehicle_update' event for each distinct vehicle updated.
 */
function flushCoalescedVehicles() {
  if (pendingVehicles.size === 0) return;

  const updates = Array.from(pendingVehicles.values());
  pendingVehicles.clear();

  for (const v of updates) {
    const eventData = {
      event_type: 'vehicle_update',
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
    };

    recordAndBroadcastEvent('vehicle_update', eventData);
  }
}

// 1-second interval timer for vehicle update coalescing
let flushTimer = setInterval(flushCoalescedVehicles, FLUSH_INTERVAL_MS);
flushTimer.unref();

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
 * Supports Last-Event-ID resumption, safe replay-to-live handoff, and back-pressure.
 */
function sseStreamHandler(req, res) {
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
      }

      // Query all persistent events created after Last-Event-ID
      const historicalRows = db.prepare(`
        SELECT id, event_type, data
        FROM stream_events
        WHERE id > ?
        ORDER BY id ASC
      `).all(parsedLastId);

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
