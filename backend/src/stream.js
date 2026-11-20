/**
 * VoltGrid Fleet Telemetry Service - Live SSE Stream (Part A5)
 * 
 * Provides Server-Sent Events (SSE) on GET /v1/stream:
 * - Monotonic event IDs and Last-Event-ID resume support with 1,000-event ring buffer.
 * - 1-second update coalescing for vehicle telemetry (so 400 req/s does not flood clients).
 * - Immediate dispatch for high-priority alert changes.
 * - Client back-pressure handling with slow-client disconnect protection.
 * - 15-second heartbeat keep-alive frames.
 */

const MAX_RING_BUFFER_SIZE = 1000;
const FLUSH_INTERVAL_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 15000;
const MAX_DROPPED_FLUSHES = 10;

let nextEventId = 1;
const eventRingBuffer = [];
const connectedClients = new Set();
const pendingVehicles = new Map();

/**
 * Adds an event to the ring buffer, capping at MAX_RING_BUFFER_SIZE.
 */
function recordEvent(type, data) {
  const event = {
    id: nextEventId++,
    type,
    data
  };

  eventRingBuffer.push(event);
  if (eventRingBuffer.length > MAX_RING_BUFFER_SIZE) {
    eventRingBuffer.shift();
  }

  return event;
}

/**
 * Formats an event object as a standard SSE frame.
 */
function formatSseMessage(event) {
  return `id: ${event.id}\nevent: ${event.type}\ndata: ${JSON.stringify(event.data)}\n\n`;
}

/**
 * Writes an SSE frame to a client respecting back-pressure.
 */
function writeToClient(client, event, payload, isCoalesced = false) {
  if (client.isPaused) {
    if (isCoalesced) {
      client.droppedFlushes++;
      if (client.droppedFlushes > MAX_DROPPED_FLUSHES) {
        // Slow client buffer overflow protection: disconnect cleanly
        try {
          client.res.end();
        } catch (_) {}
        connectedClients.delete(client);
      }
      return;
    }
  }

  try {
    const canAcceptMore = client.res.write(payload);
    if (!canAcceptMore) {
      client.isPaused = true;
      client.res.once('drain', () => {
        client.isPaused = false;
        client.droppedFlushes = 0;
      });
    }
  } catch (_) {
    connectedClients.delete(client);
  }
}

/**
 * Broadcasts an event to all connected clients.
 */
function broadcast(event, isCoalesced = false) {
  if (connectedClients.size === 0) return;
  const payload = formatSseMessage(event);

  for (const client of connectedClients) {
    writeToClient(client, event, payload, isCoalesced);
  }
}

/**
 * Publishes a vehicle state update. Coalesced into a batch flushed once every 1,000ms.
 * 
 * @param {object} vehicle - Latest state of vehicle
 */
function publishVehicleUpdate(vehicle) {
  if (!vehicle || !vehicle.vehicle_id) return;
  pendingVehicles.set(vehicle.vehicle_id, vehicle);
}

/**
 * Flushes all pending vehicle updates as a single coalesced 'vehicle_batch' event.
 */
function flushCoalescedVehicles() {
  if (pendingVehicles.size === 0) return;

  const batch = Array.from(pendingVehicles.values());
  pendingVehicles.clear();

  const event = recordEvent('vehicle_batch', {
    count: batch.length,
    vehicles: batch
  });

  broadcast(event, true);
}

// 1-second interval timer for coalesced vehicle updates
const flushTimer = setInterval(flushCoalescedVehicles, FLUSH_INTERVAL_MS);
flushTimer.unref();

/**
 * Publishes an alert change (open, acknowledged, or resolved).
 * High priority: Dispatched immediately without coalescing.
 * 
 * @param {object} alert - Alert record
 */
function publishAlertChange(alert) {
  if (!alert) return;
  const event = recordEvent('alert_change', alert);
  broadcast(event, false);
}

/**
 * Broadcasts heartbeat comments (: heartbeat\n\n) every 15 seconds.
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

const heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_INTERVAL_MS);
heartbeatTimer.unref();

/**
 * Express handler for GET /v1/stream (Server-Sent Events)
 */
function sseStreamHandler(req, res) {
  // Set required SSE response headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  res.write(': connected\n\n');

  const client = {
    id: Date.now() + Math.random(),
    res,
    isPaused: false,
    droppedFlushes: 0
  };

  connectedClients.add(client);

  // Resume support: Replay missed events if Last-Event-ID header or query param is present
  const lastEventIdHeader = req.headers['last-event-id'] || req.query.lastEventId;
  if (lastEventIdHeader !== undefined) {
    const lastId = parseInt(lastEventIdHeader, 10);
    if (!isNaN(lastId)) {
      for (const event of eventRingBuffer) {
        if (event.id > lastId) {
          const payload = formatSseMessage(event);
          writeToClient(client, event, payload, false);
        }
      }
    }
  }

  // Clean up when client disconnects
  req.on('close', () => {
    connectedClients.delete(client);
  });
}

function getConnectedClientsCount() {
  return connectedClients.size;
}

module.exports = {
  sseStreamHandler,
  publishVehicleUpdate,
  publishAlertChange,
  getConnectedClientsCount
};
