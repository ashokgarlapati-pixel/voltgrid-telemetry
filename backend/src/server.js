const express = require('express');
const db = require('./db');
const { ingestBatch } = require('./ingestion');
const { startOfflineWorker, stopOfflineWorker } = require('./offlineWorker');
const { startAlertOutboxWorker, stopAlertOutboxWorker } = require('./alertOutboxWorker');
const {
  requestIdMiddleware,
  livenessHandler,
  readinessHandler,
  getMetricsHandler
} = require('./observability');
const {
  getVehiclesHandler,
  getVehicleHistoryHandler,
  getFleetSummaryHandler,
  ackAlertHandler
} = require('./queries');
const {
  DEV_API_KEY,
  DEV_API_SECRET,
  hmacAuth,
  rateLimiter,
  validateAndInitSecurityCredentials
} = require('./security');
const { sseStreamHandler, stopStreamWorker } = require('./stream');

const app = express();
const PORT = process.env.PORT || 3000;

// Validate security configuration and initialize credentials (fails immediately on unsafe production config)
validateAndInitSecurityCredentials();

// Observability: attach/generate X-Request-Id and log structured JSON per request
app.use(requestIdMiddleware);

// Middleware for parsing JSON with rawBody preservation for HMAC signature verification
app.use(express.json({
  limit: '10mb',
  verify: (req, res, buf) => {
    req.rawBody = buf.toString('utf8');
  }
}));

// Handle invalid JSON body syntax errors gracefully
app.use((err, req, res, next) => {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({ error: 'Malformed JSON payload' });
  }
  next(err);
});

/**
 * Health Check Endpoints (Part A6)
 * - GET /health and GET /health/live: Liveness check (process up)
 * - GET /health/ready: Readiness check (database reachable)
 */
app.get('/health', livenessHandler);
app.get('/health/live', livenessHandler);
app.get('/health/ready', readinessHandler);

/**
 * GET /metrics
 * Comprehensive in-memory performance metrics and database table counts (A6)
 */
app.get('/metrics', getMetricsHandler);

/**
 * POST /v1/telemetry/batch
 * Ingests a batch of up to 500 telemetry readings with individual validation,
 * database-level idempotency, and timestamp-based vehicle state updating.
 * Protected by HMAC authentication and distributed token-bucket rate limiting.
 */
app.post('/v1/telemetry/batch', hmacAuth, rateLimiter, (req, res) => {
  let readings;

  // Accept direct array or { readings: [...] } object
  if (Array.isArray(req.body)) {
    readings = req.body;
  } else if (req.body && typeof req.body === 'object' && Array.isArray(req.body.readings)) {
    readings = req.body.readings;
  } else {
    return res.status(400).json({
      error: 'Invalid payload: expected an array of readings or { readings: [...] }'
    });
  }

  // Reject empty arrays or batches exceeding 500 readings
  if (readings.length === 0 || readings.length > 500) {
    return res.status(400).json({
      error: `Batch size must be between 1 and 500 readings (received ${readings.length})`
    });
  }

  try {
    const outcome = ingestBatch(readings);
    // Return 200 if all valid/duplicate, or 207 (Multi-Status) if any reading was rejected
    const statusCode = outcome.rejected > 0 ? 207 : 200;
    return res.status(statusCode).json(outcome);
  } catch (err) {
    console.error('[Ingestion Error]', err);
    return res.status(500).json({
      error: 'Internal server error while processing batch'
    });
  }
});

/**
 * GET /v1/completeness
 * Calculates sequence completeness fleet-wide or for an individual vehicle:
 * total_received vs total_expected = SUM(max_seq - min_seq + 1)
 */
app.get('/v1/completeness', (req, res) => {
  try {
    const { vehicle_id } = req.query;

    if (vehicle_id) {
      const v = db.prepare(`
        SELECT vehicle_id, min_seq, max_seq, received_count, updated_at
        FROM vehicles
        WHERE vehicle_id = ?
      `).get(vehicle_id);

      if (!v) {
        return res.status(404).json({ error: `Vehicle ${vehicle_id} not found` });
      }

      const expectedCount = v.max_seq - v.min_seq + 1;
      const missingCount = Math.max(0, expectedCount - v.received_count);
      const completenessPct = expectedCount > 0
        ? Number(((v.received_count / expectedCount) * 100).toFixed(2))
        : 100.0;

      // Identify specific missing sequence numbers within observed [min_seq, max_seq] range
      let missingSequences = [];
      if (missingCount > 0 && expectedCount <= 5000) {
        const rows = db.prepare(`
          SELECT seq FROM telemetry_history
          WHERE vehicle_id = ? AND seq >= ? AND seq <= ?
          ORDER BY seq ASC
        `).all(vehicle_id, v.min_seq, v.max_seq);

        const seenSeqs = new Set(rows.map(r => r.seq));
        for (let s = v.min_seq; s <= v.max_seq; s++) {
          if (!seenSeqs.has(s)) {
            missingSequences.push(s);
            if (missingSequences.length >= 100) break;
          }
        }
      }

      return res.json({
        vehicle_id: v.vehicle_id,
        min_seq: v.min_seq,
        max_seq: v.max_seq,
        received_count: v.received_count,
        expected_count: expectedCount,
        missing_count: missingCount,
        missing_sequences: missingSequences,
        completeness_pct: completenessPct,
        note: 'Completeness is measured strictly within the observed range [min_seq, max_seq]. Missing sequences prior to min_seq cannot be inferred without an external baseline.'
      });
    }

    const row = db.prepare(`
      SELECT 
        COUNT(*) AS total_vehicles,
        COALESCE(SUM(received_count), 0) AS total_received,
        COALESCE(SUM(max_seq - min_seq + 1), 0) AS total_expected
      FROM vehicles
    `).get();

    const totalVehicles = Number(row.total_vehicles || 0);
    const totalReceived = Number(row.total_received || 0);
    const totalExpected = Number(row.total_expected || 0);

    const completenessPct = totalExpected > 0
      ? Number(((totalReceived / totalExpected) * 100).toFixed(2))
      : 100.0;

    res.json({
      total_vehicles: totalVehicles,
      total_received: totalReceived,
      total_expected: totalExpected,
      completeness_pct: completenessPct
    });
  } catch (err) {
    console.error('[Completeness Query Error]', err);
    res.status(500).json({
      error: 'Failed to calculate completeness metrics'
    });
  }
});

/**
 * GET /v1/alerts
 * Quick inspection endpoint for Phase 2 verification.
 * Filters by optional query parameters: status, rule, vehicle_id.
 */
app.get('/v1/alerts', (req, res) => {
  try {
    const { status, rule, vehicle_id, limit = '100' } = req.query;
    const conditions = [];
    const params = [];

    if (status) {
      conditions.push('status = ?');
      params.push(status);
    }
    if (rule) {
      conditions.push('rule = ?');
      params.push(rule);
    }
    if (vehicle_id) {
      conditions.push('vehicle_id = ?');
      params.push(vehicle_id);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const safeLimit = Math.min(500, Math.max(1, parseInt(limit, 10) || 100));

    const sql = `
      SELECT id, vehicle_id, rule, status, version, details, opened_at, resolved_at, updated_at
      FROM alerts
      ${whereClause}
      ORDER BY opened_at DESC, id DESC
      LIMIT ?
    `;

    params.push(safeLimit);
    const rows = db.prepare(sql).all(...params);

    const alerts = rows.map(r => {
      let parsedDetails = null;
      try {
        parsedDetails = r.details ? JSON.parse(r.details) : null;
      } catch (_) {
        parsedDetails = r.details;
      }
      return {
        ...r,
        details: parsedDetails
      };
    });

    res.json({
      total: alerts.length,
      alerts
    });
  } catch (err) {
    console.error('[Alerts Query Error]', err);
    res.status(500).json({
      error: 'Failed to query alerts'
    });
  }
});

/* ==========================================================================
   PART A4: QUERY API ROUTES
   ========================================================================== */

/**
 * GET /v1/vehicles
 * Keyset-paginated vehicle fleet state with status & bounding-box filters
 */
app.get('/v1/vehicles', getVehiclesHandler);

/**
 * GET /v1/vehicles/:id/history
 * Peak-preserving downsampled time-series for a vehicle
 */
app.get('/v1/vehicles/:id/history', getVehicleHistoryHandler);

/**
 * GET /v1/fleet/summary
 * Sub-150ms fleet summary aggregated strictly from vehicles & alerts tables
 */
app.get('/v1/fleet/summary', getFleetSummaryHandler);

/**
 * POST /v1/alerts/:id/ack
 * Optimistic concurrency acknowledgment for an alert (returns 409 on version conflict)
 */
app.post('/v1/alerts/:id/ack', ackAlertHandler);

/* ==========================================================================
   PART A5: LIVE STREAMING (SERVER-SENT EVENTS)
   ========================================================================== */

/**
 * GET /v1/stream
 * Live Server-Sent Events (SSE) feed for coalesced vehicle updates and alert changes.
 * Supports Last-Event-ID reconnection, client backpressure, and heartbeat keep-alive.
 */
app.get('/v1/stream', sseStreamHandler);

// Start Express server
const server = app.listen(PORT, () => {
  const actualPort = server.address()?.port || PORT;
  console.log(`[VoltGrid API] Listening on port ${actualPort}`);
  // Start background offline detection worker
  startOfflineWorker();
  // Start background transactional alert outbox worker for crash recovery
  startAlertOutboxWorker();
});

// Graceful shutdown handling
function handleShutdown(signal) {
  console.log(`[VoltGrid API] Received ${signal}, closing server, background workers, and database...`);
  stopOfflineWorker();
  stopAlertOutboxWorker();
  stopStreamWorker();
  server.close(() => {
    try {
      db.close();
      console.log('[VoltGrid API] Database connection closed.');
    } catch (e) {
      console.error('[VoltGrid API] Error closing database:', e);
    }
    process.exit(0);
  });
}

process.on('SIGINT', () => handleShutdown('SIGINT'));
process.on('SIGTERM', () => handleShutdown('SIGTERM'));

module.exports = { app, server };
