const express = require('express');
const db = require('./db');
const { ingestBatch } = require('./ingestion');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware for parsing JSON with generous payload limit for batches of 500
app.use(express.json({ limit: '10mb' }));

// Handle invalid JSON body syntax errors gracefully
app.use((err, req, res, next) => {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({ error: 'Malformed JSON payload' });
  }
  next(err);
});

/**
 * GET /health
 * Service health check endpoint
 */
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString()
  });
});

/**
 * POST /v1/telemetry/batch
 * Ingests a batch of up to 500 telemetry readings with individual validation,
 * database-level idempotency, and timestamp-based vehicle state updating.
 */
app.post('/v1/telemetry/batch', (req, res) => {
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
 * Calculates fleet-wide sequence completeness:
 * total_received vs total_expected = SUM(max_seq - min_seq + 1)
 */
app.get('/v1/completeness', (req, res) => {
  try {
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

// Start Express server
const server = app.listen(PORT, () => {
  console.log(`[VoltGrid API] Listening on port ${PORT}`);
});

// Graceful shutdown handling
function handleShutdown(signal) {
  console.log(`[VoltGrid API] Received ${signal}, closing server and database...`);
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
