const crypto = require('crypto');
const db = require('./db');

// In-memory metrics counters (lightweight and zero external dependencies)
let requestsTotal = 0;
let errorsTotal = 0;
const startTime = Date.now();

// Prepared statements for database-level metrics
const countVehiclesStmt = db.prepare('SELECT COUNT(*) AS total FROM vehicles');
const countHistoryStmt = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history');
const countActiveAlertsStmt = db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE status != 'resolved'");

/**
 * Middleware: Attaches or generates a unique X-Request-Id and emits
 * a single-line structured JSON log on request completion.
 */
function requestIdMiddleware(req, res, next) {
  const start = Date.now();
  requestsTotal++;

  // Honor incoming X-Request-Id header if present; otherwise generate a UUID v4
  const incomingId = req.headers['x-request-id'];
  const requestId = (typeof incomingId === 'string' && incomingId.trim().length > 0)
    ? incomingId.trim()
    : crypto.randomUUID();

  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);

  // Hook into response completion to write structured log
  res.on('finish', () => {
    const durationMs = Date.now() - start;

    if (res.statusCode >= 500) {
      errorsTotal++;
    }

    const logLevel = res.statusCode >= 500 ? 'error' : (res.statusCode >= 400 ? 'warn' : 'info');

    // Single-line structured JSON logging to stdout
    const logEntry = {
      ts: new Date().toISOString(),
      level: logLevel,
      request_id: requestId,
      method: req.method,
      path: req.originalUrl || req.url,
      status: res.statusCode,
      duration_ms: durationMs
    };

    console.log(JSON.stringify(logEntry));
  });

  next();
}

/**
 * Handler for GET /metrics
 * Returns basic in-memory performance metrics and database table counts.
 */
function getMetricsHandler(req, res) {
  try {
    const uptimeSec = Math.floor((Date.now() - startTime) / 1000);

    const vehiclesCount = Number(countVehiclesStmt.get().total || 0);
    const historyCount = Number(countHistoryStmt.get().total || 0);
    const activeAlertsCount = Number(countActiveAlertsStmt.get().total || 0);

    res.json({
      uptime_sec: uptimeSec,
      requests_total: requestsTotal,
      errors_total: errorsTotal,
      database: {
        vehicles_total: vehiclesCount,
        history_total: historyCount,
        active_alerts_total: activeAlertsCount
      }
    });
  } catch (err) {
    console.error('[Metrics Error]', err);
    res.status(500).json({ error: 'Failed to collect metrics' });
  }
}

module.exports = {
  requestIdMiddleware,
  getMetricsHandler
};
