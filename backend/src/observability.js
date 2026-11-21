/**
 * VoltGrid Fleet Telemetry Service - Observability, Structured Logging & Metrics (Part A6)
 * 
 * Provides:
 * - Correlation: Generates or propagates X-Request-Id across the request lifecycle.
 * - Structured JSON logging: Single-line JSON to stdout with duration, level, method, path, status, and error tracing.
 * - Security: Never logs credentials, HMAC secrets, or authorization tokens.
 * - Health: Liveness and Readiness probes (with database connectivity verification).
 * - Metrics: Process-local operational metrics tracking requests, latency percentiles, telemetry counts,
 *   auth failures, rate-limit rejections, active SSE sockets, and database aggregates.
 */

const crypto = require('crypto');
const db = require('./db');

// In-memory process-local metric stores (lightweight, zero external dependencies)
let requestsTotal = 0;
let errorsTotal = 0;
const statusCodes = {};
const requestsByEndpoint = {};

// Reservoir of recent latencies (capped at 2,000 samples) for percentile calculations
const MAX_LATENCY_SAMPLES = 2000;
const latencySamples = [];

// Domain operational counters
let readingsAcceptedTotal = 0;
let readingsRejectedTotal = 0;
let readingsDuplicatesTotal = 0;
let authFailuresTotal = 0;
let rateLimitRejectionsTotal = 0;
let alertsCreatedTotal = 0;
let alertsResolvedTotal = 0;

const startTime = Date.now();

// Lazy database statements
let countVehiclesStmt;
let countHistoryStmt;
let countActiveAlertsStmt;

function getStatements() {
  if (!countVehiclesStmt) {
    try {
      countVehiclesStmt = db.prepare('SELECT COUNT(*) AS total FROM vehicles');
      countHistoryStmt = db.prepare('SELECT COUNT(*) AS total FROM telemetry_history');
      countActiveAlertsStmt = db.prepare("SELECT COUNT(*) AS total FROM alerts WHERE status != 'resolved'");
    } catch (_) {}
  }
}

/**
 * Calculates a percentile from an array of numbers
 */
function calculatePercentile(sortedArray, percentile) {
  if (sortedArray.length === 0) return 0;
  const index = Math.min(
    sortedArray.length - 1,
    Math.max(0, Math.ceil((percentile / 100) * sortedArray.length) - 1)
  );
  return Number(sortedArray[index].toFixed(2));
}

/**
 * Normalizes request paths for grouped endpoint metrics (collapsing dynamic IDs)
 */
function normalizePath(path) {
  if (!path) return 'unknown';
  const clean = path.split('?')[0];
  // Replace vehicle ID or numeric ID in path (e.g. /v1/vehicles/VG-00042/history -> /v1/vehicles/:id/history)
  return clean
    .replace(/\/vehicles\/VG-[A-Za-z0-9-_]+/g, '/vehicles/:id')
    .replace(/\/alerts\/\d+/g, '/alerts/:id');
}

/**
 * Express Middleware: Attaches/propagates X-Request-Id, measures latency,
 * records endpoint metrics, and outputs single-line structured JSON logs.
 */
function requestIdMiddleware(req, res, next) {
  const startHr = process.hrtime.bigint();
  requestsTotal++;

  // 1. Request ID correlation: accept incoming header or generate UUID v4
  const incomingId = req.headers['x-request-id'];
  const requestId = (typeof incomingId === 'string' && incomingId.trim().length > 0)
    ? incomingId.trim()
    : crypto.randomUUID();

  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);

  // 2. Track endpoint path for metrics
  const routeKey = `${req.method} ${normalizePath(req.originalUrl || req.url)}`;
  requestsByEndpoint[routeKey] = (requestsByEndpoint[routeKey] || 0) + 1;

  // 3. Response completion hook
  res.on('finish', () => {
    const endHr = process.hrtime.bigint();
    const durationMs = Number(endHr - startHr) / 1e6;

    // Track status code
    const statusKey = String(res.statusCode);
    statusCodes[statusKey] = (statusCodes[statusKey] || 0) + 1;

    if (res.statusCode >= 500) {
      errorsTotal++;
    }

    // Sample latency
    if (latencySamples.length < MAX_LATENCY_SAMPLES) {
      latencySamples.push(durationMs);
    } else {
      const replaceIdx = Math.floor(Math.random() * MAX_LATENCY_SAMPLES);
      latencySamples[replaceIdx] = durationMs;
    }

    // Determine structured log level
    const logLevel = res.statusCode >= 500 ? 'error' : (res.statusCode >= 400 ? 'warn' : 'info');

    // Single-line structured JSON log
    const logEntry = {
      ts: new Date().toISOString(),
      level: logLevel,
      request_id: requestId,
      method: req.method,
      path: req.originalUrl || req.url,
      status: res.statusCode,
      duration_ms: Number(durationMs.toFixed(2))
    };

    // If an error was attached during request lifecycle, include message safely
    if (res.locals?.error) {
      logEntry.error = res.locals.error.message || String(res.locals.error);
    }

    // Sanitize: Never log authorization headers, HMAC signatures, or raw request body
    console.log(JSON.stringify(logEntry));
  });

  next();
}

/**
 * Domain metric increment helpers
 */
function trackTelemetryBatch(accepted, duplicates, rejected) {
  readingsAcceptedTotal += (accepted || 0);
  readingsDuplicatesTotal += (duplicates || 0);
  readingsRejectedTotal += (rejected || 0);
}

function trackAuthFailure() {
  authFailuresTotal++;
}

function trackRateLimitRejection() {
  rateLimitRejectionsTotal++;
}

function trackAlertCreated() {
  alertsCreatedTotal++;
}

function trackAlertResolved() {
  alertsResolvedTotal++;
}

/**
 * GET /health and GET /health/live
 * Liveness Check: Confirms the Node.js application process is running.
 */
function livenessHandler(req, res) {
  const uptimeSec = Math.floor((Date.now() - startTime) / 1000);
  res.status(200).json({
    status: 'ok',
    uptime_sec: uptimeSec,
    timestamp: new Date().toISOString()
  });
}

/**
 * GET /health/ready
 * Readiness Check: Confirms that the application can query the persistent SQLite database.
 * Returns 200 if database is reachable, or 503 Service Unavailable if database is down.
 */
function readinessHandler(req, res) {
  try {
    // Probe database connectivity with minimal overhead
    const probe = db.prepare('SELECT 1 AS ready').get();
    if (probe && probe.ready === 1) {
      return res.status(200).json({
        status: 'ready',
        database: 'connected',
        timestamp: new Date().toISOString()
      });
    }
    throw new Error('Database check returned unexpected result');
  } catch (err) {
    if (res.locals) res.locals.error = err;
    return res.status(503).json({
      status: 'not_ready',
      error: 'Database connectivity check failed',
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * GET /metrics
 * Returns comprehensive operational metrics (process-local).
 */
function getMetricsHandler(req, res) {
  try {
    getStatements();
    const uptimeSec = Math.floor((Date.now() - startTime) / 1000);

    // Latency metrics calculation
    const sortedLatencies = [...latencySamples].sort((a, b) => a - b);
    const avgLatency = sortedLatencies.length > 0
      ? Number((sortedLatencies.reduce((a, b) => a + b, 0) / sortedLatencies.length).toFixed(2))
      : 0;
    const minLatency = sortedLatencies.length > 0 ? Number(sortedLatencies[0].toFixed(2)) : 0;
    const maxLatency = sortedLatencies.length > 0 ? Number(sortedLatencies[sortedLatencies.length - 1].toFixed(2)) : 0;
    const p50Latency = calculatePercentile(sortedLatencies, 50);
    const p95Latency = calculatePercentile(sortedLatencies, 95);
    const p99Latency = calculatePercentile(sortedLatencies, 99);

    // Active SSE connections count
    let activeSseCount = 0;
    try {
      const { getConnectedClientsCount } = require('./stream');
      activeSseCount = getConnectedClientsCount();
    } catch (_) {}

    // Live SQLite database row counts
    let vehiclesCount = 0;
    let historyCount = 0;
    let activeAlertsCount = 0;
    try {
      vehiclesCount = Number(countVehiclesStmt?.get()?.total || 0);
      historyCount = Number(countHistoryStmt?.get()?.total || 0);
      activeAlertsCount = Number(countActiveAlertsStmt?.get()?.total || 0);
    } catch (_) {}

    res.json({
      process: {
        uptime_sec: uptimeSec,
        memory_usage_mb: Number((process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(2)),
        node_version: process.version,
        scope: 'process-local'
      },
      requests: {
        total: requestsTotal,
        errors: errorsTotal,
        status_codes: statusCodes,
        by_endpoint: requestsByEndpoint
      },
      latency_ms: {
        min: minLatency,
        avg: avgLatency,
        p50: p50Latency,
        p95: p95Latency,
        p99: p99Latency,
        max: maxLatency,
        samples_collected: sortedLatencies.length
      },
      telemetry: {
        readings_accepted_total: readingsAcceptedTotal,
        readings_duplicates_total: readingsDuplicatesTotal,
        readings_rejected_total: readingsRejectedTotal
      },
      security: {
        auth_failures_total: authFailuresTotal,
        rate_limit_rejections_total: rateLimitRejectionsTotal
      },
      stream: {
        active_sse_connections: activeSseCount
      },
      alerts: {
        created_total: alertsCreatedTotal,
        resolved_total: alertsResolvedTotal
      },
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
  livenessHandler,
  readinessHandler,
  getMetricsHandler,
  trackTelemetryBatch,
  trackAuthFailure,
  trackRateLimitRejection,
  trackAlertCreated,
  trackAlertResolved
};
