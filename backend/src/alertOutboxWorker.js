const db = require('./db');
const { evaluateVehicleAlerts } = require('./alertEngine');

let workerTimer = null;
const OUTBOX_POLL_INTERVAL_MS = parseInt(process.env.ALERT_OUTBOX_INTERVAL_MS || '500', 10);
const MAX_ATTEMPTS = 5;

// Prepared statements for alert outbox operations
const insertOutboxStmt = db.prepare(`
  INSERT INTO alert_outbox (vehicle_id, status, created_at)
  VALUES (@vehicle_id, 'pending', @created_at)
`);

const findPendingOutboxStmt = db.prepare(`
  SELECT id, vehicle_id, attempts
  FROM alert_outbox
  WHERE status = 'pending'
  ORDER BY id ASC
  LIMIT ?
`);

const claimOutboxStmt = db.prepare(`
  UPDATE alert_outbox
  SET status = 'processing',
      attempts = attempts + 1
  WHERE id = ? AND status = 'pending'
`);

const completeOutboxStmt = db.prepare(`
  UPDATE alert_outbox
  SET status = 'completed',
      processed_at = @now,
      error = NULL
  WHERE id = @id
`);

const completeOutboxByVehiclesStmt = db.prepare(`
  UPDATE alert_outbox
  SET status = 'completed',
      processed_at = @now,
      error = NULL
  WHERE vehicle_id = @vehicle_id AND status IN ('pending', 'processing')
`);

const failOutboxStmt = db.prepare(`
  UPDATE alert_outbox
  SET status = @status,
      error = @error,
      processed_at = CASE WHEN @status = 'failed' THEN @now ELSE NULL END
  WHERE id = @id
`);

/**
 * Mark pending outbox events for given vehicle IDs as completed.
 * Used by inline ingestion to complete outbox rows immediately upon successful evaluation.
 * @param {Set<string>|Array<string>} vehicleIds
 * @param {string} nowIso
 */
function markOutboxEventsCompleted(vehicleIds, nowIso = new Date().toISOString()) {
  const distinctIds = vehicleIds instanceof Set ? vehicleIds : new Set(vehicleIds);
  for (const vid of distinctIds) {
    completeOutboxByVehiclesStmt.run({
      vehicle_id: vid,
      now: nowIso
    });
  }
}

/**
 * Processes pending alert outbox events.
 * Safe against crashes and retries without creating duplicate alerts.
 * @param {number} batchSize Maximum number of events to process in one pass
 * @param {string} nowIso Optional timestamp
 * @returns {object} Summary of processed and failed counts
 */
function processPendingOutboxEvents(batchSize = 100, nowIso = new Date().toISOString()) {
  const pendingRows = findPendingOutboxStmt.all(batchSize);
  if (!pendingRows || pendingRows.length === 0) {
    return { processedCount: 0, failedCount: 0 };
  }

  let processedCount = 0;
  let failedCount = 0;

  for (const row of pendingRows) {
    // Atomically claim the row
    const claimResult = claimOutboxStmt.run(row.id);
    if (claimResult.changes === 0) {
      // Already claimed or completed by another worker/thread
      continue;
    }

    try {
      // Evaluate all alert rules for the vehicle
      evaluateVehicleAlerts(row.vehicle_id, nowIso);

      // Mark outbox row as completed
      completeOutboxStmt.run({
        id: row.id,
        now: nowIso
      });
      processedCount++;
    } catch (err) {
      console.error(`[AlertOutboxWorker Error] Vehicle ${row.vehicle_id} (ID ${row.id}):`, err);
      const isFinalFail = (row.attempts + 1) >= MAX_ATTEMPTS;
      failOutboxStmt.run({
        id: row.id,
        status: isFinalFail ? 'failed' : 'pending',
        error: err.message,
        now: nowIso
      });
      failedCount++;
    }
  }

  return { processedCount, failedCount };
}

/**
 * Starts the background alert outbox recovery worker.
 * Runs an immediate pass on startup to drain any events orphaned by previous crashes.
 */
function startAlertOutboxWorker(intervalMs = OUTBOX_POLL_INTERVAL_MS) {
  if (workerTimer) return;
  // Immediate pass upon startup
  try {
    processPendingOutboxEvents();
  } catch (err) {
    console.error('[AlertOutboxWorker Startup Error]', err);
  }

  workerTimer = setInterval(() => {
    try {
      processPendingOutboxEvents();
    } catch (err) {
      console.error('[AlertOutboxWorker Interval Error]', err);
    }
  }, intervalMs);

  if (workerTimer.unref) {
    workerTimer.unref();
  }
}

/**
 * Stops the background alert outbox worker.
 */
function stopAlertOutboxWorker() {
  if (workerTimer) {
    clearInterval(workerTimer);
    workerTimer = null;
  }
}

module.exports = {
  insertOutboxStmt,
  markOutboxEventsCompleted,
  processPendingOutboxEvents,
  startAlertOutboxWorker,
  stopAlertOutboxWorker
};
