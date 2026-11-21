const db = require('./db');
const { evaluateVehicleAlerts } = require('./alertEngine');

let workerTimer = null;
const OUTBOX_POLL_INTERVAL_MS = parseInt(process.env.ALERT_OUTBOX_INTERVAL_MS || '500', 10);
const DEFAULT_LEASE_DURATION_MS = parseInt(process.env.ALERT_OUTBOX_LEASE_MS || '30000', 10);
const MAX_ATTEMPTS = 5;

// Prepared statements for alert outbox operations
const insertOutboxStmt = db.prepare(`
  INSERT INTO alert_outbox (vehicle_id, status, created_at)
  VALUES (@vehicle_id, 'pending', @created_at)
`);

// Finds pending records OR stale processing records whose lease has expired
const findPendingOutboxStmt = db.prepare(`
  SELECT id, vehicle_id, attempts, status, claimed_at
  FROM alert_outbox
  WHERE status = 'pending'
     OR (status = 'processing' AND (claimed_at IS NULL OR claimed_at <= @stale_cutoff))
  ORDER BY id ASC
  LIMIT @limit
`);

// Atomically claims a pending or stale processing record by setting status to 'processing'
// and updating claimed_at to the current timestamp.
// If another worker claimed the record concurrently, changes will be 0.
const claimOutboxStmt = db.prepare(`
  UPDATE alert_outbox
  SET status = 'processing',
      claimed_at = @now,
      attempts = attempts + 1
  WHERE id = @id
    AND (
      status = 'pending'
      OR (status = 'processing' AND (claimed_at IS NULL OR claimed_at <= @stale_cutoff))
    )
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
 * Reclaims stale processing records by resetting their status to 'pending'.
 * @param {number} leaseMs Lease expiration threshold in milliseconds
 * @param {string} nowIso Current ISO timestamp
 * @returns {number} Count of reclaimed records
 */
function reclaimStaleProcessingRecords(leaseMs = DEFAULT_LEASE_DURATION_MS, nowIso = new Date().toISOString()) {
  const staleCutoffIso = new Date(Date.parse(nowIso) - leaseMs).toISOString();
  const result = db.prepare(`
    UPDATE alert_outbox
    SET status = 'pending',
        claimed_at = NULL
    WHERE status = 'processing'
      AND (claimed_at IS NULL OR claimed_at <= ?)
  `).run(staleCutoffIso);
  return result.changes;
}

/**
 * Batched atomic transaction to mark pending outbox events as completed for multiple vehicles.
 */
const completeOutboxBatchTx = db.transaction((vehicleIds, nowIso) => {
  for (const vid of vehicleIds) {
    completeOutboxByVehiclesStmt.run({
      vehicle_id: vid,
      now: nowIso
    });
  }
}).immediate;

/**
 * Mark pending outbox events for given vehicle IDs as completed.
 * Used by inline ingestion to complete outbox rows immediately upon successful evaluation.
 * @param {Set<string>|Array<string>} vehicleIds
 * @param {string} nowIso
 */
function markOutboxEventsCompleted(vehicleIds, nowIso = new Date().toISOString()) {
  const distinctIds = Array.from(vehicleIds instanceof Set ? vehicleIds : new Set(vehicleIds));
  if (distinctIds.length === 0) return;
  completeOutboxBatchTx(distinctIds, nowIso);
}

/**
 * Processes pending or stale alert outbox events.
 * Safe against crashes and retries without creating duplicate alerts.
 * Uses atomic leases to guarantee mutual exclusion across concurrent workers/instances.
 * 
 * @param {number} batchSize Maximum number of events to process in one pass
 * @param {string} nowIso Optional timestamp
 * @param {number} leaseMs Optional lease duration in milliseconds
 * @returns {object} Summary of processed and failed counts
 */
function processPendingOutboxEvents(batchSize = 100, nowIso = new Date().toISOString(), leaseMs = DEFAULT_LEASE_DURATION_MS) {
  const serverNowMs = Date.parse(nowIso);
  const staleCutoffIso = new Date(serverNowMs - leaseMs).toISOString();

  const pendingRows = findPendingOutboxStmt.all({
    limit: batchSize,
    stale_cutoff: staleCutoffIso
  });

  if (!pendingRows || pendingRows.length === 0) {
    return { processedCount: 0, failedCount: 0 };
  }

  let processedCount = 0;
  let failedCount = 0;

  for (const row of pendingRows) {
    // Atomically claim the row with lease timestamp
    const claimResult = claimOutboxStmt.run({
      id: row.id,
      now: nowIso,
      stale_cutoff: staleCutoffIso
    });

    if (claimResult.changes === 0) {
      // Concurrently claimed by another worker or lease was refreshed
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
function startAlertOutboxWorker(intervalMs = OUTBOX_POLL_INTERVAL_MS, leaseMs = DEFAULT_LEASE_DURATION_MS) {
  if (workerTimer) return;
  // Immediate pass upon startup to recover orphaned / pending / stale records
  try {
    processPendingOutboxEvents(100, new Date().toISOString(), leaseMs);
  } catch (err) {
    console.error('[AlertOutboxWorker Startup Error]', err);
  }

  workerTimer = setInterval(() => {
    try {
      processPendingOutboxEvents(100, new Date().toISOString(), leaseMs);
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
  reclaimStaleProcessingRecords,
  processPendingOutboxEvents,
  startAlertOutboxWorker,
  stopAlertOutboxWorker,
  DEFAULT_LEASE_DURATION_MS
};
