const db = require('./db');
const { publishAlertChange } = require('./stream');

let workerInterval = null;
let isCheckInProgress = false;

const OFFLINE_CHECK_INTERVAL_MS = parseInt(process.env.OFFLINE_CHECK_INTERVAL_MS || '10000', 10);
const OFFLINE_THRESHOLD_SEC = parseInt(process.env.OFFLINE_THRESHOLD_SEC || '90', 10);

// Prepared statements for offline detection.
// Identifies:
// 1. Online vehicles whose last telemetry receive time (updated_at) exceeds the offline threshold.
// 2. Already-offline vehicles that do not currently have an active (non-resolved) offline alert.
// Explicitly ignores vehicles in 'maintenance' or still within the freshness threshold.
const findStaleVehiclesStmt = db.prepare(`
  SELECT vehicle_id, updated_at, status
  FROM vehicles
  WHERE updated_at <= ? AND (
    status = 'online' OR 
    (status = 'offline' AND vehicle_id NOT IN (
      SELECT vehicle_id FROM alerts WHERE rule = 'offline' AND status != 'resolved'
    ))
  )
  ORDER BY vehicle_id ASC
`);

const setVehicleOfflineStmt = db.prepare(`
  UPDATE vehicles
  SET status = 'offline'
  WHERE vehicle_id = ?
`);

const getActiveOfflineAlertStmt = db.prepare(`
  SELECT id FROM alerts
  WHERE vehicle_id = ? AND rule = 'offline' AND status != 'resolved'
`);

const insertOfflineAlertStmt = db.prepare(`
  INSERT INTO alerts (
    vehicle_id, rule, status, version, details, opened_at, updated_at
  ) VALUES (
    @vehicle_id, 'offline', 'open', 1, @details, @now, @now
  )
`);

/**
 * Checks for online vehicles that have not reported within the threshold window,
 * and recovers missing offline alerts for already-offline vehicles after restart.
 * Wrapped in an immediate transaction for atomicity.
 * Defers event publishing until after commit.
 */
const checkOfflineVehicles = db.transaction((cutoffIso, nowIso, thresholdSec = OFFLINE_THRESHOLD_SEC) => {
  const staleVehicles = findStaleVehiclesStmt.all(cutoffIso);
  let newlyOfflineCount = 0;
  const alertsToPublish = [];

  for (const v of staleVehicles) {
    // Transition vehicle status to offline if not already offline
    if (v.status !== 'offline') {
      setVehicleOfflineStmt.run(v.vehicle_id);
    }

    // Open de-duplicated offline alert if none exists
    const activeAlert = getActiveOfflineAlertStmt.get(v.vehicle_id);
    if (!activeAlert) {
      const silenceSec = Math.round((Date.parse(nowIso) - Date.parse(v.updated_at)) / 1000);
      const insertInfo = insertOfflineAlertStmt.run({
        vehicle_id: v.vehicle_id,
        details: JSON.stringify({
          silence_seconds: silenceSec,
          threshold_seconds: thresholdSec,
          last_seen: v.updated_at
        }),
        now: nowIso
      });
      const newAlert = db.prepare('SELECT * FROM alerts WHERE id = ?').get(insertInfo.lastInsertRowid);
      if (newAlert) {
        alertsToPublish.push(newAlert);
      }
      newlyOfflineCount++;
    }
  }

  return { staleCount: staleVehicles.length, newlyOfflineCount, alertsToPublish };
}).immediate;

/**
 * Executes a single offline check cycle.
 * Supports optional nowMs for injectable clock / deterministic unit testing.
 * Protected by isCheckInProgress guard so slow checks cannot overlap.
 * @param {number} thresholdSec Silence threshold in seconds (default 90)
 * @param {number} nowMs Current server epoch ms (default Date.now())
 */
function runOfflineCheck(thresholdSec = OFFLINE_THRESHOLD_SEC, nowMs = Date.now()) {
  if (isCheckInProgress) {
    return { staleCount: 0, newlyOfflineCount: 0, skipped: true };
  }
  isCheckInProgress = true;
  try {
    const cutoffMs = nowMs - (thresholdSec * 1000);
    const cutoffIso = new Date(cutoffMs).toISOString();
    const nowIso = new Date(nowMs).toISOString();

    const result = checkOfflineVehicles(cutoffIso, nowIso, thresholdSec);
    // Publish alert events strictly AFTER the transaction has committed!
    if (result.alertsToPublish && result.alertsToPublish.length > 0) {
      for (const alert of result.alertsToPublish) {
        publishAlertChange(alert);
      }
    }
    if (result.newlyOfflineCount > 0) {
      console.log(`[OfflineWorker] Flagged ${result.newlyOfflineCount} silent vehicles as offline`);
    }
    return { staleCount: result.staleCount, newlyOfflineCount: result.newlyOfflineCount };
  } catch (err) {
    console.error('[OfflineWorker Error]', err);
    return { staleCount: 0, newlyOfflineCount: 0, error: err.message };
  } finally {
    isCheckInProgress = false;
  }
}

/**
 * Returns whether the background offline worker is currently running with an active interval.
 * @returns {boolean}
 */
function isWorkerRunning() {
  return workerInterval !== null;
}

/**
 * Starts the periodic background offline worker.
 * - If worker is already running, safely no-ops without creating new timers or re-running startup recovery.
 * - Starts the first recovery check immediately upon worker startup.
 * - Continues periodic checks afterward.
 * - Prevents overlapping checks.
 * - Handles errors gracefully without terminating future intervals.
 * @param {number} intervalMs 
 * @param {number} thresholdSec 
 * @param {number|null} startupNowMs Optional epoch ms for deterministic testing
 */
function startOfflineWorker(intervalMs = OFFLINE_CHECK_INTERVAL_MS, thresholdSec = OFFLINE_THRESHOLD_SEC, startupNowMs = null) {
  if (workerInterval !== null) {
    // Already running; safe no-op. Do not create another interval, do not run startup recovery again.
    return;
  }

  console.log(`[OfflineWorker] Started background monitor (interval: ${intervalMs}ms, threshold: ${thresholdSec}s)`);

  // 1. Immediate recovery check during worker startup
  try {
    runOfflineCheck(thresholdSec, startupNowMs !== null ? startupNowMs : Date.now());
  } catch (err) {
    console.error('[OfflineWorker Immediate Startup Error]', err);
  }

  // 2. Schedule periodic checks afterward with error handling and overlap guard
  workerInterval = setInterval(() => {
    try {
      runOfflineCheck(thresholdSec);
    } catch (err) {
      console.error('[OfflineWorker Periodic Check Error]', err);
    }
  }, intervalMs);

  // Allow Node event loop to exit naturally if only this timer is running
  if (workerInterval.unref) {
    workerInterval.unref();
  }
}

/**
 * Stops the background offline worker cleanly.
 */
function stopOfflineWorker() {
  if (workerInterval !== null) {
    clearInterval(workerInterval);
    workerInterval = null;
  }
  isCheckInProgress = false;
  console.log('[OfflineWorker] Background monitor stopped');
}

module.exports = {
  runOfflineCheck,
  startOfflineWorker,
  stopOfflineWorker,
  isWorkerRunning,
  OFFLINE_CHECK_INTERVAL_MS,
  OFFLINE_THRESHOLD_SEC
};
