const db = require('./db');
const { publishAlertChange } = require('./stream');

let workerInterval = null;

const OFFLINE_CHECK_INTERVAL_MS = parseInt(process.env.OFFLINE_CHECK_INTERVAL_MS || '10000', 10);
const OFFLINE_THRESHOLD_SEC = parseInt(process.env.OFFLINE_THRESHOLD_SEC || '90', 10);

// Prepared statements for offline detection
const findStaleVehiclesStmt = db.prepare(`
  SELECT vehicle_id, updated_at
  FROM vehicles
  WHERE updated_at <= ? AND (
    status = 'online' OR 
    vehicle_id NOT IN (SELECT vehicle_id FROM alerts WHERE rule = 'offline' AND status != 'resolved')
  )
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
 * Checks for online vehicles that have not reported within the threshold window.
 * Transitions them to 'offline' and creates de-duplicated alerts.
 * Wrapped in an immediate transaction for atomicity.
 * Defer event publishing until after commit.
 */
const checkOfflineVehicles = db.transaction((cutoffIso, nowIso) => {
  const staleVehicles = findStaleVehiclesStmt.all(cutoffIso);
  let newlyOfflineCount = 0;
  const alertsToPublish = [];

  for (const v of staleVehicles) {
    // Transition vehicle status to offline
    setVehicleOfflineStmt.run(v.vehicle_id);

    // Open de-duplicated offline alert if none exists
    const activeAlert = getActiveOfflineAlertStmt.get(v.vehicle_id);
    if (!activeAlert) {
      const silenceSec = Math.round((Date.parse(nowIso) - Date.parse(v.updated_at)) / 1000);
      const insertInfo = insertOfflineAlertStmt.run({
        vehicle_id: v.vehicle_id,
        details: JSON.stringify({
          silence_seconds: silenceSec,
          threshold_seconds: OFFLINE_THRESHOLD_SEC,
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
 * @param {number} thresholdSec Silence threshold in seconds (default 90)
 * @param {number} nowMs Current server epoch ms (default Date.now())
 */
function runOfflineCheck(thresholdSec = OFFLINE_THRESHOLD_SEC, nowMs = Date.now()) {
  const cutoffMs = nowMs - (thresholdSec * 1000);
  const cutoffIso = new Date(cutoffMs).toISOString();
  const nowIso = new Date(nowMs).toISOString();

  try {
    const result = checkOfflineVehicles(cutoffIso, nowIso);
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
  }
}

/**
 * Starts the periodic background offline worker
 */
function startOfflineWorker(intervalMs = OFFLINE_CHECK_INTERVAL_MS, thresholdSec = OFFLINE_THRESHOLD_SEC) {
  if (workerInterval) {
    clearInterval(workerInterval);
  }

  console.log(`[OfflineWorker] Started background monitor (interval: ${intervalMs}ms, threshold: ${thresholdSec}s)`);
  // Run an immediate check on startup to detect any vehicles already offline during restart
  runOfflineCheck(thresholdSec);

  workerInterval = setInterval(() => {
    runOfflineCheck(thresholdSec);
  }, intervalMs);

  // Allow Node event loop to exit naturally if only this timer is running
  if (workerInterval.unref) {
    workerInterval.unref();
  }
}

/**
 * Stops the background offline worker
 */
function stopOfflineWorker() {
  if (workerInterval) {
    clearInterval(workerInterval);
    workerInterval = null;
    console.log('[OfflineWorker] Background monitor stopped');
  }
}

module.exports = {
  runOfflineCheck,
  startOfflineWorker,
  stopOfflineWorker,
  OFFLINE_CHECK_INTERVAL_MS,
  OFFLINE_THRESHOLD_SEC
};
