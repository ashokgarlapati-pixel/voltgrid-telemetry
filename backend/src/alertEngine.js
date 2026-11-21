const db = require('./db');
const { publishAlertChange } = require('./stream');
const { trackAlertCreated, trackAlertResolved } = require('./observability');

/**
 * Operating polygon coordinates for geofence rule (around Chennai city center).
 * Vehicles outside this boundary trigger a geofence_breach alert.
 */
const CHENNAI_OPERATING_POLYGON = [
  { lat: 13.0000, lng: 80.1800 },
  { lat: 13.1800, lng: 80.1800 },
  { lat: 13.1800, lng: 80.3200 },
  { lat: 13.0000, lng: 80.3200 }
];

/**
 * Ray-casting algorithm to test whether a (lat, lng) point is inside a polygon.
 * @param {number} lat 
 * @param {number} lng 
 * @param {Array<{lat: number, lng: number}>} polygon 
 * @returns {boolean}
 */
function isPointInPolygon(lat, lng, polygon = CHENNAI_OPERATING_POLYGON) {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i].lng, yi = polygon[i].lat;
    const xj = polygon[j].lng, yj = polygon[j].lat;

    const intersect = ((yi > lat) !== (yj > lat)) &&
      (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function normalizeReadings(readings) {
  if (!readings || readings.length === 0) return [];
  return [...readings].sort((a, b) => {
    const diff = Date.parse(a.ts) - Date.parse(b.ts);
    return diff !== 0 ? diff : ((a.seq || 0) - (b.seq || 0));
  });
}

/**
 * Rule 1: Overheat
 * - Trigger: 3 consecutive valid readings with battery_temp_c > 55°C.
 * - Resolve: 3 consecutive valid readings with battery_temp_c <= 50°C.
 * - Between 50°C and 55°C, maintain previous alert state (hysteresis).
 * - Tracks discrete incident episodes for complete order-independent lifecycle reconstruction.
 */
function evaluateOverheat(readings, activeAlert = null) {
  const sorted = normalizeReadings(readings);
  if (sorted.length === 0) {
    return { triggered: false, resolved: false, episodes: [] };
  }

  const episodes = [];
  let state = 'normal'; // 'normal' | 'triggered' | 'resolved'
  let hotStreak = 0;
  let coolStreak = 0;
  let openedAt = null;
  let openDetails = null;
  let resolvedAt = null;

  const alertOpenTs = activeAlert && activeAlert.opened_at ? Date.parse(activeAlert.opened_at) : null;

  for (const r of sorted) {
    const rTs = Date.parse(r.ts);

    // If an active alert is currently open/acknowledged and we reach or pass its opened_at, synchronize in-triggered state
    if (activeAlert && alertOpenTs !== null && rTs >= alertOpenTs && !openedAt && state !== 'triggered') {
      state = 'triggered';
      openedAt = activeAlert.opened_at;
      openDetails = typeof activeAlert.details === 'string'
        ? JSON.parse(activeAlert.details || '{}')
        : (activeAlert.details || { current_temp_c: r.battery_temp_c, threshold: 55.0 });
    }

    if (r.battery_temp_c > 55.0) {
      hotStreak++;
      coolStreak = 0;
      if (hotStreak >= 3) {
        if (state !== 'triggered') {
          openedAt = r.ts;
          openDetails = {
            current_temp_c: r.battery_temp_c,
            threshold: 55.0,
            consecutive_hot: hotStreak
          };
        }
        state = 'triggered';
      }
    } else if (r.battery_temp_c <= 50.0) {
      coolStreak++;
      hotStreak = 0;
      if (coolStreak >= 3) {
        if (state === 'triggered') {
          resolvedAt = r.ts;
          const resolveDetails = {
            current_temp_c: r.battery_temp_c,
            cooling_threshold: 50.0,
            consecutive_cool: coolStreak
          };
          episodes.push({
            status: 'resolved',
            openedAt,
            resolvedAt,
            openDetails,
            details: resolveDetails
          });
          openedAt = null;
          openDetails = null;
          state = 'resolved';
        }
      }
    } else {
      // Hysteresis range (50°C < temp <= 55°C): resets streaks, maintains existing state
      hotStreak = 0;
      coolStreak = 0;
    }
  }

  const latest = sorted[sorted.length - 1];
  if (state === 'triggered') {
    const currentOpenEp = {
      status: 'open',
      openedAt: openedAt || latest.ts,
      resolvedAt: null,
      details: openDetails || {
        current_temp_c: latest.battery_temp_c,
        threshold: 55.0,
        consecutive_hot: hotStreak
      }
    };
    episodes.push(currentOpenEp);

    return {
      triggered: true,
      resolved: false,
      openedAt: openedAt || latest.ts,
      details: {
        current_temp_c: latest.battery_temp_c,
        threshold: 55.0,
        consecutive_hot: hotStreak
      },
      episodes
    };
  } else if (state === 'resolved' || episodes.length > 0) {
    const lastResolvedEp = episodes.length > 0 ? episodes[episodes.length - 1] : null;
    return {
      triggered: false,
      resolved: true,
      openedAt: lastResolvedEp ? lastResolvedEp.openedAt : openedAt,
      resolvedAt: lastResolvedEp ? lastResolvedEp.resolvedAt : (resolvedAt || latest.ts),
      details: {
        current_temp_c: latest.battery_temp_c,
        cooling_threshold: 50.0,
        consecutive_cool: coolStreak
      },
      episodes
    };
  }

  return { triggered: false, resolved: false, episodes: [] };
}

/**
 * Rule 2: Rapid Discharge
 * - Trigger: While charging === false/0 (not charging), soc_pct drops by > 8 points
 *   compared to any non-charging reading within the preceding 5 minutes (300 seconds).
 * - Resolve: The 5-minute drop rate returns to <= 8 points (or the vehicle starts charging,
 *   or the 5-minute window expires).
 */
function evaluateRapidDischarge(readings, activeAlert = null) {
  const sorted = normalizeReadings(readings);
  if (sorted.length === 0) {
    return { triggered: false, resolved: false };
  }

  const latest = sorted[sorted.length - 1];

  // If vehicle is currently charging, rapid discharge is resolved
  if (Boolean(latest.charging)) {
    return {
      triggered: false,
      resolved: true,
      resolvedAt: latest.ts,
      details: { charging: true, current_soc: latest.soc_pct }
    };
  }

  const latestTimeMs = Date.parse(latest.ts);
  const fiveMinWindowStart = latestTimeMs - (5 * 60 * 1000);

  // Filter non-charging readings within preceding 5-minute event window
  const windowReadings = sorted.filter(r =>
    !r.charging &&
    Date.parse(r.ts) >= fiveMinWindowStart &&
    Date.parse(r.ts) <= latestTimeMs
  );

  // If insufficient readings in window:
  // - If alert is currently active, or history has older readings: the 5-minute window has elapsed/expired,
  //   meaning rapid drop is no longer active in the current window -> resolve.
  // - If brand new vehicle with single reading and no active alert: do nothing.
  if (windowReadings.length < 2) {
    if (activeAlert || sorted.length >= 2) {
      return {
        triggered: false,
        resolved: true,
        resolvedAt: latest.ts,
        details: {
          reason: '5_min_window_expired',
          message: 'Rapid discharge condition no longer active (window expired or sparse readings)',
          current_soc: latest.soc_pct
        }
      };
    }
    return { triggered: false, resolved: false };
  }

  // Find maximum SOC within preceding 5 minutes
  let maxSocInWindow = -Infinity;
  for (const r of windowReadings) {
    if (r.soc_pct > maxSocInWindow) {
      maxSocInWindow = r.soc_pct;
    }
  }

  const dropPoints = maxSocInWindow - latest.soc_pct;

  if (dropPoints > 8.0) {
    return {
      triggered: true,
      resolved: false,
      openedAt: latest.ts,
      details: {
        drop_points: Number(dropPoints.toFixed(1)),
        current_soc: latest.soc_pct,
        window_max_soc: maxSocInWindow,
        threshold: 8.0
      }
    };
  }

  return {
    triggered: false,
    resolved: true,
    resolvedAt: latest.ts,
    details: {
      drop_points: Number(dropPoints.toFixed(1)),
      current_soc: latest.soc_pct,
      threshold: 8.0
    }
  };
}

/**
 * Rule 3: Geofence Breach
 * - Trigger: Vehicle moves outside the operating polygon.
 * - Resolve: Vehicle returns inside the operating polygon.
 * - Evaluates readings chronologically to capture intermediate outside/inside transitions
 *   even when they occur within a single ingestion batch.
 */
function evaluateGeofenceBreach(readings, activeAlert = null) {
  const sorted = normalizeReadings(readings);
  if (sorted.length === 0) {
    return { triggered: false, resolved: false, episodes: [] };
  }

  const episodes = [];
  let inBreach = false;
  let currentOpenedAt = null;
  let currentOpenDetails = null;

  const alertOpenTs = activeAlert && activeAlert.opened_at ? Date.parse(activeAlert.opened_at) : null;

  for (const r of sorted) {
    const rTs = Date.parse(r.ts);

    // If an active alert is currently open/acknowledged and we reach or pass its opened_at, synchronize in-breach state
    if (activeAlert && alertOpenTs !== null && rTs >= alertOpenTs && !currentOpenedAt && !inBreach) {
      inBreach = true;
      currentOpenedAt = activeAlert.opened_at;
      currentOpenDetails = typeof activeAlert.details === 'string'
        ? JSON.parse(activeAlert.details || '{}')
        : (activeAlert.details || { lat: r.lat, lng: r.lng, status: 'outside_operating_polygon' });
    }

    const isInside = isPointInPolygon(r.lat, r.lng);

    if (!isInside) {
      if (!inBreach) {
        inBreach = true;
        currentOpenedAt = r.ts;
        currentOpenDetails = {
          lat: r.lat,
          lng: r.lng,
          status: 'outside_operating_polygon'
        };
      }
    } else {
      if (inBreach) {
        inBreach = false;
        episodes.push({
          status: 'resolved',
          openedAt: currentOpenedAt,
          resolvedAt: r.ts,
          openDetails: currentOpenDetails,
          details: {
            lat: r.lat,
            lng: r.lng,
            status: 'inside_operating_polygon'
          }
        });
        currentOpenedAt = null;
        currentOpenDetails = null;
      }
    }
  }

  const latest = sorted[sorted.length - 1];

  if (inBreach) {
    episodes.push({
      status: 'open',
      openedAt: currentOpenedAt || latest.ts,
      resolvedAt: null,
      details: currentOpenDetails || {
        lat: latest.lat,
        lng: latest.lng,
        status: 'outside_operating_polygon'
      }
    });

    return {
      triggered: true,
      resolved: false,
      openedAt: currentOpenedAt || latest.ts,
      details: currentOpenDetails || {
        lat: latest.lat,
        lng: latest.lng,
        status: 'outside_operating_polygon'
      },
      episodes
    };
  }

  // Currently inside polygon (not in breach)
  const lastResolvedEpisode = episodes.length > 0 ? episodes[episodes.length - 1] : null;

  return {
    triggered: false,
    resolved: true,
    openedAt: lastResolvedEpisode ? lastResolvedEpisode.openedAt : null,
    resolvedAt: lastResolvedEpisode ? lastResolvedEpisode.resolvedAt : latest.ts,
    details: {
      lat: latest.lat,
      lng: latest.lng,
      status: 'inside_operating_polygon'
    },
    episodes
  };
}

const TELEMETRY_RULES = [
  { name: 'overheat', evaluate: evaluateOverheat },
  { name: 'rapid_discharge', evaluate: evaluateRapidDischarge },
  { name: 'geofence_breach', evaluate: evaluateGeofenceBreach }
];

// Index-covered time-bounded query for vehicle telemetry history.
// Ingestion validation strictly rejects telemetry older than 10 minutes.
// Querying with ts >= MIN(now - 10m, activeAlert.opened_at) seeks directly
// via idx_history_vehicle_ts in O(log N) time (< 0.5ms) without scanning
// historical rows from hours, days, or weeks ago.
const getWindowHistoryStmt = db.prepare(`
  SELECT vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging
  FROM telemetry_history
  WHERE vehicle_id = ? AND ts >= ?
  ORDER BY ts ASC, seq ASC
`);

const getAlertByIdStmt = db.prepare('SELECT * FROM alerts WHERE id = ?');
const getActiveAlertsForVehicleStmt = db.prepare("SELECT opened_at FROM alerts WHERE vehicle_id = ? AND status != 'resolved'");

const getActiveAlertStmt = db.prepare(`
  SELECT id, status, version, details, opened_at
  FROM alerts
  WHERE vehicle_id = ? AND rule = ? AND status != 'resolved'
`);

const getLastAlertStmt = db.prepare(`
  SELECT id, status, version, opened_at, resolved_at
  FROM alerts
  WHERE vehicle_id = ? AND rule = ?
  ORDER BY id DESC
  LIMIT 1
`);

const getAlertByOpenedAtStmt = db.prepare(`
  SELECT id, status, version, opened_at, resolved_at
  FROM alerts
  WHERE vehicle_id = ? AND rule = ? AND opened_at = ?
`);

const getOverlappingAlertStmt = db.prepare(`
  SELECT id, status, version, opened_at, resolved_at
  FROM alerts
  WHERE vehicle_id = ? AND rule = ?
    AND (opened_at <= ? OR ? IS NULL)
    AND (resolved_at >= ? OR status != 'resolved')
  ORDER BY id ASC
  LIMIT 1
`);

const getActiveOfflineAlertStmt = db.prepare(`
  SELECT id, status, version, details
  FROM alerts
  WHERE vehicle_id = ? AND rule = 'offline' AND status != 'resolved'
`);

const insertAlertStmt = db.prepare(`
  INSERT INTO alerts (
    vehicle_id, rule, status, version, details, opened_at, updated_at
  ) VALUES (
    @vehicle_id, @rule, 'open', 1, @details, @now, @now
  )
`);

const insertResolvedAlertStmt = db.prepare(`
  INSERT INTO alerts (
    vehicle_id, rule, status, version, details, opened_at, resolved_at, updated_at
  ) VALUES (
    @vehicle_id, @rule, 'resolved', 2, @details, @opened_at, @resolved_at, @now
  )
`);

const resolveAlertStmt = db.prepare(`
  UPDATE alerts
  SET status = 'resolved',
      resolved_at = @resolved_at,
      version = version + 1,
      details = @details,
      updated_at = @now
  WHERE id = @id
`);

/**
 * Internal vehicle alert evaluator executing inside an active database transaction.
 * Evaluates telemetry rules (overheat, rapid_discharge, geofence_breach, offline)
 * using index-bounded history seeking, expanding to full incident window if an active
 * alert is open. Returns list of changed alert objects for post-commit publishing.
 */
function evaluateVehicleAlertsInternal(vehicleId, nowIso) {
  const nowMs = Date.parse(nowIso);
  const tenMinAgoIso = new Date(nowMs - (10 * 60 * 1000)).toISOString();
  let cutoffIso = tenMinAgoIso;

  // If there are any active unresolved alerts, extend cutoff back to opened_at
  const activeAlerts = getActiveAlertsForVehicleStmt.all(vehicleId);
  for (const a of activeAlerts) {
    if (a.opened_at && a.opened_at < cutoffIso) {
      cutoffIso = a.opened_at;
    }
  }

  let readings = getWindowHistoryStmt.all(vehicleId, cutoffIso);
  if (!readings || readings.length === 0) return [];

  const changedAlerts = [];

  // 1. If vehicle was offline and has an active offline alert, resolve it on new telemetry arrival
  const activeOffline = getActiveOfflineAlertStmt.get(vehicleId);
  if (activeOffline) {
    resolveAlertStmt.run({
      id: activeOffline.id,
      details: JSON.stringify({
        reason: 'telemetry_resumed',
        resumed_at: nowIso
      }),
      resolved_at: nowIso,
      now: nowIso
    });
    const resolved = getAlertByIdStmt.get(activeOffline.id);
    if (resolved) {
      changedAlerts.push(resolved);
    }
  }

  // 2. Evaluate telemetry rules (overheat, rapid_discharge, geofence_breach)
  for (const { name: ruleName, evaluate } of TELEMETRY_RULES) {
    const activeAlert = getActiveAlertStmt.get(vehicleId, ruleName);
    const outcome = evaluate(readings, activeAlert);

    if (outcome.episodes && outcome.episodes.length > 0) {
      for (const ep of outcome.episodes) {
        const matchingAlert = getOverlappingAlertStmt.get(
          vehicleId,
          ruleName,
          ep.resolvedAt || null,
          ep.resolvedAt || null,
          ep.openedAt
        );

        if (matchingAlert) {
          if (matchingAlert.status !== 'resolved' && ep.status === 'resolved') {
            resolveAlertStmt.run({
              id: matchingAlert.id,
              details: JSON.stringify(ep.details || {}),
              resolved_at: ep.resolvedAt || nowIso,
              now: nowIso
            });
            const resolvedAlert = getAlertByIdStmt.get(matchingAlert.id);
            if (resolvedAlert) {
              changedAlerts.push(resolvedAlert);
            }
          }
          continue;
        }

        if (ep.status === 'open') {
          const currentActive = getActiveAlertStmt.get(vehicleId, ruleName);
          if (!currentActive) {
            const insertInfo = insertAlertStmt.run({
              vehicle_id: vehicleId,
              rule: ruleName,
              details: JSON.stringify(ep.details || {}),
              now: ep.openedAt || nowIso
            });
            const newAlert = getAlertByIdStmt.get(insertInfo.lastInsertRowid);
            if (newAlert) {
              changedAlerts.push(newAlert);
            }
          }
        } else if (ep.status === 'resolved') {
          const insertInfo = insertResolvedAlertStmt.run({
            vehicle_id: vehicleId,
            rule: ruleName,
            details: JSON.stringify(ep.details || {}),
            opened_at: ep.openedAt,
            resolved_at: ep.resolvedAt || nowIso,
            now: nowIso
          });
          const resolvedAlert = getAlertByIdStmt.get(insertInfo.lastInsertRowid);
          if (resolvedAlert) {
            changedAlerts.push(resolvedAlert);
          }
        }
      }
    } else {
      if (outcome.triggered) {
        if (!activeAlert) {
          // Prevent duplicate alerts for historical incidents that were already resolved
          const lastAlert = getLastAlertStmt.get(vehicleId, ruleName);
          const incidentOpenedAt = outcome.openedAt || nowIso;
          const alreadyResolved = lastAlert && lastAlert.status === 'resolved' &&
            lastAlert.resolved_at && incidentOpenedAt <= lastAlert.resolved_at;
          const alreadyExists = getAlertByOpenedAtStmt.get(vehicleId, ruleName, incidentOpenedAt);

          if (!alreadyResolved && !alreadyExists) {
            // Genuinely new incident -> Open new alert (version = 1, status = 'open')
            const insertInfo = insertAlertStmt.run({
              vehicle_id: vehicleId,
              rule: ruleName,
              details: JSON.stringify(outcome.details || {}),
              now: incidentOpenedAt
            });
            const newAlert = getAlertByIdStmt.get(insertInfo.lastInsertRowid);
            if (newAlert) {
              changedAlerts.push(newAlert);
            }
          }
        }
        // If already open or acknowledged, maintain existing alert without duplication
      } else if (outcome.resolved) {
        if (activeAlert) {
          // Active alert exists -> Transition to 'resolved' (version incremented)
          resolveAlertStmt.run({
            id: activeAlert.id,
            details: JSON.stringify(outcome.details || {}),
            resolved_at: outcome.resolvedAt || nowIso,
            now: nowIso
          });
          const resolvedAlert = getAlertByIdStmt.get(activeAlert.id);
          if (resolvedAlert) {
            changedAlerts.push(resolvedAlert);
          }
        } else if (outcome.openedAt) {
          // Out-of-order state reconstruction: only insert if this incident was NEVER previously recorded
          const lastAlert = getLastAlertStmt.get(vehicleId, ruleName);
          const alreadyResolved = lastAlert && lastAlert.status === 'resolved' &&
            lastAlert.resolved_at && outcome.openedAt <= lastAlert.resolved_at;
          const alreadyExists = getAlertByOpenedAtStmt.get(vehicleId, ruleName, outcome.openedAt);

          if (!alreadyResolved && !alreadyExists) {
            const insertInfo = insertResolvedAlertStmt.run({
              vehicle_id: vehicleId,
              rule: ruleName,
              details: JSON.stringify(outcome.details || {}),
              opened_at: outcome.openedAt,
              resolved_at: outcome.resolvedAt || nowIso,
              now: nowIso
            });
            const resolvedAlert = getAlertByIdStmt.get(insertInfo.lastInsertRowid);
            if (resolvedAlert) {
              changedAlerts.push(resolvedAlert);
            }
          }
        }
      }
    }
  }

  return changedAlerts;
}

/**
 * Batched atomic write transaction for all vehicles affected in an ingestion batch.
 * Consolidates all vehicle evaluations into a single BEGIN IMMEDIATE write lock,
 * eliminating the overhead of 100+ separate transactions per batch.
 */
const evaluateBatchAlertsTx = db.transaction((vehicleIds, nowIso) => {
  const allChanged = [];
  for (const vid of vehicleIds) {
    const changed = evaluateVehicleAlertsInternal(vid, nowIso);
    if (changed && changed.length > 0) {
      allChanged.push(...changed);
    }
  }
  return allChanged;
}).immediate;

/**
 * Evaluates all telemetry rules for a specific vehicle.
 * Guarantees that SSE alert stream events are published strictly AFTER
 * the SQLite write transaction has successfully committed.
 * @param {string} vehicleId 
 * @param {string} nowIso 
 * @returns {Array<object>} Changed alerts
 */
function evaluateVehicleAlerts(vehicleId, nowIso = new Date().toISOString()) {
  return evaluateAlertsForVehicles([vehicleId], nowIso);
}

/**
 * Evaluates alerts for a collection of affected vehicle IDs in a single atomic transaction.
 * Called automatically at the end of batch ingestion.
 * @param {Set<string>|Array<string>} vehicleIds 
 * @param {string} nowIso 
 * @returns {Array<object>}
 */
function evaluateAlertsForVehicles(vehicleIds, nowIso = new Date().toISOString()) {
  const distinctVehicleIds = Array.from(vehicleIds instanceof Set ? vehicleIds : new Set(vehicleIds));
  if (distinctVehicleIds.length === 0) return [];

  const allChanged = evaluateBatchAlertsTx(distinctVehicleIds, nowIso);
  const publishedAlertIds = new Set();
  for (const alert of allChanged) {
    if (publishedAlertIds.has(alert.id)) continue;
    publishedAlertIds.add(alert.id);
    if (alert.status === 'open') {
      trackAlertCreated();
    } else if (alert.status === 'resolved') {
      trackAlertResolved();
    }
    publishAlertChange(alert);
  }
  return allChanged;
}

module.exports = {
  isPointInPolygon,
  evaluateOverheat,
  evaluateRapidDischarge,
  evaluateGeofenceBreach,
  evaluateVehicleAlerts,
  evaluateAlertsForVehicles,
  TELEMETRY_RULES,
  CHENNAI_OPERATING_POLYGON
};
