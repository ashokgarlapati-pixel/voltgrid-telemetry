const db = require('./db');
const { publishAlertChange } = require('./stream');

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
 * - Trigger: 3 consecutive readings with battery_temp_c > 55°C.
 * - Resolve: 3 consecutive readings with battery_temp_c <= 50°C.
 * - Between 50°C and 55°C, maintain previous state (hysteresis).
 */
function evaluateOverheat(readings) {
  const sorted = normalizeReadings(readings);
  if (sorted.length === 0) {
    return { triggered: false, resolved: false };
  }

  let state = 'normal'; // 'normal' | 'triggered' | 'resolved'
  let hotStreak = 0;
  let coolStreak = 0;

  for (const r of sorted) {
    if (r.battery_temp_c > 55.0) {
      hotStreak++;
      coolStreak = 0;
      if (hotStreak >= 3) {
        state = 'triggered';
      }
    } else if (r.battery_temp_c <= 50.0) {
      coolStreak++;
      hotStreak = 0;
      if (coolStreak >= 3) {
        state = 'resolved';
      }
    } else {
      // Hysteresis range (50°C < temp <= 55°C): resets streaks, maintains existing state
      hotStreak = 0;
      coolStreak = 0;
    }
  }

  const latest = sorted[sorted.length - 1];
  if (state === 'triggered') {
    return {
      triggered: true,
      resolved: false,
      details: {
        current_temp_c: latest.battery_temp_c,
        threshold: 55.0,
        consecutive_hot: hotStreak
      }
    };
  } else if (state === 'resolved') {
    return {
      triggered: false,
      resolved: true,
      details: {
        current_temp_c: latest.battery_temp_c,
        cooling_threshold: 50.0,
        consecutive_cool: coolStreak
      }
    };
  }

  return { triggered: false, resolved: false };
}

/**
 * Rule 2: Rapid Discharge
 * - Trigger: While charging === 0 (not charging), soc_pct drops by > 8 points
 *   compared to any non-charging reading within the preceding 5 minutes (300 seconds).
 * - Resolve: The 5-minute drop rate returns to <= 8 points (or the vehicle starts charging).
 */
function evaluateRapidDischarge(readings) {
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
      details: { charging: true, current_soc: latest.soc_pct }
    };
  }

  const latestTimeMs = Date.parse(latest.ts);
  const fiveMinWindowStart = latestTimeMs - (5 * 60 * 1000);

  // Filter non-charging readings within the 5-minute window
  const windowReadings = sorted.filter(r =>
    !r.charging &&
    Date.parse(r.ts) >= fiveMinWindowStart &&
    Date.parse(r.ts) <= latestTimeMs
  );

  if (windowReadings.length < 2) {
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
    details: {
      drop_points: Number(dropPoints.toFixed(1)),
      current_soc: latest.soc_pct,
      threshold: 8.0
    }
  };
}

/**
 * Rule 3: Geofence Breach
 * - Trigger: Latest reading by ts is outside the operating polygon.
 * - Resolve: Latest reading by ts is back inside the polygon.
 */
function evaluateGeofenceBreach(readings) {
  const sorted = normalizeReadings(readings);
  if (sorted.length === 0) {
    return { triggered: false, resolved: false };
  }

  const latest = sorted[sorted.length - 1];
  const isInside = isPointInPolygon(latest.lat, latest.lng);

  if (!isInside) {
    return {
      triggered: true,
      resolved: false,
      details: {
        lat: latest.lat,
        lng: latest.lng,
        status: 'outside_operating_polygon'
      }
    };
  }

  return {
    triggered: false,
    resolved: true,
    details: {
      lat: latest.lat,
      lng: latest.lng,
      status: 'inside_operating_polygon'
    }
  };
}

/**
 * Registry of active rules for telemetry readings.
 * Extensible design: adding a 5th rule (e.g. overspeed) during live code review
 * only requires adding an entry to this array.
 */
const TELEMETRY_RULES = [
  { name: 'overheat', evaluate: evaluateOverheat },
  { name: 'rapid_discharge', evaluate: evaluateRapidDischarge },
  { name: 'geofence_breach', evaluate: evaluateGeofenceBreach }
];

// Prepared statements for Alert lifecycle management
const getRecentHistoryStmt = db.prepare(`
  SELECT vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging
  FROM telemetry_history
  WHERE vehicle_id = ?
  ORDER BY ts ASC, seq ASC
  LIMIT 100
`);

const getActiveAlertStmt = db.prepare(`
  SELECT id, status, version, details
  FROM alerts
  WHERE vehicle_id = ? AND rule = ? AND status != 'resolved'
`);

const insertAlertStmt = db.prepare(`
  INSERT INTO alerts (
    vehicle_id, rule, status, version, details, opened_at, updated_at
  ) VALUES (
    @vehicle_id, @rule, 'open', 1, @details, @now, @now
  )
`);

const resolveAlertStmt = db.prepare(`
  UPDATE alerts
  SET status = 'resolved',
      resolved_at = @now,
      version = version + 1,
      details = @details,
      updated_at = @now
  WHERE id = @id
`);

/**
 * Evaluates all telemetry rules for a specific vehicle.
 * Ensures order-independence by replaying chronologically sorted history.
 * @param {string} vehicleId 
 * @param {string} nowIso 
 */
function evaluateVehicleAlerts(vehicleId, nowIso = new Date().toISOString()) {
  // Query chronologically sorted history (guarantees identical result regardless of arrival order)
  const readings = getRecentHistoryStmt.all(vehicleId);
  if (!readings || readings.length === 0) return;

  for (const { name: ruleName, evaluate } of TELEMETRY_RULES) {
    const outcome = evaluate(readings);
    const activeAlert = getActiveAlertStmt.get(vehicleId, ruleName);

    if (outcome.triggered) {
      if (!activeAlert) {
        // No active alert exists -> Open new alert (version = 1, status = 'open')
        const insertInfo = insertAlertStmt.run({
          vehicle_id: vehicleId,
          rule: ruleName,
          details: JSON.stringify(outcome.details || {}),
          now: nowIso
        });
        const newAlert = db.prepare('SELECT * FROM alerts WHERE id = ?').get(insertInfo.lastInsertRowid);
        if (newAlert) {
          publishAlertChange(newAlert);
        }
      }
      // If already open or acknowledged, do not create a duplicate
    } else if (outcome.resolved) {
      if (activeAlert) {
        // Active alert exists -> Transition to 'resolved'
        resolveAlertStmt.run({
          id: activeAlert.id,
          details: JSON.stringify(outcome.details || {}),
          now: nowIso
        });
        const resolvedAlert = db.prepare('SELECT * FROM alerts WHERE id = ?').get(activeAlert.id);
        if (resolvedAlert) {
          publishAlertChange(resolvedAlert);
        }
      }
    }
  }
}

/**
 * Evaluates alerts for a collection of affected vehicle IDs.
 * Called automatically at the end of batch ingestion.
 * @param {Set<string>|Array<string>} vehicleIds 
 * @param {string} nowIso 
 */
function evaluateAlertsForVehicles(vehicleIds, nowIso = new Date().toISOString()) {
  for (const vehicleId of vehicleIds) {
    evaluateVehicleAlerts(vehicleId, nowIso);
  }
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
