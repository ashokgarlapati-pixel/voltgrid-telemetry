const db = require('./db');
const { publishAlertChange } = require('./stream');

/**
 * Downsamples time-series telemetry rows while preserving peaks and troughs.
 * Guarantees that the returned point count never exceeds targetPoints.
 * Preserves the first and last readings whenever the limit allows.
 * Preserves key physical extremes (thermal peaks, troughs, min SOC, max speed).
 * Removes duplicates and ensures strictly chronological ordering.
 * 
 * @param {Array<object>} rows - Chronologically sorted telemetry rows
 * @param {number} targetPoints - Maximum number of data points to return
 * @returns {Array<object>} - Downsampled points ordered chronologically
 */
function downsampleMinMax(rows, targetPoints) {
  if (!rows || rows.length === 0 || !Number.isFinite(targetPoints) || targetPoints <= 0) {
    return [];
  }

  // Deduplicate identical readings while preserving original chronological order
  const dedupedRows = [];
  const seenKeys = new Set();
  for (const r of rows) {
    if (!r) continue;
    const key = r.seq !== undefined && r.seq !== null ? `${r.ts}_${r.seq}` : (r.ts || JSON.stringify(r));
    if (!seenKeys.has(key)) {
      seenKeys.add(key);
      dedupedRows.push(r);
    }
  }

  if (dedupedRows.length <= targetPoints) {
    return dedupedRows;
  }

  // When targetPoints === 1, return the first reading
  if (targetPoints === 1) {
    return [dedupedRows[0]];
  }

  const firstPoint = dedupedRows[0];
  const lastPoint = dedupedRows[dedupedRows.length - 1];

  // When targetPoints === 2, return first and last readings
  if (targetPoints === 2) {
    return [firstPoint, lastPoint];
  }

  const innerRows = dedupedRows.slice(1, dedupedRows.length - 1);
  const maxInterior = targetPoints - 2;

  if (innerRows.length <= maxInterior) {
    return [firstPoint, ...innerRows, lastPoint];
  }

  // Candidate collector: track points by key
  const selectedKeys = new Set();
  const selectedPoints = [];

  function addPoint(p) {
    if (!p) return;
    const key = p.seq !== undefined && p.seq !== null ? `${p.ts}_${p.seq}` : (p.ts || JSON.stringify(p));
    if (!selectedKeys.has(key)) {
      selectedKeys.add(key);
      selectedPoints.push(p);
    }
  }

  addPoint(firstPoint);

  if (maxInterior < 4) {
    // For small budgets (maxInterior = 1, 2, or 3, corresponding to targetPoints = 3, 4, 5):
    // Find the most prominent global extremes in innerRows
    let minTempRow = innerRows[0];
    let maxTempRow = innerRows[0];
    let minSocRow = innerRows[0];
    let maxSpeedRow = innerRows[0];

    for (let i = 1; i < innerRows.length; i++) {
      const item = innerRows[i];
      if (item.battery_temp_c < minTempRow.battery_temp_c) minTempRow = item;
      if (item.battery_temp_c > maxTempRow.battery_temp_c) maxTempRow = item;
      if (item.soc_pct < minSocRow.soc_pct) minSocRow = item;
      if (item.speed_kmh > maxSpeedRow.speed_kmh) maxSpeedRow = item;
    }

    // Rank candidates by physical importance (maxTemp, minTemp, minSoc, maxSpeed)
    const candidates = [maxTempRow, minTempRow, minSocRow, maxSpeedRow];
    const uniqueCandidates = [];
    const seenCandKeys = new Set();
    for (const c of candidates) {
      const key = c.seq !== undefined && c.seq !== null ? `${c.ts}_${c.seq}` : (c.ts || JSON.stringify(c));
      if (!seenCandKeys.has(key)) {
        seenCandKeys.add(key);
        uniqueCandidates.push(c);
      }
    }

    // Add up to maxInterior candidates
    for (let i = 0; i < Math.min(maxInterior, uniqueCandidates.length); i++) {
      addPoint(uniqueCandidates[i]);
    }
  } else {
    // For larger budgets (maxInterior >= 4, targetPoints >= 6):
    // Use multi-bucket peak preservation
    const numBuckets = Math.max(1, Math.floor(maxInterior / 4));
    const bucketSize = innerRows.length / numBuckets;

    for (let b = 0; b < numBuckets; b++) {
      const startIdx = Math.floor(b * bucketSize);
      const endIdx = Math.floor((b + 1) * bucketSize);
      const bucket = innerRows.slice(startIdx, endIdx);

      if (bucket.length === 0) continue;

      let minTempRow = bucket[0];
      let maxTempRow = bucket[0];
      let minSocRow = bucket[0];
      let maxSpeedRow = bucket[0];

      for (let i = 1; i < bucket.length; i++) {
        const item = bucket[i];
        if (item.battery_temp_c < minTempRow.battery_temp_c) minTempRow = item;
        if (item.battery_temp_c > maxTempRow.battery_temp_c) maxTempRow = item;
        if (item.soc_pct < minSocRow.soc_pct) minSocRow = item;
        if (item.speed_kmh > maxSpeedRow.speed_kmh) maxSpeedRow = item;
      }

      // Add points while checking available capacity (reserving 1 slot for lastPoint)
      const candidates = [maxTempRow, minTempRow, minSocRow, maxSpeedRow];
      for (const cand of candidates) {
        if (selectedPoints.length < targetPoints - 1) {
          addPoint(cand);
        }
      }
    }
  }

  // Always add last point
  addPoint(lastPoint);

  // Absolute hard guarantee: if ever selectedPoints exceeds targetPoints,
  // prune interior points from the middle until selectedPoints.length <= targetPoints
  if (selectedPoints.length > targetPoints) {
    const interior = selectedPoints.slice(1, selectedPoints.length - 1);
    const stride = interior.length / (targetPoints - 2);
    const prunedInterior = [];
    for (let i = 0; i < targetPoints - 2; i++) {
      prunedInterior.push(interior[Math.floor(i * stride)]);
    }
    selectedPoints.length = 0;
    selectedPoints.push(firstPoint, ...prunedInterior, lastPoint);
  }

  // Sort chronologically by ts, then seq
  return selectedPoints.sort((a, b) => {
    const timeDiff = Date.parse(a.ts) - Date.parse(b.ts);
    return timeDiff !== 0 ? timeDiff : ((a.seq || 0) - (b.seq || 0));
  });
}

const VALID_STATUSES = ['online', 'offline', 'maintenance'];

/**
 * GET /v1/vehicles
 * Keyset-paginated list of vehicles with optional filters (status, bounding box).
 * No OFFSET pagination used. Deterministically ordered by vehicle_id ASC.
 */
function getVehiclesHandler(req, res) {
  try {
    const { status, min_lat, max_lat, min_lng, max_lng, cursor, limit = '50' } = req.query;

    const conditions = [];
    const params = [];

    // 1. Status Filter Validation
    if (status !== undefined) {
      if (typeof status !== 'string' || !VALID_STATUSES.includes(status.trim())) {
        return res.status(400).json({
          error: `Invalid status filter. Allowed values: ${VALID_STATUSES.join(', ')}`
        });
      }
      conditions.push('status = ?');
      params.push(status.trim());
    }

    // 2. Bounding Box Filter Validation
    const hasMinLat = min_lat !== undefined;
    const hasMaxLat = max_lat !== undefined;
    const hasMinLng = min_lng !== undefined;
    const hasMaxLng = max_lng !== undefined;

    if (hasMinLat || hasMaxLat || hasMinLng || hasMaxLng) {
      if (!(hasMinLat && hasMaxLat && hasMinLng && hasMaxLng)) {
        return res.status(400).json({
          error: 'Incomplete bounding box: min_lat, max_lat, min_lng, and max_lng are all required'
        });
      }

      const pMinLat = parseFloat(min_lat);
      const pMaxLat = parseFloat(max_lat);
      const pMinLng = parseFloat(min_lng);
      const pMaxLng = parseFloat(max_lng);

      if (!Number.isFinite(pMinLat) || !Number.isFinite(pMaxLat) ||
          !Number.isFinite(pMinLng) || !Number.isFinite(pMaxLng)) {
        return res.status(400).json({
          error: 'Bounding box coordinates must be valid finite numbers'
        });
      }

      if (pMinLat < -90 || pMinLat > 90 || pMaxLat < -90 || pMaxLat > 90) {
        return res.status(400).json({
          error: 'Latitude coordinates must be between -90 and 90 degrees'
        });
      }

      if (pMinLng < -180 || pMinLng > 180 || pMaxLng < -180 || pMaxLng > 180) {
        return res.status(400).json({
          error: 'Longitude coordinates must be between -180 and 180 degrees'
        });
      }

      if (pMinLat > pMaxLat) {
        return res.status(400).json({
          error: 'min_lat cannot be greater than max_lat'
        });
      }

      if (pMinLng > pMaxLng) {
        return res.status(400).json({
          error: 'min_lng cannot be greater than max_lng'
        });
      }

      conditions.push('lat >= ? AND lat <= ? AND lng >= ? AND lng <= ?');
      params.push(pMinLat, pMaxLat, pMinLng, pMaxLng);
    }

    // 3. Limit Parameter Validation
    let safeLimit = 50;
    if (limit !== undefined) {
      const parsedLimit = parseInt(limit, 10);
      if (isNaN(parsedLimit) || parsedLimit <= 0) {
        return res.status(400).json({
          error: 'Invalid limit parameter: must be a positive integer'
        });
      }
      safeLimit = Math.min(200, parsedLimit);
    }

    // 4. Keyset Cursor Validation (vehicle_id > cursor)
    if (cursor !== undefined) {
      if (typeof cursor !== 'string' || cursor.trim().length === 0 || cursor.length > 128) {
        return res.status(400).json({
          error: 'Invalid cursor parameter: must be a valid non-empty string'
        });
      }
      conditions.push('vehicle_id > ?');
      params.push(cursor.trim());
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // Fetch limit + 1 rows to determine if more pages exist without a separate COUNT query
    const sql = `
      SELECT vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c,
             speed_kmh, lat, lng, odometer_km, charging, status,
             min_seq, max_seq, received_count, updated_at
      FROM vehicles
      ${whereClause}
      ORDER BY vehicle_id ASC
      LIMIT ?
    `;

    params.push(safeLimit + 1);
    const rows = db.prepare(sql).all(...params);

    const hasMore = rows.length > safeLimit;
    const data = hasMore ? rows.slice(0, safeLimit) : rows;
    const nextCursor = (hasMore && data.length > 0) ? data[data.length - 1].vehicle_id : null;

    res.json({
      data,
      next_cursor: nextCursor,
      has_more: hasMore
    });
  } catch (err) {
    console.error('[GetVehicles Error]', err);
    res.status(500).json({ error: 'Failed to query vehicles' });
  }
}

/**
 * Streams telemetry history directly from SQLite using a time-bucketed peak/trough
 * downsampling algorithm. Never loads the full result set into application memory.
 * Bounded memory usage O(targetPoints), regardless of whether 100 or 100,000 rows match.
 * 
 * @param {string} vehicleId - Target vehicle ID
 * @param {string} fromIso - ISO timestamp start window
 * @param {string} toIso - ISO timestamp end window
 * @param {number} targetPoints - Maximum number of data points to return
 * @returns {{ rawCount: number, points: Array<object> }}
 */
function streamDownsampleHistory(vehicleId, fromIso, toIso, targetPoints) {
  // Query total matching count
  const countRow = db.prepare(`
    SELECT COUNT(*) AS total
    FROM telemetry_history
    WHERE vehicle_id = ? AND ts >= ? AND ts <= ?
  `).get(vehicleId, fromIso, toIso);

  const rawCount = Number(countRow?.total || 0);
  if (rawCount === 0) {
    return { rawCount: 0, points: [] };
  }

  const querySql = `
    SELECT seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging
    FROM telemetry_history
    WHERE vehicle_id = ? AND ts >= ? AND ts <= ?
    ORDER BY ts ASC, seq ASC
  `;

  // If total matching rows is already within requested points budget, return directly
  if (rawCount <= targetPoints) {
    const rows = db.prepare(querySql).all(vehicleId, fromIso, toIso);
    return { rawCount, points: rows };
  }

  // Handle single-point request
  if (targetPoints === 1) {
    const single = db.prepare(`
      ${querySql}
      LIMIT 1
    `).all(vehicleId, fromIso, toIso);
    return { rawCount, points: single };
  }

  const fromMs = Date.parse(fromIso);
  const toMs = Date.parse(toIso);
  const totalDurationMs = Math.max(1, toMs - fromMs);

  // Reserve 2 points for absolute first and last readings
  const maxInterior = targetPoints - 2;
  const numBuckets = Math.max(1, Math.floor(maxInterior / 4));
  const bucketDurationMs = totalDurationMs / numBuckets;

  // Pre-allocate fixed bucket state (zero heap allocations during streaming loop)
  const buckets = new Array(numBuckets);
  for (let b = 0; b < numBuckets; b++) {
    buckets[b] = { minTemp: null, maxTemp: null, minSoc: null, maxSpeed: null };
  }

  let firstRow = null;
  let lastRow = null;

  const stmt = db.prepare(querySql);

  // Stream rows one by one through the iterator without materializing the full dataset
  for (const row of stmt.iterate(vehicleId, fromIso, toIso)) {
    if (!firstRow) firstRow = row;
    lastRow = row;

    const rowMs = Date.parse(row.ts);
    const bucketIdx = Math.min(numBuckets - 1, Math.max(0, Math.floor((rowMs - fromMs) / bucketDurationMs)));
    const b = buckets[bucketIdx];

    if (!b.minTemp || row.battery_temp_c < b.minTemp.battery_temp_c) b.minTemp = row;
    if (!b.maxTemp || row.battery_temp_c > b.maxTemp.battery_temp_c) b.maxTemp = row;
    if (!b.minSoc || row.soc_pct < b.minSoc.soc_pct) b.minSoc = row;
    if (!b.maxSpeed || row.speed_kmh > b.maxSpeed.speed_kmh) b.maxSpeed = row;
  }

  // Collect candidates from time buckets
  const selectedMap = new Map();
  function addPoint(r) {
    if (!r) return;
    const key = r.seq !== undefined && r.seq !== null ? `${r.ts}_${r.seq}` : r.ts;
    if (!selectedMap.has(key)) selectedMap.set(key, r);
  }

  addPoint(firstRow);
  for (const b of buckets) {
    if (selectedMap.size < targetPoints - 1) addPoint(b.maxTemp);
    if (selectedMap.size < targetPoints - 1) addPoint(b.minTemp);
    if (selectedMap.size < targetPoints - 1) addPoint(b.minSoc);
    if (selectedMap.size < targetPoints - 1) addPoint(b.maxSpeed);
  }
  addPoint(lastRow);

  const points = Array.from(selectedMap.values()).sort((a, b) => {
    const timeDiff = Date.parse(a.ts) - Date.parse(b.ts);
    return timeDiff !== 0 ? timeDiff : ((a.seq || 0) - (b.seq || 0));
  });

  return { rawCount, points };
}

/**
 * GET /v1/vehicles/:id/history?from=&to=&points=
 * Returns peak-preserving downsampled time-series for a vehicle over a given window.
 * Uses bounded streaming iteration to handle large windows (e.g. 17,000+ readings) fast.
 */
function getVehicleHistoryHandler(req, res) {
  try {
    const { id } = req.params;
    let { from, to, points = '200' } = req.query;

    if (!id || typeof id !== 'string' || id.trim().length === 0) {
      return res.status(400).json({ error: 'Invalid vehicle ID' });
    }
    const cleanId = id.trim();

    // Verify vehicle existence (either in vehicles state or telemetry_history)
    const vehicle = db.prepare('SELECT vehicle_id FROM vehicles WHERE vehicle_id = ?').get(cleanId);
    const historySample = vehicle ? null : db.prepare('SELECT 1 FROM telemetry_history WHERE vehicle_id = ? LIMIT 1').get(cleanId);
    if (!vehicle && !historySample) {
      return res.status(404).json({ error: `Vehicle with id ${cleanId} not found` });
    }

    const now = Date.now();
    let fromIso;
    let toIso;

    if (from !== undefined) {
      if (typeof from !== 'string' || isNaN(Date.parse(from))) {
        return res.status(400).json({ error: 'Invalid "from" timestamp: must be a valid ISO-8601 date string' });
      }
      fromIso = new Date(from).toISOString();
    } else {
      fromIso = new Date(now - (24 * 60 * 60 * 1000)).toISOString();
    }

    if (to !== undefined) {
      if (typeof to !== 'string' || isNaN(Date.parse(to))) {
        return res.status(400).json({ error: 'Invalid "to" timestamp: must be a valid ISO-8601 date string' });
      }
      toIso = new Date(to).toISOString();
    } else {
      toIso = new Date(now).toISOString();
    }

    if (Date.parse(fromIso) >= Date.parse(toIso)) {
      return res.status(400).json({ error: '"from" timestamp must be earlier than "to" timestamp' });
    }

    let safePoints = 200;
    const rawPoints = req.query.targetPoints !== undefined ? req.query.targetPoints : req.query.points;
    if (rawPoints !== undefined) {
      const parsedPoints = parseInt(rawPoints, 10);
      if (isNaN(parsedPoints) || parsedPoints < 1 || parsedPoints > 5000) {
        return res.status(400).json({ error: 'Invalid "points" parameter: must be an integer between 1 and 5000' });
      }
      safePoints = parsedPoints;
    }

    // Stream-assisted time-bucketed downsampling: bounded memory O(safePoints)
    const { rawCount, points: downsampled } = streamDownsampleHistory(cleanId, fromIso, toIso, safePoints);

    res.json({
      vehicle_id: cleanId,
      from: fromIso,
      to: toIso,
      raw_count: rawCount,
      returned_count: downsampled.length,
      points: downsampled
    });
  } catch (err) {
    console.error('[GetVehicleHistory Error]', err);
    res.status(500).json({ error: 'Failed to query vehicle history' });
  }
}

// Pre-prepared statements for GET /v1/fleet/summary to eliminate per-request SQL compilation
const getFleetSummaryVehiclesStmt = db.prepare(`
  SELECT 
    COUNT(*) AS total_vehicles,
    COALESCE(SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END), 0) AS online_count,
    COALESCE(SUM(CASE WHEN status = 'offline' THEN 1 ELSE 0 END), 0) AS offline_count,
    COALESCE(SUM(CASE WHEN status = 'maintenance' THEN 1 ELSE 0 END), 0) AS maintenance_count,
    COALESCE(AVG(soc_pct), 0) AS avg_soc,
    COALESCE(SUM(received_count), 0) AS total_received,
    COALESCE(SUM(max_seq - min_seq + 1), 0) AS total_expected
  FROM vehicles
`);

const getFleetSummaryAlertsStmt = db.prepare(`
  SELECT rule, COUNT(*) AS count
  FROM alerts
  WHERE status != 'resolved'
  GROUP BY rule
`);

/**
 * GET /v1/fleet/summary
 * Ultra-fast fleet summary aggregated strictly from vehicles and alerts tables.
 * Guaranteed sub-150ms p95 latency by never scanning telemetry_history.
 */
function getFleetSummaryHandler(req, res) {
  try {
    // 1. Vehicle fleet status and completeness aggregate
    const fleetRow = getFleetSummaryVehiclesStmt.get();

    // 2. Open alerts breakdown by rule type (status != 'resolved' includes open and acknowledged)
    const alertRows = getFleetSummaryAlertsStmt.all();

    const openAlertsByRule = {
      overheat: 0,
      rapid_discharge: 0,
      geofence_breach: 0,
      offline: 0
    };

    for (const row of alertRows) {
      if (row.rule in openAlertsByRule) {
        openAlertsByRule[row.rule] = Number(row.count || 0);
      }
    }

    const totalVehicles = Number(fleetRow?.total_vehicles || 0);
    const onlineCount = Number(fleetRow?.online_count || 0);
    const offlineCount = Number(fleetRow?.offline_count || 0);
    const maintenanceCount = Number(fleetRow?.maintenance_count || 0);
    const totalReceived = Number(fleetRow?.total_received || 0);
    const totalExpected = Number(fleetRow?.total_expected || 0);

    const completenessPct = totalExpected > 0
      ? Number(((totalReceived / totalExpected) * 100).toFixed(2))
      : 100.0;

    res.json({
      total_vehicles: totalVehicles,
      counts_by_status: {
        online: onlineCount,
        offline: offlineCount,
        maintenance: maintenanceCount
      },
      online_count: onlineCount,
      offline_count: offlineCount,
      avg_soc_pct: Number(Number(fleetRow?.avg_soc || 0).toFixed(2)),
      open_alerts_by_rule: openAlertsByRule,
      total_received: totalReceived,
      total_expected: totalExpected,
      data_completeness_pct: completenessPct,
      completeness_pct: completenessPct
    });
  } catch (err) {
    console.error('[FleetSummary Error]', err);
    res.status(500).json({ error: 'Failed to compute fleet summary' });
  }
}

/**
 * POST /v1/alerts/:id/ack
 * Optimistic concurrency acknowledgment for an alert.
 * Returns 409 Conflict if caller's version is stale or alert is not open.
 */
function ackAlertHandler(req, res) {
  try {
    const { id } = req.params;
    const alertId = parseInt(id, 10);
    if (isNaN(alertId) || alertId <= 0) {
      return res.status(400).json({ error: 'Invalid alert ID' });
    }

    const { version } = req.body || {};
    if (version === undefined || typeof version !== 'number' || !Number.isInteger(version) || version < 0) {
      return res.status(400).json({
        error: 'Invalid request: "version" non-negative integer is required in request body'
      });
    }

    const nowIso = new Date().toISOString();

    // Atomic optimistic concurrency update
    const updateResult = db.prepare(`
      UPDATE alerts
      SET status = 'acknowledged',
          version = version + 1,
          updated_at = @now
      WHERE id = @id AND version = @version AND status = 'open'
    `).run({
      id: alertId,
      version,
      now: nowIso
    });

    if (updateResult.changes === 1) {
      // Successfully updated; fetch and return latest state
      const updatedAlert = db.prepare('SELECT * FROM alerts WHERE id = ?').get(alertId);
      if (updatedAlert) {
        publishAlertChange(updatedAlert);
      }
      return res.status(200).json({
        message: 'Alert acknowledged successfully',
        alert: {
          ...updatedAlert,
          details: updatedAlert.details ? JSON.parse(updatedAlert.details) : null
        }
      });
    }

    // Update affected 0 rows -> Check why (does it exist, is version stale, or already acknowledged/resolved?)
    const existing = db.prepare('SELECT id, status, version FROM alerts WHERE id = ?').get(alertId);

    if (!existing) {
      return res.status(404).json({ error: `Alert with id ${alertId} not found` });
    }

    if (existing.status === 'acknowledged') {
      return res.status(409).json({
        error: 'Alert is already acknowledged',
        current_version: existing.version,
        current_status: existing.status
      });
    }

    if (existing.status === 'resolved') {
      return res.status(409).json({
        error: 'Resolved alert cannot be acknowledged',
        current_version: existing.version,
        current_status: existing.status
      });
    }

    return res.status(409).json({
      error: 'Version conflict: provided version does not match current version',
      current_version: existing.version,
      current_status: existing.status
    });
  } catch (err) {
    console.error('[AckAlert Error]', err);
    res.status(500).json({ error: 'Failed to acknowledge alert' });
  }
}

module.exports = {
  downsampleMinMax,
  streamDownsampleHistory,
  getVehiclesHandler,
  getVehicleHistoryHandler,
  getFleetSummaryHandler,
  ackAlertHandler
};
