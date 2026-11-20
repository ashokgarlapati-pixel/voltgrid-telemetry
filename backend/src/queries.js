const db = require('./db');
const { publishAlertChange } = require('./stream');

/**
 * Downsamples time-series telemetry rows while preserving peaks and troughs.
 * Divides data into equal buckets and retains the min/max values for key physical metrics.
 * 
 * @param {Array<object>} rows - Chronologically sorted telemetry rows
 * @param {number} targetPoints - Maximum number of data points to return
 * @returns {Array<object>} - Downsampled points ordered chronologically
 */
function downsampleMinMax(rows, targetPoints) {
  if (!rows || rows.length <= targetPoints) {
    return rows || [];
  }

  const firstPoint = rows[0];
  const lastPoint = rows[rows.length - 1];

  const innerRows = rows.slice(1, -1);
  const budget = Math.max(2, targetPoints - 2);
  const numBuckets = Math.max(1, Math.floor(budget / 4));
  const bucketSize = innerRows.length / numBuckets;

  const sampledSet = new Set();
  sampledSet.add(firstPoint);

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

    sampledSet.add(minTempRow);
    sampledSet.add(maxTempRow);
    sampledSet.add(minSocRow);
    sampledSet.add(maxSpeedRow);
  }

  sampledSet.add(lastPoint);

  return Array.from(sampledSet).sort((a, b) => {
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
 * GET /v1/vehicles/:id/history?from=&to=&points=
 * Returns peak-preserving downsampled time-series for a vehicle over a given window.
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
    if (points !== undefined) {
      const parsedPoints = parseInt(points, 10);
      if (isNaN(parsedPoints) || parsedPoints < 2 || parsedPoints > 5000) {
        return res.status(400).json({ error: 'Invalid "points" parameter: must be an integer between 2 and 5000' });
      }
      safePoints = parsedPoints;
    }

    // Fast indexed query on (vehicle_id, ts)
    const sql = `
      SELECT seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging
      FROM telemetry_history
      WHERE vehicle_id = ? AND ts >= ? AND ts <= ?
      ORDER BY ts ASC, seq ASC
    `;

    const rawRows = db.prepare(sql).all(cleanId, fromIso, toIso);
    const downsampled = downsampleMinMax(rawRows, safePoints);

    res.json({
      vehicle_id: cleanId,
      from: fromIso,
      to: toIso,
      raw_count: rawRows.length,
      returned_count: downsampled.length,
      points: downsampled
    });
  } catch (err) {
    console.error('[GetVehicleHistory Error]', err);
    res.status(500).json({ error: 'Failed to query vehicle history' });
  }
}

/**
 * GET /v1/fleet/summary
 * Ultra-fast fleet summary aggregated strictly from vehicles and alerts tables.
 * Guaranteed sub-150ms p95 latency by never scanning telemetry_history.
 */
function getFleetSummaryHandler(req, res) {
  try {
    // 1. Vehicle fleet status and completeness aggregate
    const fleetRow = db.prepare(`
      SELECT 
        COUNT(*) AS total_vehicles,
        COALESCE(SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END), 0) AS online_count,
        COALESCE(SUM(CASE WHEN status = 'offline' THEN 1 ELSE 0 END), 0) AS offline_count,
        COALESCE(SUM(CASE WHEN status = 'maintenance' THEN 1 ELSE 0 END), 0) AS maintenance_count,
        COALESCE(AVG(soc_pct), 0) AS avg_soc,
        COALESCE(SUM(received_count), 0) AS total_received,
        COALESCE(SUM(max_seq - min_seq + 1), 0) AS total_expected
      FROM vehicles
    `).get();

    // 2. Open alerts breakdown by rule type (status != 'resolved' includes open and acknowledged)
    const alertRows = db.prepare(`
      SELECT rule, COUNT(*) AS count
      FROM alerts
      WHERE status != 'resolved'
      GROUP BY rule
    `).all();

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
  getVehiclesHandler,
  getVehicleHistoryHandler,
  getFleetSummaryHandler,
  ackAlertHandler
};
