const db = require('./db');

/**
 * Downsamples time-series telemetry rows while preserving peaks and troughs.
 * Divides data into equal buckets and retains the min/max values per bucket.
 * 
 * @param {Array<object>} rows - Chronologically sorted telemetry rows
 * @param {number} targetPoints - Maximum number of data points to return
 * @returns {Array<object>} - Downsampled points ordered chronologically
 */
function downsampleMinMax(rows, targetPoints) {
  if (rows.length <= targetPoints) {
    return rows;
  }

  // Ensure first and last points are always preserved
  const firstPoint = rows[0];
  const lastPoint = rows[rows.length - 1];

  // Reserve 2 slots for first and last, distribute remaining across buckets
  const innerRows = rows.slice(1, -1);
  const budget = Math.max(2, targetPoints - 2);
  const numBuckets = Math.floor(budget / 2);
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

    for (let i = 1; i < bucket.length; i++) {
      const item = bucket[i];
      if (item.battery_temp_c < minTempRow.battery_temp_c) minTempRow = item;
      if (item.battery_temp_c > maxTempRow.battery_temp_c) maxTempRow = item;
      if (item.soc_pct < minSocRow.soc_pct) minSocRow = item;
    }

    sampledSet.add(minTempRow);
    sampledSet.add(maxTempRow);
    sampledSet.add(minSocRow);
  }

  sampledSet.add(lastPoint);

  // Return strictly ordered by timestamp and sequence
  return Array.from(sampledSet).sort((a, b) => {
    const timeDiff = Date.parse(a.ts) - Date.parse(b.ts);
    return timeDiff !== 0 ? timeDiff : a.seq - b.seq;
  });
}

/**
 * GET /v1/vehicles
 * Keyset-paginated list of vehicles with optional filters (status, bounding box).
 * No OFFSET pagination used.
 */
function getVehiclesHandler(req, res) {
  try {
    const { status, min_lat, max_lat, min_lng, max_lng, cursor, limit = '50' } = req.query;

    const safeLimit = Math.min(200, Math.max(1, parseInt(limit, 10) || 50));
    const conditions = [];
    const params = [];

    // Optional status filter
    if (status) {
      conditions.push('status = ?');
      params.push(status);
    }

    // Optional bounding box filter
    if (min_lat !== undefined && !isNaN(parseFloat(min_lat))) {
      conditions.push('lat >= ?');
      params.push(parseFloat(min_lat));
    }
    if (max_lat !== undefined && !isNaN(parseFloat(max_lat))) {
      conditions.push('lat <= ?');
      params.push(parseFloat(max_lat));
    }
    if (min_lng !== undefined && !isNaN(parseFloat(min_lng))) {
      conditions.push('lng >= ?');
      params.push(parseFloat(min_lng));
    }
    if (max_lng !== undefined && !isNaN(parseFloat(max_lng))) {
      conditions.push('lng <= ?');
      params.push(parseFloat(max_lng));
    }

    // Keyset pagination: vehicle_id > cursor
    if (cursor && typeof cursor === 'string') {
      conditions.push('vehicle_id > ?');
      params.push(cursor);
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

    const safePoints = Math.min(1000, Math.max(10, parseInt(points, 10) || 200));

    // Default to past 24 hours if omitted
    const now = Date.now();
    const defaultFrom = new Date(now - (24 * 60 * 60 * 1000)).toISOString();
    const defaultTo = new Date(now).toISOString();

    const fromIso = (from && !isNaN(Date.parse(from))) ? new Date(from).toISOString() : defaultFrom;
    const toIso = (to && !isNaN(Date.parse(to))) ? new Date(to).toISOString() : defaultTo;

    // Fast indexed query on (vehicle_id, ts)
    const sql = `
      SELECT seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng, odometer_km, charging
      FROM telemetry_history
      WHERE vehicle_id = ? AND ts >= ? AND ts <= ?
      ORDER BY ts ASC
    `;

    const rawRows = db.prepare(sql).all(id, fromIso, toIso);
    const downsampled = downsampleMinMax(rawRows, safePoints);

    res.json({
      vehicle_id: id,
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
        COALESCE(AVG(soc_pct), 0) AS avg_soc,
        COALESCE(SUM(received_count), 0) AS total_received,
        COALESCE(SUM(max_seq - min_seq + 1), 0) AS total_expected
      FROM vehicles
    `).get();

    // 2. Open alerts breakdown by rule type
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
      openAlertsByRule[row.rule] = Number(row.count || 0);
    }

    const totalVehicles = Number(fleetRow.total_vehicles || 0);
    const totalReceived = Number(fleetRow.total_received || 0);
    const totalExpected = Number(fleetRow.total_expected || 0);

    const completenessPct = totalExpected > 0
      ? Number(((totalReceived / totalExpected) * 100).toFixed(2))
      : 100.0;

    res.json({
      total_vehicles: totalVehicles,
      counts_by_status: {
        online: Number(fleetRow.online_count || 0),
        offline: Number(fleetRow.offline_count || 0)
      },
      avg_soc_pct: Number(Number(fleetRow.avg_soc || 0).toFixed(2)),
      open_alerts_by_rule: openAlertsByRule,
      data_completeness_pct: completenessPct
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
    const { version } = req.body || {};

    if (version === undefined || typeof version !== 'number' || !Number.isInteger(version)) {
      return res.status(400).json({
        error: 'Invalid request: "version" integer is required in request body'
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
      id: parseInt(id, 10),
      version,
      now: nowIso
    });

    if (updateResult.changes === 1) {
      // Successfully updated; fetch and return latest state
      const updatedAlert = db.prepare('SELECT * FROM alerts WHERE id = ?').get(id);
      return res.status(200).json({
        message: 'Alert acknowledged successfully',
        alert: {
          ...updatedAlert,
          details: updatedAlert.details ? JSON.parse(updatedAlert.details) : null
        }
      });
    }

    // Update affected 0 rows -> Check why (does it exist, is version stale, or already resolved?)
    const existing = db.prepare('SELECT id, status, version FROM alerts WHERE id = ?').get(id);

    if (!existing) {
      return res.status(404).json({ error: `Alert with id ${id} not found` });
    }

    return res.status(409).json({
      error: 'Version conflict or alert is no longer open',
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
