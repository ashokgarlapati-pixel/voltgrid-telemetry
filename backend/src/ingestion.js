const db = require('./db');
const { evaluateAlertsForVehicles } = require('./alertEngine');
const { insertOutboxStmt, markOutboxEventsCompleted } = require('./alertOutboxWorker');
const { publishVehicleUpdate, publishAlertChange } = require('./stream');
const { trackTelemetryBatch } = require('./observability');

/**
 * Documented Acceptable Telemetry Ranges:
 * - vehicle_id: non-empty string, trimmed length >= 1
 * - seq: non-negative integer (0 <= seq)
 * - ts: valid ISO-8601 timestamp string (parseable by Date.parse)
 * - soc_pct: finite number between 0.0% and 100.0%
 * - battery_temp_c: finite number between -40.0°C and 120.0°C (extreme automotive operating envelope)
 * - speed_kmh: finite number between 0.0 km/h and 200.0 km/h
 * - lat: finite latitude between -90.0° and +90.0°
 * - lng: finite longitude between -180.0° and +180.0°
 * - odometer_km: finite non-negative number (>= 0.0)
 * - charging: boolean (true or false)
 * - fault_codes: array where every element is a valid string
 * 
 * Validates a single telemetry reading individually.
 * Returns null if valid, or an error message string if invalid.
 */
const TEN_MINUTES_MS = 10 * 60 * 1000;
const THIRTY_SECONDS_MS = 30 * 1000;

function validateReading(reading, serverNowMs = Date.now()) {
  if (!reading || typeof reading !== 'object' || Array.isArray(reading)) {
    return 'Reading must be a non-null object';
  }
  if (typeof reading.vehicle_id !== 'string' || reading.vehicle_id.trim().length === 0) {
    return 'vehicle_id must be a non-empty string';
  }
  if (typeof reading.seq !== 'number' || !Number.isInteger(reading.seq) || reading.seq < 0) {
    return 'seq must be a non-negative integer';
  }
  if (typeof reading.ts !== 'string' || reading.ts.trim().length === 0 || isNaN(Date.parse(reading.ts)) || isNaN(new Date(reading.ts).getTime())) {
    return 'ts must be a valid ISO timestamp string';
  }
  const readingTimeMs = Date.parse(reading.ts);
  if (readingTimeMs < serverNowMs - TEN_MINUTES_MS) {
    return 'ts cannot be older than 10 minutes';
  }
  if (readingTimeMs > serverNowMs + THIRTY_SECONDS_MS) {
    return 'ts cannot be more than 30 seconds in the future';
  }
  if (!Number.isFinite(reading.soc_pct) || reading.soc_pct < 0 || reading.soc_pct > 100) {
    return 'soc_pct must be a finite number between 0 and 100';
  }
  if (!Number.isFinite(reading.battery_temp_c) || reading.battery_temp_c < -40 || reading.battery_temp_c > 120) {
    return 'battery_temp_c must be a finite number between -40 and 120';
  }
  if (!Number.isFinite(reading.speed_kmh) || reading.speed_kmh < 0 || reading.speed_kmh > 200) {
    return 'speed_kmh must be a finite number between 0 and 200';
  }
  if (!Number.isFinite(reading.lat) || reading.lat < -90 || reading.lat > 90) {
    return 'lat must be a finite number between -90 and 90';
  }
  if (!Number.isFinite(reading.lng) || reading.lng < -180 || reading.lng > 180) {
    return 'lng must be a finite number between -180 and 180';
  }
  if (!Number.isFinite(reading.odometer_km) || reading.odometer_km < 0) {
    return 'odometer_km must be a finite non-negative number';
  }
  if (typeof reading.charging !== 'boolean') {
    return 'charging must be a boolean';
  }
  if (!Array.isArray(reading.fault_codes) || !reading.fault_codes.every(code => typeof code === 'string')) {
    return 'fault_codes must be an array of strings';
  }
  return null;
}

// Prepared statements for batch operations
const insertHistoryStmt = db.prepare(`
  INSERT OR IGNORE INTO telemetry_history (
    vehicle_id, seq, ts, soc_pct, battery_temp_c,
    speed_kmh, lat, lng, odometer_km, charging,
    fault_codes, received_at
  ) VALUES (
    @vehicle_id, @seq, @ts, @soc_pct, @battery_temp_c,
    @speed_kmh, @lat, @lng, @odometer_km, @charging,
    @fault_codes, @received_at
  )
`);

const upsertVehicleStmt = db.prepare(`
  INSERT INTO vehicles (
    vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c,
    speed_kmh, lat, lng, odometer_km, charging, status,
    min_seq, max_seq, received_count, updated_at
  ) VALUES (
    @vehicle_id, @last_ts, @last_seq, @soc_pct, @battery_temp_c,
    @speed_kmh, @lat, @lng, @odometer_km, @charging, 'online',
    @seq, @seq, 1, @updated_at
  )
  ON CONFLICT(vehicle_id) DO UPDATE SET
    -- Documented deterministic ordering rule:
    -- 1. Reading with strictly higher device timestamp (last_ts) wins.
    -- 2. If device timestamps are equal, monotonic sequence counter (last_seq) breaks ties.
    -- Server arrival time is NEVER used to determine current vehicle state.
    last_ts = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.last_ts ELSE vehicles.last_ts END,
    last_seq = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.last_seq ELSE vehicles.last_seq END,
    soc_pct = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.soc_pct ELSE vehicles.soc_pct END,
    battery_temp_c = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.battery_temp_c ELSE vehicles.battery_temp_c END,
    speed_kmh = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.speed_kmh ELSE vehicles.speed_kmh END,
    lat = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.lat ELSE vehicles.lat END,
    lng = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.lng ELSE vehicles.lng END,
    odometer_km = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.odometer_km ELSE vehicles.odometer_km END,
    charging = CASE 
      WHEN excluded.last_ts > vehicles.last_ts OR (excluded.last_ts = vehicles.last_ts AND excluded.last_seq > vehicles.last_seq) 
      THEN excluded.charging ELSE vehicles.charging END,
    status = 'online',
    min_seq = MIN(vehicles.min_seq, excluded.min_seq),
    max_seq = MAX(vehicles.max_seq, excluded.max_seq),
    received_count = vehicles.received_count + 1,
    updated_at = excluded.updated_at
`);

// Resolves any active offline alert when new data arrives
const resolveOfflineAlertStmt = db.prepare(`
  UPDATE alerts
  SET status = 'resolved',
      resolved_at = @now,
      version = version + 1,
      updated_at = @now
  WHERE vehicle_id = @vehicle_id AND rule = 'offline' AND status != 'resolved'
`);

const getActiveOfflineAlertStmt = db.prepare("SELECT id FROM alerts WHERE vehicle_id = ? AND rule = 'offline' AND status != 'resolved'");
const getAlertByIdStmt = db.prepare('SELECT * FROM alerts WHERE id = ?');
const getVehicleByIdStmt = db.prepare('SELECT * FROM vehicles WHERE vehicle_id = ?');

/**
 * Transaction wrapped in db.transaction(...).immediate() (BEGIN IMMEDIATE)
 * to acquire write locks upfront and prevent deadlocks under high concurrency.
 */
const executeBatchTransaction = db.transaction((validItems, nowIso) => {
  const transactionResults = [];
  const acceptedVehicleIds = new Set();
  const alertsToPublish = [];
  let accepted = 0;
  let duplicates = 0;

  for (const item of validItems) {
    const { reading, index } = item;

    // Normalize timestamp to ISO string format
    const normalizedTs = new Date(reading.ts).toISOString();

    // 1. Insert into telemetry_history with INSERT OR IGNORE
    const historyInfo = insertHistoryStmt.run({
      vehicle_id: reading.vehicle_id,
      seq: reading.seq,
      ts: normalizedTs,
      soc_pct: reading.soc_pct,
      battery_temp_c: reading.battery_temp_c,
      speed_kmh: reading.speed_kmh,
      lat: reading.lat,
      lng: reading.lng,
      odometer_km: reading.odometer_km,
      charging: reading.charging ? 1 : 0,
      fault_codes: JSON.stringify(reading.fault_codes),
      received_at: nowIso
    });

    if (historyInfo.changes === 0) {
      // Primary key conflict (vehicle_id, seq) -> duplicate reading
      duplicates++;
      transactionResults.push({
        index,
        vehicle_id: reading.vehicle_id,
        seq: reading.seq,
        status: 'duplicate'
      });
      // Duplicate reading: DO NOT update vehicles table or increment received_count
      continue;
    }

    // Newly accepted reading -> upsert vehicle state
    accepted++;
    acceptedVehicleIds.add(reading.vehicle_id);

    upsertVehicleStmt.run({
      vehicle_id: reading.vehicle_id,
      last_ts: normalizedTs,
      last_seq: reading.seq,
      soc_pct: reading.soc_pct,
      battery_temp_c: reading.battery_temp_c,
      speed_kmh: reading.speed_kmh,
      lat: reading.lat,
      lng: reading.lng,
      odometer_km: reading.odometer_km,
      charging: reading.charging ? 1 : 0,
      seq: reading.seq,
      updated_at: nowIso
    });

    // Resolve any active 'offline' alert as the vehicle is now actively reporting
    const activeOffline = getActiveOfflineAlertStmt.get(reading.vehicle_id);
    if (activeOffline) {
      resolveOfflineAlertStmt.run({
        vehicle_id: reading.vehicle_id,
        now: nowIso
      });
      const resolvedAlert = getAlertByIdStmt.get(activeOffline.id);
      if (resolvedAlert) {
        // Collect resolved alerts to be broadcast strictly AFTER the transaction has successfully committed
        alertsToPublish.push(resolvedAlert);
      }
    }

    transactionResults.push({
      index,
      vehicle_id: reading.vehicle_id,
      seq: reading.seq,
      status: 'accepted'
    });
  }

  // Record transactional outbox event for crash-resilient alert evaluation
  for (const vid of acceptedVehicleIds) {
    insertOutboxStmt.run({
      vehicle_id: vid,
      created_at: nowIso
    });
  }

  return { accepted, duplicates, transactionResults, acceptedVehicleIds, alertsToPublish };
}).immediate;

/**
 * Ingests a batch of telemetry readings.
 * Validates each reading individually, handles idempotency and timestamp ordering,
 * tracks sequence gaps, and triggers order-independent alert evaluations.
 */
function ingestBatch(readings, nowIso = new Date().toISOString()) {
  const serverNowMs = Date.parse(nowIso);
  const validItems = [];
  const results = new Array(readings.length);
  let rejected = 0;

  // Step 1: Individual validation (partial success guarantee)
  for (let i = 0; i < readings.length; i++) {
    const reading = readings[i];
    const validationError = validateReading(reading, serverNowMs);

    if (validationError) {
      rejected++;
      const resultItem = {
        index: i,
        status: 'rejected',
        error: validationError
      };
      if (reading && typeof reading.vehicle_id === 'string' && reading.vehicle_id.trim().length > 0) {
        resultItem.vehicle_id = reading.vehicle_id;
      }
      if (reading && typeof reading.seq === 'number') {
        resultItem.seq = reading.seq;
      }
      results[i] = resultItem;
    } else {
      validItems.push({ reading, index: i });
    }
  }

  // Step 2: Execute atomic database transaction for all valid readings
  let accepted = 0;
  let duplicates = 0;

  if (validItems.length > 0) {
    const txResult = executeBatchTransaction(validItems, nowIso);
    accepted = txResult.accepted;
    duplicates = txResult.duplicates;

    for (const item of txResult.transactionResults) {
      results[item.index] = item;
    }

    // Publish resolved offline alerts strictly AFTER the transaction has successfully committed!
    if (txResult.alertsToPublish && txResult.alertsToPublish.length > 0) {
      const publishedAlertIds = new Set();
      for (const alert of txResult.alertsToPublish) {
        if (!publishedAlertIds.has(alert.id)) {
          publishedAlertIds.add(alert.id);
          publishAlertChange(alert);
        }
      }
    }

    // Step 3: Trigger order-independent alert rule evaluation for vehicles with new readings
    if (txResult.acceptedVehicleIds && txResult.acceptedVehicleIds.size > 0) {
      evaluateAlertsForVehicles(txResult.acceptedVehicleIds, nowIso);

      // Mark transactional outbox events as completed
      markOutboxEventsCompleted(txResult.acceptedVehicleIds, nowIso);

      // Publish latest vehicle state to SSE stream (coalesced 1s buffer)
      for (const vid of txResult.acceptedVehicleIds) {
        const v = getVehicleByIdStmt.get(vid);
        if (v) {
          publishVehicleUpdate(v);
        }
      }
    }
  }

  trackTelemetryBatch(accepted, duplicates, rejected);

  return {
    total: readings.length,
    accepted,
    duplicates,
    rejected,
    summary: {
      total: readings.length,
      accepted,
      duplicates,
      rejected
    },
    results
  };
}

module.exports = {
  validateReading,
  ingestBatch,
  executeBatchTransaction
};
