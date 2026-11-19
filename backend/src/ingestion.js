const db = require('./db');

/**
 * Validates a single telemetry reading according to Phase 1 rules.
 * Returns null if valid, or an error message string if invalid.
 */
function validateReading(reading) {
  if (!reading || typeof reading !== 'object') {
    return 'Reading must be a non-null object';
  }
  if (typeof reading.vehicle_id !== 'string' || reading.vehicle_id.trim().length === 0) {
    return 'vehicle_id must be a non-empty string';
  }
  if (typeof reading.seq !== 'number' || !Number.isInteger(reading.seq) || reading.seq < 0) {
    return 'seq must be a non-negative integer';
  }
  if (typeof reading.ts !== 'string' || reading.ts.trim().length === 0 || isNaN(Date.parse(reading.ts))) {
    return 'ts must be a valid ISO timestamp string';
  }
  if (typeof reading.soc_pct !== 'number' || isNaN(reading.soc_pct) || reading.soc_pct < 0 || reading.soc_pct > 100) {
    return 'soc_pct must be a number between 0 and 100';
  }
  if (typeof reading.battery_temp_c !== 'number' || isNaN(reading.battery_temp_c) || reading.battery_temp_c < -40 || reading.battery_temp_c > 120) {
    return 'battery_temp_c must be a number between -40 and 120';
  }
  if (typeof reading.speed_kmh !== 'number' || isNaN(reading.speed_kmh) || reading.speed_kmh < 0 || reading.speed_kmh > 200) {
    return 'speed_kmh must be a number between 0 and 200';
  }
  if (typeof reading.lat !== 'number' || isNaN(reading.lat) || reading.lat < -90 || reading.lat > 90) {
    return 'lat must be a number between -90 and 90';
  }
  if (typeof reading.lng !== 'number' || isNaN(reading.lng) || reading.lng < -180 || reading.lng > 180) {
    return 'lng must be a number between -180 and 180';
  }
  if (typeof reading.odometer_km !== 'number' || isNaN(reading.odometer_km) || reading.odometer_km < 0) {
    return 'odometer_km must be a non-negative number';
  }
  if (typeof reading.charging !== 'boolean') {
    return 'charging must be a boolean';
  }
  if (!Array.isArray(reading.fault_codes)) {
    return 'fault_codes must be an array';
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
    last_ts = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.last_ts ELSE vehicles.last_ts END,
    last_seq = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.last_seq ELSE vehicles.last_seq END,
    soc_pct = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.soc_pct ELSE vehicles.soc_pct END,
    battery_temp_c = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.battery_temp_c ELSE vehicles.battery_temp_c END,
    speed_kmh = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.speed_kmh ELSE vehicles.speed_kmh END,
    lat = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.lat ELSE vehicles.lat END,
    lng = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.lng ELSE vehicles.lng END,
    odometer_km = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.odometer_km ELSE vehicles.odometer_km END,
    charging = CASE WHEN excluded.last_ts > vehicles.last_ts THEN excluded.charging ELSE vehicles.charging END,
    status = 'online',
    min_seq = MIN(vehicles.min_seq, excluded.min_seq),
    max_seq = MAX(vehicles.max_seq, excluded.max_seq),
    received_count = vehicles.received_count + 1,
    updated_at = excluded.updated_at
`);

/**
 * Transaction wrapped in db.transaction(...).immediate() (BEGIN IMMEDIATE)
 * to acquire write locks upfront and prevent deadlocks under high concurrency.
 */
const executeBatchTransaction = db.transaction((validItems, nowIso) => {
  const transactionResults = [];
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

    transactionResults.push({
      index,
      vehicle_id: reading.vehicle_id,
      seq: reading.seq,
      status: 'accepted'
    });
  }

  return { accepted, duplicates, transactionResults };
}).immediate;

/**
 * Ingests a batch of telemetry readings.
 * Validates each reading individually, handles idempotency and timestamp ordering,
 * and tracks sequence min/max gaps.
 */
function ingestBatch(readings) {
  const nowIso = new Date().toISOString();
  const validItems = [];
  const results = new Array(readings.length);
  let rejected = 0;

  // Step 1: Individual validation (partial success guarantee)
  for (let i = 0; i < readings.length; i++) {
    const reading = readings[i];
    const validationError = validateReading(reading);

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
  }

  return {
    accepted,
    duplicates,
    rejected,
    results
  };
}

module.exports = {
  validateReading,
  ingestBatch
};
