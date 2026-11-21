/**
 * VoltGrid Fleet Telemetry Service - Messy Traffic Simulator
 * 
 * Simulates high-throughput real-world vehicle telemetry with configurable network anomalies:
 * - Clock drift per vehicle (-30s to +30s)
 * - Out-of-order / late arrivals via a delayed queue
 * - Duplicate transmissions
 * - Corrupted / malformed payloads
 * 
 * Uses standard Node.js native fetch (no external npm dependencies required).
 */

const crypto = require('crypto');

const API_URL = process.env.API_URL || 'http://localhost:3000/v1/telemetry/batch';
const API_KEY = process.env.API_KEY || 'voltgrid-dev-key';
const API_SECRET = process.env.API_SECRET || 'voltgrid-dev-secret';
const NUM_VEHICLES = parseInt(process.env.NUM_VEHICLES || '2000', 10);
const REPORT_INTERVAL_MS = parseInt(process.env.REPORT_INTERVAL_MS || '5000', 10);
const DUPLICATE_RATE = parseFloat(process.env.DUPLICATE_RATE || '0.05');
const LATE_RATE = parseFloat(process.env.LATE_RATE || '0.10');
const MAX_LATE_MS = parseInt(process.env.MAX_LATE_MS || '600000', 10);
const MAX_DRIFT_MS = parseInt(process.env.MAX_DRIFT_MS || '30000', 10);
const MALFORMED_RATE = parseFloat(process.env.MALFORMED_RATE || '0.02');
const BATCH_SIZE = Math.min(500, parseInt(process.env.BATCH_SIZE || '500', 10));

console.log('='.repeat(70));
console.log('VoltGrid Messy Traffic Simulator starting...');
console.log(`Target API URL      : ${API_URL}`);
console.log(`Total Vehicles      : ${NUM_VEHICLES} (VG-00001 to VG-${String(NUM_VEHICLES).padStart(5, '0')})`);
console.log(`Reporting Interval  : ${REPORT_INTERVAL_MS}ms per vehicle (~${Math.round(NUM_VEHICLES / (REPORT_INTERVAL_MS / 1000))} readings/sec)`);
console.log(`Duplicate Rate      : ${(DUPLICATE_RATE * 100).toFixed(1)}%`);
console.log(`Late Arrival Rate   : ${(LATE_RATE * 100).toFixed(1)}% (max delay: ${MAX_LATE_MS}ms)`);
console.log(`Max Clock Drift     : ±${MAX_DRIFT_MS}ms`);
console.log(`Malformed Rate      : ${(MALFORMED_RATE * 100).toFixed(1)}%`);
console.log(`Max Batch Size      : ${BATCH_SIZE}`);
console.log('='.repeat(70));

// Initialize simulated fleet
const vehicles = Array.from({ length: NUM_VEHICLES }, (_, i) => {
  const idNum = String(i + 1).padStart(5, '0');
  return {
    vehicle_id: `VG-${idNum}`,
    seq: 0,
    // Each vehicle has a persistent random clock drift in range [-MAX_DRIFT_MS, +MAX_DRIFT_MS]
    clock_drift_ms: Math.floor((Math.random() * 2 - 1) * MAX_DRIFT_MS),
    soc_pct: 60 + Math.random() * 35,
    battery_temp_c: 25 + Math.random() * 20,
    speed_kmh: 15 + Math.random() * 65,
    lat: 13.0827 + (Math.random() - 0.5) * 0.08,
    lng: 80.2707 + (Math.random() - 0.5) * 0.08,
    odometer_km: 1000 + i * 2.5,
    charging: false,
    fault_codes: Math.random() < 0.03 ? ['POA80'] : []
  };
});

// Buffer for holding out-of-order readings
const delayedQueue = [];

// Ring buffer of recently sent valid readings to sample realistic duplicates
const sentHistory = [];
const MAX_HISTORY = 4000;

// Cumulative counters for logging
let totalSentCount = 0;
let totalAcceptedCount = 0;
let totalDuplicatesCount = 0;
let totalRejectedCount = 0;

let vehicleIndexCursor = 0;

/**
 * Creates a slightly updated reading for a vehicle
 */
function generateNextReading(v) {
  v.seq += 1;

  // Simulate vehicle operational physics
  v.speed_kmh = Math.max(0, Math.min(130, v.speed_kmh + (Math.random() - 0.5) * 10));
  v.odometer_km += (v.speed_kmh * (REPORT_INTERVAL_MS / 3600000));
  v.soc_pct = Math.max(5, v.soc_pct - 0.05);
  v.battery_temp_c = Math.max(15, Math.min(65, v.battery_temp_c + (Math.random() - 0.5) * 1.5));
  v.lat += (Math.random() - 0.5) * 0.001;
  v.lng += (Math.random() - 0.5) * 0.001;

  // Apply device clock drift to reading timestamp
  const deviceTime = Date.now() + v.clock_drift_ms;

  return {
    vehicle_id: v.vehicle_id,
    seq: v.seq,
    ts: new Date(deviceTime).toISOString(),
    soc_pct: Number(v.soc_pct.toFixed(1)),
    battery_temp_c: Number(v.battery_temp_c.toFixed(1)),
    speed_kmh: Number(v.speed_kmh.toFixed(1)),
    lat: Number(v.lat.toFixed(4)),
    lng: Number(v.lng.toFixed(4)),
    odometer_km: Number(v.odometer_km.toFixed(1)),
    charging: v.charging,
    fault_codes: [...v.fault_codes]
  };
}

/**
 * Corrupts a reading payload to simulate malformed telemetry
 */
function corruptPayload(reading) {
  const corrupted = { ...reading };
  const mutation = Math.floor(Math.random() * 6);

  switch (mutation) {
    case 0:
      // Out of bounds state of charge
      corrupted.soc_pct = 140.0;
      break;
    case 1:
      // Missing or empty vehicle_id
      delete corrupted.vehicle_id;
      break;
    case 2:
      // Out of bounds battery temperature
      corrupted.battery_temp_c = -85.0;
      break;
    case 3:
      // Negative speed
      corrupted.speed_kmh = -25.0;
      break;
    case 4:
      // Invalid timestamp string
      corrupted.ts = 'INVALID_TIMESTAMP_STRING';
      break;
    case 5:
      // Non-array fault_codes
      corrupted.fault_codes = 'POA80';
      break;
  }

  return corrupted;
}

/**
 * Sends a single batch via HTTP POST to the ingestion API, signed with HMAC-SHA256
 */
async function sendBatch(batch) {
  try {
    const rawBody = JSON.stringify(batch);
    const timestamp = Date.now().toString();
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${rawBody}`).digest('hex');

    const res = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Timestamp': timestamp,
        'X-Signature': signature
      },
      body: rawBody
    });

    const body = await res.json();

    if (res.status === 200 || res.status === 207) {
      if (totalSentCount === 0 && batch.length > 0) {
        console.log('[Simulator] Sample live telemetry reading generated:');
        console.log(JSON.stringify(batch[0], null, 2));
        console.log('-'.repeat(70));
      }

      totalSentCount += batch.length;
      totalAcceptedCount += body.accepted;
      totalDuplicatesCount += body.duplicates;
      totalRejectedCount += body.rejected;

      console.log(
        `[Simulator] HTTP ${res.status} | Batch size: ${batch.length} ` +
        `-> Accepted: ${body.accepted}, Duplicates: ${body.duplicates}, Rejected: ${body.rejected} ` +
        `| DelayedQueue: ${delayedQueue.length}`
      );
    } else if (res.status === 429) {
      const retryAfter = res.headers.get('retry-after') || '1';
      console.warn(`[Simulator] Rate limit exceeded (HTTP 429)! Backing off for ${retryAfter}s`);
    } else {
      console.warn(`[Simulator] Batch returned unexpected status HTTP ${res.status}:`, body);
    }
  } catch (err) {
    console.error(`[Simulator] Failed to send batch to ${API_URL}:`, err.message);
  }
}

/**
 * Main simulation tick executed every 1 second
 */
async function tick() {
  const readingsToSend = [];

  // Determine how many vehicles report in this 1-second slice
  const sliceSize = Math.ceil(NUM_VEHICLES / (REPORT_INTERVAL_MS / 1000));
  const reportingVehicles = [];

  for (let i = 0; i < sliceSize; i++) {
    reportingVehicles.push(vehicles[vehicleIndexCursor]);
    vehicleIndexCursor = (vehicleIndexCursor + 1) % NUM_VEHICLES;
  }

  // 1. Generate fresh readings
  for (const v of reportingVehicles) {
    const reading = generateNextReading(v);

    // Check if this reading should be delayed (late arrival)
    if (Math.random() < LATE_RATE) {
      // Ensure reading age at release does not exceed MAX_LATE_MS (10 minutes) even with negative clock drift
      const maxAllowedDelay = Math.max(1000, MAX_LATE_MS - Math.max(0, -v.clock_drift_ms));
      const delayMs = Math.floor(Math.random() * maxAllowedDelay);
      delayedQueue.push({
        reading,
        releaseAt: Date.now() + delayMs
      });
    } else {
      readingsToSend.push(reading);
    }
  }

  // 2. Release overdue readings from the delayed queue
  const now = Date.now();
  let delayedReleased = 0;
  for (let i = delayedQueue.length - 1; i >= 0; i--) {
    if (delayedQueue[i].releaseAt <= now) {
      readingsToSend.push(delayedQueue[i].reading);
      delayedQueue.splice(i, 1);
      delayedReleased++;
    }
  }

  // 3. Inject realistic duplicates from recent sent history
  if (sentHistory.length > 0 && DUPLICATE_RATE > 0) {
    const dupCount = Math.round(readingsToSend.length * DUPLICATE_RATE);
    for (let i = 0; i < dupCount; i++) {
      const randomIdx = Math.floor(Math.random() * sentHistory.length);
      readingsToSend.push({ ...sentHistory[randomIdx] });
    }
  }

  // 4. Inject malformed payloads according to MALFORMED_RATE
  if (MALFORMED_RATE > 0 && readingsToSend.length > 0) {
    const malformedCount = Math.round(readingsToSend.length * MALFORMED_RATE);
    for (let i = 0; i < malformedCount; i++) {
      const targetIdx = Math.floor(Math.random() * readingsToSend.length);
      readingsToSend[targetIdx] = corruptPayload(readingsToSend[targetIdx]);
    }
  }

  // 5. Store valid-looking readings into sentHistory ring buffer
  for (const r of readingsToSend) {
    if (r.vehicle_id && typeof r.seq === 'number' && !isNaN(Date.parse(r.ts))) {
      sentHistory.push(r);
      if (sentHistory.length > MAX_HISTORY) {
        sentHistory.shift();
      }
    }
  }

  // 6. Split into chunks of max 500 and dispatch
  if (readingsToSend.length > 0) {
    for (let i = 0; i < readingsToSend.length; i += BATCH_SIZE) {
      const chunk = readingsToSend.slice(i, i + BATCH_SIZE);
      await sendBatch(chunk);
    }
  }
}

let timer = null;

// Log summary on shutdown
function handleExit() {
  if (timer) clearInterval(timer);
  console.log('\n' + '='.repeat(70));
  console.log('VoltGrid Simulator Stopped. Cumulative Statistics:');
  console.log(`  Total Readings Sent : ${totalSentCount}`);
  console.log(`  Total Accepted      : ${totalAcceptedCount}`);
  console.log(`  Total Duplicates    : ${totalDuplicatesCount}`);
  console.log(`  Total Rejected      : ${totalRejectedCount}`);
  console.log(`  Remaining in Delayed: ${delayedQueue.length}`);
  console.log('='.repeat(70));
  process.exit(0);
}

// Start simulator loop if run directly
if (require.main === module) {
  timer = setInterval(tick, 1000);
  process.on('SIGINT', handleExit);
  process.on('SIGTERM', handleExit);
  tick();
}

module.exports = {
  MAX_LATE_MS,
  MAX_DRIFT_MS,
  generateNextReading,
  corruptPayload,
  vehicles,
  delayedQueue,
  tick
};
