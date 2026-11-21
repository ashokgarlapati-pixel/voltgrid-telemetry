#!/usr/bin/env node
'use strict';

/**
 * VoltGrid Fleet Telemetry Service - 5 Million Readings Benchmark Runner (Part A4 / A6)
 * 
 * Verifies that GET /v1/fleet/summary maintains p95 response time below 150 ms
 * against a reproducible dataset containing 5,000,000 telemetry readings.
 * 
 * Key Features:
 * - Idempotent dataset generation: Checks existing count before insertion to avoid duplicates.
 * - Realistic multi-vehicle distribution: 1,000 vehicles, 5,000 readings each, sequential timestamps,
 *   varying SOC, battery temperatures, speed, Chennai coordinates, and active alerts.
 * - Concurrency benchmark with warm-up phase and microsecond-precision latency tracking.
 * - Computes p50, p90, p95, p99, average latency, throughput (req/s), and error rate.
 * - Full hardware, environment, and SQLite query plan reporting.
 */

const fs = require('fs');
const path = require('path');
const http = require('http');
const os = require('os');

process.env.NODE_ENV = process.env.NODE_ENV || 'test';

// Parse CLI flags and environment variables
const args = process.argv.slice(2);
function getArg(prefix, defaultVal) {
  const match = args.find(a => a.startsWith(prefix));
  if (match) return match.split('=')[1];
  return defaultVal;
}
const hasFlag = (flag) => args.includes(flag);

if (hasFlag('--help') || hasFlag('-h')) {
  console.log(`
VoltGrid Fleet Summary 5M Readings Benchmark
Usage: node scripts/benchmark_5m.js [options]

Options:
  --target=<n>       Total telemetry readings (default: 5000000)
  --vehicles=<n>     Total fleet vehicles (default: 1000)
  --concurrency=<n>  Concurrent benchmark workers (default: 10)
  --requests=<n>     Total benchmark requests (default: 500)
  --warmup=<n>       Warm-up requests before measuring (default: 50)
  --db=<path>        Custom SQLite database path (default: data/benchmark_5m.db)
  --force            Regenerate dataset even if target count already exists
  --clean            Delete benchmark database file after completion
  --help, -h         Show this help message
`);
  process.exit(0);
}

const TARGET_READINGS = parseInt(getArg('--target', process.env.TARGET_READINGS || '5000000'), 10);
const VEHICLE_COUNT = parseInt(getArg('--vehicles', process.env.VEHICLES || '1000'), 10);
const CONCURRENCY = parseInt(getArg('--concurrency', process.env.CONCURRENCY || '10'), 10);
const TOTAL_REQUESTS = parseInt(getArg('--requests', process.env.REQUESTS || '500'), 10);
const WARMUP_REQUESTS = parseInt(getArg('--warmup', process.env.WARMUP || '50'), 10);
const FORCE_GEN = hasFlag('--force') || process.env.FORCE_GEN === 'true';
const CLEAN_AFTER = hasFlag('--clean') || process.env.CLEAN_AFTER === 'true';

const DEFAULT_DB_PATH = path.resolve(__dirname, '../data/benchmark_5m.db');
const BENCH_DB_PATH = path.resolve(getArg('--db', process.env.BENCH_DB_PATH || DEFAULT_DB_PATH));

// Ensure target database directory exists
const dbDir = path.dirname(BENCH_DB_PATH);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

// Point environment to benchmark DB before loading database module
process.env.DB_PATH = BENCH_DB_PATH;
process.env.PORT = '0'; // Ephemeral port for isolated testing

const db = require('../backend/src/db');

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, index)];
}

/**
 * 1. Seed or Verify Benchmark Dataset (Idempotent)
 */
async function ensureDataset() {
  console.log('='.repeat(75));
  console.log(' VoltGrid 5,000,000 Telemetry Readings Benchmark Dataset Setup');
  console.log('='.repeat(75));
  console.log(`Database Target Path : ${BENCH_DB_PATH}`);
  console.log(`Target Readings      : ${TARGET_READINGS.toLocaleString()}`);
  console.log(`Target Vehicles      : ${VEHICLE_COUNT.toLocaleString()}`);

  // Check current reading and vehicle counts
  const countRow = db.prepare('SELECT COUNT(*) AS count FROM telemetry_history').get();
  const currentCount = Number(countRow?.count || 0);

  const vehicleCountRow = db.prepare('SELECT COUNT(DISTINCT vehicle_id) AS vCount FROM telemetry_history').get();
  const currentVehicleCount = Number(vehicleCountRow?.vCount || 0);

  const vehiclesTableCountRow = db.prepare('SELECT COUNT(*) AS count FROM vehicles').get();
  const currentVehiclesTableCount = Number(vehiclesTableCountRow?.count || 0);

  console.log(`Existing Readings    : ${currentCount.toLocaleString()} across ${currentVehicleCount.toLocaleString()} vehicles (vehicles table: ${currentVehiclesTableCount.toLocaleString()})`);

  // Verify whether the exact intended benchmark dataset already exists
  const isExactDataset = (currentCount === TARGET_READINGS) &&
                         (currentVehicleCount === VEHICLE_COUNT) &&
                         (currentVehiclesTableCount === VEHICLE_COUNT);

  if (isExactDataset && !FORCE_GEN) {
    console.log(`\n[Idempotency Check] Database already contains exactly ${currentCount.toLocaleString()} benchmark readings across ${currentVehicleCount.toLocaleString()} vehicles.`);
    console.log('Skipping dataset generation to avoid duplicate data.\n');
    return;
  }

  // If dirty, partial, or mismatched dataset exists (or force requested), clean before regenerating
  if (currentCount > 0 || currentVehiclesTableCount > 0) {
    console.log(`\n[Dataset Setup] Current record state (${currentCount.toLocaleString()} readings, ${currentVehiclesTableCount.toLocaleString()} vehicles) does not match benchmark target (${TARGET_READINGS.toLocaleString()} readings, ${VEHICLE_COUNT.toLocaleString()} vehicles).`);
    console.log('Purging existing records to ensure clean, non-duplicate dataset...');
    db.exec('DELETE FROM telemetry_history;');
    db.exec('DELETE FROM vehicles;');
    db.exec('DELETE FROM alerts;');
  }

  // 1. Populate Vehicles Table
  console.log(`\nSeeding ${VEHICLE_COUNT.toLocaleString()} vehicle states in 'vehicles' table...`);
  const insertVehicle = db.prepare(`
    INSERT OR REPLACE INTO vehicles (
      vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
      lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, datetime('now'))
  `);

  const basePerVehicle = Math.floor(TARGET_READINGS / VEHICLE_COUNT);
  const remainder = TARGET_READINGS % VEHICLE_COUNT;
  const nowIso = new Date().toISOString();

  db.exec('BEGIN TRANSACTION');
  for (let v = 1; v <= VEHICLE_COUNT; v++) {
    const vid = `VG-${String(v).padStart(5, '0')}`;
    const readingsForThisVehicle = basePerVehicle + (v <= remainder ? 1 : 0);
    const status = v <= Math.floor(VEHICLE_COUNT * 0.85)
      ? 'online'
      : (v <= Math.floor(VEHICLE_COUNT * 0.97) ? 'offline' : 'maintenance');
    const soc = Number((20 + (v % 76)).toFixed(1));
    const temp = Number((25 + (v % 28)).toFixed(1));
    const speed = status === 'online' ? Number((20 + (v % 60)).toFixed(1)) : 0;
    const lat = Number((13.0827 + ((v % 50) - 25) * 0.002).toFixed(4));
    const lng = Number((80.2707 + ((v % 50) - 25) * 0.002).toFixed(4));
    const odo = Number((1000 + v * 12.5).toFixed(1));
    const charging = status === 'online' && (v % 8 === 0) ? 1 : 0;

    insertVehicle.run(
      vid, nowIso, readingsForThisVehicle, soc, temp, speed,
      lat, lng, odo, charging, status, readingsForThisVehicle, readingsForThisVehicle
    );
  }
  db.exec('COMMIT');

  // 2. Populate Active Alerts
  console.log("Seeding realistic active and acknowledged alerts in 'alerts' table...");
  const insertAlert = db.prepare(`
    INSERT INTO alerts (vehicle_id, rule, status, version, details, opened_at, updated_at)
    VALUES (?, ?, ?, 1, ?, datetime('now', '-10 minutes'), datetime('now'))
  `);

  const rules = ['overheat', 'rapid_discharge', 'geofence_breach', 'offline'];
  db.exec('BEGIN TRANSACTION');
  for (let a = 1; a <= 60; a++) {
    const vid = `VG-${String(a).padStart(5, '0')}`;
    const rule = rules[a % rules.length];
    const status = (a % 4 === 0) ? 'acknowledged' : 'open';
    const details = JSON.stringify({ note: 'Simulated alert for benchmark verification', rule });
    insertAlert.run(vid, rule, status, details);
  }
  db.exec('COMMIT');

  // 3. Populate Exact Telemetry Readings (guaranteed exactly TARGET_READINGS)
  console.log(`Inserting ${TARGET_READINGS.toLocaleString()} telemetry records into 'telemetry_history'...`);
  const insertTelemetry = db.prepare(`
    INSERT OR IGNORE INTO telemetry_history (
      vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
      lat, lng, odometer_km, charging, fault_codes, received_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', datetime('now'))
  `);

  const BATCH_SIZE = 50000;
  const baseTs = Date.parse('2026-11-20T00:00:00.000Z');
  const startTime = Date.now();
  let totalInserted = 0;

  db.exec('PRAGMA synchronous = OFF;'); // High-speed bulk insertion mode
  db.exec('BEGIN TRANSACTION;');

  for (let v = 1; v <= VEHICLE_COUNT; v++) {
    const vid = `VG-${String(v).padStart(5, '0')}`;
    const readingsForThisVehicle = basePerVehicle + (v <= remainder ? 1 : 0);

    for (let s = 1; s <= readingsForThisVehicle; s++) {
      const ts = new Date(baseTs + s * 5000).toISOString();
      const soc = Number((95.0 - ((s % 500) * 0.1)).toFixed(1));
      const temp = Number((28.0 + ((s % 100) * 0.2)).toFixed(1));
      const speed = Number((s % 80).toFixed(1));
      const lat = Number((13.0827 + ((s % 40) - 20) * 0.001).toFixed(4));
      const lng = Number((80.2707 + ((s % 40) - 20) * 0.001).toFixed(4));
      const odo = Number((1000 + s * 0.05).toFixed(1));
      const charging = (s % 50 === 0) ? 1 : 0;

      insertTelemetry.run(vid, s, ts, soc, temp, speed, lat, lng, odo, charging);
      totalInserted++;

      if (totalInserted % BATCH_SIZE === 0) {
        db.exec('COMMIT;');
        const elapsedSec = (Date.now() - startTime) / 1000;
        const rate = Math.round(totalInserted / elapsedSec);
        process.stdout.write(`\r  Progress: ${totalInserted.toLocaleString()} / ${TARGET_READINGS.toLocaleString()} (${Math.round((totalInserted / TARGET_READINGS) * 100)}%) - Rate: ${rate.toLocaleString()} rows/s`);
        if (totalInserted < TARGET_READINGS) {
          db.exec('BEGIN TRANSACTION;');
        }
      }
    }
  }

  // Final commit if remaining uncommitted rows
  try {
    db.exec('COMMIT;');
  } catch (_) {}

  db.exec('PRAGMA synchronous = NORMAL;'); // Restore safe durability mode
  db.exec('PRAGMA wal_checkpoint(PASSIVE);');

  // Verify exact dataset counts
  const verifyHistoryRow = db.prepare('SELECT COUNT(*) AS count FROM telemetry_history').get();
  const finalHistoryCount = Number(verifyHistoryRow?.count || 0);

  const verifyVehiclesRow = db.prepare('SELECT COUNT(*) AS count FROM vehicles').get();
  const finalVehiclesCount = Number(verifyVehiclesRow?.count || 0);

  if (finalHistoryCount !== TARGET_READINGS) {
    throw new Error(`Dataset verification failed: expected exactly ${TARGET_READINGS} telemetry readings, but found ${finalHistoryCount}`);
  }
  if (finalVehiclesCount !== VEHICLE_COUNT) {
    throw new Error(`Dataset verification failed: expected exactly ${VEHICLE_COUNT} vehicles, but found ${finalVehiclesCount}`);
  }

  const genDuration = (Date.now() - startTime) / 1000;
  console.log(`\n\nDataset generation complete: Exactly ${finalHistoryCount.toLocaleString()} records across ${finalVehiclesCount.toLocaleString()} vehicles in ${genDuration.toFixed(2)}s (${Math.round(totalInserted / genDuration).toLocaleString()} rows/s)`);

  const fileStat = fs.statSync(BENCH_DB_PATH);
  console.log(`Database File Size on Disk: ${(fileStat.size / (1024 * 1024)).toFixed(2)} MiB (${(fileStat.size / 1e6).toFixed(2)} MB / ${fileStat.size.toLocaleString()} bytes)\n`);
}

/**
 * 2. Execute HTTP Requests with Microsecond Accuracy
 */
function requestFleetSummary(baseUrl) {
  return new Promise((resolve) => {
    const url = new URL('/v1/fleet/summary', baseUrl);
    const start = process.hrtime.bigint();

    const req = http.get(url, (res) => {
      let body = '';
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => {
        const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
        let json = null;
        try { json = JSON.parse(body); } catch (_) {}
        resolve({
          statusCode: res.statusCode,
          elapsedMs,
          data: json,
          error: null
        });
      });
    });

    req.on('error', (err) => {
      const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
      resolve({
        statusCode: 0,
        elapsedMs,
        data: null,
        error: err.message
      });
    });
  });
}

/**
 * 3. Run Benchmark Suite
 */
async function runBenchmark() {
  await ensureDataset();

  console.log('='.repeat(75));
  console.log(' Starting In-Process Fleet Telemetry Service for Benchmark');
  console.log('='.repeat(75));

  const { server } = require('../backend/src/server');
  const port = server.address().port;
  const baseUrl = `http://127.0.0.1:${port}`;
  console.log(`API Server listening at: ${baseUrl}`);

  // Warm-up Phase
  console.log(`\n[Warm-up Phase] Dispatching ${WARMUP_REQUESTS} warm-up requests to prime V8 JIT & SQLite buffers...`);
  for (let i = 0; i < WARMUP_REQUESTS; i++) {
    const res = await requestFleetSummary(baseUrl);
    if (res.statusCode !== 200) {
      console.warn(`[Warm-up Warning] Request ${i + 1} returned status ${res.statusCode}`);
    }
  }
  console.log('[Warm-up Phase] Completed successfully.\n');

  // Concurrency Benchmark Phase
  console.log('='.repeat(75));
  console.log(` Executing Concurrency Benchmark: ${TOTAL_REQUESTS} Requests (Concurrency: ${CONCURRENCY} VUs)`);
  console.log('='.repeat(75));

  const latencies = [];
  let successfulRequests = 0;
  let failedRequests = 0;
  let samplePayload = null;

  let completed = 0;
  const requestsPerWorker = Math.floor(TOTAL_REQUESTS / CONCURRENCY);

  async function workerTask() {
    for (let r = 0; r < requestsPerWorker; r++) {
      const result = await requestFleetSummary(baseUrl);
      latencies.push(result.elapsedMs);
      if (result.statusCode === 200 && result.data && typeof result.data.total_vehicles === 'number') {
        successfulRequests++;
        if (!samplePayload) samplePayload = result.data;
      } else {
        failedRequests++;
      }
      completed++;
      if (completed % 50 === 0 || completed === TOTAL_REQUESTS) {
        process.stdout.write(`\r  Benchmarked: ${completed} / ${TOTAL_REQUESTS} requests...`);
      }
    }
  }

  const benchStart = process.hrtime.bigint();
  const workers = Array.from({ length: CONCURRENCY }, () => workerTask());
  await Promise.all(workers);
  const benchDurationSec = Number(process.hrtime.bigint() - benchStart) / 1e9;

  console.log('\n\nBenchmark run completed in ' + benchDurationSec.toFixed(3) + ' seconds.');

  // Calculate Statistics
  const p50 = percentile(latencies, 50);
  const p90 = percentile(latencies, 90);
  const p95 = percentile(latencies, 95);
  const p99 = percentile(latencies, 99);
  const min = Math.min(...latencies);
  const max = Math.max(...latencies);
  const avg = latencies.reduce((sum, v) => sum + v, 0) / latencies.length;
  const throughput = Math.round(TOTAL_REQUESTS / benchDurationSec);
  const errorRate = ((failedRequests / TOTAL_REQUESTS) * 100).toFixed(2);

  // SQLite EXPLAIN QUERY PLAN
  const explainSummary = db.prepare(`
    EXPLAIN QUERY PLAN 
    SELECT 
      COUNT(*) AS total_vehicles,
      COALESCE(SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END), 0) AS online_count,
      COALESCE(SUM(CASE WHEN status = 'offline' THEN 1 ELSE 0 END), 0) AS offline_count,
      COALESCE(SUM(CASE WHEN status = 'maintenance' THEN 1 ELSE 0 END), 0) AS maintenance_count,
      COALESCE(AVG(soc_pct), 0) AS avg_soc,
      COALESCE(SUM(received_count), 0) AS total_received,
      COALESCE(SUM(max_seq - min_seq + 1), 0) AS total_expected
    FROM vehicles
  `).all();

  const explainAlerts = db.prepare(`
    EXPLAIN QUERY PLAN 
    SELECT rule, COUNT(*) AS count
    FROM alerts
    WHERE status != 'resolved'
    GROUP BY rule
  `).all();

  // Print Detailed Benchmark Results
  const cpus = os.cpus();
  const dbStat = fs.statSync(BENCH_DB_PATH);

  console.log('='.repeat(75));
  console.log(' OFFICIAL BENCHMARK RESULTS — GET /v1/fleet/summary');
  console.log('='.repeat(75));
  console.log(`Telemetry Rows in History : ${TARGET_READINGS.toLocaleString()}`);
  console.log(`Vehicles in State Table   : ${VEHICLE_COUNT.toLocaleString()}`);
  console.log(`Database Size on Disk     : ${(dbStat.size / (1024 * 1024)).toFixed(2)} MiB (${(dbStat.size / 1e6).toFixed(2)} MB / ${dbStat.size.toLocaleString()} bytes)`);
  console.log(`Hardware Environment     : ${cpus[0]?.model || 'Generic CPU'} (${cpus.length} cores, ${(os.totalmem() / (1024**3)).toFixed(1)} GB RAM)`);
  console.log(`OS & Node.js Version      : ${os.type()} ${os.release()} | Node ${process.version}`);
  console.log(`SQLite Engine Driver      : ${db.engineName || 'better-sqlite3'}`);
  console.log(`Concurrency & Requests    : ${CONCURRENCY} VUs | ${TOTAL_REQUESTS} requests (${WARMUP_REQUESTS} warm-up)`);
  console.log('-'.repeat(75));
  console.log(`Throughput                : ${throughput.toLocaleString()} req/s`);
  console.log(`Total Requests Completed  : ${TOTAL_REQUESTS} (Success: ${successfulRequests}, Fail: ${failedRequests})`);
  console.log(`Error Rate                : ${errorRate}%`);
  console.log(`Min Latency               : ${min.toFixed(2)} ms`);
  console.log(`Average Latency           : ${avg.toFixed(2)} ms`);
  console.log(`p50 Latency               : ${p50.toFixed(2)} ms`);
  console.log(`p90 Latency               : ${p90.toFixed(2)} ms`);
  console.log(`p95 Latency               : ${p95.toFixed(2)} ms`);
  console.log(`p99 Latency               : ${p99.toFixed(2)} ms`);
  console.log(`Max Latency               : ${max.toFixed(2)} ms`);
  console.log('-'.repeat(75));
  console.log('SLA Verification Target   : p95 < 150.0 ms');
  const meetsSla = p95 < 150.0;
  console.log(`SLA Target Status         : ${meetsSla ? 'PASSED (Target Satisfied)' : 'FAILED (Target Exceeded)'}`);
  if (!meetsSla) {
    console.error(`\n[ERROR: SLA BREACH] Fleet Summary p95 response time (${p95.toFixed(2)} ms) exceeded the required 150.0 ms SLA target!`);
  }
  console.log('='.repeat(75));

  console.log('\n[SQLite Execution Plans]');
  console.log(' vehicles table query plan:');
  explainSummary.forEach(row => console.log(`   ${row.detail || JSON.stringify(row)}`));
  console.log(' alerts table query plan:');
  explainAlerts.forEach(row => console.log(`   ${row.detail || JSON.stringify(row)}`));

  console.log('\n[Sample Verified API Payload]');
  console.log(JSON.stringify(samplePayload, null, 2));

  // Graceful Shutdown: stop background workers first
  try {
    const { stopOfflineWorker } = require('../backend/src/offlineWorker');
    stopOfflineWorker();
  } catch (_) {}
  try {
    const { stopStreamWorker } = require('../backend/src/stream');
    stopStreamWorker();
  } catch (_) {}

  server.close(() => {
    try {
      db.close();
    } catch (_) {}
    if (CLEAN_AFTER && fs.existsSync(BENCH_DB_PATH)) {
      try {
        fs.unlinkSync(BENCH_DB_PATH);
        if (fs.existsSync(`${BENCH_DB_PATH}-wal`)) fs.unlinkSync(`${BENCH_DB_PATH}-wal`);
        if (fs.existsSync(`${BENCH_DB_PATH}-shm`)) fs.unlinkSync(`${BENCH_DB_PATH}-shm`);
        console.log(`[Clean] Removed benchmark database file at ${BENCH_DB_PATH}`);
      } catch (_) {}
    }
    process.exit(meetsSla ? 0 : 1);
  });
}

runBenchmark().catch((err) => {
  console.error('[Benchmark Error]', err);
  process.exit(1);
});
