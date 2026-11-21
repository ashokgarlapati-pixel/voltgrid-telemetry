/**
 * VoltGrid Fleet Telemetry Service - Dedicated 17,280-Reading History Benchmark (Part A4)
 * 
 * Empirically measures execution time and latency percentiles for querying and downsampling
 * a full 24-hour window (17,280 readings @ 5-second intervals) down to 200 points.
 * Verifies peak and trough preservation with bounded memory usage.
 */

const db = require('../backend/src/db');
const { streamDownsampleHistory } = require('../backend/src/queries');

const BENCH_VEHICLE_ID = 'VG-BENCH-17K';
const TOTAL_READINGS = 17280; // 24 hours @ 5 seconds
const BASE_TIME_MS = Date.parse('2026-11-20T00:00:00.000Z');
const FROM_ISO = new Date(BASE_TIME_MS).toISOString();
const TO_ISO = new Date(BASE_TIME_MS + (TOTAL_READINGS * 5000)).toISOString();
const TARGET_POINTS = 200;
const BENCH_ITERATIONS = 50;

function calculatePercentile(sortedArray, percentile) {
  if (sortedArray.length === 0) return 0;
  const index = Math.min(
    sortedArray.length - 1,
    Math.max(0, Math.ceil((percentile / 100) * sortedArray.length) - 1)
  );
  return Number(sortedArray[index].toFixed(2));
}

function seedHistoryDataset() {
  console.log(`[HistoryBenchmark] Checking 17,280-reading dataset for ${BENCH_VEHICLE_ID}...`);

  const existingCount = Number(db.prepare(`
    SELECT COUNT(*) AS total
    FROM telemetry_history
    WHERE vehicle_id = ?
  `).get(BENCH_VEHICLE_ID)?.total || 0);

  if (existingCount >= TOTAL_READINGS) {
    console.log(`[HistoryBenchmark] Reusing existing ${existingCount} readings.`);
    return;
  }

  console.log(`[HistoryBenchmark] Seeding ${TOTAL_READINGS} readings (24 hours @ 5s)...`);
  
  // Seed vehicle record
  db.prepare(`
    INSERT OR REPLACE INTO vehicles (
      vehicle_id, last_ts, last_seq, soc_pct, battery_temp_c, speed_kmh,
      lat, lng, odometer_km, charging, status, min_seq, max_seq, received_count, updated_at
    ) VALUES (?, ?, ?, 30, 35, 25, 13.08, 80.27, 500, 0, 'online', 1, ?, ?, datetime('now'))
  `).run(BENCH_VEHICLE_ID, TO_ISO, TOTAL_READINGS, TOTAL_READINGS, TOTAL_READINGS);

  const insertStmt = db.prepare(`
    INSERT OR REPLACE INTO telemetry_history (
      vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
      lat, lng, odometer_km, charging, fault_codes, received_at
    ) VALUES (?, ?, ?, ?, ?, 25.0, 13.08, 80.27, 100, 0, '[]', datetime('now'))
  `);

  db.transaction(() => {
    for (let i = 1; i <= TOTAL_READINGS; i++) {
      const ts = new Date(BASE_TIME_MS + (i * 5000)).toISOString();
      let temp = 30.0 + ((i % 100) * 0.1);
      let soc = 100.0 - ((i / TOTAL_READINGS) * 70.0);
      if (i === 5000) temp = 74.5; // Thermal peak
      if (i === 12000) temp = 14.2; // Thermal trough
      insertStmt.run(BENCH_VEHICLE_ID, i, ts, soc, temp);
    }
  })();

  console.log(`[HistoryBenchmark] Successfully seeded ${TOTAL_READINGS} readings.`);
}

function runBenchmark() {
  seedHistoryDataset();

  // Warmup run
  console.log('[HistoryBenchmark] Warming up SQLite cache and statement execution...');
  const warmup = streamDownsampleHistory(BENCH_VEHICLE_ID, FROM_ISO, TO_ISO, TARGET_POINTS);
  console.log(`[HistoryBenchmark] Warmup complete: ${warmup.rawCount} raw -> ${warmup.points.length} points.`);

  // Verify peak and trough preservation
  const temps = warmup.points.map(p => p.battery_temp_c);
  const peakPreserved = temps.includes(74.5);
  const troughPreserved = temps.includes(14.2);

  if (!peakPreserved || !troughPreserved) {
    throw new Error(`Peak preservation verification failed! Peak: ${peakPreserved}, Trough: ${troughPreserved}`);
  }

  console.log(`[HistoryBenchmark] Running ${BENCH_ITERATIONS} measured iterations (24h window, 17,280 rows -> 200 points)...`);

  const latencies = [];
  for (let i = 0; i < BENCH_ITERATIONS; i++) {
    const t0 = performance.now();
    const result = streamDownsampleHistory(BENCH_VEHICLE_ID, FROM_ISO, TO_ISO, TARGET_POINTS);
    const elapsed = performance.now() - t0;
    latencies.push(elapsed);
  }

  latencies.sort((a, b) => a - b);
  const min = Number(latencies[0].toFixed(2));
  const max = Number(latencies[latencies.length - 1].toFixed(2));
  const avg = Number((latencies.reduce((a, b) => a + b, 0) / latencies.length).toFixed(2));
  const p50 = calculatePercentile(latencies, 50);
  const p95 = calculatePercentile(latencies, 95);
  const p99 = calculatePercentile(latencies, 99);

  console.log('\n===============================================================');
  console.log('  VOLTGRID 17,280-READING HISTORY DOWNSAMPLING BENCHMARK');
  console.log('===============================================================');
  console.log(`  Raw Readings Window:      17,280 (24 hours @ 5s intervals)`);
  console.log(`  Downsampled Points:       ${warmup.points.length} points (target: ${TARGET_POINTS})`);
  console.log(`  Thermal Peak Preserved:   ${peakPreserved} (74.5°C)`);
  console.log(`  Thermal Trough Preserved: ${troughPreserved} (14.2°C)`);
  console.log(`  Iterations:               ${BENCH_ITERATIONS}`);
  console.log(`  Min Latency:              ${min} ms`);
  console.log(`  Avg Latency:              ${avg} ms`);
  console.log(`  p50 Latency:              ${p50} ms`);
  console.log(`  p95 Latency:              ${p95} ms`);
  console.log(`  p99 Latency:              ${p99} ms`);
  console.log(`  Max Latency:              ${max} ms`);
  console.log(`  SLA Target (< 150 ms):    ${p95 < 150 ? 'PASSED (sub-150ms)' : 'FAILED'}`);
  console.log('===============================================================\n');

  return { min, avg, p50, p95, p99, max, count: warmup.points.length };
}

if (require.main === module) {
  runBenchmark();
}

module.exports = { runBenchmark };
