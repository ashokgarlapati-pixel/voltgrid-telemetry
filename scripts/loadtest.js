#!/usr/bin/env node
'use strict';

/**
 * VoltGrid Fleet Telemetry Service - Native Multi-Endpoint Load Testing Tool
 * 
 * Simulates concurrent virtual users (VUs) executing a realistic mixed operational workload:
 * - Telemetry Batch Ingestion (POST /v1/telemetry/batch with HMAC-SHA256 signatures, duplicates, and malformed readings)
 * - Vehicle Fleet Listing (GET /v1/vehicles with keyset cursor pagination & status filter)
 * - Vehicle History Time-Series (GET /v1/vehicles/:id/history with peak-preserving downsampling)
 * - Fleet Health Summary (GET /v1/fleet/summary aggregating fleet status and active alerts)
 * 
 * Provides configurable concurrency (VUs), duration, warm-up period, and calculates exact
 * p50, p95, p99 latencies, throughput (req/sec), and HTTP status breakdowns.
 */

const crypto = require('crypto');
const http = require('http');

// Configurable Parameters
const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';
const VUS = parseInt(process.env.VUS || '10', 10);
const DURATION_SEC = parseInt(process.env.DURATION_SEC || '10', 10);
const WARMUP_SEC = parseInt(process.env.WARMUP_SEC || '2', 10);
const BATCH_SIZE = Math.min(500, parseInt(process.env.BATCH_SIZE || '50', 10));
const DUPLICATE_RATE = parseFloat(process.env.DUPLICATE_RATE || '0.05');
const MALFORMED_RATE = parseFloat(process.env.MALFORMED_RATE || '0.02');
const API_KEY = process.env.API_KEY || 'voltgrid-dev-key';
const API_SECRET = process.env.API_SECRET || 'voltgrid-dev-secret';

const parsedBase = new URL(BASE_URL);
let HOSTNAME = parsedBase.hostname;
let PORT = parsedBase.port || (parsedBase.protocol === 'https:' ? 443 : 80);
let spawnedServer = null;
let spawnedOfflineWorker = null;
let spawnedStreamWorker = null;

/**
 * Checks if the target server is already reachable
 */
function probeServerHealth(host, port) {
  return new Promise((resolve) => {
    const req = http.get({ hostname: host, port, path: '/health', timeout: 1000 }, (res) => {
      resolve(res.statusCode === 200);
    });
    req.on('error', () => resolve(false));
    req.on('timeout', () => { req.destroy(); resolve(false); });
  });
}

// Global metric collectors
const metrics = {
  totalRequests: 0,
  successfulRequests: 0,
  failedRequests: 0,
  totalReadingsIngested: 0,
  byEndpoint: {
    ingestion: { name: 'POST /v1/telemetry/batch', counts: { success: 0, fail: 0 }, latencies: [] },
    vehicles: { name: 'GET /v1/vehicles', counts: { success: 0, fail: 0 }, latencies: [] },
    history: { name: 'GET /v1/vehicles/:id/history', counts: { success: 0, fail: 0 }, latencies: [] },
    summary: { name: 'GET /v1/fleet/summary', counts: { success: 0, fail: 0 }, latencies: [] }
  },
  allLatencies: []
};

let seqCounter = 1000;
let timestampNonce = Date.now();

/**
 * Generates a realistic telemetry batch payload
 */
function generateBatchPayload(size) {
  const readings = [];
  const now = Date.now();

  for (let i = 0; i < size; i++) {
    const vId = (Math.floor(Math.random() * 2000) + 1);
    const vehicleId = `VG-${String(vId).padStart(5, '0')}`;
    const isDuplicate = Math.random() < DUPLICATE_RATE;
    const isMalformed = Math.random() < MALFORMED_RATE;

    const seq = isDuplicate ? Math.max(1, seqCounter - 5) : (++seqCounter);

    const reading = {
      vehicle_id: isMalformed ? '' : vehicleId,
      seq: isMalformed ? -99 : seq,
      ts: new Date(now - Math.floor(Math.random() * 15000)).toISOString(),
      soc_pct: Number((40 + Math.random() * 55).toFixed(1)),
      battery_temp_c: Number((28 + Math.random() * 25).toFixed(1)),
      speed_kmh: Number((Math.random() * 70).toFixed(1)),
      lat: Number((13.0827 + (Math.random() - 0.5) * 0.05).toFixed(4)),
      lng: Number((80.2707 + (Math.random() - 0.5) * 0.05).toFixed(4)),
      odometer_km: Number((1200 + i * 0.1).toFixed(1)),
      charging: Math.random() < 0.15,
      fault_codes: Math.random() < 0.02 ? ['P0A80'] : []
    };

    readings.push(reading);
  }

  return readings;
}

/**
 * Dispatches an HTTP request and measures exact elapsed latency in ms
 */
function sendRequest(options, postData) {
  return new Promise((resolve) => {
    const startTime = process.hrtime.bigint();

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        const endTime = process.hrtime.bigint();
        const elapsedMs = Number(endTime - startTime) / 1e6;
        resolve({
          statusCode: res.statusCode,
          elapsedMs,
          body
        });
      });
    });

    req.on('error', (err) => {
      const endTime = process.hrtime.bigint();
      const elapsedMs = Number(endTime - startTime) / 1e6;
      resolve({
        statusCode: 0,
        elapsedMs,
        error: err.message
      });
    });

    if (postData) {
      req.write(postData);
    }
    req.end();
  });
}

/**
 * Executes a single task step according to the workload distribution
 */
async function executeWorkloadStep(isWarmup) {
  const roll = Math.random();
  let endpointKey;
  let options;
  let postData = null;

  if (roll < 0.50) {
    // 50% Ingestion batches
    endpointKey = 'ingestion';
    const readings = generateBatchPayload(BATCH_SIZE);
    postData = JSON.stringify(readings);
    const timestamp = (timestampNonce++).toString();
    const signature = crypto.createHmac('sha256', API_SECRET).update(`${timestamp}.${postData}`).digest('hex');

    options = {
      hostname: HOSTNAME,
      port: PORT,
      path: '/v1/telemetry/batch',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Timestamp': timestamp,
        'X-Signature': signature,
        'Content-Length': Buffer.byteLength(postData)
      }
    };
  } else if (roll < 0.75) {
    // 25% Vehicle listing with keyset cursor pagination
    endpointKey = 'vehicles';
    const status = Math.random() < 0.5 ? 'online' : 'offline';
    options = {
      hostname: HOSTNAME,
      port: PORT,
      path: `/v1/vehicles?limit=50&status=${status}`,
      method: 'GET'
    };
  } else if (roll < 0.90) {
    // 15% Vehicle history queries
    endpointKey = 'history';
    const randId = Math.floor(Math.random() * 2000) + 1;
    const vehicleId = `VG-${String(randId).padStart(5, '0')}`;
    options = {
      hostname: HOSTNAME,
      port: PORT,
      path: `/v1/vehicles/${vehicleId}/history?points=200`,
      method: 'GET'
    };
  } else {
    // 10% Fleet summary queries
    endpointKey = 'summary';
    options = {
      hostname: HOSTNAME,
      port: PORT,
      path: '/v1/fleet/summary',
      method: 'GET'
    };
  }

  const result = await sendRequest(options, postData);

  if (isWarmup) {
    return; // Do not record warm-up measurements
  }

  // Record metrics
  metrics.totalRequests++;
  const isSuccess = (result.statusCode === 200 || result.statusCode === 207);

  if (isSuccess) {
    metrics.successfulRequests++;
    metrics.byEndpoint[endpointKey].counts.success++;
    if (endpointKey === 'ingestion') {
      metrics.totalReadingsIngested += BATCH_SIZE;
    }
  } else {
    metrics.failedRequests++;
    metrics.byEndpoint[endpointKey].counts.fail++;
  }

  metrics.byEndpoint[endpointKey].latencies.push(result.elapsedMs);
  metrics.allLatencies.push(result.elapsedMs);
}

/**
 * Runs a single virtual user loop for the specified duration
 */
async function runVirtualUser(vuId, stopTime, isWarmup) {
  while (Date.now() < stopTime) {
    await executeWorkloadStep(isWarmup);
  }
}

/**
 * Calculates percentile from an array of numbers
 */
function calculatePercentile(sortedArray, percentile) {
  if (sortedArray.length === 0) return 0;
  const index = Math.min(
    sortedArray.length - 1,
    Math.max(0, Math.ceil((percentile / 100) * sortedArray.length) - 1)
  );
  return sortedArray[index];
}

/**
 * Main Load Testing Orchestrator
 */
async function runLoadTest() {
  // Check if target server is live
  const isLive = await probeServerHealth(HOSTNAME, PORT);
  if (!isLive) {
    console.log(`[Auto-Start] Server at ${BASE_URL} not reachable. Spawning in-process VoltGrid API server...`);
    process.env.PORT = '0'; // Ephemeral port
    const { server } = require('../backend/src/server');
    const { stopOfflineWorker } = require('../backend/src/offlineWorker');
    const { stopStreamWorker } = require('../backend/src/stream');
    const db = require('../backend/src/db');

    // Elevate rate limits for the load test
    db.prepare('UPDATE api_keys SET rate_limit_per_sec = 100000, burst_capacity = 100000 WHERE api_key = ?').run(API_KEY);
    db.prepare('INSERT OR REPLACE INTO rate_limits (api_key, tokens, last_refill_ms) VALUES (?, 100000, ?)').run(API_KEY, Date.now());

    spawnedServer = server;
    spawnedOfflineWorker = stopOfflineWorker;
    spawnedStreamWorker = stopStreamWorker;

    // Wait for server to bind
    await new Promise((resolve) => {
      if (server.listening) {
        resolve();
      } else {
        server.on('listening', resolve);
      }
    });

    const addr = server.address();
    HOSTNAME = '127.0.0.1';
    PORT = addr.port;
    console.log(`[Auto-Start] Server started and listening on http://${HOSTNAME}:${PORT}`);
  }

  console.log('='.repeat(78));
  console.log('VoltGrid Fleet Telemetry Service — Real-Time Systems Load Testing');
  console.log('='.repeat(78));
  console.log(`Target URL        : http://${HOSTNAME}:${PORT}`);
  console.log(`Virtual Users     : ${VUS} concurrent VUs`);
  console.log(`Warm-Up Period    : ${WARMUP_SEC}s`);
  console.log(`Execution Duration: ${DURATION_SEC}s`);
  console.log(`Batch Size        : ${BATCH_SIZE} readings/batch`);
  console.log(`Workload Mix      : 50% Ingest, 25% List, 15% History, 10% Summary`);
  console.log('='.repeat(78));

  // 1. Warm-up phase
  if (WARMUP_SEC > 0) {
    console.log(`\n[Warm-Up] Running ${WARMUP_SEC}s warm-up with ${VUS} VUs...`);
    const warmupStop = Date.now() + (WARMUP_SEC * 1000);
    const warmupWorkers = [];
    for (let i = 0; i < VUS; i++) {
      warmupWorkers.push(runVirtualUser(i, warmupStop, true));
    }
    await Promise.all(warmupWorkers);
    console.log('[Warm-Up] Completed. Resetting performance counters.');
  }

  // 2. Active load test phase
  console.log(`\n[Load Test] Executing active load test for ${DURATION_SEC}s...`);
  const activeStart = Date.now();
  const activeStop = activeStart + (DURATION_SEC * 1000);

  const workers = [];
  for (let i = 0; i < VUS; i++) {
    workers.push(runVirtualUser(i, activeStop, false));
  }
  await Promise.all(workers);

  const totalDurationSec = (Date.now() - activeStart) / 1000;
  const throughputRps = (metrics.totalRequests / totalDurationSec);
  const ingestionRate = (metrics.totalReadingsIngested / totalDurationSec);

  // Sort latencies
  metrics.allLatencies.sort((a, b) => a - b);
  for (const ep of Object.values(metrics.byEndpoint)) {
    ep.latencies.sort((a, b) => a - b);
  }

  // Print Summary Table
  console.log('\n' + '='.repeat(78));
  console.log('LOAD TEST RESULTS SUMMARY');
  console.log('='.repeat(78));
  console.log(`Total Duration         : ${totalDurationSec.toFixed(2)} seconds`);
  console.log(`Total Requests         : ${metrics.totalRequests}`);
  console.log(`Successful Requests    : ${metrics.successfulRequests} (${((metrics.successfulRequests / (metrics.totalRequests || 1)) * 100).toFixed(2)}%)`);
  console.log(`Failed Requests        : ${metrics.failedRequests} (${((metrics.failedRequests / (metrics.totalRequests || 1)) * 100).toFixed(2)}%)`);
  console.log(`Overall Throughput     : ${throughputRps.toFixed(2)} requests/sec`);
  console.log(`Telemetry Ingestion    : ${metrics.totalReadingsIngested} readings (${ingestionRate.toFixed(0)} readings/sec)`);

  console.log('\n-------------------------- Latency Metrics (ms) ------------------------------');
  console.log('Endpoint                         | Req Count |  p50 (ms) |  p95 (ms) |  p99 (ms) | Max (ms) ');
  console.log('-'.repeat(78));

  for (const [key, ep] of Object.entries(metrics.byEndpoint)) {
    const p50 = calculatePercentile(ep.latencies, 50).toFixed(2);
    const p95 = calculatePercentile(ep.latencies, 95).toFixed(2);
    const p99 = calculatePercentile(ep.latencies, 99).toFixed(2);
    const max = ep.latencies.length > 0 ? ep.latencies[ep.latencies.length - 1].toFixed(2) : '0.00';
    const totalCount = ep.latencies.length;

    console.log(`${ep.name.padEnd(32)} | ${String(totalCount).padStart(9)} | ${p50.padStart(9)} | ${p95.padStart(9)} | ${p99.padStart(9)} | ${max.padStart(8)}`);
  }

  const allP50 = calculatePercentile(metrics.allLatencies, 50).toFixed(2);
  const allP95 = calculatePercentile(metrics.allLatencies, 95).toFixed(2);
  const allP99 = calculatePercentile(metrics.allLatencies, 99).toFixed(2);
  const allMax = metrics.allLatencies.length > 0 ? metrics.allLatencies[metrics.allLatencies.length - 1].toFixed(2) : '0.00';

  console.log('-'.repeat(78));
  console.log(`${'AGGREGATED ALL ENDPOINTS'.padEnd(32)} | ${String(metrics.totalRequests).padStart(9)} | ${allP50.padStart(9)} | ${allP95.padStart(9)} | ${allP99.padStart(9)} | ${allMax.padStart(8)}`);
  console.log('='.repeat(78) + '\n');

  // Clean shutdown of spawned server if any
  if (spawnedServer) {
    console.log('[Auto-Start] Gracefully shutting down in-process test server...');
    if (typeof spawnedOfflineWorker === 'function') spawnedOfflineWorker();
    if (typeof spawnedStreamWorker === 'function') spawnedStreamWorker();
    spawnedServer.close();
  }

  return metrics;
}

// Execute if invoked directly from CLI
if (require.main === module) {
  runLoadTest().catch((err) => {
    console.error('Fatal load test error:', err);
    process.exit(1);
  });
}

module.exports = {
  runLoadTest,
  generateBatchPayload
};
