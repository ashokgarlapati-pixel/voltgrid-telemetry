const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');

const db = require('../src/db');
const {
  DEV_API_KEY,
  DEV_API_SECRET,
  validateAndInitSecurityCredentials
} = require('../src/security');

describe('Part A2 / Issue 15: Production HMAC Credentials & Development Rejection Tests', () => {
  let initialDbKeys;

  before(() => {
    // Save current api_keys state to restore after unit tests
    initialDbKeys = db.prepare('SELECT api_key, secret, rate_limit_per_sec, burst_capacity FROM api_keys').all();
  });

  after(() => {
    // Restore initial api_keys and ensure dev key is back for other test suites
    db.prepare('DELETE FROM api_keys').run();
    const insertStmt = db.prepare('INSERT OR IGNORE INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity) VALUES (?, ?, ?, ?)');
    for (const k of initialDbKeys) {
      insertStmt.run(k.api_key, k.secret, k.rate_limit_per_sec, k.burst_capacity);
    }
    validateAndInitSecurityCredentials({ NODE_ENV: 'test' });
  });

  describe('Unit Validation of validateAndInitSecurityCredentials', () => {
    test('Non-production environments (test/development) permit default development credentials', () => {
      const resTest = validateAndInitSecurityCredentials({ NODE_ENV: 'test' });
      assert.strictEqual(resTest.isProduction, false);

      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.ok(devRow, 'Development key must exist in non-production');
      assert.strictEqual(devRow.secret, DEV_API_SECRET);

      const resDev = validateAndInitSecurityCredentials({ NODE_ENV: 'development' });
      assert.strictEqual(resDev.isProduction, false);
    });

    test('Production startup throws when credentials are missing and database only has dev credentials', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({ NODE_ENV: 'production' });
        },
        (err) => {
          assert.match(err.message, /\[Security Configuration Error\] Production startup rejected: No production HMAC credentials configured/);
          return true;
        }
      );

      // Verify development key was purged from database
      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.strictEqual(devRow, undefined, 'Development key must be purged in production');
    });

    test('Production startup rejects default development key ID', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            API_KEY: 'voltgrid-dev-key',
            API_SECRET: 'a-sufficiently-long-production-secret-token'
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use development key \("voltgrid-dev-key"\) in production/);
          return true;
        }
      );
    });

    test('Production startup rejects default development secret', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            API_KEY: 'prod-fleet-alpha',
            API_SECRET: 'voltgrid-dev-secret'
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use development secret \("voltgrid-dev-secret"\) in production/);
          return true;
        }
      );
    });

    test('Production startup rejects weak or short secrets (< 16 characters)', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            API_KEY: 'prod-fleet-alpha',
            API_SECRET: 'short-secret'
          });
        },
        (err) => {
          assert.match(err.message, /Secret for key "prod-fleet-alpha" is too short \(minimum 16 characters required\)/);
          return true;
        }
      );
    });

    test('Production startup rejects incomplete credentials (API_KEY without API_SECRET or vice versa)', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            API_KEY: 'prod-fleet-alpha'
          });
        },
        (err) => {
          assert.match(err.message, /Incomplete production credentials: both API_KEY and API_SECRET must be provided/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            API_SECRET: 'a-sufficiently-long-production-secret-token'
          });
        },
        (err) => {
          assert.match(err.message, /Incomplete production credentials: both API_KEY and API_SECRET must be provided/);
          return true;
        }
      );
    });

    test('Production startup rejects malformed VOLTGRID_API_KEYS configuration', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: '{ "not": "an array" }'
          });
        },
        (err) => {
          assert.match(err.message, /Failed to parse VOLTGRID_API_KEYS JSON/);
          return true;
        }
      );
    });

    test('Production configuration successfully loads valid API_KEY and API_SECRET and purges dev credentials', () => {
      const prodKey = 'prod-fleet-001';
      const prodSecret = 'super-secure-production-secret-key-32chars';

      const res = validateAndInitSecurityCredentials({
        NODE_ENV: 'production',
        API_KEY: prodKey,
        API_SECRET: prodSecret
      });

      assert.strictEqual(res.isProduction, true);
      assert.strictEqual(res.keysLoaded, 1);

      // Verify dev key is purged
      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.strictEqual(devRow, undefined, 'Development key must not exist');

      // Verify production key is inserted
      const prodRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(prodKey);
      assert.ok(prodRow, 'Production key must be stored in database');
      assert.strictEqual(prodRow.secret, prodSecret);
    });

    test('Production configuration successfully loads multiple keys via VOLTGRID_API_KEYS JSON array', () => {
      const jsonKeys = JSON.stringify([
        {
          api_key: 'fleet-north',
          secret: 'secret-token-for-north-fleet-min-16c',
          rate_limit_per_sec: 50,
          burst_capacity: 100
        },
        {
          api_key: 'fleet-south',
          secret: 'secret-token-for-south-fleet-min-16c',
          rate_limit_per_sec: 30,
          burst_capacity: 60
        }
      ]);

      const res = validateAndInitSecurityCredentials({
        NODE_ENV: 'production',
        VOLTGRID_API_KEYS: jsonKeys
      });

      assert.strictEqual(res.isProduction, true);
      assert.strictEqual(res.keysLoaded, 2);

      const north = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get('fleet-north');
      const south = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get('fleet-south');

      assert.ok(north);
      assert.strictEqual(north.rate_limit_per_sec, 50);
      assert.strictEqual(north.burst_capacity, 100);

      assert.ok(south);
      assert.strictEqual(south.rate_limit_per_sec, 30);
      assert.strictEqual(south.burst_capacity, 60);
    });
  });

  describe('Process-Level Server Startup & Authentication Rejection in Production Mode', () => {
    const isolateDbPath = path.join(os.tmpdir(), `voltgrid-prod-isolate-${crypto.randomUUID()}.db`);

    after(() => {
      try {
        if (fs.existsSync(isolateDbPath)) fs.unlinkSync(isolateDbPath);
        if (fs.existsSync(`${isolateDbPath}-wal`)) fs.unlinkSync(`${isolateDbPath}-wal`);
        if (fs.existsSync(`${isolateDbPath}-shm`)) fs.unlinkSync(`${isolateDbPath}-shm`);
      } catch (_) {}
    });

    test('Server process with NODE_ENV=production fails startup when credentials are not configured', async () => {
      const exitResult = await new Promise((resolve) => {
        const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
          env: {
            ...process.env,
            PORT: '0',
            DB_PATH: isolateDbPath,
            NODE_ENV: 'production',
            API_KEY: '',
            API_SECRET: '',
            VOLTGRID_API_KEYS: ''
          },
          stdio: ['ignore', 'pipe', 'pipe']
        });

        let stderr = '';
        proc.stderr.on('data', chunk => { stderr += chunk.toString(); });
        proc.on('exit', (code, signal) => {
          resolve({ code, signal, stderr });
        });
      });

      assert.notStrictEqual(exitResult.code, 0, 'Process must exit with non-zero failure code');
      assert.match(
        exitResult.stderr,
        /\[Security Configuration Error\] Production startup rejected: No production HMAC credentials configured/,
        'Process stderr must contain clear startup failure reason'
      );
    });

    test('Server process with NODE_ENV=production fails startup when using default development key', async () => {
      const exitResult = await new Promise((resolve) => {
        const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
          env: {
            ...process.env,
            PORT: '0',
            DB_PATH: isolateDbPath,
            NODE_ENV: 'production',
            API_KEY: 'voltgrid-dev-key',
            API_SECRET: 'some-valid-length-secret-32chars'
          },
          stdio: ['ignore', 'pipe', 'pipe']
        });

        let stderr = '';
        proc.stderr.on('data', chunk => { stderr += chunk.toString(); });
        proc.on('exit', (code, signal) => {
          resolve({ code, signal, stderr });
        });
      });

      assert.notStrictEqual(exitResult.code, 0);
      assert.match(exitResult.stderr, /Cannot use development key \("voltgrid-dev-key"\) in production/);
    });

    test('Server process with NODE_ENV=production boots with secure credentials and rejects dev credentials', async () => {
      const PROD_KEY = 'voltgrid-prod-enterprise';
      const PROD_SECRET = 'enterprise-grade-high-entropy-secret-key-32c';

      let serverProc;
      let serverPort;

      const bootResult = await new Promise((resolve, reject) => {
        const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
          env: {
            ...process.env,
            PORT: '0',
            DB_PATH: isolateDbPath,
            NODE_ENV: 'production',
            API_KEY: PROD_KEY,
            API_SECRET: PROD_SECRET
          },
          stdio: ['ignore', 'pipe', 'pipe']
        });

        serverProc = proc;
        let stdout = '';
        let stderr = '';

        proc.stdout.on('data', chunk => {
          stdout += chunk.toString();
          const match = stdout.match(/\[VoltGrid API\] Listening on port (\d+)/);
          if (match) {
            serverPort = parseInt(match[1], 10);
            resolve({ port: serverPort, stdout });
          }
        });

        proc.stderr.on('data', chunk => { stderr += chunk.toString(); });

        proc.on('exit', (code, signal) => {
          if (!serverPort) {
            reject(new Error(`Server exited prematurely code=${code} signal=${signal}\nStderr: ${stderr}\nStdout: ${stdout}`));
          }
        });
      });

      assert.ok(bootResult.port > 0, 'Server must be listening on an assigned port');
      assert.match(
        bootResult.stdout,
        /\[Security\] Production HMAC authentication initialized with 1 configured key\(s\)\. Development credentials purged\./,
        'Server stdout must confirm production key initialization and dev key purge'
      );

      const baseUrl = `http://127.0.0.1:${serverPort}`;

      try {
        const validReading = {
          vehicle_id: 'VG-PROD-001',
          seq: 1,
          ts: new Date().toISOString(),
          soc_pct: 88.5,
          battery_temp_c: 32.0,
          speed_kmh: 45.0,
          lat: 37.7749,
          lng: -122.4194,
          odometer_km: 1500.0,
          charging: false,
          fault_codes: []
        };
        const rawBody = JSON.stringify([validReading]);
        const tsHeader = Date.now().toString();

        // 1. Attempt authentication with default development credentials -> Must fail HTTP 401
        const devSig = crypto.createHmac('sha256', DEV_API_SECRET).update(`${tsHeader}.${rawBody}`).digest('hex');
        const devRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': DEV_API_KEY,
            'X-Timestamp': tsHeader,
            'X-Signature': devSig
          },
          body: rawBody
        });

        assert.strictEqual(devRes.status, 401, 'Development key in production must return HTTP 401');
        const devBody = await devRes.json();
        assert.ok(
          devBody.error.includes('Development credentials are not permitted in production') ||
          devBody.error.includes('Invalid or unknown API key'),
          `Error response must indicate invalid/rejected dev credentials (received: ${devBody.error})`
        );

        // 2. Authenticate with properly configured production credentials -> Must succeed HTTP 200
        const prodSig = crypto.createHmac('sha256', PROD_SECRET).update(`${tsHeader}.${rawBody}`).digest('hex');
        const prodRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': PROD_KEY,
            'X-Timestamp': tsHeader,
            'X-Signature': prodSig
          },
          body: rawBody
        });

        assert.strictEqual(prodRes.status, 200, 'Production credentials must authenticate successfully');
        const prodData = await prodRes.json();
        assert.strictEqual(prodData.summary.accepted, 1);

        // 3. Attempt with production key but incorrect secret -> Must fail HTTP 401 Invalid HMAC signature
        const wrongSig = crypto.createHmac('sha256', 'wrong-secret-random-32-chars-key').update(`${tsHeader}.${rawBody}`).digest('hex');
        const wrongRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': PROD_KEY,
            'X-Timestamp': tsHeader,
            'X-Signature': wrongSig
          },
          body: rawBody
        });

        assert.strictEqual(wrongRes.status, 401);
        const wrongBody = await wrongRes.json();
        assert.strictEqual(wrongBody.error, 'Invalid HMAC signature');
      } finally {
        // Cleanly kill spawned server
        serverProc.kill('SIGTERM');
      }
    });
  });
});
