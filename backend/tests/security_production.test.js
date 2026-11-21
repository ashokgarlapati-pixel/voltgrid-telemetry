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
  validateSecretStrength,
  validateAndInitSecurityCredentials
} = require('../src/security');

describe('Part A2 / Issue 15: Production & Staging Credential Security Tests', () => {
  let initialDbKeys;

  before(() => {
    // Save current api_keys state to restore after unit tests
    initialDbKeys = db.prepare('SELECT api_key, secret, rate_limit_per_sec, burst_capacity FROM api_keys').all();
  });

  after(() => {
    // Restore initial api_keys and ensure dev key is restored for other test suites
    db.prepare('DELETE FROM api_keys').run();
    const insertStmt = db.prepare('INSERT OR IGNORE INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity) VALUES (?, ?, ?, ?)');
    for (const k of initialDbKeys) {
      insertStmt.run(k.api_key, k.secret, k.rate_limit_per_sec, k.burst_capacity);
    }
    validateAndInitSecurityCredentials({ NODE_ENV: 'test' });
  });

  describe('1. Environment-Specific Credential Handling', () => {
    test('Development and test environments permit default development credentials', () => {
      const resTest = validateAndInitSecurityCredentials({ NODE_ENV: 'test' });
      assert.strictEqual(resTest.isProduction, false);

      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.ok(devRow, 'Development key must exist in non-production');
      assert.strictEqual(devRow.secret, DEV_API_SECRET);

      const resDev = validateAndInitSecurityCredentials({ NODE_ENV: 'development' });
      assert.strictEqual(resDev.isProduction, false);

      // Unset/empty NODE_ENV defaults safely to local development workflow
      const resUnset = validateAndInitSecurityCredentials({});
      assert.strictEqual(resUnset.isProduction, false);
    });

    test('Production environment rejects missing credentials and purges dev defaults', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({ NODE_ENV: 'production' });
        },
        (err) => {
          assert.match(err.message, /Startup rejected in environment "production": No valid HMAC credentials configured/);
          return true;
        }
      );

      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.strictEqual(devRow, undefined, 'Development key must be purged in production');
    });

    test('Staging environment rejects missing credentials and purges dev defaults', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({ NODE_ENV: 'staging' });
        },
        (err) => {
          assert.match(err.message, /Startup rejected in environment "staging": No valid HMAC credentials configured/);
          return true;
        }
      );

      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.strictEqual(devRow, undefined, 'Development key must be purged in staging');
    });

    test('Staging environment rejects default development key ID and secret', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'staging',
            API_KEY: 'voltgrid-dev-key',
            API_SECRET: crypto.randomBytes(32).toString('hex')
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use development key \("voltgrid-dev-key"\)/);
          assert.match(err.message, /environment "staging"/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'staging',
            API_KEY: 'staging-fleet-ops',
            API_SECRET: 'voltgrid-dev-secret'
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use default development secret \("voltgrid-dev-secret"\)/);
          return true;
        }
      );
    });

    test('Unknown / unsupported deployment environment rejects missing credentials and dev defaults', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({ NODE_ENV: 'unknown_env_42' });
        },
        (err) => {
          assert.match(err.message, /Startup rejected in environment "unknown_env_42": No valid HMAC credentials configured/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'custom_cluster',
            API_KEY: 'voltgrid-dev-key',
            API_SECRET: crypto.randomBytes(32).toString('hex')
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use development key \("voltgrid-dev-key"\)/);
          assert.match(err.message, /environment "custom_cluster"/);
          return true;
        }
      );
    });

    test('Staging environment boots successfully with explicitly configured credentials', () => {
      const stagingKey = 'staging-fleet-001';
      const stagingSecret = crypto.randomBytes(32).toString('hex');

      const res = validateAndInitSecurityCredentials({
        NODE_ENV: 'staging',
        API_KEY: stagingKey,
        API_SECRET: stagingSecret
      });

      assert.strictEqual(res.environment, 'staging');
      assert.strictEqual(res.isProduction, false);
      assert.strictEqual(res.keysLoaded, 1);

      const devRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(DEV_API_KEY);
      assert.strictEqual(devRow, undefined, 'Dev key must remain purged in staging');

      const keyRow = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get(stagingKey);
      assert.ok(keyRow);
      assert.strictEqual(keyRow.secret, stagingSecret);
    });
  });

  describe('2. Secret Strength & Entropy Validation', () => {
    test('Rejects secrets shorter than 16 characters (structural baseline failure)', () => {
      assert.throws(
        () => {
          validateSecretStrength('test-key', 'short123456789', 'production');
        },
        (err) => {
          assert.match(err.message, /Minimum length of 16 characters required/);
          assert.match(err.message, /Minimum length is a necessary structural baseline, not a guarantee of entropy/);
          // Crucial: verify raw secret is not leaked in the error
          assert.strictEqual(err.message.includes('short123456789'), false);
          return true;
        }
      );
    });

    test('Rejects 16+ character secrets with low character diversity / low entropy (e.g. repetitive patterns)', () => {
      // 16 characters but only 1 unique character
      assert.throws(
        () => {
          validateSecretStrength('test-key', 'aaaaaaaaaaaaaaaa', 'production');
        },
        (err) => {
          assert.match(err.message, /Insufficient character diversity and entropy \(1 unique characters\)/);
          assert.strictEqual(err.message.includes('aaaaaaaaaaaaaaaa'), false);
          return true;
        }
      );

      // 16 characters but only 2 unique characters
      assert.throws(
        () => {
          validateSecretStrength('test-key', 'abababababababab', 'production');
        },
        (err) => {
          assert.match(err.message, /Insufficient character diversity and entropy \(2 unique characters\)/);
          assert.strictEqual(err.message.includes('abababababababab'), false);
          return true;
        }
      );
    });

    test('Accepts legitimate cryptographically generated hex secrets (e.g. 32-byte hex)', () => {
      const cryptoHex = crypto.randomBytes(32).toString('hex');
      const validated = validateSecretStrength('test-key', cryptoHex, 'production');
      assert.strictEqual(validated, cryptoHex);
    });

    test('Accepts legitimate base64 secrets (e.g. 32-byte base64)', () => {
      const cryptoBase64 = crypto.randomBytes(32).toString('base64');
      const validated = validateSecretStrength('test-key', cryptoBase64, 'production');
      assert.strictEqual(validated, cryptoBase64);
    });

    test('Accepts strong unpredictable alphanumeric passphrases with sufficient entropy', () => {
      const strongSecret = 'K9#mQ2$vL8!pZ4@wR6^tY1*bN7&c';
      const validated = validateSecretStrength('test-key', strongSecret, 'production');
      assert.strictEqual(validated, strongSecret);
    });

    test('Rejects default development secret regardless of environment', () => {
      assert.throws(
        () => {
          validateSecretStrength('test-key', DEV_API_SECRET, 'production');
        },
        (err) => {
          assert.match(err.message, /Cannot use default development secret/);
          return true;
        }
      );
    });
  });

  describe('3. Credential Array Validation (VOLTGRID_API_KEYS)', () => {
    test('Rejects development key ID in VOLTGRID_API_KEYS array', () => {
      const secret = crypto.randomBytes(32).toString('hex');
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'voltgrid-dev-key', secret }])
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use development key \("voltgrid-dev-key"\)/);
          return true;
        }
      );
    });

    test('Rejects development secret in VOLTGRID_API_KEYS array', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'prod-fleet-alpha', secret: 'voltgrid-dev-secret' }])
          });
        },
        (err) => {
          assert.match(err.message, /Cannot use default development secret/);
          return true;
        }
      );
    });

    test('Rejects duplicate key IDs in VOLTGRID_API_KEYS array', () => {
      const sec1 = crypto.randomBytes(32).toString('hex');
      const sec2 = crypto.randomBytes(32).toString('hex');
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([
              { api_key: 'duplicate-key-id', secret: sec1 },
              { api_key: 'duplicate-key-id', secret: sec2 }
            ])
          });
        },
        (err) => {
          assert.match(err.message, /Duplicate API key ID "duplicate-key-id" in VOLTGRID_API_KEYS configuration/);
          return true;
        }
      );
    });

    test('Rejects duplicate key ID across API_KEY and VOLTGRID_API_KEYS', () => {
      const sec1 = crypto.randomBytes(32).toString('hex');
      const sec2 = crypto.randomBytes(32).toString('hex');
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            API_KEY: 'shared-fleet-id',
            API_SECRET: sec1,
            VOLTGRID_API_KEYS: JSON.stringify([
              { api_key: 'shared-fleet-id', secret: sec2 }
            ])
          });
        },
        (err) => {
          assert.match(err.message, /Duplicate API key ID "shared-fleet-id" between API_KEY and VOLTGRID_API_KEYS/);
          return true;
        }
      );
    });

    test('Rejects missing or non-string secret in VOLTGRID_API_KEYS', () => {
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'valid-id' }])
          });
        },
        (err) => {
          assert.match(err.message, /Secret for key "valid-id" must be a string/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'valid-id', secret: 1234567890123456 }])
          });
        },
        (err) => {
          assert.match(err.message, /Secret for key "valid-id" must be a string/);
          return true;
        }
      );
    });

    test('Rejects missing or empty api_key in VOLTGRID_API_KEYS', () => {
      const sec = crypto.randomBytes(32).toString('hex');
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ secret: sec }])
          });
        },
        (err) => {
          assert.match(err.message, /missing a valid non-empty string api_key/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: '   ', secret: sec }])
          });
        },
        (err) => {
          assert.match(err.message, /missing a valid non-empty string api_key/);
          return true;
        }
      );
    });

    test('Rejects invalid rate_limit_per_sec configuration', () => {
      const sec = crypto.randomBytes(32).toString('hex');
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'fleet-a', secret: sec, rate_limit_per_sec: -10 }])
          });
        },
        (err) => {
          assert.match(err.message, /Invalid rate_limit_per_sec for key "fleet-a": must be a positive finite number/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'fleet-a', secret: sec, rate_limit_per_sec: 0 }])
          });
        },
        (err) => {
          assert.match(err.message, /Invalid rate_limit_per_sec for key "fleet-a": must be a positive finite number/);
          return true;
        }
      );
    });

    test('Rejects invalid burst_capacity configuration', () => {
      const sec = crypto.randomBytes(32).toString('hex');
      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'fleet-b', secret: sec, burst_capacity: -5 }])
          });
        },
        (err) => {
          assert.match(err.message, /Invalid burst_capacity for key "fleet-b": must be a positive integer/);
          return true;
        }
      );

      assert.throws(
        () => {
          validateAndInitSecurityCredentials({
            NODE_ENV: 'production',
            VOLTGRID_API_KEYS: JSON.stringify([{ api_key: 'fleet-b', secret: sec, burst_capacity: 12.5 }])
          });
        },
        (err) => {
          assert.match(err.message, /Invalid burst_capacity for key "fleet-b": must be a positive integer/);
          return true;
        }
      );
    });

    test('Accepts valid VOLTGRID_API_KEYS array with multiple unique device groups', () => {
      const secEast = crypto.randomBytes(32).toString('hex');
      const secWest = crypto.randomBytes(32).toString('hex');

      const res = validateAndInitSecurityCredentials({
        NODE_ENV: 'production',
        VOLTGRID_API_KEYS: JSON.stringify([
          { api_key: 'fleet-east-ops', secret: secEast, rate_limit_per_sec: 50, burst_capacity: 100 },
          { api_key: 'fleet-west-ops', secret: secWest, rate_limit_per_sec: 30, burst_capacity: 60 }
        ])
      });

      assert.strictEqual(res.keysLoaded, 2);

      const east = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get('fleet-east-ops');
      const west = db.prepare('SELECT * FROM api_keys WHERE api_key = ?').get('fleet-west-ops');

      assert.ok(east);
      assert.strictEqual(east.secret, secEast);
      assert.strictEqual(east.rate_limit_per_sec, 50);
      assert.strictEqual(east.burst_capacity, 100);

      assert.ok(west);
      assert.strictEqual(west.secret, secWest);
      assert.strictEqual(west.rate_limit_per_sec, 30);
      assert.strictEqual(west.burst_capacity, 60);
    });
  });

  describe('4. Process-Level Server Startup & Authentication Rejection across Environments', () => {
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
      assert.match(exitResult.stderr, /Startup rejected in environment "production": No valid HMAC credentials configured/);
    });

    test('Server process with NODE_ENV=staging fails startup when credentials are not configured', async () => {
      const exitResult = await new Promise((resolve) => {
        const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
          env: {
            ...process.env,
            PORT: '0',
            DB_PATH: isolateDbPath,
            NODE_ENV: 'staging',
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

      assert.notStrictEqual(exitResult.code, 0, 'Staging process without credentials must fail startup');
      assert.match(exitResult.stderr, /Startup rejected in environment "staging": No valid HMAC credentials configured/);
    });

    test('Server process with unknown NODE_ENV value fails startup when credentials are not configured', async () => {
      const exitResult = await new Promise((resolve) => {
        const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
          env: {
            ...process.env,
            PORT: '0',
            DB_PATH: isolateDbPath,
            NODE_ENV: 'unknown_cloud_region',
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

      assert.notStrictEqual(exitResult.code, 0, 'Unknown environment process without credentials must fail startup');
      assert.match(exitResult.stderr, /Startup rejected in environment "unknown_cloud_region": No valid HMAC credentials configured/);
    });

    test('Server process in staging boots with secure credentials and rejects dev credentials at runtime', async () => {
      const STAGING_KEY = 'voltgrid-staging-enterprise';
      const STAGING_SECRET = crypto.randomBytes(32).toString('hex');

      let serverProc;
      let serverPort;

      const bootResult = await new Promise((resolve, reject) => {
        const proc = spawn(process.execPath, [path.join(__dirname, '../src/server.js')], {
          env: {
            ...process.env,
            PORT: '0',
            DB_PATH: isolateDbPath,
            NODE_ENV: 'staging',
            API_KEY: STAGING_KEY,
            API_SECRET: STAGING_SECRET
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
        /\[Security\] HMAC authentication initialized for environment "staging" with 1 configured key\(s\)\. Development credentials purged\./,
        'Server stdout must confirm staging key initialization and dev key purge'
      );

      const baseUrl = `http://127.0.0.1:${serverPort}`;

      try {
        const validReading = {
          vehicle_id: 'VG-STAGE-001',
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

        // 1. Attempt authentication with default development credentials in staging -> Must fail HTTP 401
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

        assert.strictEqual(devRes.status, 401, 'Development key in staging must return HTTP 401');
        const devBody = await devRes.json();
        assert.ok(
          devBody.error.includes('Development credentials are not permitted in this environment') ||
          devBody.error.includes('Invalid or unknown API key'),
          `Error response must indicate invalid/rejected dev credentials (received: ${devBody.error})`
        );

        // 2. Authenticate with properly configured staging credentials -> Must succeed HTTP 200
        const stagingSig = crypto.createHmac('sha256', STAGING_SECRET).update(`${tsHeader}.${rawBody}`).digest('hex');
        const stageRes = await fetch(`${baseUrl}/v1/telemetry/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': STAGING_KEY,
            'X-Timestamp': tsHeader,
            'X-Signature': stagingSig
          },
          body: rawBody
        });

        assert.strictEqual(stageRes.status, 200, 'Staging credentials must authenticate successfully');
        const stageData = await stageRes.json();
        assert.strictEqual(stageData.summary.accepted, 1);
      } finally {
        serverProc.kill('SIGTERM');
      }
    });
  });
});
