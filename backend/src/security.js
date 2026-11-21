const crypto = require('crypto');
const db = require('./db');
const { trackAuthFailure, trackRateLimitRejection } = require('./observability');

const FIVE_MINUTES_MS = 5 * 60 * 1000;
const CLEANUP_INTERVAL_MS = 60 * 1000; // Clean expired nonces at most once every 60 seconds

const DEV_API_KEY = 'voltgrid-dev-key';
const DEV_API_SECRET = 'voltgrid-dev-secret';

// Prepared statements for api_keys lookup
const getApiKeyStmt = db.prepare('SELECT api_key, secret, rate_limit_per_sec, burst_capacity FROM api_keys WHERE api_key = ?');

// Prepared statements for API key lifecycle and production safeguards
const purgeDevKeysStmt = db.prepare('DELETE FROM api_keys WHERE api_key = ? OR secret = ?');
const ensureDevKeyStmt = db.prepare('INSERT OR IGNORE INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity) VALUES (?, ?, 20, 40)');
const upsertApiKeyStmt = db.prepare(`
  INSERT INTO api_keys (api_key, secret, rate_limit_per_sec, burst_capacity)
  VALUES (?, ?, ?, ?)
  ON CONFLICT(api_key) DO UPDATE SET
    secret = excluded.secret,
    rate_limit_per_sec = excluded.rate_limit_per_sec,
    burst_capacity = excluded.burst_capacity
`);

// Prepared statements for replay nonce handling
const purgeExpiredNoncesStmt = db.prepare('DELETE FROM used_nonces WHERE expires_at < ?');
const insertNonceStmt = db.prepare('INSERT OR IGNORE INTO used_nonces (signature, expires_at) VALUES (?, ?)');

// Prepared statements for distributed rate limiting
const getRateLimitStmt = db.prepare('SELECT tokens, last_refill_ms FROM rate_limits WHERE api_key = ?');
const upsertRateLimitStmt = db.prepare(`
  INSERT INTO rate_limits (api_key, tokens, last_refill_ms)
  VALUES (?, ?, ?)
  ON CONFLICT(api_key) DO UPDATE SET
    tokens = excluded.tokens,
    last_refill_ms = excluded.last_refill_ms
`);

// Atomic transaction to record and verify request signature uniqueness (BEGIN IMMEDIATE)
const recordNonceTx = db.transaction((sig, now) => {
  const expiresAt = now + FIVE_MINUTES_MS;
  const info = insertNonceStmt.run(sig, expiresAt);
  return info.changes > 0;
});

// Periodic, throttled cleanup of expired nonces (does NOT run on every request)
let lastCleanupMs = 0;
function maybeCleanExpiredNonces(now) {
  if (now - lastCleanupMs > CLEANUP_INTERVAL_MS) {
    lastCleanupMs = now;
    try {
      purgeExpiredNoncesStmt.run(now);
    } catch (_) {}
  }
}

// Atomic transaction to check and consume a rate limit token (BEGIN IMMEDIATE)
const consumeTokenTx = db.transaction((apiKey, rateLimitPerSec, burstCapacity, now) => {
  const row = getRateLimitStmt.get(apiKey);

  let tokens = burstCapacity;
  let lastRefill = now;

  if (row) {
    tokens = Number(row.tokens);
    lastRefill = Number(row.last_refill_ms);
  }

  // Refill tokens based on elapsed fractional seconds
  const elapsedSec = Math.max(0, (now - lastRefill) / 1000);
  const refilledTokens = Math.min(burstCapacity, tokens + (elapsedSec * rateLimitPerSec));

  if (refilledTokens < 1) {
    const needed = 1 - refilledTokens;
    const retryAfterSec = Math.max(1, Math.ceil(needed / Math.max(0.001, rateLimitPerSec)));
    return { allowed: false, retryAfterSec };
  }

  // Consume exactly 1 token and preserve non-negative balance
  const updatedTokens = Math.max(0, refilledTokens - 1);
  upsertRateLimitStmt.run(apiKey, updatedTokens, now);

  return { allowed: true };
});

/**
 * Resilient transaction runner that catches transient SQLITE_BUSY contention
 * across multiple API processes and safely retries before bubbling up.
 */
function executeWithBusyRetry(fn, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return fn();
    } catch (err) {
      if ((err.code === 'SQLITE_BUSY' || (err.message && err.message.includes('busy'))) && attempt < maxRetries - 1) {
        const start = Date.now();
        while (Date.now() - start < 10) {} // 10ms jittered spin
        continue;
      }
      throw err;
    }
  }
}

/**
 * Express middleware for HMAC-SHA256 request authentication and replay protection.
 * Requires X-API-Key, X-Timestamp, and X-Signature headers.
 */
function hmacAuth(req, res, next) {
  try {
    const apiKey = req.headers['x-api-key'] || req.headers['X-API-Key'];
    const rawTimestamp = req.headers['x-timestamp'] || req.headers['X-Timestamp'];
    const rawSignature = req.headers['x-signature'] || req.headers['X-Signature'];

    if (!apiKey || !rawTimestamp || !rawSignature) {
      trackAuthFailure();
      return res.status(401).json({
        error: 'Missing required authentication headers (X-API-Key, X-Timestamp, X-Signature)'
      });
    }

    // In production, reject development credentials immediately (defense-in-depth)
    if (process.env.NODE_ENV === 'production' && (apiKey === DEV_API_KEY || rawSignature === DEV_API_SECRET)) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Development credentials are not permitted in production' });
    }

    const apiKeyRow = getApiKeyStmt.get(apiKey);
    if (!apiKeyRow) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Invalid or unknown API key' });
    }

    if (process.env.NODE_ENV === 'production' && apiKeyRow.secret === DEV_API_SECRET) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Development credentials are not permitted in production' });
    }

    // Parse and validate request timestamp: supports epoch ms (13 digits), epoch sec (10 digits), and ISO strings
    let reqTimeMs = NaN;
    if (typeof rawTimestamp === 'string' && rawTimestamp.trim().length > 0) {
      const trimmed = rawTimestamp.trim();
      if (/^\d+$/.test(trimmed)) {
        const num = Number(trimmed);
        reqTimeMs = num < 10000000000 ? num * 1000 : num;
      } else {
        reqTimeMs = Date.parse(trimmed);
      }
    }

    const nowMs = Date.now();
    if (isNaN(reqTimeMs) || Math.abs(nowMs - reqTimeMs) > FIVE_MINUTES_MS) {
      trackAuthFailure();
      return res.status(401).json({
        error: 'Request timestamp is outside the allowed 5-minute window'
      });
    }

    // Verify HMAC-SHA256 signature: HMAC(secret, "${timestamp}.${rawBody}")
    const rawBody = req.rawBody || '';
    const hmac = crypto.createHmac('sha256', apiKeyRow.secret);
    hmac.update(`${rawTimestamp}.${rawBody}`);
    const expectedSig = hmac.digest('hex');

    const expectedBuf = Buffer.from(expectedSig, 'utf8');
    const providedBuf = Buffer.from(String(rawSignature).trim(), 'utf8');

    if (expectedBuf.length !== providedBuf.length || !crypto.timingSafeEqual(expectedBuf, providedBuf)) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Invalid HMAC signature' });
    }

    // Replay protection: Ensure signature has not been seen within validity window
    const cleanSig = String(rawSignature).trim();
    const isNew = executeWithBusyRetry(() => recordNonceTx.immediate(cleanSig, nowMs));
    if (!isNew) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Replayed request signature' });
    }

    // Run throttled cleanup of expired nonces in background (never blocks on every request)
    maybeCleanExpiredNonces(nowMs);

    // Attach sanitized API key information for downstream middleware (never leak secret)
    req.apiKey = {
      api_key: apiKeyRow.api_key,
      rate_limit_per_sec: Number(apiKeyRow.rate_limit_per_sec),
      burst_capacity: Number(apiKeyRow.burst_capacity)
    };

    next();
  } catch (err) {
    console.error('[HMAC Auth Error]', err);
    res.status(500).json({ error: 'Authentication processing failed' });
  }
}

/**
 * Express middleware for distributed per-key token-bucket rate limiting.
 * Synchronizes state via shared SQLite rate_limits table across instances.
 */
function rateLimiter(req, res, next) {
  try {
    const apiKey = req.apiKey?.api_key || (process.env.NODE_ENV === 'production' ? 'unauthenticated' : DEV_API_KEY);
    const rateLimitPerSec = req.apiKey?.rate_limit_per_sec ?? 20;
    const burstCapacity = req.apiKey?.burst_capacity ?? 40;

    const result = executeWithBusyRetry(() =>
      consumeTokenTx.immediate(apiKey, rateLimitPerSec, burstCapacity, Date.now())
    );

    if (!result.allowed) {
      trackRateLimitRejection();
      res.setHeader('Retry-After', String(result.retryAfterSec));
      return res.status(429).json({
        error: 'Rate limit exceeded',
        retry_after_sec: result.retryAfterSec
      });
    }

    next();
  } catch (err) {
    console.error('[RateLimiter Error]', err);
    res.status(500).json({ error: 'Rate limit processing failed' });
  }
}

/**
 * Validates and initializes HMAC credentials based on the execution environment.
 * - Non-production (development / test): ensures default development credentials exist.
 * - Production: enforces secure configuration, validates secret entropy (>=16 chars),
 *   purges development defaults, and throws a fatal configuration error on missing or unsafe keys.
 * 
 * Never logs or exposes raw secrets.
 */
function validateAndInitSecurityCredentials(env = process.env) {
  const isProduction = env.NODE_ENV === 'production';

  if (!isProduction) {
    // In development and test environments, ensure development credentials exist
    ensureDevKeyStmt.run(DEV_API_KEY, DEV_API_SECRET);
    return { isProduction: false, keysLoaded: 1 };
  }

  // PRODUCTION SAFEGUARDS:
  // 1. Purge seeded development credentials from the database immediately
  purgeDevKeysStmt.run(DEV_API_KEY, DEV_API_SECRET);

  // 2. Parse configured production credentials from secure configuration
  const keysToLoad = [];

  if (env.VOLTGRID_API_KEYS) {
    try {
      const parsed = JSON.parse(env.VOLTGRID_API_KEYS);
      if (!Array.isArray(parsed)) {
        throw new Error('VOLTGRID_API_KEYS must be a JSON array of key objects');
      }
      keysToLoad.push(...parsed);
    } catch (err) {
      throw new Error(`[Security Configuration Error] Failed to parse VOLTGRID_API_KEYS JSON: ${err.message}`);
    }
  }

  if (env.API_KEY || env.API_SECRET) {
    if (!env.API_KEY || !env.API_SECRET) {
      throw new Error('[Security Configuration Error] Incomplete production credentials: both API_KEY and API_SECRET must be provided.');
    }
    keysToLoad.push({
      api_key: env.API_KEY,
      secret: env.API_SECRET,
      rate_limit_per_sec: env.API_RATE_LIMIT ? Number(env.API_RATE_LIMIT) : 20,
      burst_capacity: env.API_BURST_CAPACITY ? Number(env.API_BURST_CAPACITY) : 40
    });
  }

  // 3. Reject startup if no production credentials are provided in configuration or database
  if (keysToLoad.length === 0) {
    const existing = db.prepare(`
      SELECT api_key, secret FROM api_keys WHERE api_key != ? AND secret != ?
    `).all(DEV_API_KEY, DEV_API_SECRET);

    if (existing.length === 0) {
      throw new Error(
        '[Security Configuration Error] Production startup rejected: No production HMAC credentials configured. ' +
        'Production must not use development defaults. Supply production credentials via VOLTGRID_API_KEYS or API_KEY and API_SECRET.'
      );
    }

    for (const row of existing) {
      if (!row.secret || row.secret.length < 16) {
        throw new Error(`[Security Configuration Error] Insecure database key "${row.api_key}": secret must be at least 16 characters.`);
      }
    }

    console.log(`[Security] Production HMAC authentication verified with ${existing.length} pre-provisioned database key(s). Development credentials purged.`);
    return { isProduction: true, keysLoaded: existing.length };
  }

  // 4. Validate configured keys against insecure development defaults and length/entropy requirements
  for (const entry of keysToLoad) {
    const key = (entry.api_key || '').trim();
    const secret = (entry.secret || '').trim();

    if (!key) {
      throw new Error('[Security Configuration Error] Production api_key cannot be empty.');
    }
    if (key === DEV_API_KEY) {
      throw new Error('[Security Configuration Error] Insecure production configuration: Cannot use development key ("voltgrid-dev-key") in production.');
    }
    if (!secret) {
      throw new Error(`[Security Configuration Error] Production secret for key "${key}" cannot be empty.`);
    }
    if (secret === DEV_API_SECRET) {
      throw new Error(`[Security Configuration Error] Insecure production configuration: Cannot use development secret ("voltgrid-dev-secret") in production.`);
    }
    if (secret.length < 16) {
      throw new Error(`[Security Configuration Error] Insecure production configuration: Secret for key "${key}" is too short (minimum 16 characters required).`);
    }

    const rate = Number(entry.rate_limit_per_sec) || 20;
    const burst = Number(entry.burst_capacity) || 40;

    upsertApiKeyStmt.run(key, secret, rate, burst);
  }

  // Re-verify that dev credentials remain completely absent
  purgeDevKeysStmt.run(DEV_API_KEY, DEV_API_SECRET);

  console.log(`[Security] Production HMAC authentication initialized with ${keysToLoad.length} configured key(s). Development credentials purged.`);
  return { isProduction: true, keysLoaded: keysToLoad.length };
}

module.exports = {
  DEV_API_KEY,
  DEV_API_SECRET,
  hmacAuth,
  rateLimiter,
  validateAndInitSecurityCredentials,
  consumeTokenTx,
  recordNonceTx,
  maybeCleanExpiredNonces
};
