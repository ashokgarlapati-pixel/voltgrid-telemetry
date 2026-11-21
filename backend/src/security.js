const crypto = require('crypto');
const db = require('./db');
const { trackAuthFailure, trackRateLimitRejection } = require('./observability');

const FIVE_MINUTES_MS = 5 * 60 * 1000;
const CLEANUP_INTERVAL_MS = 60 * 1000; // Clean expired nonces at most once every 60 seconds

const DEV_API_KEY = 'voltgrid-dev-key';
const DEV_API_SECRET = 'voltgrid-dev-secret';

const DEV_PERMITTED_ENVIRONMENTS = new Set(['development', 'test']);

/**
 * Checks whether the current execution environment explicitly permits development credentials.
 * Only 'development' and 'test' (or unset in local developer workflows) are permitted.
 * All deployment environments (staging, production, qa, uat, etc.) or unknown environments
 * strictly require explicitly configured credentials.
 */
function isDevelopmentPermitted(env = process.env) {
  const nodeEnv = env.NODE_ENV !== undefined ? String(env.NODE_ENV).trim().toLowerCase() : 'development';
  return DEV_PERMITTED_ENVIRONMENTS.has(nodeEnv);
}

/**
 * Validates cryptographic secret strength for deployment environments.
 * - Distinguishes minimum structural length from actual cryptographic entropy (unpredictability).
 * - Enforces minimum length baseline (>= 16 characters).
 * - Enforces character diversity / entropy heuristic (>= 8 unique characters) to reject repetitive patterns,
 *   while cleanly accepting legitimate cryptographic formats (hex tokens, base64 strings, alphanumeric hashes).
 * - Rejects known default development secrets.
 * - NEVER prints or logs raw secrets in exception messages, logs, or metrics.
 */
function validateSecretStrength(keyId, secret, envName) {
  if (typeof secret !== 'string') {
    throw new Error(`[Security Configuration Error] Secret for key "${keyId}" must be a string.`);
  }
  const trimmed = secret.trim();
  if (trimmed.length === 0) {
    throw new Error(`[Security Configuration Error] Secret for key "${keyId}" cannot be empty.`);
  }
  if (trimmed === DEV_API_SECRET) {
    throw new Error(`[Security Configuration Error] Insecure configuration in environment "${envName}": Cannot use default development secret ("voltgrid-dev-secret") for key "${keyId}".`);
  }
  if (trimmed.length < 16) {
    throw new Error(
      `[Security Configuration Error] Insecure secret for key "${keyId}": Minimum length of 16 characters required (received ${trimmed.length}). ` +
      `Minimum length is a necessary structural baseline, not a guarantee of entropy.`
    );
  }

  // Entropy & unpredictability check: reject repetitive or low-entropy patterns
  const uniqueChars = new Set(trimmed).size;
  if (uniqueChars < 8) {
    throw new Error(
      `[Security Configuration Error] Insecure secret for key "${keyId}": Insufficient character diversity and entropy (${uniqueChars} unique characters). ` +
      `Secrets must be cryptographically generated or sufficiently unpredictable.`
    );
  }

  return trimmed;
}

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

    // In deployment / non-dev environments, reject development credentials immediately (defense-in-depth)
    const isDevAllowed = isDevelopmentPermitted(process.env);
    if (!isDevAllowed && (apiKey === DEV_API_KEY || rawSignature === DEV_API_SECRET)) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Development credentials are not permitted in this environment' });
    }

    const apiKeyRow = getApiKeyStmt.get(apiKey);
    if (!apiKeyRow) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Invalid or unknown API key' });
    }

    if (!isDevAllowed && apiKeyRow.secret === DEV_API_SECRET) {
      trackAuthFailure();
      return res.status(401).json({ error: 'Development credentials are not permitted in this environment' });
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
    const isDevAllowed = isDevelopmentPermitted(process.env);
    const apiKey = req.apiKey?.api_key || (isDevAllowed ? DEV_API_KEY : 'unauthenticated');
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
 * - Development and test environments: permits default development credentials.
 * - Staging, production, and unknown/unsupported deployment environments:
 *   - Strictly purges development defaults from the database.
 *   - Requires explicitly configured credentials (via VOLTGRID_API_KEYS or API_KEY/API_SECRET, or pre-provisioned in database).
 *   - Rejects development key IDs, development secrets, duplicate key IDs, missing/malformed secrets, and invalid rate limits.
 *   - Distinguishes structural minimum length (>=16 chars) from cryptographic entropy (>=8 unique chars).
 *   - Throws a fatal configuration error on startup if credentials are missing or unsafe.
 * 
 * Never logs or exposes raw secrets in exceptions, logs, or metrics.
 */
function validateAndInitSecurityCredentials(env = process.env) {
  const currentEnv = env.NODE_ENV !== undefined ? String(env.NODE_ENV).trim().toLowerCase() : 'development';
  const isDevAllowed = isDevelopmentPermitted(env);

  if (isDevAllowed) {
    // In development and test environments, ensure development credentials exist
    ensureDevKeyStmt.run(DEV_API_KEY, DEV_API_SECRET);
    return { isProduction: false, environment: currentEnv, keysLoaded: 1 };
  }

  // DEPLOYMENT SAFEGUARDS (production, staging, unknown environments):
  // 1. Purge seeded development credentials from the database immediately
  purgeDevKeysStmt.run(DEV_API_KEY, DEV_API_SECRET);

  // 2. Parse and validate configured credentials
  const keysToLoad = [];
  const seenKeyIds = new Set();

  if (env.VOLTGRID_API_KEYS) {
    let parsed;
    try {
      parsed = JSON.parse(env.VOLTGRID_API_KEYS);
    } catch (err) {
      throw new Error(`[Security Configuration Error] Failed to parse VOLTGRID_API_KEYS JSON: ${err.message}`);
    }

    if (!Array.isArray(parsed)) {
      throw new Error('[Security Configuration Error] VOLTGRID_API_KEYS must be a JSON array of key objects.');
    }

    for (let i = 0; i < parsed.length; i++) {
      const entry = parsed[i];
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
        throw new Error(`[Security Configuration Error] VOLTGRID_API_KEYS entry at index ${i} must be an object with api_key and secret.`);
      }

      if (typeof entry.api_key !== 'string' || entry.api_key.trim().length === 0) {
        throw new Error(`[Security Configuration Error] VOLTGRID_API_KEYS entry at index ${i} is missing a valid non-empty string api_key.`);
      }
      const keyId = entry.api_key.trim();

      if (keyId === DEV_API_KEY) {
        throw new Error(`[Security Configuration Error] Insecure configuration in environment "${currentEnv}": Cannot use development key ("voltgrid-dev-key").`);
      }

      if (seenKeyIds.has(keyId)) {
        throw new Error(`[Security Configuration Error] Duplicate API key ID "${keyId}" in VOLTGRID_API_KEYS configuration.`);
      }
      seenKeyIds.add(keyId);

      // Validate secret strength (entropy + length) without logging the secret
      const validatedSecret = validateSecretStrength(keyId, entry.secret, currentEnv);

      // Validate rate limit configuration
      let rateLimit = 20;
      let burstCap = 40;

      if (entry.rate_limit_per_sec !== undefined) {
        const r = Number(entry.rate_limit_per_sec);
        if (!Number.isFinite(r) || r <= 0) {
          throw new Error(`[Security Configuration Error] Invalid rate_limit_per_sec for key "${keyId}": must be a positive finite number.`);
        }
        rateLimit = r;
      }

      if (entry.burst_capacity !== undefined) {
        const b = Number(entry.burst_capacity);
        if (!Number.isInteger(b) || b <= 0) {
          throw new Error(`[Security Configuration Error] Invalid burst_capacity for key "${keyId}": must be a positive integer.`);
        }
        burstCap = b;
      }

      keysToLoad.push({
        api_key: keyId,
        secret: validatedSecret,
        rate_limit_per_sec: rateLimit,
        burst_capacity: burstCap
      });
    }
  }

  if (env.API_KEY || env.API_SECRET) {
    if (!env.API_KEY || !env.API_SECRET) {
      throw new Error(`[Security Configuration Error] Incomplete credentials in environment "${currentEnv}": both API_KEY and API_SECRET must be provided.`);
    }

    const keyId = String(env.API_KEY).trim();
    if (!keyId) {
      throw new Error('[Security Configuration Error] API_KEY cannot be empty.');
    }
    if (keyId === DEV_API_KEY) {
      throw new Error(`[Security Configuration Error] Insecure configuration in environment "${currentEnv}": Cannot use development key ("voltgrid-dev-key").`);
    }

    if (seenKeyIds.has(keyId)) {
      throw new Error(`[Security Configuration Error] Duplicate API key ID "${keyId}" between API_KEY and VOLTGRID_API_KEYS.`);
    }
    seenKeyIds.add(keyId);

    const validatedSecret = validateSecretStrength(keyId, env.API_SECRET, currentEnv);

    let rateLimit = 20;
    let burstCap = 40;

    if (env.API_RATE_LIMIT !== undefined && env.API_RATE_LIMIT !== '') {
      const r = Number(env.API_RATE_LIMIT);
      if (!Number.isFinite(r) || r <= 0) {
        throw new Error('[Security Configuration Error] Invalid API_RATE_LIMIT: must be a positive finite number.');
      }
      rateLimit = r;
    }

    if (env.API_BURST_CAPACITY !== undefined && env.API_BURST_CAPACITY !== '') {
      const b = Number(env.API_BURST_CAPACITY);
      if (!Number.isInteger(b) || b <= 0) {
        throw new Error('[Security Configuration Error] Invalid API_BURST_CAPACITY: must be a positive integer.');
      }
      burstCap = b;
    }

    keysToLoad.push({
      api_key: keyId,
      secret: validatedSecret,
      rate_limit_per_sec: rateLimit,
      burst_capacity: burstCap
    });
  }

  // 3. Reject startup if no credentials provided in configuration or pre-provisioned in database
  if (keysToLoad.length === 0) {
    const existing = db.prepare(`
      SELECT api_key, secret FROM api_keys WHERE api_key != ? AND secret != ?
    `).all(DEV_API_KEY, DEV_API_SECRET);

    if (existing.length === 0) {
      throw new Error(
        `[Security Configuration Error] Startup rejected in environment "${currentEnv}": No valid HMAC credentials configured. ` +
        `Deployment environments cannot use development defaults. Supply credentials via VOLTGRID_API_KEYS or API_KEY and API_SECRET.`
      );
    }

    for (const row of existing) {
      validateSecretStrength(row.api_key, row.secret, currentEnv);
    }

    console.log(`[Security] HMAC authentication verified for environment "${currentEnv}" with ${existing.length} pre-provisioned database key(s). Development credentials purged.`);
    return { isProduction: currentEnv === 'production', environment: currentEnv, keysLoaded: existing.length };
  }

  // 4. Upsert validated credentials into database
  for (const entry of keysToLoad) {
    upsertApiKeyStmt.run(entry.api_key, entry.secret, entry.rate_limit_per_sec, entry.burst_capacity);
  }

  // Final check to guarantee dev credentials remain purged
  purgeDevKeysStmt.run(DEV_API_KEY, DEV_API_SECRET);

  console.log(`[Security] HMAC authentication initialized for environment "${currentEnv}" with ${keysToLoad.length} configured key(s). Development credentials purged.`);
  return { isProduction: currentEnv === 'production', environment: currentEnv, keysLoaded: keysToLoad.length };
}

module.exports = {
  DEV_API_KEY,
  DEV_API_SECRET,
  DEV_PERMITTED_ENVIRONMENTS,
  isDevelopmentPermitted,
  validateSecretStrength,
  validateAndInitSecurityCredentials,
  hmacAuth,
  rateLimiter,
  consumeTokenTx,
  recordNonceTx,
  maybeCleanExpiredNonces
};
