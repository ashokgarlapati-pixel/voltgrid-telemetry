const crypto = require('crypto');
const db = require('./db');

const FIVE_MINUTES_MS = 5 * 60 * 1000;

// Prepared statements for api_keys lookup
const getApiKeyStmt = db.prepare('SELECT api_key, secret, rate_limit_per_sec, burst_capacity FROM api_keys WHERE api_key = ?');

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

// Atomic transaction to record and verify request nonce
const recordNonceTx = db.transaction((sig, now) => {
  purgeExpiredNoncesStmt.run(now);
  const expiresAt = now + FIVE_MINUTES_MS;
  const info = insertNonceStmt.run(sig, expiresAt);
  return info.changes > 0;
});

// Atomic transaction to check and consume a rate limit token
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
    const retryAfterSec = Math.max(1, Math.ceil((1 - refilledTokens) / rateLimitPerSec));
    return { allowed: false, retryAfterSec };
  }

  // Consume exactly 1 token
  const updatedTokens = refilledTokens - 1;
  upsertRateLimitStmt.run(apiKey, updatedTokens, now);

  return { allowed: true };
});

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
      return res.status(401).json({
        error: 'Missing required authentication headers (X-API-Key, X-Timestamp, X-Signature)'
      });
    }

    const apiKeyRow = getApiKeyStmt.get(apiKey);
    if (!apiKeyRow) {
      return res.status(401).json({ error: 'Invalid or unknown API key' });
    }

    // 5-minute timestamp window check (handles epoch ms or ISO strings)
    const parsedNum = Number(rawTimestamp);
    const reqTimeMs = (!isNaN(parsedNum) && parsedNum > 1000000000) ? parsedNum : Date.parse(rawTimestamp);
    const nowMs = Date.now();

    if (isNaN(reqTimeMs) || Math.abs(nowMs - reqTimeMs) > FIVE_MINUTES_MS) {
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
      return res.status(401).json({ error: 'Invalid HMAC signature' });
    }

    // Replay protection: Ensure signature has not been seen within validity window
    const isNew = recordNonceTx.immediate(String(rawSignature).trim(), nowMs);
    if (!isNew) {
      return res.status(401).json({ error: 'Replayed request signature' });
    }

    // Attach authenticated API key information for downstream middleware
    req.apiKey = apiKeyRow;
    next();
  } catch (err) {
    console.error('[HMAC Auth Error]', err);
    res.status(500).json({ error: 'Authentication processing failed' });
  }
}

/**
 * Express middleware for distributed per-key token-bucket rate limiting.
 * Synchronizes state via SQLite rate_limits table across instances.
 */
function rateLimiter(req, res, next) {
  try {
    const apiKey = req.apiKey?.api_key || 'voltgrid-dev-key';
    const rateLimitPerSec = req.apiKey?.rate_limit_per_sec || 20;
    const burstCapacity = req.apiKey?.burst_capacity || 40;

    const result = consumeTokenTx.immediate(apiKey, rateLimitPerSec, burstCapacity, Date.now());

    if (!result.allowed) {
      res.setHeader('Retry-After', result.retryAfterSec);
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

module.exports = {
  hmacAuth,
  rateLimiter
};
