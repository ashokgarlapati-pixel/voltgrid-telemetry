const fs = require('fs');
const path = require('path');

// Determine database file path from environment or default to local data directory
const dbPath = process.env.DB_PATH || path.resolve(__dirname, '../../data/voltgrid.db');

// Ensure target directory exists before opening SQLite database
const dbDir = path.dirname(dbPath);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

let db;
let engineName = 'better-sqlite3';

try {
  // Primary engine: better-sqlite3 (standard for Docker / Linux LTS)
  const Database = require('better-sqlite3');
  db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('synchronous = NORMAL');
} catch (err) {
  // Resilient fallback: Node.js built-in SQLite (allows running directly on Windows without Docker/C++ toolchain)
  const { DatabaseSync } = require('node:sqlite');
  engineName = 'node:sqlite (built-in fallback)';
  db = new DatabaseSync(dbPath);
  db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA busy_timeout = 5000;');
  db.exec('PRAGMA synchronous = NORMAL;');

  // Polyfill transaction with immediate locking for node:sqlite
  db.transaction = function (fn) {
    const runner = function (...args) {
      db.exec('BEGIN IMMEDIATE');
      try {
        const result = fn(...args);
        db.exec('COMMIT');
        return result;
      } catch (e) {
        db.exec('ROLLBACK');
        throw e;
      }
    };
    runner.immediate = runner;
    runner.deferred = runner;
    runner.exclusive = runner;
    return runner;
  };

  db.pragma = function (sql) {
    const statement = sql.trim().toUpperCase().startsWith('PRAGMA') ? sql : `PRAGMA ${sql}`;
    db.exec(statement);
  };
}

// Run database migrations automatically in sorted alphabetical order with schema_migrations tracking
function runMigrations() {
  const migrationsDir = path.resolve(__dirname, '../migrations');
  if (!fs.existsSync(migrationsDir)) {
    console.warn(`[DB] Warning: Migrations directory not found at ${migrationsDir}`);
    return;
  }

  // 1. Ensure migration tracking table exists
  db.exec(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY,
      applied_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  // 2. Query already applied migration versions
  const rows = db.prepare('SELECT version FROM schema_migrations').all();
  const applied = new Set(rows.map(r => r.version));

  // 3. Scan and execute pending migrations in alphabetical order
  const sqlFiles = fs.readdirSync(migrationsDir)
    .filter(file => file.endsWith('.sql'))
    .sort();

  for (const file of sqlFiles) {
    if (applied.has(file)) {
      continue;
    }

    const filePath = path.join(migrationsDir, file);
    const sql = fs.readFileSync(filePath, 'utf8');

    // Execute migration inside an atomic block
    db.exec('BEGIN IMMEDIATE;');
    try {
      db.exec(sql);
      db.prepare("INSERT INTO schema_migrations (version, applied_at) VALUES (?, datetime('now'))").run(file);
      db.exec('COMMIT;');
      console.log(`[DB] Applied migration: ${file}`);
    } catch (err) {
      db.exec('ROLLBACK;');
      console.error(`[DB] Migration failed on ${file}:`, err);
      throw err;
    }
  }
}

runMigrations();

console.log(`[DB] SQLite database ready at ${dbPath} [Engine: ${engineName}] (WAL mode, busy_timeout=5000ms)`);

module.exports = db;
