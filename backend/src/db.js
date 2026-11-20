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

// Run database migrations automatically in sorted alphabetical order
function runMigrations() {
  const migrationsDir = path.resolve(__dirname, '../migrations');
  if (fs.existsSync(migrationsDir)) {
    const sqlFiles = fs.readdirSync(migrationsDir)
      .filter(file => file.endsWith('.sql'))
      .sort();

    for (const file of sqlFiles) {
      const filePath = path.join(migrationsDir, file);
      const sql = fs.readFileSync(filePath, 'utf8');
      db.exec(sql);
      console.log(`[DB] Applied migration: ${file}`);
    }
  } else {
    console.warn(`[DB] Warning: Migrations directory not found at ${migrationsDir}`);
  }
}

runMigrations();

console.log(`[DB] SQLite database ready at ${dbPath} [Engine: ${engineName}] (WAL mode, busy_timeout=5000ms)`);

module.exports = db;
