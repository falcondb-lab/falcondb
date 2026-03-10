use std::sync::atomic::AtomicU64;
use std::sync::OnceLock;
use std::time::Instant;

static DEFAULT_TABLE_ENGINE: OnceLock<String> = OnceLock::new();
static NODE_ROLE: OnceLock<String> = OnceLock::new();
static SERVER_START_INSTANT: OnceLock<Instant> = OnceLock::new();
static SERVER_START_EPOCH_MS: OnceLock<i64> = OnceLock::new();
static FALCON_EDITION: OnceLock<String> = OnceLock::new();

/// Global database-level statistics (pg_stat_database compatible).
pub struct DbStats {
    pub xact_commit: AtomicU64,
    pub xact_rollback: AtomicU64,
    pub tup_returned: AtomicU64,
    pub tup_fetched: AtomicU64,
    pub tup_inserted: AtomicU64,
    pub tup_updated: AtomicU64,
    pub tup_deleted: AtomicU64,
    pub blks_read: AtomicU64,
    pub blks_hit: AtomicU64,
    pub temp_files: AtomicU64,
    pub temp_bytes: AtomicU64,
    pub deadlocks: AtomicU64,
    pub checksum_failures: AtomicU64,
    pub sessions: AtomicU64,
    pub sessions_fatal: AtomicU64,
    pub sessions_killed: AtomicU64,
    pub active_time_us: AtomicU64,
    pub idle_in_txn_time_us: AtomicU64,
    pub blk_read_time_us: AtomicU64,
    pub blk_write_time_us: AtomicU64,
}

static DB_STATS: DbStats = DbStats {
    xact_commit: AtomicU64::new(0),
    xact_rollback: AtomicU64::new(0),
    tup_returned: AtomicU64::new(0),
    tup_fetched: AtomicU64::new(0),
    tup_inserted: AtomicU64::new(0),
    tup_updated: AtomicU64::new(0),
    tup_deleted: AtomicU64::new(0),
    blks_read: AtomicU64::new(0),
    blks_hit: AtomicU64::new(0),
    temp_files: AtomicU64::new(0),
    temp_bytes: AtomicU64::new(0),
    deadlocks: AtomicU64::new(0),
    checksum_failures: AtomicU64::new(0),
    sessions: AtomicU64::new(0),
    sessions_fatal: AtomicU64::new(0),
    sessions_killed: AtomicU64::new(0),
    active_time_us: AtomicU64::new(0),
    idle_in_txn_time_us: AtomicU64::new(0),
    blk_read_time_us: AtomicU64::new(0),
    blk_write_time_us: AtomicU64::new(0),
};

pub fn set_default_table_engine(engine: String) {
    let _ = DEFAULT_TABLE_ENGINE.set(engine);
}

pub fn default_table_engine() -> &'static str {
    DEFAULT_TABLE_ENGINE
        .get()
        .map(|s| s.as_str())
        .unwrap_or("rowstore")
}

pub fn set_falcon_edition(edition: String) {
    let _ = FALCON_EDITION.set(edition);
}

/// Returns the product edition name: "Community", "Standard", "Enterprise", "Analytics".
pub fn falcon_edition() -> &'static str {
    FALCON_EDITION
        .get()
        .map(|s| s.as_str())
        .unwrap_or("Community")
}

pub fn set_node_role(role: String) {
    let _ = NODE_ROLE.set(role);
}

pub fn node_role() -> &'static str {
    NODE_ROLE.get().map(|s| s.as_str()).unwrap_or("standalone")
}

// ── Server start time ────────────────────────────────────────────

pub fn init_server_start_time() {
    let _ = SERVER_START_INSTANT.set(Instant::now());
    let epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let _ = SERVER_START_EPOCH_MS.set(epoch_ms);
}

pub fn server_uptime_secs() -> f64 {
    SERVER_START_INSTANT
        .get()
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(0.0)
}

pub fn server_start_epoch_ms() -> i64 {
    SERVER_START_EPOCH_MS.get().copied().unwrap_or(0)
}

// ── Database stats ───────────────────────────────────────────────

pub fn db_stats() -> &'static DbStats {
    &DB_STATS
}
