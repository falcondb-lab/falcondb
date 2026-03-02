use serde::{Deserialize, Serialize};

/// Current config schema version. Bump when config format changes.
pub const CURRENT_CONFIG_VERSION: u32 = 5;

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FalconConfig {
    /// Config schema version. Used for automated migration on upgrades.
    #[serde(default = "default_config_version")]
    pub config_version: u32,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub spill: SpillConfig,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub gc: GcSectionConfig,
    #[serde(default)]
    pub ustm: UstmSectionConfig,
    /// v1.0.8: Compression profile (off / balanced / aggressive).
    #[serde(default = "default_compression_profile")]
    pub compression_profile: String,
    /// v1.0.8: WAL backend mode (auto / posix / win_async / raw_experimental).
    #[serde(default = "default_wal_mode")]
    pub wal_mode: String,
    /// v1.0.8: Gateway configuration.
    #[serde(default)]
    pub gateway: GatewayConfig,
    /// v1.2.0: Production safety mode. When enabled, startup validates that
    /// the configuration meets minimum safety requirements for production use.
    #[serde(default)]
    pub production_safety: ProductionSafetyConfig,
    /// v1.2.0: Change Data Capture (CDC) configuration.
    #[serde(default)]
    pub cdc: CdcConfig,
    /// v1.2.0: Point-in-Time Recovery (PITR) / WAL archiving configuration.
    #[serde(default)]
    pub pitr: PitrConfig,
    /// v1.2.0: Multi-tenant isolation and metering configuration.
    #[serde(default)]
    pub multi_tenant: MultiTenantConfig,
    /// v1.2.0: Transparent Data Encryption (TDE) configuration.
    #[serde(default)]
    pub tde: TdeConfig,
    /// Raft consensus configuration.
    /// Active when `replication.role = "raft_member"`.
    #[serde(default)]
    pub raft: RaftConfig,
    /// v1.2.1: Automatic shard rebalancing configuration.
    #[serde(default)]
    pub rebalance: RebalanceSectionConfig,
}

const fn default_config_version() -> u32 { CURRENT_CONFIG_VERSION }

/// Automatic shard rebalancing configuration section in falcon.toml.
///
/// When enabled, a background task periodically evaluates shard loads and
/// migrates rows in small non-blocking batches to keep shards balanced.
///
/// Example:
/// ```toml
/// [rebalance]
/// enabled = true
/// check_interval_ms = 30000
/// imbalance_threshold = 1.25
/// batch_size = 512
/// min_donor_rows = 100
/// cooldown_ms = 5000
/// start_paused = false
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceSectionConfig {
    /// Enable automatic shard rebalancing (default: false).
    #[serde(default)]
    pub enabled: bool,
    /// How often (ms) the rebalancer evaluates shard loads (default: 30000 = 30s).
    #[serde(default = "default_rebalance_check_interval_ms")]
    pub check_interval_ms: u64,
    /// Trigger rebalancing when max_shard_rows > threshold × avg_shard_rows.
    /// E.g. 1.25 means 25% above average triggers a move. Default: 1.25.
    #[serde(default = "default_rebalance_imbalance_threshold")]
    pub imbalance_threshold: f64,
    /// Number of rows to migrate per batch (default: 512).
    /// Keeping this moderate allows interleaving with concurrent DML.
    #[serde(default = "default_rebalance_batch_size")]
    pub batch_size: usize,
    /// Minimum rows a shard must have before it can donate rows (default: 100).
    /// Avoids thrashing tiny shards.
    #[serde(default = "default_rebalance_min_donor_rows")]
    pub min_donor_rows: u64,
    /// Cooldown (ms) between successive rebalance runs (default: 5000).
    /// Prevents oscillation.
    #[serde(default = "default_rebalance_cooldown_ms")]
    pub cooldown_ms: u64,
    /// Start in paused state; requires manual resume via admin API (default: false).
    #[serde(default)]
    pub start_paused: bool,
}

const fn default_rebalance_check_interval_ms() -> u64 { 30_000 }
fn default_rebalance_imbalance_threshold() -> f64 { 1.25 }
const fn default_rebalance_batch_size() -> usize { 512 }
const fn default_rebalance_min_donor_rows() -> u64 { 100 }
const fn default_rebalance_cooldown_ms() -> u64 { 5_000 }

impl Default for RebalanceSectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval_ms: default_rebalance_check_interval_ms(),
            imbalance_threshold: default_rebalance_imbalance_threshold(),
            batch_size: default_rebalance_batch_size(),
            min_donor_rows: default_rebalance_min_donor_rows(),
            cooldown_ms: default_rebalance_cooldown_ms(),
            start_paused: false,
        }
    }
}

impl RebalanceSectionConfig {
    /// Validate the rebalance configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.imbalance_threshold < 1.0 {
            return Err(format!(
                "rebalance.imbalance_threshold must be >= 1.0, got {}",
                self.imbalance_threshold
            ));
        }
        if self.enabled && self.batch_size == 0 {
            return Err("rebalance.batch_size must be >= 1 when enabled".into());
        }
        if self.enabled && self.check_interval_ms == 0 {
            return Err("rebalance.check_interval_ms must be >= 1 when enabled".into());
        }
        Ok(())
    }
}

/// Change Data Capture (CDC) configuration section in falcon.toml.
///
/// CDC captures row-level changes (INSERT, UPDATE, DELETE) and DDL events
/// into an in-memory ring buffer, consumable via PostgreSQL logical replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Enable CDC event capture (default: true).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Maximum number of change events in the ring buffer (default: 100_000).
    /// Oldest events are evicted when the buffer is full.
    #[serde(default = "default_cdc_buffer_size")]
    pub buffer_size: usize,
}

const fn default_cdc_buffer_size() -> usize { 100_000 }

/// Point-in-Time Recovery (PITR) configuration section in falcon.toml.
///
/// PITR enables recovery to any point in time by archiving WAL segments
/// to a local directory. Combine with base backups for full data protection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PitrConfig {
    /// Enable WAL archiving for PITR (default: false).
    #[serde(default)]
    pub enabled: bool,
    /// Directory where completed WAL segments are archived.
    /// Required when enabled = true.
    #[serde(default)]
    pub archive_dir: String,
    /// WAL segment retention in hours (0 = keep forever).
    /// Archived segments older than this are removed during GC.
    #[serde(default)]
    pub retention_hours: u64,
}

impl Default for PitrConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            archive_dir: String::new(),
            retention_hours: 0,
        }
    }
}

/// Multi-tenant isolation and metering configuration section in falcon.toml.
///
/// When enabled, tenant quota enforcement (QPS, memory, concurrent txns) and
/// per-tenant resource metering are active. Tenants are created via SQL:
/// `CREATE TENANT name [MAX_QPS n] [MAX_STORAGE_BYTES n]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiTenantConfig {
    /// Enable multi-tenant isolation (default: false).
    /// When false, all sessions use SYSTEM_TENANT_ID with unlimited quotas.
    #[serde(default)]
    pub enabled: bool,
    /// Enable per-tenant resource metering for billing (default: false).
    #[serde(default)]
    pub metering_enabled: bool,
    /// Default max QPS for newly created tenants (0 = unlimited).
    #[serde(default)]
    pub default_max_qps: u64,
    /// Default max concurrent transactions for newly created tenants (0 = unlimited).
    #[serde(default)]
    pub default_max_concurrent_txns: u32,
    /// Default max memory bytes for newly created tenants (0 = unlimited).
    #[serde(default)]
    pub default_max_memory_bytes: u64,
    /// Default max storage bytes for newly created tenants (0 = unlimited).
    #[serde(default)]
    pub default_max_storage_bytes: u64,
}

/// Transparent Data Encryption (TDE) configuration section in falcon.toml.
///
/// When enabled, WAL records and data blocks are encrypted with AES-256-GCM.
/// The master key is derived from a passphrase via PBKDF2-HMAC-SHA256.
/// The passphrase is read from the environment variable named by `key_env_var`
/// (never stored in the config file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TdeConfig {
    /// Enable transparent data encryption (default: false).
    #[serde(default)]
    pub enabled: bool,
    /// Environment variable name that holds the master key passphrase.
    /// The passphrase is never stored in the config file.
    #[serde(default = "default_tde_key_env_var")]
    pub key_env_var: String,
    /// Encrypt WAL records (default: true when TDE is enabled).
    #[serde(default = "default_true")]
    pub encrypt_wal: bool,
    /// Encrypt data/table blocks (default: true when TDE is enabled).
    #[serde(default = "default_true")]
    pub encrypt_data: bool,
}

fn default_tde_key_env_var() -> String { "FALCON_TDE_KEY".into() }

impl Default for TdeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            key_env_var: default_tde_key_env_var(),
            encrypt_wal: true,
            encrypt_data: true,
        }
    }
}

impl Default for MultiTenantConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            metering_enabled: false,
            default_max_qps: 0,
            default_max_concurrent_txns: 0,
            default_max_memory_bytes: 0,
            default_max_storage_bytes: 0,
        }
    }
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            buffer_size: default_cdc_buffer_size(),
        }
    }
}

/// Production Safety Mode configuration.
///
/// When `enforce = true`, FalconDB validates all safety properties at startup
/// and **refuses to start** if any are violated. When `enforce = false` (default),
/// violations are logged as WARN but startup proceeds.
///
/// This is FalconDB's "no foot-guns in production" guard rail.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ProductionSafetyConfig {
    /// Enable production safety enforcement. Default: false (warn only).
    #[serde(default)]
    pub enforce: bool,
}


/// A single production safety check result.
#[derive(Debug, Clone)]
pub struct SafetyViolation {
    /// Short identifier (e.g. "PS-1").
    pub id: &'static str,
    /// Human-readable description of the violation.
    pub message: String,
    /// Severity: "CRITICAL" (blocks startup in enforce mode) or "WARN".
    pub severity: &'static str,
}

/// Validate a FalconConfig against production safety rules.
/// Returns a list of violations (empty = all checks pass).
pub fn validate_production_safety(config: &FalconConfig) -> Vec<SafetyViolation> {
    let mut violations = Vec::new();

    // PS-1: WAL must be enabled
    if !config.storage.wal_enabled {
        violations.push(SafetyViolation {
            id: "PS-1",
            message: "WAL is disabled (storage.wal_enabled=false). \
                      Data will not survive crashes. \
                      The Deterministic Commit Guarantee (DCG) is void.".into(),
            severity: "CRITICAL",
        });
    }

    // PS-2: WAL sync mode must be fsync or fdatasync (not "none")
    if config.wal.sync_mode == "none" {
        violations.push(SafetyViolation {
            id: "PS-2",
            message: "WAL sync_mode is 'none'. \
                      Committed data may be lost on crash. \
                      Set wal.sync_mode = 'fsync' or 'fdatasync'.".into(),
            severity: "CRITICAL",
        });
    }

    // PS-3: Authentication should not be Trust in production
    if config.server.auth.method == AuthMethod::Trust {
        violations.push(SafetyViolation {
            id: "PS-3",
            message: "Authentication method is 'trust' (no password required). \
                      Any client can connect and modify data. \
                      Set server.auth.method = 'scram-sha-256'.".into(),
            severity: "CRITICAL",
        });
    }

    // PS-4: Memory limits should be configured
    if config.memory.shard_hard_limit_bytes == 0 {
        violations.push(SafetyViolation {
            id: "PS-4",
            message: "No memory hard limit configured (memory.shard_hard_limit_bytes=0). \
                      Unbounded memory usage may cause OOM kill. \
                      Set a hard limit appropriate for your hardware.".into(),
            severity: "WARN",
        });
    }

    // PS-5: Statement timeout should be set
    if config.server.statement_timeout_ms == 0 {
        violations.push(SafetyViolation {
            id: "PS-5",
            message: "No statement timeout configured (server.statement_timeout_ms=0). \
                      Runaway queries may hold locks indefinitely. \
                      Set a timeout (e.g. 30000 for 30s).".into(),
            severity: "WARN",
        });
    }

    // PS-6: TLS should be enabled in production
    if config.server.tls.cert_path.is_empty() || config.server.tls.key_path.is_empty() {
        violations.push(SafetyViolation {
            id: "PS-6",
            message: "TLS is not configured. \
                      Client connections are unencrypted. \
                      Set server.tls.cert_path and server.tls.key_path.".into(),
            severity: "WARN",
        });
    }

    // PS-7: Shutdown drain timeout should be > 0
    if config.server.shutdown_drain_timeout_secs == 0 {
        violations.push(SafetyViolation {
            id: "PS-7",
            message: "Shutdown drain timeout is 0. \
                      Active connections will be killed immediately on shutdown. \
                      Set server.shutdown_drain_timeout_secs >= 10.".into(),
            severity: "WARN",
        });
    }

    violations
}

/// GC configuration section in falcon.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcSectionConfig {
    /// Enable background GC (default: true).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Interval between GC sweeps in milliseconds (default: 1000).
    #[serde(default = "default_gc_interval")]
    pub interval_ms: u64,
    /// Max keys per sweep (0 = unlimited).
    #[serde(default)]
    pub batch_size: usize,
    /// Minimum version chain length before GC considers a key (default: 2).
    #[serde(default = "default_min_chain_length")]
    pub min_chain_length: usize,
}

const fn default_true() -> bool { true }
const fn default_gc_interval() -> u64 { 1000 }
const fn default_min_chain_length() -> usize { 2 }

impl Default for GcSectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: 1000,
            batch_size: 0,
            min_chain_length: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// PG wire protocol listen address.
    pub pg_listen_addr: String,
    /// Admin HTTP listen address.
    pub admin_listen_addr: String,
    /// Node ID in cluster.
    pub node_id: u64,
    /// Max concurrent connections.
    pub max_connections: usize,
    /// Statement timeout in milliseconds (0 = no timeout).
    #[serde(default)]
    pub statement_timeout_ms: u64,
    /// Connection idle timeout in milliseconds (0 = no timeout).
    #[serde(default)]
    pub idle_timeout_ms: u64,
    /// Graceful shutdown drain timeout in seconds.
    /// After receiving SIGINT/SIGTERM, the server waits up to this many seconds
    /// for active connections to finish before forcing exit. Default: 30s.
    #[serde(default = "default_shutdown_drain_timeout_secs")]
    pub shutdown_drain_timeout_secs: u64,
    /// Slow transaction detection threshold in microseconds (0 = disabled).
    /// Transactions exceeding this latency are logged as WARN. Default: 100ms.
    #[serde(default = "default_slow_txn_threshold_us")]
    pub slow_txn_threshold_us: u64,
    /// Authentication configuration.
    #[serde(default)]
    pub auth: AuthConfig,
    /// TLS configuration. When cert and key paths are both set, SSL connections
    /// are accepted (server responds 'S' to SSLRequest and upgrades the stream).
    #[serde(default)]
    pub tls: TlsConfig,
}

/// TLS/SSL configuration for the PG wire protocol listener.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to the PEM-encoded server certificate.
    #[serde(default)]
    pub cert_path: String,
    /// Path to the PEM-encoded private key.
    #[serde(default)]
    pub key_path: String,
}

impl TlsConfig {
    /// Returns true when both cert and key paths are configured.
    pub const fn is_enabled(&self) -> bool {
        !self.cert_path.is_empty() && !self.key_path.is_empty()
    }
}

/// Authentication method.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    /// No authentication — any user/password accepted.
    #[default]
    Trust,
    /// SCRAM-SHA-256 authentication (PostgreSQL 10+).
    #[serde(rename = "scram-sha-256")]
    ScramSha256,
    /// Cleartext password (PG auth type 3).
    Password,
    /// MD5 hashed password (PG auth type 5).
    Md5,
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication method: trust, password, md5, or scram-sha-256.
    pub method: AuthMethod,
    /// Required password (cleartext). Used by `password` and `md5` methods.
    /// For SCRAM, prefer using the `users` list with stored verifiers instead.
    #[serde(default)]
    pub password: String,
    /// Required username. If empty, any username is accepted (legacy single-user mode).
    #[serde(default)]
    pub username: String,
    /// Named user credentials for SCRAM-SHA-256.
    /// Each entry has a username and a SCRAM verifier string.
    /// Format: `SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>`
    #[serde(default)]
    pub users: Vec<UserCredential>,
    /// Allowed client CIDR ranges (e.g. `["127.0.0.1/32", "::1/128"]`).
    /// Empty means all addresses are allowed.
    #[serde(default)]
    pub allow_cidrs: Vec<String>,
}

/// A named user credential for SCRAM-SHA-256 authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCredential {
    /// Username (case-sensitive, matches PostgreSQL behavior).
    pub username: String,
    /// SCRAM-SHA-256 verifier string.
    /// Format: `SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>`
    /// Can be generated with `falcon user add --username ... --password ...`
    #[serde(default)]
    pub scram: String,
    /// Plaintext password (legacy, used only if `scram` is empty).
    /// Prefer `scram` verifier in production.
    #[serde(default)]
    pub password: String,
}

const fn default_shutdown_drain_timeout_secs() -> u64 {
    30
}

const fn default_slow_txn_threshold_us() -> u64 {
    100_000 // 100ms
}

fn default_wal_backend() -> String { "file".to_owned() }
const fn default_group_commit_window_us() -> u64 { 200 }
fn default_compression_profile() -> String { "balanced".to_owned() }
fn default_wal_mode() -> String { "auto".to_owned() }

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            method: AuthMethod::Trust,
            password: String::new(),
            username: String::new(),
            users: Vec::new(),
            allow_cidrs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum memory budget in bytes (0 = unlimited).
    pub memory_limit_bytes: u64,
    /// Enable WAL persistence.
    pub wal_enabled: bool,
    /// Data directory for WAL and snapshots.
    pub data_dir: String,
    /// Write-path enforcement level for OLTP purity on Primary nodes.
    /// Controls what happens when a write touches columnstore/disk-rowstore.
    /// Default: Warn (backward-compatible). Production Primary: HardDeny.
    #[serde(default)]
    pub write_path_enforcement: WritePathEnforcement,
    /// Whether LSM tables sync every write to disk (true = durable, false = faster).
    /// Set false for bulk-load workloads; set true for production OLTP durability.
    /// Default: false (WAL already provides crash-recovery guarantees).
    #[serde(default)]
    pub lsm_sync_writes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Enable group commit.
    pub group_commit: bool,
    /// Group commit flush interval in microseconds.
    pub flush_interval_us: u64,
    /// Sync mode: "fsync", "fdatasync", or "none".
    pub sync_mode: String,
    /// Max WAL segment size in bytes.
    pub segment_size_bytes: u64,
    /// Durability policy: when is a commit considered durable.
    /// Default: LocalFsync. Use QuorumAck for stronger replication guarantees.
    #[serde(default)]
    pub durability_policy: DurabilityPolicy,
    /// WAL backlog admission threshold in bytes.
    /// When the WAL backlog (written but not yet replicated) exceeds this,
    /// new write transactions are rejected. 0 = disabled.
    #[serde(default)]
    pub backlog_admission_threshold_bytes: u64,
    /// Replication lag admission threshold in milliseconds.
    /// When the slowest replica is lagging more than this, new write
    /// transactions are rejected. 0 = disabled.
    #[serde(default)]
    pub replication_lag_admission_threshold_ms: u64,
    /// v1.0.7: WAL backend mode. Values: "file" (default), "win_async_file".
    #[serde(default = "default_wal_backend")]
    pub backend: String,
    /// v1.0.7: Enable FILE_FLAG_NO_BUFFERING on Windows (requires NTFS, aligned writes).
    #[serde(default)]
    pub no_buffering: bool,
    /// v1.0.7: Group commit coalescing window in microseconds (0 = immediate).
    #[serde(default = "default_group_commit_window_us")]
    pub group_commit_window_us: u64,
}

/// Configuration for spill-to-disk (external sort, hash aggregation overflow).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpillConfig {
    /// Maximum number of rows to hold in memory before spilling to disk.
    /// 0 = never spill (pure in-memory).
    /// Default: 500_000 rows — production workloads should always have a bound.
    #[serde(default = "default_spill_memory_rows_threshold")]
    pub memory_rows_threshold: usize,
    /// Temporary directory for spill files. Uses system temp dir if empty.
    #[serde(default)]
    pub temp_dir: String,
    /// Maximum number of sorted runs to merge at once (k-way merge fan-in).
    #[serde(default = "default_spill_merge_fan_in")]
    pub merge_fan_in: usize,
    /// Maximum number of groups in a hash aggregation before rejecting.
    /// Prevents OOM from unbounded GROUP BY on high-cardinality columns.
    /// 0 = unlimited (not recommended for production).
    /// Default: 1_000_000 groups.
    #[serde(default = "default_hash_agg_group_limit")]
    pub hash_agg_group_limit: usize,
    /// When memory pressure is at or above this level, the sort spill threshold
    /// is automatically halved to reduce memory consumption.
    /// Values: "soft", "hard", "emergency", "none" (disable reactive spill).
    /// Default: "soft" — start spilling earlier when memory is under pressure.
    #[serde(default = "default_pressure_spill_trigger")]
    pub pressure_spill_trigger: String,
}

const fn default_spill_memory_rows_threshold() -> usize { 500_000 }
const fn default_spill_merge_fan_in() -> usize { 16 }
const fn default_hash_agg_group_limit() -> usize { 1_000_000 }
fn default_pressure_spill_trigger() -> String { "soft".to_owned() }

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            memory_rows_threshold: default_spill_memory_rows_threshold(),
            temp_dir: String::new(),
            merge_fan_in: default_spill_merge_fan_in(),
            hash_agg_group_limit: default_hash_agg_group_limit(),
            pressure_spill_trigger: default_pressure_spill_trigger(),
        }
    }
}

/// Write-path enforcement level for OLTP purity on Primary nodes.
///
/// Controls what happens when a write transaction touches a non-rowstore
/// (columnstore or disk-rowstore) table on a Primary node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WritePathEnforcement {
    /// Log a warning but allow the operation (default, backward-compatible).
    #[default]
    Warn,
    /// Return an error immediately on the first violation in a transaction.
    FailFast,
    /// Hard-deny: return an error and abort the transaction.
    HardDeny,
}

/// Durability policy for WAL commit acknowledgement.
///
/// Controls when a commit is considered durable and can be returned to the client.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DurabilityPolicy {
    /// Commit is durable after local WAL fsync (default).
    #[default]
    LocalFsync,
    /// Commit is durable after a quorum of replicas have acked the WAL record.
    /// Requires at least one replica to be connected; falls back to LocalFsync if none.
    QuorumAck,
    /// Commit is durable after all connected replicas have acked.
    /// Strongest guarantee; highest latency.
    AllAck,
}

/// Node role in the cluster.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Primary: accepts reads and writes, streams WAL to replicas.
    /// **OLTP-only**: ColumnStore and DiskRowStore are forbidden on this role.
    Primary,
    /// Replica (follower): receives WAL from primary, serves read-only queries.
    Replica,
    /// Analytics: read-only replica that may use ColumnStore / vectorised scan.
    /// Receives WAL but never participates in write-txn commit path.
    Analytics,
    /// Standalone: single-node mode, no replication (M1 default).
    #[default]
    Standalone,
    /// RaftMember: participates in Raft consensus replication.
    /// Writes are proposed through Raft before being applied to the storage engine.
    /// Leader node accepts writes; followers serve read-only queries.
    /// Configure the cluster via the `[raft]` section in falcon.toml.
    #[serde(rename = "raft_member")]
    RaftMember,
}

impl NodeRole {
    /// Returns true if this role is allowed to serve write transactions.
    /// RaftMember nodes determine writability dynamically (only the Raft leader
    /// accepts writes at runtime); we return true here so the executor is
    /// created in read-write mode and the Raft layer enforces leader-only writes.
    pub const fn is_writable(&self) -> bool {
        matches!(self, Self::Primary | Self::Standalone | Self::RaftMember)
    }

    /// Returns true if columnstore / analytical storage paths are permitted.
    /// Primary nodes must never touch columnar storage on the write path.
    pub const fn allows_columnstore(&self) -> bool {
        matches!(self, Self::Analytics | Self::Standalone)
    }

    /// Returns true if this role uses Raft consensus for replication.
    pub const fn is_raft_member(&self) -> bool {
        matches!(self, Self::RaftMember)
    }
}

/// Raft consensus configuration.
///
/// Used when `replication.role = "raft_member"`. Each shard runs a Raft group
/// of `node_ids`. The local node's ID is `node_id`. Peer addresses for the
/// gRPC Raft transport are specified in `peers` as `"<node_id>=<addr>"` pairs.
///
/// Example falcon.toml section for a 3-node cluster:
/// ```toml
/// [raft]
/// node_id = 1
/// node_ids = [1, 2, 3]
/// raft_listen_addr = "0.0.0.0:50052"
/// peers = ["2=http://node2:50052", "3=http://node3:50052"]
/// failover_poll_interval_ms = 200
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// This node's Raft node ID (must be unique within the cluster, > 0).
    #[serde(default = "default_raft_node_id")]
    pub node_id: u64,
    /// All node IDs in this Raft cluster (including this node).
    #[serde(default = "default_raft_node_ids")]
    pub node_ids: Vec<u64>,
    /// gRPC listen address for the Raft transport server.
    #[serde(default = "default_raft_listen_addr")]
    pub raft_listen_addr: String,
    /// Peer addresses in the format `"<node_id>=<grpc_uri>"`,
    /// e.g. `"2=http://node2:50052"`. Omit the entry for the local node.
    #[serde(default)]
    pub peers: Vec<String>,
    /// How often (ms) the `RaftFailoverWatcher` polls for leader changes.
    #[serde(default = "default_raft_poll_interval_ms")]
    pub failover_poll_interval_ms: u64,
    /// Raft heartbeat interval in milliseconds.
    #[serde(default = "default_raft_heartbeat_ms")]
    pub heartbeat_interval_ms: u64,
    /// Raft election timeout minimum in milliseconds.
    #[serde(default = "default_raft_election_min_ms")]
    pub election_timeout_min_ms: u64,
    /// Raft election timeout maximum in milliseconds.
    #[serde(default = "default_raft_election_max_ms")]
    pub election_timeout_max_ms: u64,
}

fn default_raft_node_id() -> u64 { 1 }
fn default_raft_node_ids() -> Vec<u64> { vec![1] }
fn default_raft_listen_addr() -> String { "0.0.0.0:50052".to_owned() }
fn default_raft_poll_interval_ms() -> u64 { 200 }
fn default_raft_heartbeat_ms() -> u64 { 50 }
fn default_raft_election_min_ms() -> u64 { 150 }
fn default_raft_election_max_ms() -> u64 { 300 }

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: default_raft_node_id(),
            node_ids: default_raft_node_ids(),
            raft_listen_addr: default_raft_listen_addr(),
            peers: Vec::new(),
            failover_poll_interval_ms: default_raft_poll_interval_ms(),
            heartbeat_interval_ms: default_raft_heartbeat_ms(),
            election_timeout_min_ms: default_raft_election_min_ms(),
            election_timeout_max_ms: default_raft_election_max_ms(),
        }
    }
}

impl RaftConfig {
    /// Parse the `peers` list into `(node_id, addr)` pairs.
    pub fn parsed_peers(&self) -> Vec<(u64, String)> {
        self.peers
            .iter()
            .filter_map(|entry| {
                let mut parts = entry.splitn(2, '=');
                let id_str = parts.next()?;
                let addr = parts.next()?;
                let id: u64 = id_str.trim().parse().ok()?;
                Some((id, addr.trim().to_owned()))
            })
            .collect()
    }

    /// Validate the Raft configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.node_id == 0 {
            return Err("raft.node_id must be > 0".into());
        }
        if self.node_ids.is_empty() {
            return Err("raft.node_ids must not be empty".into());
        }
        if !self.node_ids.contains(&self.node_id) {
            return Err(format!(
                "raft.node_id {} must be present in raft.node_ids {:?}",
                self.node_id, self.node_ids
            ));
        }
        if self.raft_listen_addr.is_empty() {
            return Err("raft.raft_listen_addr must not be empty".into());
        }
        Ok(())
    }
}

/// Replication configuration for M2 multi-node deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// This node's role in the cluster.
    pub role: NodeRole,
    /// gRPC listen address for replication service (primary serves, replica connects).
    pub grpc_listen_addr: String,
    /// Primary's gRPC endpoint (only used by replicas to connect).
    pub primary_endpoint: String,
    /// Maximum records per WAL chunk sent to replicas.
    pub max_records_per_chunk: usize,
    /// Replication poll interval in milliseconds (replica pulls WAL from primary).
    pub poll_interval_ms: u64,
    /// Maximum backoff interval in milliseconds when replication fails (exponential backoff cap).
    pub max_backoff_ms: u64,
    /// gRPC connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Number of shards this node is responsible for.
    pub shard_count: u64,
}

impl ReplicationConfig {
    /// P0-4: Validate replication configuration.
    /// Ensures single authoritative replication model per shard and
    /// role-specific settings are coherent.
    pub fn validate(&self) -> Result<(), String> {
        // Primary/Replica/Analytics must have a valid gRPC address
        // RaftMember uses its own raft.raft_listen_addr, not replication.grpc_listen_addr
        if !matches!(self.role, NodeRole::Standalone | NodeRole::RaftMember)
            && self.grpc_listen_addr.is_empty()
        {
            return Err("grpc_listen_addr must be set for non-standalone nodes".into());
        }

        // Replica/Analytics must have a valid primary endpoint
        if matches!(self.role, NodeRole::Replica | NodeRole::Analytics)
            && self.primary_endpoint.is_empty()
        {
            return Err("primary_endpoint must be set for replica/analytics nodes".into());
        }

        // Shard count must be at least 1
        if self.shard_count == 0 {
            return Err("shard_count must be >= 1".into());
        }

        // WAL replication chunk size must be reasonable
        if self.max_records_per_chunk == 0 {
            return Err("max_records_per_chunk must be >= 1".into());
        }

        Ok(())
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: NodeRole::Standalone,
            grpc_listen_addr: "0.0.0.0:50051".to_owned(),
            primary_endpoint: "http://127.0.0.1:50051".to_owned(),
            max_records_per_chunk: 1000,
            poll_interval_ms: 100,
            max_backoff_ms: 30_000,
            connect_timeout_ms: 5_000,
            shard_count: 1,
        }
    }
}

/// USTM (User-Space Tiered Memory) engine configuration.
///
/// Controls the three-zone page cache that replaces mmap for SST reads.
/// When `enabled = false`, the storage engine bypasses USTM entirely.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UstmSectionConfig {
    /// Enable the USTM page cache (default: true).
    pub enabled: bool,
    /// Hot zone capacity in bytes (MemTable + index internals).
    pub hot_capacity_bytes: u64,
    /// Warm zone capacity in bytes (SST page cache).
    pub warm_capacity_bytes: u64,
    /// LIRS-2 LIR set capacity (protected high-frequency pages).
    pub lirs_lir_capacity: usize,
    /// LIRS-2 HIR-resident capacity (eviction candidates).
    pub lirs_hir_capacity: usize,
    /// Maximum background IOPS for compaction/GC I/O.
    pub background_iops_limit: u64,
    /// Maximum prefetch IOPS.
    pub prefetch_iops_limit: u64,
    /// Enable query-aware prefetcher.
    pub prefetch_enabled: bool,
    /// Default page size in bytes.
    pub page_size: u32,
}

impl Default for UstmSectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            hot_capacity_bytes: 512 * 1024 * 1024,   // 512 MB
            warm_capacity_bytes: 256 * 1024 * 1024,   // 256 MB
            lirs_lir_capacity: 4096,
            lirs_hir_capacity: 1024,
            background_iops_limit: 500,
            prefetch_iops_limit: 200,
            prefetch_enabled: true,
            page_size: 8192,
        }
    }
}

/// Memory budget and backpressure configuration.
///
/// Defines hierarchical memory limits (shard → node → cluster) and the
/// backpressure policy applied when memory pressure is detected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Per-shard soft memory limit in bytes.
    /// When usage >= soft_limit, new write transactions are delayed or rejected.
    /// 0 = no soft limit (backpressure disabled).
    #[serde(default)]
    pub shard_soft_limit_bytes: u64,
    /// Per-shard hard memory limit in bytes.
    /// When usage >= hard_limit, ALL new transactions are rejected.
    /// 0 = no hard limit.
    #[serde(default)]
    pub shard_hard_limit_bytes: u64,
    /// Per-node memory limit in bytes.
    /// sum(shard hard limits) should not exceed this.
    /// 0 = no node limit.
    #[serde(default)]
    pub node_limit_bytes: u64,
    /// Cluster-level logical memory limit in bytes.
    /// sum(node limits) should not exceed this.
    /// 0 = no cluster limit.
    #[serde(default)]
    pub cluster_limit_bytes: u64,
    /// Backpressure policy when shard is in PRESSURE state.
    #[serde(default)]
    pub pressure_policy: PressurePolicy,
    /// Maximum write-set size (number of keys) per transaction under PRESSURE.
    /// 0 = no per-txn write limit.
    #[serde(default)]
    pub max_txn_write_keys: usize,
    /// Maximum memory bytes a single transaction may allocate under PRESSURE.
    /// 0 = no per-txn memory limit.
    #[serde(default)]
    pub max_txn_write_bytes: u64,
    /// Whether to enable backpressure (default: true).
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Policy for handling new write transactions when shard is under PRESSURE.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PressurePolicy {
    /// Reject new write transactions immediately with an error.
    #[default]
    Reject,
    /// Delay new write transactions (yield before proceeding).
    Delay,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            shard_soft_limit_bytes: 0,
            shard_hard_limit_bytes: 0,
            node_limit_bytes: 0,
            cluster_limit_bytes: 0,
            pressure_policy: PressurePolicy::Reject,
            max_txn_write_keys: 0,
            max_txn_write_bytes: 0,
            enabled: true,
        }
    }
}

/// v1.0.8: Gateway configuration section in falcon.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Gateway role: "smart_gateway" (default), "dedicated_gateway", "compute_only".
    #[serde(default = "default_gateway_role")]
    pub role: String,
    /// Maximum concurrent inflight requests (0 = unlimited).
    #[serde(default)]
    pub max_inflight: u64,
    /// Maximum concurrent forwarded requests (0 = unlimited).
    #[serde(default)]
    pub max_forwarded: u64,
    /// Forward request timeout in milliseconds.
    #[serde(default = "default_forward_timeout_ms")]
    pub forward_timeout_ms: u64,
    /// Topology cache staleness threshold in seconds.
    #[serde(default = "default_topology_staleness_secs")]
    pub topology_staleness_secs: u64,
}

fn default_gateway_role() -> String { "smart_gateway".to_owned() }
const fn default_forward_timeout_ms() -> u64 { 5000 }
const fn default_topology_staleness_secs() -> u64 { 30 }

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            role: default_gateway_role(),
            max_inflight: 0,
            max_forwarded: 0,
            forward_timeout_ms: default_forward_timeout_ms(),
            topology_staleness_secs: default_topology_staleness_secs(),
        }
    }
}

impl Default for FalconConfig {
    fn default() -> Self {
        Self {
            config_version: CURRENT_CONFIG_VERSION,
            server: ServerConfig {
                pg_listen_addr: "0.0.0.0:5433".to_owned(),
                admin_listen_addr: "0.0.0.0:8080".to_owned(),
                node_id: 1,
                max_connections: 1024,
                statement_timeout_ms: 0,
                idle_timeout_ms: 0,
                shutdown_drain_timeout_secs: 30,
                slow_txn_threshold_us: 100_000,
                auth: AuthConfig::default(),
                tls: TlsConfig::default(),
            },
            storage: StorageConfig {
                memory_limit_bytes: 0,
                wal_enabled: true,
                data_dir: "./falcon_data".to_owned(),
                write_path_enforcement: WritePathEnforcement::Warn,
                lsm_sync_writes: false,
            },
            wal: WalConfig {
                group_commit: true,
                flush_interval_us: 1000,
                sync_mode: "fdatasync".to_owned(),
                segment_size_bytes: 64 * 1024 * 1024,
                durability_policy: DurabilityPolicy::LocalFsync,
                backlog_admission_threshold_bytes: 0,
                replication_lag_admission_threshold_ms: 0,
                backend: default_wal_backend(),
                no_buffering: false,
                group_commit_window_us: default_group_commit_window_us(),
            },
            replication: ReplicationConfig::default(),
            spill: SpillConfig::default(),
            memory: MemoryConfig::default(),
            gc: GcSectionConfig::default(),
            ustm: UstmSectionConfig::default(),
            compression_profile: default_compression_profile(),
            wal_mode: default_wal_mode(),
            gateway: GatewayConfig::default(),
            production_safety: ProductionSafetyConfig::default(),
            cdc: CdcConfig::default(),
            pitr: PitrConfig::default(),
            multi_tenant: MultiTenantConfig::default(),
            tde: TdeConfig::default(),
            raft: RaftConfig::default(),
            rebalance: RebalanceSectionConfig::default(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Deprecated field checker — backward-compatible config migration
// ═══════════════════════════════════════════════════════════════════════════

/// A deprecated config field mapping.
#[derive(Debug, Clone)]
pub struct DeprecatedField {
    /// The old field path (e.g. "cedar.data_dir").
    pub old_path: String,
    /// The new field path (e.g. "storage.data_dir"), or empty if removed.
    pub new_path: String,
    /// Version when the field was deprecated.
    pub deprecated_since: String,
    /// Version when the field will be removed (empty = not yet scheduled).
    pub removed_in: String,
}

/// Result of checking a TOML config string for deprecated fields.
#[derive(Debug, Clone)]
pub struct DeprecatedFieldReport {
    /// Warnings for deprecated fields found.
    pub warnings: Vec<String>,
    /// Number of deprecated fields detected.
    pub deprecated_count: usize,
}

impl DeprecatedFieldReport {
    pub const fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Checks TOML config text for deprecated/renamed fields and emits warnings.
///
/// This enables backward-compatible config: old field names are warned but not errored.
pub struct DeprecatedFieldChecker {
    fields: Vec<DeprecatedField>,
}

impl DeprecatedFieldChecker {
    /// Create a checker with the built-in deprecated field registry.
    pub fn new() -> Self {
        Self {
            fields: vec![
                // CedarDB → FalconDB rename (v0.4)
                DeprecatedField {
                    old_path: "cedar".into(),
                    new_path: "server".into(),
                    deprecated_since: "v0.4.0".into(),
                    removed_in: "v1.0.0".into(),
                },
                DeprecatedField {
                    old_path: "cedar_data_dir".into(),
                    new_path: "storage.data_dir".into(),
                    deprecated_since: "v0.4.0".into(),
                    removed_in: "v1.0.0".into(),
                },
                // Old sync_mode values
                DeprecatedField {
                    old_path: "wal.sync".into(),
                    new_path: "wal.sync_mode".into(),
                    deprecated_since: "v0.3.0".into(),
                    removed_in: "v1.0.0".into(),
                },
                // Old replication field names
                DeprecatedField {
                    old_path: "replication.master_endpoint".into(),
                    new_path: "replication.primary_endpoint".into(),
                    deprecated_since: "v0.2.0".into(),
                    removed_in: "v1.0.0".into(),
                },
                DeprecatedField {
                    old_path: "replication.slave_mode".into(),
                    new_path: "replication.role".into(),
                    deprecated_since: "v0.2.0".into(),
                    removed_in: "v1.0.0".into(),
                },
                // Old memory config
                DeprecatedField {
                    old_path: "storage.max_memory".into(),
                    new_path: "memory.node_limit_bytes".into(),
                    deprecated_since: "v0.6.0".into(),
                    removed_in: "v1.0.0".into(),
                },
            ],
        }
    }

    /// Add a custom deprecated field mapping.
    pub fn add_field(&mut self, field: DeprecatedField) {
        self.fields.push(field);
    }

    /// Check a raw TOML config string for deprecated fields.
    /// Returns warnings (not errors) for each deprecated field found.
    pub fn check_toml(&self, toml_text: &str) -> DeprecatedFieldReport {
        let mut warnings = Vec::new();
        let mut deprecated_count = 0;

        for field in &self.fields {
            // Simple line-based detection: check if the old field path appears as a key
            let patterns = Self::field_patterns(&field.old_path);
            for pattern in &patterns {
                if toml_text.contains(pattern) {
                    deprecated_count += 1;
                    if field.new_path.is_empty() {
                        warnings.push(format!(
                            "Config field '{}' is deprecated since {} and will be removed in {}. This field has no replacement.",
                            field.old_path, field.deprecated_since, field.removed_in
                        ));
                    } else {
                        warnings.push(format!(
                            "Config field '{}' is deprecated since {}. Use '{}' instead. Will be removed in {}.",
                            field.old_path, field.deprecated_since, field.new_path, field.removed_in
                        ));
                    }
                    break; // Don't double-count
                }
            }
        }

        DeprecatedFieldReport {
            warnings,
            deprecated_count,
        }
    }

    /// Emit warnings via tracing for any deprecated fields found.
    pub fn check_and_warn(&self, toml_text: &str) -> DeprecatedFieldReport {
        let report = self.check_toml(toml_text);
        for warning in &report.warnings {
            tracing::warn!("{}", warning);
        }
        report
    }

    /// Get the full registry of deprecated fields (for SHOW command).
    pub fn registry(&self) -> &[DeprecatedField] {
        &self.fields
    }

    /// Generate patterns to search for a field path in TOML text.
    fn field_patterns(path: &str) -> Vec<String> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut patterns = Vec::new();
        if parts.len() == 1 {
            // Top-level section: [cedar] or cedar.
            patterns.push(format!("[{}]", parts[0]));
            patterns.push(format!("{} =", parts[0]));
            patterns.push(format!("{}=", parts[0]));
        } else {
            // Nested field: replication.master_endpoint
            let key = match parts.last() {
                Some(k) => k,
                None => return patterns,
            };
            patterns.push(format!("{key} ="));
            patterns.push(format!("{key}="));
        }
        patterns
    }
}

impl Default for DeprecatedFieldChecker {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot for observability.
#[derive(Debug, Clone)]
pub struct DeprecatedFieldCheckerSnapshot {
    pub total_registered: usize,
    pub fields: Vec<(String, String, String, String)>, // (old, new, since, removed_in)
}

impl DeprecatedFieldChecker {
    pub fn snapshot(&self) -> DeprecatedFieldCheckerSnapshot {
        DeprecatedFieldCheckerSnapshot {
            total_registered: self.fields.len(),
            fields: self
                .fields
                .iter()
                .map(|f| {
                    (
                        f.old_path.clone(),
                        f.new_path.clone(),
                        f.deprecated_since.clone(),
                        f.removed_in.clone(),
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── NodeRole helper tests ──

    #[test]
    fn test_node_role_is_writable() {
        assert!(NodeRole::Primary.is_writable());
        assert!(!NodeRole::Replica.is_writable());
        assert!(!NodeRole::Analytics.is_writable());
        assert!(NodeRole::Standalone.is_writable());
    }

    #[test]
    fn test_node_role_allows_columnstore() {
        assert!(!NodeRole::Primary.allows_columnstore());
        assert!(!NodeRole::Replica.allows_columnstore());
        assert!(NodeRole::Analytics.allows_columnstore());
        assert!(NodeRole::Standalone.allows_columnstore());
    }

    // ── ReplicationConfig validation tests ──

    #[test]
    fn test_default_config_valid() {
        let config = ReplicationConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_primary_missing_grpc_addr_rejected() {
        let mut config = ReplicationConfig::default();
        config.role = NodeRole::Primary;
        config.grpc_listen_addr = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replica_missing_primary_endpoint_rejected() {
        let mut config = ReplicationConfig::default();
        config.role = NodeRole::Replica;
        config.primary_endpoint = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_analytics_missing_primary_endpoint_rejected() {
        let mut config = ReplicationConfig::default();
        config.role = NodeRole::Analytics;
        config.primary_endpoint = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_shard_count_rejected() {
        let mut config = ReplicationConfig::default();
        config.shard_count = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_max_records_per_chunk_rejected() {
        let mut config = ReplicationConfig::default();
        config.max_records_per_chunk = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_valid_primary_config() {
        let mut config = ReplicationConfig::default();
        config.role = NodeRole::Primary;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_valid_replica_config() {
        let mut config = ReplicationConfig::default();
        config.role = NodeRole::Replica;
        config.primary_endpoint = "http://primary:50051".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_valid_analytics_config() {
        let mut config = ReplicationConfig::default();
        config.role = NodeRole::Analytics;
        config.primary_endpoint = "http://primary:50051".to_string();
        assert!(config.validate().is_ok());
    }

    // ── DeprecatedFieldChecker tests ──

    #[test]
    fn test_deprecated_checker_no_deprecated_fields() {
        let checker = DeprecatedFieldChecker::new();
        let toml = r#"
[server]
pg_listen_addr = "0.0.0.0:5433"

[storage]
data_dir = "./falcon_data"
"#;
        let report = checker.check_toml(toml);
        assert!(!report.has_warnings());
        assert_eq!(report.deprecated_count, 0);
    }

    #[test]
    fn test_deprecated_checker_detects_cedar_section() {
        let checker = DeprecatedFieldChecker::new();
        let toml = r#"
[cedar]
pg_listen_addr = "0.0.0.0:5433"
"#;
        let report = checker.check_toml(toml);
        assert!(report.has_warnings());
        assert_eq!(report.deprecated_count, 1);
        assert!(report.warnings[0].contains("cedar"));
        assert!(report.warnings[0].contains("server"));
    }

    #[test]
    fn test_deprecated_checker_detects_master_endpoint() {
        let checker = DeprecatedFieldChecker::new();
        let toml = r#"
[replication]
master_endpoint = "http://primary:50051"
"#;
        let report = checker.check_toml(toml);
        assert!(report.has_warnings());
        assert!(report.warnings[0].contains("master_endpoint"));
        assert!(report.warnings[0].contains("primary_endpoint"));
    }

    #[test]
    fn test_deprecated_checker_detects_multiple() {
        let checker = DeprecatedFieldChecker::new();
        let toml = r#"
[cedar]
pg_listen_addr = "0.0.0.0:5433"

[replication]
master_endpoint = "http://primary:50051"
slave_mode = true
"#;
        let report = checker.check_toml(toml);
        assert_eq!(report.deprecated_count, 3);
        assert_eq!(report.warnings.len(), 3);
    }

    #[test]
    fn test_deprecated_checker_custom_field() {
        let mut checker = DeprecatedFieldChecker::new();
        checker.add_field(DeprecatedField {
            old_path: "custom.old_field".into(),
            new_path: "custom.new_field".into(),
            deprecated_since: "v0.9.0".into(),
            removed_in: "v1.0.0".into(),
        });
        let toml = "old_field = 42\n";
        let report = checker.check_toml(toml);
        assert!(report.has_warnings());
    }

    #[test]
    fn test_deprecated_checker_snapshot() {
        let checker = DeprecatedFieldChecker::new();
        let snap = checker.snapshot();
        assert_eq!(snap.total_registered, 6);
        assert_eq!(snap.fields.len(), 6);
    }

    // ── RebalanceSectionConfig tests ──

    #[test]
    fn test_rebalance_default_valid() {
        let config = RebalanceSectionConfig::default();
        assert!(config.validate().is_ok());
        assert!(!config.enabled);
        assert_eq!(config.check_interval_ms, 30_000);
        assert!((config.imbalance_threshold - 1.25).abs() < f64::EPSILON);
        assert_eq!(config.batch_size, 512);
        assert_eq!(config.min_donor_rows, 100);
        assert_eq!(config.cooldown_ms, 5_000);
        assert!(!config.start_paused);
    }

    #[test]
    fn test_rebalance_threshold_below_one_rejected() {
        let mut config = RebalanceSectionConfig::default();
        config.enabled = true;
        config.imbalance_threshold = 0.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rebalance_zero_batch_size_rejected_when_enabled() {
        let mut config = RebalanceSectionConfig::default();
        config.enabled = true;
        config.batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rebalance_zero_batch_size_ok_when_disabled() {
        let mut config = RebalanceSectionConfig::default();
        config.enabled = false;
        config.batch_size = 0;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rebalance_zero_interval_rejected_when_enabled() {
        let mut config = RebalanceSectionConfig::default();
        config.enabled = true;
        config.check_interval_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rebalance_toml_deserialization() {
        let toml_str = r#"
enabled = true
check_interval_ms = 15000
imbalance_threshold = 1.5
batch_size = 256
min_donor_rows = 50
cooldown_ms = 10000
start_paused = true
"#;
        let config: RebalanceSectionConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.check_interval_ms, 15_000);
        assert!((config.imbalance_threshold - 1.5).abs() < f64::EPSILON);
        assert_eq!(config.batch_size, 256);
        assert_eq!(config.min_donor_rows, 50);
        assert_eq!(config.cooldown_ms, 10_000);
        assert!(config.start_paused);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rebalance_toml_defaults() {
        let toml_str = "";
        let config: RebalanceSectionConfig = toml::from_str(toml_str).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.batch_size, 512);
    }

    #[test]
    fn test_falcon_config_with_rebalance_section() {
        let config = FalconConfig::default();
        assert!(!config.rebalance.enabled);
        assert!(config.rebalance.validate().is_ok());
    }
}
