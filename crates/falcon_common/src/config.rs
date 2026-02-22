use serde::{Deserialize, Serialize};

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FalconConfig {
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
}

/// GC configuration section in falcon.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcSectionConfig {
    /// Enable background GC (default: true).
    pub enabled: bool,
    /// Interval between GC sweeps in milliseconds (default: 1000).
    pub interval_ms: u64,
    /// Max keys per sweep (0 = unlimited).
    pub batch_size: usize,
    /// Minimum version chain length before GC considers a key (default: 2).
    pub min_chain_length: usize,
}

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
    pub fn is_enabled(&self) -> bool {
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
    /// Authentication method: trust, password, or md5.
    pub method: AuthMethod,
    /// Required password (cleartext). Used by `password` and `md5` methods.
    /// In production, this should come from an env var or secrets manager.
    #[serde(default)]
    pub password: String,
    /// Required username. If empty, any username is accepted.
    #[serde(default)]
    pub username: String,
}

fn default_shutdown_drain_timeout_secs() -> u64 {
    30
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            method: AuthMethod::Trust,
            password: String::new(),
            username: String::new(),
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
}

/// Configuration for spill-to-disk (external sort, hash aggregation overflow).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpillConfig {
    /// Maximum number of rows to hold in memory before spilling to disk.
    /// 0 = never spill (pure in-memory, the default for small workloads).
    pub memory_rows_threshold: usize,
    /// Temporary directory for spill files. Uses system temp dir if empty.
    pub temp_dir: String,
    /// Maximum number of sorted runs to merge at once (k-way merge fan-in).
    pub merge_fan_in: usize,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            memory_rows_threshold: 0,
            temp_dir: String::new(),
            merge_fan_in: 16,
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
}

impl NodeRole {
    /// Returns true if this role is allowed to serve write transactions.
    pub fn is_writable(&self) -> bool {
        matches!(self, NodeRole::Primary | NodeRole::Standalone)
    }

    /// Returns true if columnstore / analytical storage paths are permitted.
    /// Primary nodes must never touch columnar storage on the write path.
    pub fn allows_columnstore(&self) -> bool {
        matches!(self, NodeRole::Analytics | NodeRole::Standalone)
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
        if self.role != NodeRole::Standalone && self.grpc_listen_addr.is_empty() {
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
            grpc_listen_addr: "0.0.0.0:50051".to_string(),
            primary_endpoint: "http://127.0.0.1:50051".to_string(),
            max_records_per_chunk: 1000,
            poll_interval_ms: 100,
            max_backoff_ms: 30_000,
            connect_timeout_ms: 5_000,
            shard_count: 1,
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
    pub shard_soft_limit_bytes: u64,
    /// Per-shard hard memory limit in bytes.
    /// When usage >= hard_limit, ALL new transactions are rejected.
    /// 0 = no hard limit.
    pub shard_hard_limit_bytes: u64,
    /// Per-node memory limit in bytes.
    /// sum(shard hard limits) should not exceed this.
    /// 0 = no node limit.
    pub node_limit_bytes: u64,
    /// Cluster-level logical memory limit in bytes.
    /// sum(node limits) should not exceed this.
    /// 0 = no cluster limit.
    pub cluster_limit_bytes: u64,
    /// Backpressure policy when shard is in PRESSURE state.
    #[serde(default)]
    pub pressure_policy: PressurePolicy,
    /// Maximum write-set size (number of keys) per transaction under PRESSURE.
    /// 0 = no per-txn write limit.
    pub max_txn_write_keys: usize,
    /// Maximum memory bytes a single transaction may allocate under PRESSURE.
    /// 0 = no per-txn memory limit.
    pub max_txn_write_bytes: u64,
    /// Whether to enable backpressure (default: true).
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

impl Default for FalconConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                pg_listen_addr: "0.0.0.0:5433".to_string(),
                admin_listen_addr: "0.0.0.0:8080".to_string(),
                node_id: 1,
                max_connections: 1024,
                statement_timeout_ms: 0,
                idle_timeout_ms: 0,
                shutdown_drain_timeout_secs: 30,
                auth: AuthConfig::default(),
                tls: TlsConfig::default(),
            },
            storage: StorageConfig {
                memory_limit_bytes: 0,
                wal_enabled: true,
                data_dir: "./falcon_data".to_string(),
                write_path_enforcement: WritePathEnforcement::Warn,
            },
            wal: WalConfig {
                group_commit: true,
                flush_interval_us: 1000,
                sync_mode: "fdatasync".to_string(),
                segment_size_bytes: 64 * 1024 * 1024,
                durability_policy: DurabilityPolicy::LocalFsync,
                backlog_admission_threshold_bytes: 0,
                replication_lag_admission_threshold_ms: 0,
            },
            replication: ReplicationConfig::default(),
            spill: SpillConfig::default(),
            memory: MemoryConfig::default(),
            gc: GcSectionConfig::default(),
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
    pub fn has_warnings(&self) -> bool {
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
            let key = parts.last().unwrap();
            patterns.push(format!("{} =", key));
            patterns.push(format!("{}=", key));
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
}
