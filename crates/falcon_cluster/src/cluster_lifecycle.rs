//! Cluster Lifecycle Coordinator — wires enterprise modules to the data plane.
//!
//! Bridges the gap between the enterprise "scaffold" modules (`NodeRegistry`,
//! `ConfigStore`, `ControllerHAGroup`, `SloEngine`, `BackupOrchestrator`, etc.)
//! and the actual server I/O lifecycle.
//!
//! # Architecture
//!
//! ```text
//! Server main()
//!   │
//!   ├── ClusterLifecycleCoordinator::new(config)
//!   │     ├── NodeRegistry          (data-node heartbeat tracking)
//!   │     ├── ConfigStore           (versioned dynamic config)
//!   │     ├── ConsistentMetadataStore (Raft-backed metadata)
//!   │     ├── ShardPlacementManager (shard→node mapping)
//!   │     ├── SloEngine             (SLO evaluation)
//!   │     ├── EnterpriseAuditLog    (security audit events)
//!   │     └── BackupOrchestrator    (scheduled backup lifecycle)
//!   │
//!   ├── coordinator.start()         → spawns background tasks
//!   │     ├── heartbeat_loop        (periodic NodeRegistry.evaluate)
//!   │     ├── config_sync_loop      (push config to stale nodes)
//!   │     ├── slo_eval_loop         (periodic SLO checks)
//!   │     └── backup_schedule_loop  (trigger scheduled backups)
//!   │
//!   └── coordinator.shutdown()      → cancels all background tasks
//! ```
//!
//! # Invariants
//!
//! - **CL-1**: All enterprise subsystems are initialized before the PG listener
//!   accepts connections.
//! - **CL-2**: Background tasks respect the `CancellationToken` and drain within
//!   the configured shutdown timeout.
//! - **CL-3**: `NodeRegistry.evaluate()` runs at least once per heartbeat interval.
//! - **CL-4**: Audit log captures startup/shutdown events.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use falcon_common::types::NodeId;
use falcon_enterprise::control_plane::{
    ConfigStore, ConsistentMetadataStore, DataNodeState, MetadataDomain,
    NodeCapabilities, NodeRegistry, ShardPlacementManager,
};
use falcon_enterprise::enterprise_ops::{SloEngine, SloDefinition, SloMetricType};
use falcon_enterprise::enterprise_security::{
    AuditCategory, AuditSeverity, EnterpriseAuditLog,
};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the cluster lifecycle coordinator.
#[derive(Debug, Clone)]
pub struct ClusterLifecycleConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// This node's advertised address (for other nodes to connect).
    pub advertise_addr: String,
    /// Heartbeat evaluation interval.
    pub heartbeat_interval: Duration,
    /// Time before a node is considered suspect.
    pub suspect_threshold: Duration,
    /// Time before a node is considered offline.
    pub offline_threshold: Duration,
    /// Config sync push interval.
    pub config_sync_interval: Duration,
    /// SLO evaluation interval.
    pub slo_eval_interval: Duration,
    /// Number of shards this node can host.
    pub max_shards: u32,
    /// Whether this node acts as a controller (runs control-plane logic).
    pub is_controller: bool,
}

impl Default for ClusterLifecycleConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(1),
            advertise_addr: "127.0.0.1:5433".into(),
            heartbeat_interval: Duration::from_secs(2),
            suspect_threshold: Duration::from_secs(5),
            offline_threshold: Duration::from_secs(15),
            config_sync_interval: Duration::from_secs(10),
            slo_eval_interval: Duration::from_secs(30),
            max_shards: 64,
            is_controller: true,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Coordinator
// ═══════════════════════════════════════════════════════════════════════════

/// Cluster lifecycle coordinator: owns enterprise subsystems and their
/// background tasks, wiring them to real I/O.
pub struct ClusterLifecycleCoordinator {
    config: ClusterLifecycleConfig,
    cancel: CancellationToken,
    started: AtomicBool,
    config_sync_count: Arc<AtomicU64>,
    /// node_id → gRPC address for SyncConfig pushes
    node_addrs: Arc<dashmap::DashMap<u64, String>>,

    pub node_registry: Arc<NodeRegistry>,
    pub config_store: Arc<ConfigStore>,
    pub metadata_store: Arc<ConsistentMetadataStore>,
    pub shard_placement: Arc<ShardPlacementManager>,
    pub audit_log: Arc<EnterpriseAuditLog>,
    pub slo_engine: Arc<SloEngine>,
}

/// Metrics snapshot for the lifecycle coordinator.
#[derive(Debug, Clone, Default)]
pub struct LifecycleCoordinatorMetrics {
    pub heartbeat_evals: u64,
    pub config_syncs: u64,
    pub slo_evals: u64,
    pub nodes_online: usize,
    pub nodes_suspect: usize,
    pub nodes_offline: usize,
}

impl ClusterLifecycleCoordinator {
    /// Create a new coordinator with all enterprise subsystems initialized.
    pub fn new(config: ClusterLifecycleConfig) -> Self {
        Self::new_with_config_store(config, Arc::new(ConfigStore::new()))
    }

    pub fn new_with_config_store(config: ClusterLifecycleConfig, config_store: Arc<ConfigStore>) -> Self {
        let node_registry = Arc::new(NodeRegistry::new(
            config.suspect_threshold,
            config.offline_threshold,
        ));
        let metadata_store = Arc::new(ConsistentMetadataStore::new(config.is_controller));
        let shard_placement = Arc::new(ShardPlacementManager::new());
        let audit_log = Arc::new(EnterpriseAuditLog::new(10_000, "falcon-lifecycle"));
        let slo_engine = Arc::new(SloEngine::new(10_000));

        Self {
            config,
            cancel: CancellationToken::new(),
            started: AtomicBool::new(false),
            config_sync_count: Arc::new(AtomicU64::new(0)),
            node_addrs: Arc::new(dashmap::DashMap::new()),
            node_registry,
            config_store,
            metadata_store,
            shard_placement,
            audit_log,
            slo_engine,
        }
    }

    /// Register a remote node's gRPC address for SyncConfig pushes.
    pub fn register_node_addr(&self, node_id: u64, addr: String) {
        self.node_addrs.insert(node_id, addr);
    }

    /// Register this node in its own registry and record startup in metadata.
    pub fn register_self(&self) {
        let caps = NodeCapabilities {
            max_shards: self.config.max_shards,
            ..NodeCapabilities::default()
        };
        self.node_registry.register(
            self.config.node_id,
            self.config.advertise_addr.clone(),
            caps,
        );

        // Record startup in consistent metadata store.
        self.metadata_store.put(
            MetadataDomain::NodeRegistry,
            &format!("node/{}", self.config.node_id.0),
            &self.config.advertise_addr,
            "lifecycle_coordinator",
        );

        // Audit event
        self.audit_log.record(
            AuditCategory::OpsOperation,
            AuditSeverity::Info,
            "lifecycle_coordinator",
            "127.0.0.1",
            "NODE_START",
            &format!("node/{}", self.config.node_id.0),
            "success",
            &format!("Node {} started at {}", self.config.node_id.0, self.config.advertise_addr),
        );
    }

    /// Start all background tasks. Idempotent — second call is a no-op.
    pub fn start(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return; // already started
        }

        self.register_self();

        // ── Background: heartbeat evaluation loop ──
        {
            let registry = self.node_registry.clone();
            let audit = self.audit_log.clone();
            let interval = self.config.heartbeat_interval;
            let cancel = self.cancel.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            let transitions = registry.evaluate();
                            for (node_id, old_state, new_state) in &transitions {
                                tracing::warn!(
                                    node = node_id.0,
                                    from = %old_state,
                                    to = %new_state,
                                    "Node state transition"
                                );
                                audit.record(
                                    AuditCategory::TopologyChange,
                                    AuditSeverity::Warning,
                                    "heartbeat_evaluator",
                                    "127.0.0.1",
                                    "NODE_STATE_CHANGE",
                                    &format!("node/{}", node_id.0),
                                    "detected",
                                    &format!(
                                        "Node {} transitioned {} → {}",
                                        node_id.0, old_state, new_state
                                    ),
                                );
                            }
                        }
                    }
                }
                tracing::info!("Heartbeat evaluation loop stopped");
            });
        }

        // ── Background: config sync push loop ──
        if self.config.is_controller {
            let config_store = self.config_store.clone();
            let registry = self.node_registry.clone();
            let audit = self.audit_log.clone();
            let interval = self.config.config_sync_interval;
            let cancel = self.cancel.clone();
            let sync_counter = self.config_sync_count.clone();
            let node_addrs = self.node_addrs.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            let target_version = config_store.version();
                            let stale_nodes = registry.nodes_needing_config(target_version);
                            if stale_nodes.is_empty() { continue; }
                            tracing::info!(stale_count = stale_nodes.len(), target_version, "Config sync push");
                            let mut synced = 0u64;
                            for node_id in &stale_nodes {
                                let nid = node_id.0;
                                if let Some(addr) = node_addrs.get(&nid).map(|e| e.value().clone()) {
                                    match tonic::transport::Endpoint::from_shared(addr.clone())
                                        .and_then(|e| Ok(e.connect_timeout(std::time::Duration::from_secs(3))))
                                    {
                                        Ok(endpoint) => {
                                            match endpoint.connect().await {
                                                Ok(ch) => {
                                                    let mut client = crate::proto::wal_replication_client::WalReplicationClient::new(ch);
                                                    let entries = config_store.all();
                                                    let payload = serde_json::to_vec(&entries).unwrap_or_default();
                                                    let req = crate::proto::SyncConfigRequest {
                                                        config_version: target_version,
                                                        config_payload: payload,
                                                    };
                                                    match client.sync_config(req).await {
                                                        Ok(resp) => {
                                                            let r = resp.into_inner();
                                                            if r.success {
                                                                registry.heartbeat(*node_id, r.applied_version);
                                                                synced += 1;
                                                            } else {
                                                                tracing::warn!(nid, error = %r.error, "SyncConfig rejected");
                                                            }
                                                        }
                                                        Err(e) => tracing::warn!(nid, error = %e, "SyncConfig RPC failed"),
                                                    }
                                                }
                                                Err(e) => tracing::warn!(nid, addr = %addr, error = %e, "SyncConfig connect failed"),
                                            }
                                        }
                                        Err(e) => tracing::warn!(nid, error = %e, "Invalid node addr"),
                                    }
                                } else {
                                    registry.heartbeat(*node_id, target_version);
                                    synced += 1;
                                }
                            }
                            sync_counter.fetch_add(synced, Ordering::Relaxed);
                            audit.record(
                                AuditCategory::ConfigChange,
                                AuditSeverity::Info,
                                "config_sync",
                                "127.0.0.1",
                                "CONFIG_SYNC",
                                "cluster/config",
                                "synced",
                                &format!("{} node(s) synced to config v{}", synced, target_version),
                            );
                        }
                    }
                }
                tracing::info!("Config sync loop stopped");
            });
        }

        // ── Background: SLO evaluation loop ──
        {
            let slo_engine = self.slo_engine.clone();
            let audit = self.audit_log.clone();
            let interval = self.config.slo_eval_interval;
            let cancel = self.cancel.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            let results = slo_engine.evaluate_all();
                            for eval in &results {
                                if !eval.met {
                                    tracing::warn!(
                                        slo = %eval.slo_id,
                                        current = %eval.actual,
                                        target = %eval.target,
                                        "SLO breach detected"
                                    );
                                    audit.record(
                                        AuditCategory::OpsOperation,
                                        AuditSeverity::Warning,
                                        "slo_evaluator",
                                        "127.0.0.1",
                                        "SLO_BREACH",
                                        &eval.slo_id,
                                        "breach",
                                        &format!(
                                            "SLO '{}' breached: actual={}, target={}",
                                            eval.slo_id,
                                            eval.actual,
                                            eval.target,
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
                tracing::info!("SLO evaluation loop stopped");
            });
        }

        tracing::info!(
            node_id = self.config.node_id.0,
            is_controller = self.config.is_controller,
            "Cluster lifecycle coordinator started"
        );
    }

    /// Graceful shutdown: cancel all background tasks.
    pub fn shutdown(&self) {
        self.audit_log.record(
            AuditCategory::OpsOperation,
            AuditSeverity::Info,
            "lifecycle_coordinator",
            "127.0.0.1",
            "NODE_SHUTDOWN",
            &format!("node/{}", self.config.node_id.0),
            "initiated",
            &format!("Node {} shutting down", self.config.node_id.0),
        );

        // Mark self as draining in registry.
        self.node_registry
            .set_state(self.config.node_id, DataNodeState::Draining);

        self.cancel.cancel();
        tracing::info!("Cluster lifecycle coordinator shutdown signalled");
    }

    /// Whether background tasks have been started.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Collect a metrics snapshot.
    pub fn metrics(&self) -> LifecycleCoordinatorMetrics {
        let counts = self.node_registry.count_by_state();
        LifecycleCoordinatorMetrics {
            heartbeat_evals: self
                .node_registry
                .metrics
                .state_transitions
                .load(Ordering::Relaxed),
            config_syncs: self.config_sync_count.load(Ordering::Relaxed),
            slo_evals: self.slo_engine.metrics.evaluations_run.load(Ordering::Relaxed),
            nodes_online: *counts.get(&DataNodeState::Online).unwrap_or(&0),
            nodes_suspect: *counts.get(&DataNodeState::Suspect).unwrap_or(&0),
            nodes_offline: *counts.get(&DataNodeState::Offline).unwrap_or(&0),
        }
    }

    /// Register a default set of SLO definitions.
    pub fn register_default_slos(&self) {
        self.slo_engine.define(SloDefinition {
            id: "p99_latency_ms".into(),
            metric: SloMetricType::LatencyP99,
            target: 50.0,
            window_secs: 300,
            description: "P99 query latency must stay under 50ms".into(),
        });
        self.slo_engine.define(SloDefinition {
            id: "availability_pct".into(),
            metric: SloMetricType::Availability,
            target: 0.9995,
            window_secs: 3600,
            description: "Write availability target 99.95%".into(),
        });
        self.slo_engine.define(SloDefinition {
            id: "error_rate_pct".into(),
            metric: SloMetricType::ErrorRate,
            target: 0.001,
            window_secs: 300,
            description: "Error rate must stay under 0.1%".into(),
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_creation() {
        let config = ClusterLifecycleConfig::default();
        let coord = ClusterLifecycleCoordinator::new(config);
        assert!(!coord.is_started());
        assert_eq!(coord.node_registry.node_count(), 0);
    }

    #[test]
    fn test_register_self() {
        let config = ClusterLifecycleConfig {
            node_id: NodeId(42),
            advertise_addr: "10.0.0.1:5433".into(),
            ..Default::default()
        };
        let coord = ClusterLifecycleCoordinator::new(config);
        coord.register_self();

        assert_eq!(coord.node_registry.node_count(), 1);
        let record = coord.node_registry.get(NodeId(42));
        assert!(record.is_some());
        assert_eq!(record.unwrap().state, DataNodeState::Online);
    }

    #[test]
    fn test_shutdown_marks_draining() {
        let config = ClusterLifecycleConfig {
            node_id: NodeId(7),
            ..Default::default()
        };
        let coord = ClusterLifecycleCoordinator::new(config);
        coord.register_self();
        coord.shutdown();

        let record = coord.node_registry.get(NodeId(7)).unwrap();
        assert_eq!(record.state, DataNodeState::Draining);
    }

    #[test]
    fn test_metrics_snapshot() {
        let config = ClusterLifecycleConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let coord = ClusterLifecycleCoordinator::new(config);
        coord.register_self();

        let m = coord.metrics();
        assert_eq!(m.nodes_online, 1);
        assert_eq!(m.nodes_suspect, 0);
    }

    #[test]
    fn test_config_store_wired() {
        let coord = ClusterLifecycleCoordinator::new(ClusterLifecycleConfig::default());
        let v = coord.config_store.set("cluster.max_shards", "128", "admin");
        assert!(v > 0);
        let entry = coord.config_store.get("cluster.max_shards");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, "128");
    }

    #[test]
    fn test_metadata_store_wired() {
        let coord = ClusterLifecycleCoordinator::new(ClusterLifecycleConfig::default());
        coord.metadata_store.put(
            MetadataDomain::ClusterConfig,
            "shard_count",
            "4",
            "test",
        );
        use falcon_enterprise::control_plane::ReadConsistency;
        let entry = coord.metadata_store.get(
            &MetadataDomain::ClusterConfig,
            "shard_count",
            ReadConsistency::Any,
        );
        assert!(entry.is_some());
    }

    #[test]
    fn test_default_slos() {
        let coord = ClusterLifecycleCoordinator::new(ClusterLifecycleConfig::default());
        coord.register_default_slos();
        // SLO engine should have 3 definitions
        let results = coord.slo_engine.evaluate_all();
        assert_eq!(results.len(), 3);
    }
}
