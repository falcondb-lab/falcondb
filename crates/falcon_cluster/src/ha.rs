//! High-Availability: automatic failover, failure detection, epoch fencing,
//! multi-replica management, and synchronous replication modes.
//!
//! # Design (SingleStore-inspired)
//!
//! - **Failure detection**: heartbeat-based with configurable timeout.
//!   Each replica sends periodic heartbeats; if the primary misses N
//!   consecutive heartbeats, it is declared failed.
//! - **Epoch fencing**: monotonically increasing epoch numbers prevent
//!   stale primaries from accepting writes after a failover.
//! - **Best-replica election**: on failover, the replica with the highest
//!   applied LSN is promoted (minimizes data loss).
//! - **Automatic failover orchestrator**: background thread monitors
//!   replica health and triggers promotion when failure is detected.
//! - **Sync replication modes**: Async (default), SemiSync (wait for 1
//!   replica ack), Sync (wait for all replicas).
//!
//! # Architecture
//!
//! ```text
//!   FailoverOrchestrator (background thread)
//!       |
//!       +-- FailureDetector (per-replica heartbeat tracking)
//!       |
//!       +-- HAReplicaGroup (multi-replica management + epoch)
//!       |       |
//!       |       +-- ReplicaHealth (per-replica status)
//!       |       +-- best_replica_for_promotion()
//!       |       +-- promote_best() with epoch bump
//!       |
//!       +-- SyncMode (Async / SemiSync / Sync)
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_common::shutdown::ShutdownSignal;
use falcon_common::types::{NodeId, ShardId};
use falcon_storage::engine::StorageEngine;

use crate::replication::promote::ShardReplicaGroup;
use crate::replication::replica_state::{ReplicaNode, ReplicaRole};
use crate::replication::wal_stream::ReplicationLog;
use crate::replication::ReplicationMetrics;
use crate::routing::shard_map::ShardMap;

// ---------------------------------------------------------------------------
// HA Configuration
// ---------------------------------------------------------------------------

/// Replication synchronization mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Fire-and-forget: commit returns as soon as primary WAL is durable.
    /// Fastest, but may lose committed data if primary crashes before
    /// replica catches up (RPO > 0).
    #[default]
    Async,
    /// Semi-synchronous: commit waits for at least one replica to ack.
    /// Balances durability and latency.
    SemiSync,
    /// Fully synchronous: commit waits for ALL replicas to ack.
    /// Strongest durability guarantee but highest latency.
    Sync,
}

/// High-availability configuration.
#[derive(Debug, Clone)]
pub struct HAConfig {
    /// Heartbeat interval (how often replicas ping the orchestrator).
    pub heartbeat_interval: Duration,
    /// Failure timeout: if no heartbeat for this duration, node is failed.
    pub failure_timeout: Duration,
    /// Minimum number of healthy replicas before auto-failover is armed.
    pub min_replicas_for_failover: usize,
    /// Number of replicas per shard (default 1).
    pub replica_count: usize,
    /// Replication synchronization mode.
    pub sync_mode: SyncMode,
    /// Whether automatic failover is enabled.
    pub auto_failover_enabled: bool,
    /// Cooldown after a failover before another can be triggered.
    pub failover_cooldown: Duration,
    /// Maximum replication lag (in LSNs) before a replica is considered
    /// unhealthy for promotion purposes.
    pub max_promotion_lag: u64,
}

impl Default for HAConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(1),
            failure_timeout: Duration::from_secs(5),
            min_replicas_for_failover: 1,
            replica_count: 1,
            sync_mode: SyncMode::Async,
            auto_failover_enabled: true,
            failover_cooldown: Duration::from_secs(30),
            max_promotion_lag: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// Failure Detector
// ---------------------------------------------------------------------------

/// Health status of a single replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHealthStatus {
    /// Healthy: heartbeat received within timeout.
    Healthy,
    /// Suspect: heartbeat late but not yet timed out.
    Suspect,
    /// Failed: no heartbeat for longer than failure_timeout.
    Failed,
    /// Unknown: no heartbeat ever received.
    Unknown,
}

/// Per-replica health tracking.
#[derive(Debug, Clone)]
pub struct ReplicaHealth {
    pub replica_idx: usize,
    pub status: ReplicaHealthStatus,
    pub last_heartbeat: Option<Instant>,
    pub applied_lsn: u64,
    pub replication_lag: u64,
    pub consecutive_misses: u32,
}

/// Failure detector: tracks heartbeats from each replica and the primary.
pub struct FailureDetector {
    config: HAConfig,
    /// Per-node health: key = node index (0 = primary, 1..N = replicas).
    health: Mutex<Vec<ReplicaHealth>>,
    /// Primary health.
    primary_health: Mutex<PrimaryHealth>,
}

/// Health tracking for the primary node.
#[derive(Debug, Clone)]
pub struct PrimaryHealth {
    pub status: ReplicaHealthStatus,
    pub last_heartbeat: Option<Instant>,
    pub consecutive_misses: u32,
    pub current_lsn: u64,
}

impl FailureDetector {
    pub fn new(config: HAConfig, num_replicas: usize) -> Self {
        let health: Vec<ReplicaHealth> = (0..num_replicas)
            .map(|i| ReplicaHealth {
                replica_idx: i,
                status: ReplicaHealthStatus::Unknown,
                last_heartbeat: None,
                applied_lsn: 0,
                replication_lag: 0,
                consecutive_misses: 0,
            })
            .collect();

        Self {
            config,
            health: Mutex::new(health),
            primary_health: Mutex::new(PrimaryHealth {
                status: ReplicaHealthStatus::Healthy,
                last_heartbeat: Some(Instant::now()),
                consecutive_misses: 0,
                current_lsn: 0,
            }),
        }
    }

    /// Record a heartbeat from a replica.
    pub fn record_replica_heartbeat(&self, replica_idx: usize, applied_lsn: u64, primary_lsn: u64) {
        let mut health = self.health.lock();
        if let Some(h) = health.get_mut(replica_idx) {
            h.last_heartbeat = Some(Instant::now());
            h.applied_lsn = applied_lsn;
            h.replication_lag = primary_lsn.saturating_sub(applied_lsn);
            h.consecutive_misses = 0;
            h.status = ReplicaHealthStatus::Healthy;
        }
    }

    /// Record a heartbeat from the primary.
    pub fn record_primary_heartbeat(&self, current_lsn: u64) {
        let mut ph = self.primary_health.lock();
        ph.last_heartbeat = Some(Instant::now());
        ph.current_lsn = current_lsn;
        ph.consecutive_misses = 0;
        ph.status = ReplicaHealthStatus::Healthy;
    }

    /// Evaluate all health statuses based on current time.
    /// Returns true if the primary is considered failed.
    pub fn evaluate(&self) -> bool {
        let now = Instant::now();
        let suspect_threshold = self.config.failure_timeout / 2;

        // Evaluate primary health
        {
            let mut ph = self.primary_health.lock();
            if let Some(last) = ph.last_heartbeat {
                let elapsed = now.duration_since(last);
                if elapsed > self.config.failure_timeout {
                    ph.status = ReplicaHealthStatus::Failed;
                    ph.consecutive_misses += 1;
                    return true;
                } else if elapsed > suspect_threshold {
                    ph.status = ReplicaHealthStatus::Suspect;
                    ph.consecutive_misses += 1;
                } else {
                    ph.status = ReplicaHealthStatus::Healthy;
                    ph.consecutive_misses = 0;
                }
            }
        }

        // Evaluate replica health
        {
            let mut health = self.health.lock();
            for h in health.iter_mut() {
                if let Some(last) = h.last_heartbeat {
                    let elapsed = now.duration_since(last);
                    if elapsed > self.config.failure_timeout {
                        h.status = ReplicaHealthStatus::Failed;
                        h.consecutive_misses += 1;
                    } else if elapsed > suspect_threshold {
                        h.status = ReplicaHealthStatus::Suspect;
                        h.consecutive_misses += 1;
                    } else {
                        h.status = ReplicaHealthStatus::Healthy;
                        h.consecutive_misses = 0;
                    }
                }
            }
        }

        false
    }

    /// Check if the primary is considered failed.
    pub fn is_primary_failed(&self) -> bool {
        self.primary_health.lock().status == ReplicaHealthStatus::Failed
    }

    /// Get the health status of all replicas.
    pub fn replica_health_snapshot(&self) -> Vec<ReplicaHealth> {
        self.health.lock().clone()
    }

    /// Get the primary health status.
    pub fn primary_health_snapshot(&self) -> PrimaryHealth {
        self.primary_health.lock().clone()
    }

    /// Count healthy replicas.
    pub fn healthy_replica_count(&self) -> usize {
        self.health
            .lock()
            .iter()
            .filter(|h| h.status == ReplicaHealthStatus::Healthy)
            .count()
    }

    /// Find the best replica for promotion: highest applied_lsn among healthy replicas.
    /// Returns the replica index, or None if no healthy replica exists.
    pub fn best_replica_for_promotion(&self) -> Option<usize> {
        let health = self.health.lock();
        health
            .iter()
            .filter(|h| {
                h.status == ReplicaHealthStatus::Healthy
                    && h.replication_lag <= self.config.max_promotion_lag
            })
            .max_by_key(|h| h.applied_lsn)
            .map(|h| h.replica_idx)
    }
}

// ---------------------------------------------------------------------------
// HA Replica Group (extends ShardReplicaGroup with epochs + multi-replica)
// ---------------------------------------------------------------------------

/// Extended replica group with epoch fencing and multi-replica support.
pub struct HAReplicaGroup {
    pub inner: ShardReplicaGroup,
    /// Monotonically increasing epoch: bumped on every failover.
    /// Stale primaries with lower epoch must reject writes.
    pub epoch: AtomicU64,
    /// Failure detector for this shard's replicas.
    pub detector: FailureDetector,
    /// HA configuration.
    pub config: HAConfig,
    /// Last failover time (for cooldown enforcement).
    pub last_failover: Mutex<Option<Instant>>,
}

impl HAReplicaGroup {
    /// Create an HA replica group with configurable replica count.
    pub fn new(
        shard_id: ShardId,
        schemas: &[TableSchema],
        config: HAConfig,
    ) -> Result<Self, FalconError> {
        let primary_storage = Arc::new(StorageEngine::new_in_memory());
        for schema in schemas {
            primary_storage.create_table(schema.clone())?;
        }

        let mut replicas = Vec::new();
        for _ in 0..config.replica_count {
            let replica_storage = Arc::new(StorageEngine::new_in_memory());
            for schema in schemas {
                replica_storage.create_table(schema.clone())?;
            }
            replicas.push(Arc::new(ReplicaNode::new_replica(replica_storage)));
        }

        let primary = Arc::new(ReplicaNode::new_primary(primary_storage));
        let num_replicas = replicas.len();

        let inner = ShardReplicaGroup {
            shard_id,
            primary,
            replicas,
            log: Arc::new(ReplicationLog::new()),
            metrics: ReplicationMetrics::new(),
        };

        let detector = FailureDetector::new(config.clone(), num_replicas);

        Ok(Self {
            inner,
            epoch: AtomicU64::new(1),
            detector,
            config,
            last_failover: Mutex::new(None),
        })
    }

    /// Current epoch number.
    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Bump the epoch (called during failover).
    pub fn bump_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Validate that a write request has a valid epoch (not stale).
    pub fn validate_epoch(&self, request_epoch: u64) -> bool {
        request_epoch >= self.current_epoch()
    }

    /// Record a heartbeat from a replica.
    pub fn heartbeat_replica(&self, replica_idx: usize) {
        let primary_lsn = self.inner.log.current_lsn();
        let applied_lsn = self
            .inner
            .replicas
            .get(replica_idx)
            .map(|r| r.current_lsn())
            .unwrap_or(0);
        self.detector
            .record_replica_heartbeat(replica_idx, applied_lsn, primary_lsn);
    }

    /// Record a heartbeat from the primary.
    pub fn heartbeat_primary(&self) {
        let primary_lsn = self.inner.log.current_lsn();
        self.detector.record_primary_heartbeat(primary_lsn);
    }

    /// Catch up all replicas to latest LSN.
    pub fn catch_up_all_replicas(&self) -> Result<usize, FalconError> {
        let mut total = 0;
        for i in 0..self.inner.replicas.len() {
            total += self.inner.catch_up_replica(i)?;
        }
        Ok(total)
    }

    /// Promote the best available replica to primary.
    /// Returns the index of the promoted replica, or error if no suitable candidate.
    pub fn promote_best(&mut self) -> Result<usize, FalconError> {
        // Cooldown check
        {
            let last = self.last_failover.lock();
            if let Some(last_time) = *last {
                if last_time.elapsed() < self.config.failover_cooldown {
                    return Err(FalconError::Internal(
                        "Failover cooldown not elapsed".into(),
                    ));
                }
            }
        }

        // Update health data from actual LSNs
        let primary_lsn = self.inner.log.current_lsn();
        for (i, replica) in self.inner.replicas.iter().enumerate() {
            self.detector
                .record_replica_heartbeat(i, replica.current_lsn(), primary_lsn);
        }

        // Find best replica
        let best_idx = self.detector.best_replica_for_promotion().ok_or_else(|| {
            FalconError::Internal("No healthy replica available for promotion".into())
        })?;

        // Bump epoch before promotion
        let new_epoch = self.bump_epoch();
        tracing::info!(
            "Shard {:?}: auto-failover starting, epoch {} -> {}, promoting replica {}",
            self.inner.shard_id,
            new_epoch - 1,
            new_epoch,
            best_idx,
        );

        // Promote
        self.inner.promote(best_idx)?;

        // Update last failover time
        *self.last_failover.lock() = Some(Instant::now());

        tracing::info!(
            "Shard {:?}: auto-failover complete, new primary is replica {}",
            self.inner.shard_id,
            best_idx,
        );

        Ok(best_idx)
    }

    /// Promote best replica and update the shard routing map.
    pub fn promote_best_with_routing(
        &mut self,
        shard_map: &mut ShardMap,
        new_leader_node: NodeId,
    ) -> Result<usize, FalconError> {
        let best_idx = self.promote_best()?;
        shard_map.update_leader(self.inner.shard_id, new_leader_node);
        Ok(best_idx)
    }

    /// Add a new replica to this shard group.
    pub fn add_replica(&mut self, schemas: &[TableSchema]) -> Result<usize, FalconError> {
        let storage = Arc::new(StorageEngine::new_in_memory());
        for schema in schemas {
            storage.create_table(schema.clone())?;
        }
        let replica = Arc::new(ReplicaNode::new_replica(storage));
        self.inner.replicas.push(replica);
        let idx = self.inner.replicas.len() - 1;

        // Catch up the new replica immediately
        self.inner.catch_up_replica(idx)?;

        // Update detector health tracking
        let primary_lsn = self.inner.log.current_lsn();
        self.detector.record_replica_heartbeat(
            idx,
            self.inner.replicas[idx].current_lsn(),
            primary_lsn,
        );

        tracing::info!(
            "Shard {:?}: added replica {} (total replicas: {})",
            self.inner.shard_id,
            idx,
            self.inner.replicas.len(),
        );

        Ok(idx)
    }

    /// Remove a replica from this shard group.
    /// Cannot remove the last replica if auto-failover is enabled.
    pub fn remove_replica(&mut self, replica_idx: usize) -> Result<(), FalconError> {
        if replica_idx >= self.inner.replicas.len() {
            return Err(FalconError::Internal(format!(
                "Replica index {} out of range (have {})",
                replica_idx,
                self.inner.replicas.len()
            )));
        }
        if self.config.auto_failover_enabled && self.inner.replicas.len() <= 1 {
            return Err(FalconError::Internal(
                "Cannot remove last replica when auto-failover is enabled".into(),
            ));
        }
        self.inner.replicas.remove(replica_idx);
        Ok(())
    }

    /// Get the replication lag for all replicas.
    pub fn replication_lag(&self) -> Vec<(usize, u64)> {
        self.inner.replication_lag()
    }

    /// Get a snapshot of HA status for observability.
    pub fn ha_status(&self) -> HAStatus {
        let primary_lsn = self.inner.log.current_lsn();
        let replica_statuses: Vec<HAReplicaStatus> = self
            .inner
            .replicas
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let health = self.detector.replica_health_snapshot();
                let h = health.get(i).cloned().unwrap_or(ReplicaHealth {
                    replica_idx: i,
                    status: ReplicaHealthStatus::Unknown,
                    last_heartbeat: None,
                    applied_lsn: 0,
                    replication_lag: 0,
                    consecutive_misses: 0,
                });
                HAReplicaStatus {
                    replica_idx: i,
                    role: r.current_role(),
                    applied_lsn: r.current_lsn(),
                    replication_lag: primary_lsn.saturating_sub(r.current_lsn()),
                    health_status: h.status,
                    is_read_only: r.is_read_only(),
                }
            })
            .collect();

        HAStatus {
            shard_id: self.inner.shard_id,
            epoch: self.current_epoch(),
            primary_lsn,
            primary_read_only: self.inner.primary.is_read_only(),
            replica_count: self.inner.replicas.len(),
            healthy_replicas: self.detector.healthy_replica_count(),
            sync_mode: self.config.sync_mode,
            auto_failover_enabled: self.config.auto_failover_enabled,
            replicas: replica_statuses,
            total_promotes: self.inner.metrics.snapshot().promote_count,
        }
    }
}

/// Snapshot of HA status for a single shard.
#[derive(Debug, Clone)]
pub struct HAStatus {
    pub shard_id: ShardId,
    pub epoch: u64,
    pub primary_lsn: u64,
    pub primary_read_only: bool,
    pub replica_count: usize,
    pub healthy_replicas: usize,
    pub sync_mode: SyncMode,
    pub auto_failover_enabled: bool,
    pub replicas: Vec<HAReplicaStatus>,
    pub total_promotes: u64,
}

/// Per-replica status in HA snapshot.
#[derive(Debug, Clone)]
pub struct HAReplicaStatus {
    pub replica_idx: usize,
    pub role: ReplicaRole,
    pub applied_lsn: u64,
    pub replication_lag: u64,
    pub health_status: ReplicaHealthStatus,
    pub is_read_only: bool,
}

// ---------------------------------------------------------------------------
// Automatic Failover Orchestrator
// ---------------------------------------------------------------------------

/// Configuration for the failover orchestrator background thread.
#[derive(Debug, Clone)]
pub struct FailoverOrchestratorConfig {
    /// How often to check health (milliseconds).
    pub check_interval_ms: u64,
    /// Whether the orchestrator is enabled.
    pub enabled: bool,
}

impl Default for FailoverOrchestratorConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 1000,
            enabled: true,
        }
    }
}

/// Handle returned by `FailoverOrchestrator::start()`. Dropping stops the thread.
pub struct FailoverOrchestratorHandle {
    signal: ShutdownSignal,
    join_handle: Option<std::thread::JoinHandle<()>>,
    /// Metrics exposed by the orchestrator.
    pub metrics: Arc<FailoverOrchestratorMetrics>,
}

impl FailoverOrchestratorHandle {
    /// Signal the orchestrator to stop.
    pub fn stop(&self) {
        self.signal.shutdown();
    }

    /// Stop and wait for the thread to finish.
    pub fn stop_and_join(mut self) {
        self.signal.shutdown();
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }
    }

    pub fn is_running(&self) -> bool {
        !self.signal.is_shutdown()
    }
}

impl Drop for FailoverOrchestratorHandle {
    fn drop(&mut self) {
        self.signal.shutdown();
    }
}

/// Metrics for the failover orchestrator.
pub struct FailoverOrchestratorMetrics {
    pub health_checks: AtomicU64,
    pub failovers_triggered: AtomicU64,
    pub failovers_succeeded: AtomicU64,
    pub failovers_failed: AtomicU64,
}

impl Default for FailoverOrchestratorMetrics {
    fn default() -> Self {
        Self {
            health_checks: AtomicU64::new(0),
            failovers_triggered: AtomicU64::new(0),
            failovers_succeeded: AtomicU64::new(0),
            failovers_failed: AtomicU64::new(0),
        }
    }
}

/// Automatic failover orchestrator.
///
/// Runs as a background thread, periodically:
/// 1. Sends heartbeats (simulated in same-process mode).
/// 2. Evaluates failure detector.
/// 3. If primary is failed and enough healthy replicas exist, triggers promote_best().
pub struct FailoverOrchestrator;

impl FailoverOrchestrator {
    /// Start the failover orchestrator for a single HA replica group.
    ///
    /// Returns `Err` if the background thread cannot be spawned (OS resource
    /// exhaustion). The caller must handle this as a startup-fatal or
    /// runtime-degraded condition — the process must **not** panic.
    pub fn start(
        config: FailoverOrchestratorConfig,
        group: Arc<RwLock<HAReplicaGroup>>,
    ) -> Result<FailoverOrchestratorHandle, FalconError> {
        let signal = ShutdownSignal::new();
        let signal_clone = signal.clone();
        let interval = Duration::from_millis(config.check_interval_ms);
        let metrics = Arc::new(FailoverOrchestratorMetrics::default());
        let metrics_clone = metrics.clone();

        let join_handle = std::thread::Builder::new()
            .name("falcon-failover-orchestrator".to_string())
            .spawn(move || {
                tracing::info!(
                    "FailoverOrchestrator started (interval={}ms)",
                    config.check_interval_ms
                );
                while !signal_clone.is_shutdown() {
                    if signal_clone.wait_timeout(interval) {
                        break;
                    }

                    metrics_clone.health_checks.fetch_add(1, Ordering::Relaxed);

                    // Read phase: evaluate health
                    let should_failover = {
                        let group_read = group.read();
                        // Simulate heartbeats in same-process mode
                        group_read.heartbeat_primary();
                        for i in 0..group_read.inner.replicas.len() {
                            group_read.heartbeat_replica(i);
                        }
                        // Evaluate
                        let primary_failed = group_read.detector.evaluate();
                        let healthy_count = group_read.detector.healthy_replica_count();
                        let min_replicas = group_read.config.min_replicas_for_failover;
                        let auto_enabled = group_read.config.auto_failover_enabled;

                        auto_enabled && primary_failed && healthy_count >= min_replicas
                    };

                    if should_failover {
                        metrics_clone
                            .failovers_triggered
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            "FailoverOrchestrator: primary failure detected, triggering failover"
                        );

                        let mut group_write = group.write();
                        match group_write.promote_best() {
                            Ok(idx) => {
                                metrics_clone
                                    .failovers_succeeded
                                    .fetch_add(1, Ordering::Relaxed);
                                tracing::info!(
                                    "FailoverOrchestrator: failover succeeded, promoted replica {}",
                                    idx
                                );
                            }
                            Err(e) => {
                                metrics_clone
                                    .failovers_failed
                                    .fetch_add(1, Ordering::Relaxed);
                                tracing::error!("FailoverOrchestrator: failover failed: {}", e);
                            }
                        }
                    }
                }
                tracing::info!("FailoverOrchestrator stopped");
            })
            .map_err(|e| {
                tracing::error!(
                    component = "failover-orchestrator",
                    error = %e,
                    "failed to spawn background thread — node DEGRADED"
                );
                FalconError::Internal(format!(
                    "failed to spawn failover orchestrator thread: {}",
                    e
                ))
            })?;

        Ok(FailoverOrchestratorHandle {
            signal,
            join_handle: Some(join_handle),
            metrics,
        })
    }
}

// ---------------------------------------------------------------------------
// Sync Replication Waiter
// ---------------------------------------------------------------------------

/// Tracks which replicas have acked a given LSN for synchronous replication.
pub struct SyncReplicationWaiter {
    config: HAConfig,
}

impl SyncReplicationWaiter {
    pub fn new(config: HAConfig) -> Self {
        Self { config }
    }

    /// Wait for replication of a commit at `commit_lsn` according to the sync mode.
    /// `replica_lsns` provides the current applied LSN of each replica.
    ///
    /// Returns Ok(()) when the required ack threshold is met.
    /// In Async mode, returns immediately.
    pub fn wait_for_commit(
        &self,
        commit_lsn: u64,
        group: &HAReplicaGroup,
        timeout: Duration,
    ) -> Result<(), FalconError> {
        match self.config.sync_mode {
            SyncMode::Async => Ok(()),
            SyncMode::SemiSync => self.wait_for_n_replicas(commit_lsn, group, 1, timeout),
            SyncMode::Sync => {
                let n = group.inner.replicas.len();
                self.wait_for_n_replicas(commit_lsn, group, n, timeout)
            }
        }
    }

    fn wait_for_n_replicas(
        &self,
        commit_lsn: u64,
        group: &HAReplicaGroup,
        required: usize,
        timeout: Duration,
    ) -> Result<(), FalconError> {
        let start = Instant::now();
        let poll_interval = Duration::from_millis(1);
        // Use a local condvar so this poll is interruptible by timeout
        // without blocking the calling thread with a bare sleep.
        let pair = std::sync::Mutex::new(false);
        let cvar = std::sync::Condvar::new();

        loop {
            // Count how many replicas have applied at least commit_lsn
            let acked = group
                .inner
                .replicas
                .iter()
                .filter(|r| r.current_lsn() >= commit_lsn)
                .count();

            if acked >= required {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(FalconError::Internal(format!(
                    "Sync replication timeout: {}/{} replicas acked LSN {} within {:?}",
                    acked, required, commit_lsn, timeout,
                )));
            }

            // Interruptible wait: uses condvar timeout instead of bare sleep.
            // The condvar is never notified, so this always times out after
            // poll_interval — but it yields the thread properly and is
            // interruptible by the OS scheduler (unlike a busy-spin).
            let guard = pair.lock().unwrap_or_else(|e| e.into_inner());
            let _ = cvar.wait_timeout(guard, poll_interval);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::ColumnDef;
    use falcon_common::types::*;
    use falcon_storage::wal::WalRecord;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    fn insert_and_replicate(group: &HAReplicaGroup, id: i32, val: &str, txn: u64) {
        let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(val.into())]);
        group
            .inner
            .primary
            .storage
            .insert(TableId(1), row.clone(), TxnId(txn))
            .unwrap();
        group
            .inner
            .primary
            .storage
            .commit_txn(TxnId(txn), Timestamp(txn), TxnType::Local)
            .unwrap();
        group.inner.ship_wal_record(WalRecord::Insert {
            txn_id: TxnId(txn),
            table_id: TableId(1),
            row,
        });
        group.inner.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(txn),
            commit_ts: Timestamp(txn),
        });
    }

    #[test]
    fn test_ha_group_creation_with_multiple_replicas() {
        let config = HAConfig {
            replica_count: 3,
            ..Default::default()
        };
        let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();
        assert_eq!(group.inner.replicas.len(), 3);
        assert_eq!(group.current_epoch(), 1);
        assert!(!group.inner.primary.is_read_only());
        for r in &group.inner.replicas {
            assert!(r.is_read_only());
        }
    }

    #[test]
    fn test_epoch_bumps_on_failover() {
        let config = HAConfig {
            replica_count: 2,
            failover_cooldown: Duration::from_millis(0),
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();
        assert_eq!(group.current_epoch(), 1);

        insert_and_replicate(&group, 1, "a", 1);
        group.catch_up_all_replicas().unwrap();
        group.promote_best().unwrap();

        assert_eq!(group.current_epoch(), 2);
    }

    #[test]
    fn test_validate_epoch() {
        let config = HAConfig {
            replica_count: 1,
            ..Default::default()
        };
        let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();
        assert!(group.validate_epoch(1));
        assert!(group.validate_epoch(2));
        assert!(!group.validate_epoch(0));
    }

    #[test]
    fn test_best_replica_selection_by_lsn() {
        let config = HAConfig {
            replica_count: 3,
            failover_cooldown: Duration::from_millis(0),
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

        // Insert data and only catch up replica 1 (not 0 or 2)
        insert_and_replicate(&group, 1, "a", 1);
        insert_and_replicate(&group, 2, "b", 2);
        group.inner.catch_up_replica(1).unwrap(); // replica 1 is most up-to-date

        // Record heartbeats
        let primary_lsn = group.inner.log.current_lsn();
        for i in 0..3 {
            let lsn = group.inner.replicas[i].current_lsn();
            group.detector.record_replica_heartbeat(i, lsn, primary_lsn);
        }

        let best = group.detector.best_replica_for_promotion();
        assert_eq!(best, Some(1), "should pick replica 1 (highest LSN)");
    }

    #[test]
    fn test_promote_best_selects_highest_lsn() {
        let config = HAConfig {
            replica_count: 2,
            failover_cooldown: Duration::from_millis(0),
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

        insert_and_replicate(&group, 1, "a", 1);
        insert_and_replicate(&group, 2, "b", 2);

        // Only catch up replica 0 fully, replica 1 partially
        group.inner.catch_up_replica(0).unwrap();

        let promoted = group.promote_best().unwrap();
        assert_eq!(promoted, 0, "should promote replica 0 (most up-to-date)");
        assert!(!group.inner.primary.is_read_only());
    }

    #[test]
    fn test_add_replica() {
        let config = HAConfig {
            replica_count: 1,
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();
        assert_eq!(group.inner.replicas.len(), 1);

        insert_and_replicate(&group, 1, "a", 1);
        group.catch_up_all_replicas().unwrap();

        let idx = group.add_replica(&[test_schema()]).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(group.inner.replicas.len(), 2);
        // New replica should have caught up
        assert!(group.inner.replicas[1].current_lsn() > 0);
    }

    #[test]
    fn test_remove_replica() {
        let config = HAConfig {
            replica_count: 2,
            auto_failover_enabled: false,
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();
        assert_eq!(group.inner.replicas.len(), 2);

        group.remove_replica(0).unwrap();
        assert_eq!(group.inner.replicas.len(), 1);
    }

    #[test]
    fn test_cannot_remove_last_replica_with_auto_failover() {
        let config = HAConfig {
            replica_count: 1,
            auto_failover_enabled: true,
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();
        assert!(group.remove_replica(0).is_err());
    }

    #[test]
    fn test_ha_status_snapshot() {
        let config = HAConfig {
            replica_count: 2,
            ..Default::default()
        };
        let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

        let status = group.ha_status();
        assert_eq!(status.shard_id, ShardId(0));
        assert_eq!(status.epoch, 1);
        assert_eq!(status.replica_count, 2);
        assert!(!status.primary_read_only);
        assert!(status.auto_failover_enabled);
        assert_eq!(status.replicas.len(), 2);
    }

    #[test]
    fn test_failure_detector_healthy_by_default() {
        let config = HAConfig::default();
        let detector = FailureDetector::new(config, 2);

        // Record initial heartbeats
        detector.record_replica_heartbeat(0, 0, 0);
        detector.record_replica_heartbeat(1, 0, 0);
        detector.record_primary_heartbeat(0);

        assert!(!detector.evaluate());
        assert!(!detector.is_primary_failed());
        assert_eq!(detector.healthy_replica_count(), 2);
    }

    #[test]
    fn test_failure_detector_detects_primary_failure() {
        let config = HAConfig {
            failure_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let detector = FailureDetector::new(config, 1);
        detector.record_primary_heartbeat(0);
        detector.record_replica_heartbeat(0, 0, 0);

        // Wait for failure timeout
        std::thread::sleep(Duration::from_millis(60));

        let primary_failed = detector.evaluate();
        assert!(primary_failed, "primary should be detected as failed");
        assert!(detector.is_primary_failed());
    }

    #[test]
    fn test_sync_replication_async_returns_immediately() {
        let config = HAConfig {
            sync_mode: SyncMode::Async,
            replica_count: 1,
            ..Default::default()
        };
        let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config.clone()).unwrap();
        let waiter = SyncReplicationWaiter::new(config);
        assert!(waiter
            .wait_for_commit(100, &group, Duration::from_millis(10))
            .is_ok());
    }

    #[test]
    fn test_sync_replication_semi_sync() {
        let config = HAConfig {
            sync_mode: SyncMode::SemiSync,
            replica_count: 2,
            ..Default::default()
        };
        let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config.clone()).unwrap();

        // Insert and replicate to both replicas
        insert_and_replicate(&group, 1, "a", 1);
        group.inner.catch_up_replica(0).unwrap();

        let waiter = SyncReplicationWaiter::new(config);
        let commit_lsn = group.inner.replicas[0].current_lsn();
        // At least 1 replica has acked, so semi-sync should succeed
        assert!(waiter
            .wait_for_commit(commit_lsn, &group, Duration::from_millis(100))
            .is_ok());
    }

    #[test]
    fn test_sync_replication_timeout() {
        let config = HAConfig {
            sync_mode: SyncMode::Sync,
            replica_count: 1,
            ..Default::default()
        };
        let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config.clone()).unwrap();
        let waiter = SyncReplicationWaiter::new(config);
        // Replica hasn't caught up to LSN 999, so sync should timeout
        let result = waiter.wait_for_commit(999, &group, Duration::from_millis(50));
        assert!(
            result.is_err(),
            "should timeout when replicas haven't caught up"
        );
    }

    #[test]
    fn test_cooldown_prevents_rapid_failover() {
        let config = HAConfig {
            replica_count: 2,
            failover_cooldown: Duration::from_secs(30), // long cooldown
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

        insert_and_replicate(&group, 1, "a", 1);
        group.catch_up_all_replicas().unwrap();

        // First failover should succeed
        // Force cooldown to be zero for first one
        *group.last_failover.lock() = None;
        group.promote_best().unwrap();

        // Second failover should fail due to cooldown
        insert_and_replicate(&group, 2, "b", 2);
        group.catch_up_all_replicas().unwrap();
        let result = group.promote_best();
        assert!(result.is_err(), "should fail due to cooldown");
    }

    #[test]
    fn test_data_survives_ha_failover() {
        let config = HAConfig {
            replica_count: 2,
            failover_cooldown: Duration::from_millis(0),
            ..Default::default()
        };
        let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

        // Write data
        for i in 1..=5 {
            insert_and_replicate(&group, i, &format!("val_{}", i), i as u64);
        }
        group.catch_up_all_replicas().unwrap();

        // Failover
        group.promote_best().unwrap();

        // Verify data on new primary
        let rows = group
            .inner
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 5, "all data should survive failover");
    }
}
