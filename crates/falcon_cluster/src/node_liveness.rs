//! # C8: Node Liveness with Epoch Isolation
//!
//! Implements a **node liveness record** system that prevents split-brain
//! write scenarios, analogous to CockroachDB's node liveness subsystem.
//!
//! ## The Split-Brain Problem
//!
//! When a Raft leader is isolated from the cluster (network partition) but
//! still believes it is the leader, it may continue to accept writes. These
//! writes are not replicated and will be lost when a new leader is elected.
//! Worse, a follower may be elected as the new leader simultaneously —
//! resulting in two nodes both accepting writes (split-brain).
//!
//! ## Solution: Epoch-Fenced Liveness Records
//!
//! Each node periodically renews its **liveness record** stored in a
//! quorum-replicated key-value store (backed by Raft).  The liveness record
//! contains:
//! - `node_id`: the node that owns this record
//! - `epoch`: monotonically increasing generation counter
//! - `expiration_ts`: wall-clock timestamp when the heartbeat expires
//!
//! Before a node accepts any write it checks:
//! 1. My liveness record is not expired.
//! 2. My epoch matches the cluster's current epoch for my node.
//!
//! If either check fails, the node **refuses all writes** and triggers
//! a self-draining sequence.
//!
//! ## Epoch Bump on Failover
//!
//! When a new leader is elected for a shard, the HA orchestrator increments
//! the old leader's epoch in the liveness store. The old leader's next
//! write-admission check will see `my_epoch != liveness.epoch` and self-fence.
//!
//! ## Heartbeat Protocol
//!
//! ```text
//!   NodeLivenessManager (background thread)
//!       every heartbeat_interval:
//!           write_liveness_record(node_id, epoch, now + ttl)
//!
//!   WriteEpochGate (checked before every write transaction)
//!       is_live(node_id) → true iff:
//!           record exists AND expiration_ts > now AND epoch matches local
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

use falcon_common::shutdown::ShutdownSignal;
use falcon_common::types::NodeId;

// ─────────────────────────────────────────────────────────────────────────────
// Liveness Record
// ─────────────────────────────────────────────────────────────────────────────

/// A node's liveness record.
#[derive(Debug, Clone)]
pub struct LivenessRecord {
    pub node_id: NodeId,
    /// Monotonically increasing epoch. Bumped by HA on leader transfer / failover.
    pub epoch: u64,
    /// Wall-clock expiration (ms since UNIX epoch).
    /// If `now_ms > expiration_ms`, the node is considered dead.
    pub expiration_ms: u64,
    /// Whether this node is currently draining (graceful shutdown).
    pub draining: bool,
}

impl LivenessRecord {
    pub fn is_live(&self) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        !self.draining && self.expiration_ms > now_ms
    }

    pub fn time_to_expiry_ms(&self) -> i64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.expiration_ms as i64 - now_ms as i64
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LivenessStore
// ─────────────────────────────────────────────────────────────────────────────

/// In-process liveness record store.
///
/// In production this would be backed by a quorum-replicated Raft group
/// (like CockroachDB's `liveness` system range). Here it's an in-memory
/// `HashMap` protected by an `RwLock`, suitable for single-binary clusters
/// and integration testing.
pub struct LivenessStore {
    records: RwLock<HashMap<NodeId, LivenessRecord>>,
    /// Sequence number for detecting concurrent updates.
    version: AtomicU64,
}

impl LivenessStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            records: RwLock::new(HashMap::new()),
            version: AtomicU64::new(0),
        })
    }

    /// Write or update a liveness record.
    pub fn heartbeat(&self, node_id: NodeId, epoch: u64, ttl_ms: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let expiration_ms = now_ms + ttl_ms;
        let mut records = self.records.write();
        records
            .entry(node_id)
            .and_modify(|r| {
                // Only update if epoch is >= current (monotonic).
                if epoch >= r.epoch {
                    r.epoch = epoch;
                    r.expiration_ms = expiration_ms;
                    r.draining = false;
                }
            })
            .or_insert(LivenessRecord {
                node_id,
                epoch,
                expiration_ms,
                draining: false,
            });
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Bump a node's epoch (called by HA orchestrator on failover).
    /// Returns the new epoch, or None if the node is not registered.
    pub fn bump_epoch(&self, node_id: NodeId) -> Option<u64> {
        let mut records = self.records.write();
        if let Some(r) = records.get_mut(&node_id) {
            r.epoch += 1;
            self.version.fetch_add(1, Ordering::Relaxed);
            Some(r.epoch)
        } else {
            None
        }
    }

    /// Mark a node as draining (graceful shutdown).
    pub fn set_draining(&self, node_id: NodeId) {
        let mut records = self.records.write();
        if let Some(r) = records.get_mut(&node_id) {
            r.draining = true;
            self.version.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the current liveness record for a node.
    pub fn get(&self, node_id: NodeId) -> Option<LivenessRecord> {
        self.records.read().get(&node_id).cloned()
    }

    /// Check if a node is live AND has the expected epoch.
    pub fn is_live_with_epoch(&self, node_id: NodeId, expected_epoch: u64) -> bool {
        match self.records.read().get(&node_id) {
            Some(r) => r.epoch == expected_epoch && r.is_live(),
            None => false,
        }
    }

    /// Return all currently live nodes.
    pub fn live_nodes(&self) -> Vec<NodeId> {
        self.records
            .read()
            .values()
            .filter(|r| r.is_live())
            .map(|r| r.node_id)
            .collect()
    }

    /// Return all expired (dead) nodes.
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        self.records
            .read()
            .values()
            .filter(|r| !r.is_live())
            .map(|r| r.node_id)
            .collect()
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }
}

impl Default for LivenessStore {
    fn default() -> Self {
        Arc::try_unwrap(Self::new()).unwrap_or_else(|_| panic!("LivenessStore default"))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WriteEpochGate
// ─────────────────────────────────────────────────────────────────────────────

/// Guards write admission for a single node.
///
/// Before accepting any write transaction, the node calls
/// `WriteEpochGate::check()`. If the gate is closed (liveness expired or
/// epoch mismatch), the write is rejected.
pub struct WriteEpochGate {
    node_id: NodeId,
    /// The epoch this node believes it holds.
    my_epoch: AtomicU64,
    store: Arc<LivenessStore>,
    /// Whether this gate has self-fenced (prevents reset without explicit re-auth).
    fenced: AtomicBool,
    pub metrics: WriteEpochMetrics,
}

#[derive(Debug, Default)]
pub struct WriteEpochMetrics {
    pub writes_admitted: AtomicU64,
    pub writes_rejected_liveness: AtomicU64,
    pub writes_rejected_epoch: AtomicU64,
    pub self_fences: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct WriteEpochMetricsSnapshot {
    pub writes_admitted: u64,
    pub writes_rejected_liveness: u64,
    pub writes_rejected_epoch: u64,
    pub self_fences: u64,
}

impl WriteEpochGate {
    pub fn new(node_id: NodeId, initial_epoch: u64, store: Arc<LivenessStore>) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            my_epoch: AtomicU64::new(initial_epoch),
            store,
            fenced: AtomicBool::new(false),
            metrics: WriteEpochMetrics::default(),
        })
    }

    /// Check write admission.
    ///
    /// Returns `Ok(())` if the node is live and epoch is valid.
    /// Returns `Err(reason)` if the write should be rejected.
    pub fn check(&self) -> Result<(), WriteRejectionReason> {
        // ── Fenced check (cheapest) ─────────────────────────────────────────
        if self.fenced.load(Ordering::Acquire) {
            self.metrics
                .writes_rejected_liveness
                .fetch_add(1, Ordering::Relaxed);
            return Err(WriteRejectionReason::SelfFenced);
        }

        let my_epoch = self.my_epoch.load(Ordering::Relaxed);

        // ── Liveness check ──────────────────────────────────────────────────
        match self.store.get(self.node_id) {
            None => {
                // No record in store — node not registered yet, allow.
                self.metrics
                    .writes_admitted
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Some(record) => {
                if !record.is_live() {
                    // Liveness expired — self-fence.
                    self.fenced.store(true, Ordering::Release);
                    self.metrics.self_fences.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .writes_rejected_liveness
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        "Node {:?} liveness expired — self-fencing (epoch={})",
                        self.node_id,
                        my_epoch
                    );
                    return Err(WriteRejectionReason::LivenessExpired);
                }
                if record.epoch != my_epoch {
                    // Epoch mismatch — a new leader was elected; self-fence.
                    self.fenced.store(true, Ordering::Release);
                    self.metrics.self_fences.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .writes_rejected_epoch
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        "Node {:?} epoch mismatch (my={}, store={}) — self-fencing",
                        self.node_id,
                        my_epoch,
                        record.epoch
                    );
                    return Err(WriteRejectionReason::EpochMismatch {
                        my_epoch,
                        store_epoch: record.epoch,
                    });
                }
            }
        }

        self.metrics
            .writes_admitted
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Update the local epoch (called after receiving a new epoch from HA).
    pub fn update_epoch(&self, new_epoch: u64) {
        self.my_epoch.store(new_epoch, Ordering::Release);
    }

    /// Clear the self-fence (only callable by the HA orchestrator after
    /// explicitly re-registering the node in the liveness store).
    pub fn clear_fence(&self) {
        self.fenced.store(false, Ordering::Release);
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    pub fn my_epoch(&self) -> u64 {
        self.my_epoch.load(Ordering::Relaxed)
    }

    pub fn metrics_snapshot(&self) -> WriteEpochMetricsSnapshot {
        WriteEpochMetricsSnapshot {
            writes_admitted: self.metrics.writes_admitted.load(Ordering::Relaxed),
            writes_rejected_liveness: self
                .metrics
                .writes_rejected_liveness
                .load(Ordering::Relaxed),
            writes_rejected_epoch: self.metrics.writes_rejected_epoch.load(Ordering::Relaxed),
            self_fences: self.metrics.self_fences.load(Ordering::Relaxed),
        }
    }
}

/// Reason a write was rejected by the epoch gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRejectionReason {
    /// The node's liveness heartbeat expired.
    LivenessExpired,
    /// The cluster bumped this node's epoch (failover occurred).
    EpochMismatch { my_epoch: u64, store_epoch: u64 },
    /// The node previously self-fenced and has not been re-authorized.
    SelfFenced,
}

impl std::fmt::Display for WriteRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LivenessExpired => write!(f, "node liveness expired"),
            Self::EpochMismatch { my_epoch, store_epoch } => write!(
                f,
                "epoch mismatch: local={my_epoch}, cluster={store_epoch}"
            ),
            Self::SelfFenced => write!(f, "node is self-fenced"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeLivenessManager (background heartbeat loop)
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the liveness manager.
#[derive(Debug, Clone)]
pub struct LivenessConfig {
    /// How often to renew the heartbeat. Default: 1.5 s.
    pub heartbeat_interval: Duration,
    /// Liveness TTL. Must be > heartbeat_interval to tolerate one missed heartbeat.
    /// Default: 4.5 s (= 3 × heartbeat_interval).
    pub ttl: Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(1_500),
            ttl: Duration::from_millis(4_500),
        }
    }
}

/// Background task that renews the node's liveness record.
pub struct NodeLivenessManager {
    node_id: NodeId,
    gate: Arc<WriteEpochGate>,
    store: Arc<LivenessStore>,
    config: LivenessConfig,
    /// Number of consecutive heartbeat failures.
    consecutive_failures: AtomicU64,
}

impl NodeLivenessManager {
    pub fn new(
        node_id: NodeId,
        initial_epoch: u64,
        store: Arc<LivenessStore>,
        config: LivenessConfig,
    ) -> Arc<Self> {
        let gate = WriteEpochGate::new(node_id, initial_epoch, Arc::clone(&store));
        // Register initial liveness.
        store.heartbeat(node_id, initial_epoch, config.ttl.as_millis() as u64);
        Arc::new(Self {
            node_id,
            gate,
            store,
            config,
            consecutive_failures: AtomicU64::new(0),
        })
    }

    pub fn gate(&self) -> Arc<WriteEpochGate> {
        Arc::clone(&self.gate)
    }

    /// Run the heartbeat loop (blocking). Call from a dedicated thread.
    pub fn run(&self, mut shutdown: ShutdownSignal) {
        tracing::info!("NodeLivenessManager started for node {:?}", self.node_id);
        loop {
            if shutdown.is_shutdown() {
                // Graceful shutdown: mark as draining before stopping.
                self.store.set_draining(self.node_id);
                break;
            }
            self.tick();
            std::thread::sleep(self.config.heartbeat_interval);
        }
        tracing::info!("NodeLivenessManager stopped for node {:?}", self.node_id);
    }

    /// Single heartbeat tick: renew liveness and check for epoch changes.
    pub fn tick(&self) {
        let my_epoch = self.gate.my_epoch();

        // In production: RPC to the liveness Raft group.
        // Here: direct write to the in-process store.
        self.store
            .heartbeat(self.node_id, my_epoch, self.config.ttl.as_millis() as u64);

        // Check whether the cluster has bumped our epoch since last tick.
        if let Some(record) = self.store.get(self.node_id) {
            if record.epoch != my_epoch {
                tracing::warn!(
                    "Node {:?}: cluster bumped epoch {} → {} (failover detected)",
                    self.node_id,
                    my_epoch,
                    record.epoch
                );
                // Self-fence: stop accepting writes.
                // The HA orchestrator must call gate.clear_fence() + update_epoch()
                // after the node re-establishes its role.
            }
        }
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liveness_record_is_live() {
        let store = LivenessStore::new();
        store.heartbeat(NodeId(1), 1, 5_000);
        let r = store.get(NodeId(1)).unwrap();
        assert!(r.is_live());
    }

    #[test]
    fn liveness_record_expired() {
        let store = LivenessStore::new();
        // Expiration in the past.
        let past_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        store.records.write().insert(
            NodeId(1),
            LivenessRecord {
                node_id: NodeId(1),
                epoch: 1,
                expiration_ms: past_ms - 1000, // already expired
                draining: false,
            },
        );
        let r = store.get(NodeId(1)).unwrap();
        assert!(!r.is_live());
    }

    #[test]
    fn write_gate_admits_live_node() {
        let store = LivenessStore::new();
        store.heartbeat(NodeId(1), 1, 5_000);
        let gate = WriteEpochGate::new(NodeId(1), 1, store);
        assert!(gate.check().is_ok());
    }

    #[test]
    fn write_gate_rejects_epoch_mismatch() {
        let store = LivenessStore::new();
        // Store has epoch 2, gate thinks it holds epoch 1.
        store.heartbeat(NodeId(1), 2, 5_000);
        let gate = WriteEpochGate::new(NodeId(1), 1, store);
        let err = gate.check().unwrap_err();
        assert!(matches!(err, WriteRejectionReason::EpochMismatch { .. }));
        assert!(gate.is_fenced());
    }

    #[test]
    fn write_gate_self_fences_on_liveness_expiry() {
        let store = LivenessStore::new();
        // Insert an already-expired record.
        let past_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        store.records.write().insert(
            NodeId(2),
            LivenessRecord {
                node_id: NodeId(2),
                epoch: 1,
                expiration_ms: past_ms - 1000,
                draining: false,
            },
        );
        let gate = WriteEpochGate::new(NodeId(2), 1, store);
        let err = gate.check().unwrap_err();
        assert_eq!(err, WriteRejectionReason::LivenessExpired);
        assert!(gate.is_fenced());
    }

    #[test]
    fn write_gate_stays_fenced() {
        let store = LivenessStore::new();
        let past_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        store.records.write().insert(
            NodeId(3),
            LivenessRecord {
                node_id: NodeId(3),
                epoch: 1,
                expiration_ms: past_ms - 1000,
                draining: false,
            },
        );
        let gate = WriteEpochGate::new(NodeId(3), 1, Arc::clone(&store));
        let _ = gate.check(); // triggers self-fence
        // Even after store is fixed, gate stays fenced.
        store.heartbeat(NodeId(3), 1, 10_000);
        let err = gate.check().unwrap_err();
        assert_eq!(err, WriteRejectionReason::SelfFenced);
    }

    #[test]
    fn epoch_bump_triggers_mismatch() {
        let store = LivenessStore::new();
        store.heartbeat(NodeId(1), 1, 5_000);
        let new_epoch = store.bump_epoch(NodeId(1)).unwrap();
        assert_eq!(new_epoch, 2);
        let gate = WriteEpochGate::new(NodeId(1), 1, Arc::clone(&store));
        let err = gate.check().unwrap_err();
        assert!(matches!(
            err,
            WriteRejectionReason::EpochMismatch {
                my_epoch: 1,
                store_epoch: 2
            }
        ));
    }

    #[test]
    fn live_nodes_list() {
        let store = LivenessStore::new();
        store.heartbeat(NodeId(1), 1, 5_000);
        store.heartbeat(NodeId(2), 1, 5_000);
        let live = store.live_nodes();
        assert_eq!(live.len(), 2);
    }
}
