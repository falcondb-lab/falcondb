//! Primary-Replica replication via WAL shipping.
//!
//! Architecture:
//! - Each shard has 1 Primary and N Replicas.
//! - Primary writes WAL records and ships them via `ReplicationTransport`.
//! - Replicas subscribe from their last `applied_lsn` and apply in order.
//! - Ack-based tracking: replicas periodically ack their applied_lsn to primary.
//! - Promote: manual trigger to promote a replica to primary with read-only fencing.
//!
//! Transport layer is abstracted via `ReplicationTransport` trait:
//! - `InProcessTransport`: same-process simulation (direct memory access).
//! - `ChannelTransport`: tokio mpsc channel-based (simulates cross-process gRPC streaming).
//!
//! Commit ack semantics (M1, Scheme A):
//! - commit ack = primary WAL durable (fsync). Does NOT wait for replica.
//! - RPO may be > 0 if primary crashes before replica catches up.

pub mod catchup;
pub mod promote;
pub mod replica_state;
pub mod runner;
pub mod wal_stream;

// Re-export all public types at the module level for backward compatibility.
pub use catchup::{apply_wal_record_to_engine, WriteOp};
pub use promote::ShardReplicaGroup;
pub use replica_state::{ReplicaNode, ReplicaRole};
pub use runner::{
    ReplicaRunner, ReplicaRunnerConfig, ReplicaRunnerHandle, ReplicaRunnerMetrics,
    ReplicaRunnerMetricsSnapshot,
};
pub use wal_stream::{
    AsyncReplicationTransport, ChannelTransport, InProcessTransport, LsnWalRecord, ReplicationLog,
    ReplicationTransport, WalChunk,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// ReplicationMetrics — observability counters
// ---------------------------------------------------------------------------

/// Replication metrics for observability (per shard).
#[derive(Debug)]
pub struct ReplicationMetrics {
    pub promote_count: AtomicU64,
    pub last_failover_time_ms: AtomicU64,
    failover_start: Mutex<Option<Instant>>,
}

impl Default for ReplicationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationMetrics {
    pub fn new() -> Self {
        Self {
            promote_count: AtomicU64::new(0),
            last_failover_time_ms: AtomicU64::new(0),
            failover_start: Mutex::new(None),
        }
    }

    /// Mark the start of a failover operation.
    pub fn start_failover(&self) {
        *self.failover_start.lock() = Some(Instant::now());
    }

    /// Mark the end of a failover, recording duration.
    pub fn complete_failover(&self) {
        if let Some(start) = self.failover_start.lock().take() {
            let ms = start.elapsed().as_millis() as u64;
            self.last_failover_time_ms.store(ms, Ordering::SeqCst);
        }
        self.promote_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> ReplicationMetricsSnapshot {
        ReplicationMetricsSnapshot {
            promote_count: self.promote_count.load(Ordering::SeqCst),
            last_failover_time_ms: self.last_failover_time_ms.load(Ordering::SeqCst),
        }
    }
}

/// Snapshot of replication metrics for reporting.
#[derive(Debug, Clone, Default)]
pub struct ReplicationMetricsSnapshot {
    pub promote_count: u64,
    pub last_failover_time_ms: u64,
}
