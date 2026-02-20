//! Replica node state: role, LSN tracking, read-only fencing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use cedar_storage::engine::StorageEngine;

/// Role of a replication node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    Primary,
    Replica,
}

/// State of a single replica node.
pub struct ReplicaNode {
    pub role: RwLock<ReplicaRole>,
    pub storage: Arc<StorageEngine>,
    /// The LSN up to which this node has applied WAL records.
    pub applied_lsn: AtomicU64,
    /// Read-only fence: when true, this node rejects writes.
    pub read_only: AtomicBool,
}

impl ReplicaNode {
    pub fn new_primary(storage: Arc<StorageEngine>) -> Self {
        Self {
            role: RwLock::new(ReplicaRole::Primary),
            storage,
            applied_lsn: AtomicU64::new(0),
            read_only: AtomicBool::new(false),
        }
    }

    pub fn new_replica(storage: Arc<StorageEngine>) -> Self {
        Self {
            role: RwLock::new(ReplicaRole::Replica),
            storage,
            applied_lsn: AtomicU64::new(0),
            read_only: AtomicBool::new(true),
        }
    }

    pub fn current_role(&self) -> ReplicaRole {
        *self.role.read()
    }

    pub fn current_lsn(&self) -> u64 {
        self.applied_lsn.load(Ordering::SeqCst)
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }

    /// Fence this node: mark read-only so no new writes are accepted.
    pub fn fence(&self) {
        self.read_only.store(true, Ordering::SeqCst);
    }

    /// Unfence this node: allow writes (used when promoted to primary).
    pub fn unfence(&self) {
        self.read_only.store(false, Ordering::SeqCst);
    }
}
