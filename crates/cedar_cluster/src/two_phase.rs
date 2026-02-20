//! Two-Phase Commit (2PC) coordinator for multi-shard write transactions.
//!
//! Ensures atomicity across shards: either ALL shards commit or ALL abort.
//!
//! Protocol:
//! 1. **Prepare phase**: Each participant shard executes the write and
//!    transitions to Prepared state. If any shard fails, all abort.
//! 2. **Commit phase**: Coordinator sends Commit to all prepared shards.
//!    If commit fails on any shard after prepare, the coordinator logs
//!    the inconsistency (in production, a recovery log resolves this).

use std::sync::Arc;
use std::time::{Duration, Instant};

use cedar_common::error::CedarError;
use cedar_common::types::ShardId;
use cedar_storage::engine::StorageEngine;
use cedar_txn::manager::TxnManager;

use crate::sharded_engine::ShardedEngine;

/// Outcome of a single shard's participation in a 2PC transaction.
#[derive(Debug)]
pub struct ShardParticipant {
    pub shard_id: ShardId,
    /// The per-shard txn_id allocated during prepare.
    pub txn_id: cedar_common::types::TxnId,
    /// Whether this shard successfully prepared.
    pub prepared: bool,
}

/// Result of a 2PC transaction.
#[derive(Debug)]
pub struct TwoPhaseResult {
    pub committed: bool,
    pub participants: Vec<ShardParticipant>,
    pub prepare_latency_us: u64,
    pub commit_latency_us: u64,
}

/// Coordinates two-phase commit across multiple shards.
pub struct TwoPhaseCoordinator {
    engine: Arc<ShardedEngine>,
    timeout: Duration,
}

impl TwoPhaseCoordinator {
    pub fn new(engine: Arc<ShardedEngine>, timeout: Duration) -> Self {
        Self { engine, timeout }
    }

    /// Execute a multi-shard write transaction with 2PC.
    ///
    /// `write_fn` is called on each shard with (storage, txn_mgr, txn_id).
    /// It should execute the write operations but NOT commit — the coordinator
    /// handles commit/abort.
    ///
    /// Returns `TwoPhaseResult` indicating overall commit/abort.
    pub fn execute<F>(
        &self,
        target_shards: &[ShardId],
        isolation: cedar_common::types::IsolationLevel,
        write_fn: F,
    ) -> Result<TwoPhaseResult, CedarError>
    where
        F: Fn(
                &StorageEngine,
                &TxnManager,
                cedar_common::types::TxnId,
            ) -> Result<(), CedarError>
            + Send
            + Sync,
    {
        let total_start = Instant::now();

        // ── Phase 1: Prepare ──
        // Begin transactions on all shards and execute writes.
        let mut participants: Vec<ShardParticipant> = Vec::with_capacity(target_shards.len());
        let mut all_prepared = true;

        for &shard_id in target_shards {
            let shard = self.engine.shard(shard_id).ok_or_else(|| {
                CedarError::Internal(format!("Shard {:?} not found", shard_id))
            })?;

            let txn = shard.txn_mgr.begin(isolation);
            let txn_id = txn.txn_id;

            // Execute the write on this shard
            match write_fn(&shard.storage, &shard.txn_mgr, txn_id) {
                Ok(()) => {
                    participants.push(ShardParticipant {
                        shard_id,
                        txn_id,
                        prepared: true,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        "2PC prepare failed on shard {:?}: {}",
                        shard_id,
                        e
                    );
                    // Mark as not prepared
                    participants.push(ShardParticipant {
                        shard_id,
                        txn_id,
                        prepared: false,
                    });
                    all_prepared = false;
                    break;
                }
            }

            // Check timeout
            if total_start.elapsed() > self.timeout {
                all_prepared = false;
                break;
            }
        }

        let prepare_latency_us = total_start.elapsed().as_micros() as u64;

        // ── Phase 2: Commit or Abort ──
        let commit_start = Instant::now();

        if all_prepared {
            // All shards prepared — commit all.
            for p in &participants {
                let shard = self.engine.shard(p.shard_id).unwrap();
                if let Err(e) = shard.txn_mgr.commit(p.txn_id) {
                    // In production, this would be logged to a recovery journal.
                    // The coordinator would retry or trigger manual resolution.
                    tracing::error!(
                        "2PC commit failed on shard {:?} after prepare: {}",
                        p.shard_id,
                        e
                    );
                    // Continue committing other shards — cannot undo a commit.
                }
            }
        } else {
            // At least one shard failed — abort all.
            for p in &participants {
                if p.prepared {
                    let shard = self.engine.shard(p.shard_id).unwrap();
                    let _ = shard.txn_mgr.abort(p.txn_id);
                }
            }
        }

        let commit_latency_us = commit_start.elapsed().as_micros() as u64;

        Ok(TwoPhaseResult {
            committed: all_prepared,
            participants,
            prepare_latency_us,
            commit_latency_us,
        })
    }
}
