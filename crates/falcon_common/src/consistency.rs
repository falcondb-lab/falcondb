//! Consistency Semantics Module for FalconDB.
//!
//! This module formalizes the consistency boundaries of the system:
//! - **Commit Points**: Logical, Durable, and Client-Visible commit stages.
//! - **Commit Policies**: Configurable durability guarantees per-transaction.
//! - **WAL Invariants**: Formal properties the WAL must uphold.
//! - **Replication Invariants**: Prefix property, apply ordering, ACK semantics.
//! - **Failover Rules**: Promote preconditions and post-promote guarantees.
//! - **Read Semantics**: Isolation levels and replica read staleness.
//! - **Error Classification**: Determinate vs indeterminate commit outcomes.
//!
//! Every invariant in this module has a corresponding test in the consistency
//! test suite (`tests/consistency_*.rs`).

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::types::{Timestamp, TxnId};

// ═══════════════════════════════════════════════════════════════════════════
// §1: Commit Point Model
// ═══════════════════════════════════════════════════════════════════════════

/// The three distinct commit points a transaction passes through.
///
/// **Invariant CP-1**: These three points are strictly ordered:
///   `LogicalCommit ≤ DurableCommit ≤ ClientVisibleCommit`
///
/// **Invariant CP-2**: A transaction MUST NOT become client-visible before
///   it is durable under the active commit policy.
///
/// **Invariant CP-3**: A crash after LogicalCommit but before DurableCommit
///   MAY lose the transaction (depending on the commit policy).
///
/// **Invariant CP-4**: A crash after DurableCommit MUST NOT lose the
///   transaction under any recovery path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CommitPoint {
    /// Transaction marked as committed in MVCC (in-memory).
    /// Visible to concurrent readers but NOT yet durable.
    LogicalCommit,

    /// WAL record has reached the durability threshold defined by the
    /// active [`CommitPolicy`]. The transaction is crash-safe.
    DurableCommit,

    /// Client has received the success response. The transaction is
    /// externally observable and MUST NOT be rolled back.
    ClientVisibleCommit,
}

impl fmt::Display for CommitPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogicalCommit => write!(f, "logical_commit"),
            Self::DurableCommit => write!(f, "durable_commit"),
            Self::ClientVisibleCommit => write!(f, "client_visible_commit"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2: Commit Policy
// ═══════════════════════════════════════════════════════════════════════════

/// Commit policy defines what I/O must complete before a commit becomes
/// [`CommitPoint::DurableCommit`] and can be acknowledged to the client.
///
/// **Invariant POL-1**: The system MUST NOT send a client ACK until ALL
///   I/O operations required by the active policy have completed.
///
/// **Invariant POL-2**: If the active policy cannot be satisfied (e.g. no
///   replicas available for `PrimaryPlusReplicaAck`), the system MUST either:
///   (a) degrade to a weaker policy and flag the transaction as `degraded`, or
///   (b) reject the transaction with an explicit error.
///
/// **Invariant POL-3**: Policy degradation MUST be observable via metrics
///   and the `DurabilityAnnotation` on the transaction record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum CommitPolicy {
    /// Local WAL fsync only. Crash-safe on the primary.
    /// - Client ACK after: local WAL fsync
    /// - Crash loss window: 0 (local)
    /// - Replica lag: unbounded
    #[default]
    LocalWalSync,

    /// Primary WAL written (buffered, no fsync). Fastest, weakest.
    /// - Client ACK after: WAL buffer write (no sync)
    /// - Crash loss window: up to `flush_interval_us` of data
    /// - Use case: analytics ingestion, non-critical writes
    PrimaryWalOnly,

    /// Primary WAL fsync + N replica ACKs.
    /// - Client ACK after: local fsync AND N replicas have acked
    /// - Crash loss window: 0 if ≥1 replica survives
    /// - `required_acks`: number of replicas that must ACK
    PrimaryPlusReplicaAck {
        required_acks: u32,
    },

    /// Raft majority commit (if Raft consensus is enabled).
    /// - Client ACK after: Raft log entry committed by majority
    /// - Crash loss window: 0 (Raft guarantee)
    RaftMajority,
}


impl fmt::Display for CommitPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalWalSync => write!(f, "LOCAL_WAL_SYNC"),
            Self::PrimaryWalOnly => write!(f, "PRIMARY_WAL_ONLY"),
            Self::PrimaryPlusReplicaAck { required_acks } => {
                write!(f, "PRIMARY_PLUS_REPLICA_ACK({})", required_acks)
            }
            Self::RaftMajority => write!(f, "RAFT_MAJORITY"),
        }
    }
}

impl CommitPolicy {
    /// Returns true if this policy requires local WAL fsync.
    pub fn requires_local_fsync(&self) -> bool {
        matches!(self, Self::LocalWalSync | Self::PrimaryPlusReplicaAck { .. })
    }

    /// Returns true if this policy requires replica acknowledgement.
    pub fn requires_replica_ack(&self) -> bool {
        matches!(self, Self::PrimaryPlusReplicaAck { .. } | Self::RaftMajority)
    }

    /// Returns the minimum number of replica ACKs required (0 for local-only).
    pub fn required_replica_acks(&self) -> u32 {
        match self {
            Self::PrimaryPlusReplicaAck { required_acks } => *required_acks,
            Self::RaftMajority => 1, // simplified; actual majority is N/2+1
            _ => 0,
        }
    }

    /// Describes what the client may lose after a crash under this policy.
    pub fn crash_loss_description(&self) -> &'static str {
        match self {
            Self::LocalWalSync => "No data loss on primary crash (WAL is fsync'd)",
            Self::PrimaryWalOnly => "May lose up to flush_interval_us of recent commits",
            Self::PrimaryPlusReplicaAck { .. } => {
                "No data loss if at least one acked replica survives"
            }
            Self::RaftMajority => "No data loss if majority of Raft nodes survive",
        }
    }

    /// Whether failover may produce duplicate commits under this policy.
    pub fn allows_duplicate_after_failover(&self) -> bool {
        // Only PrimaryWalOnly can produce duplicates: the primary may have
        // ACK'd the client but the WAL was not replicated before crash.
        // After failover, the client may retry and the new primary has no
        // record of the original commit.
        matches!(self, Self::PrimaryWalOnly)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: Commit Outcome Classification
// ═══════════════════════════════════════════════════════════════════════════

/// Classification of a transaction's commit outcome from the client's
/// perspective. This is critical for error reporting.
///
/// **Invariant OUT-1**: If the client received `Committed`, the transaction
///   MUST survive any single-node crash (under the active commit policy).
///
/// **Invariant OUT-2**: If the client received `Aborted`, the transaction
///   MUST NOT be visible after recovery.
///
/// **Invariant OUT-3**: `Indeterminate` MUST only be returned when the
///   system genuinely cannot determine the outcome (e.g. network partition
///   during commit). The client MUST retry or query transaction status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOutcome {
    /// Transaction committed and acknowledged. MUST survive crashes.
    Committed {
        txn_id: TxnId,
        commit_ts: Timestamp,
        policy: CommitPolicy,
        degraded: bool,
    },

    /// Transaction aborted. MUST NOT be visible after recovery.
    Aborted {
        txn_id: TxnId,
        reason: String,
    },

    /// Transaction outcome is unknown. Client MUST retry or check status.
    /// Maps to PG SQLSTATE `08006` (connection_failure) or `40001` (serialization_failure).
    Indeterminate {
        txn_id: TxnId,
        reason: String,
    },
}

impl CommitOutcome {
    pub fn txn_id(&self) -> TxnId {
        match self {
            Self::Committed { txn_id, .. } => *txn_id,
            Self::Aborted { txn_id, .. } => *txn_id,
            Self::Indeterminate { txn_id, .. } => *txn_id,
        }
    }

    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed { .. })
    }

    pub fn is_indeterminate(&self) -> bool {
        matches!(self, Self::Indeterminate { .. })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: WAL Invariants
// ═══════════════════════════════════════════════════════════════════════════

/// Formal invariants that the WAL subsystem MUST uphold.
/// Each invariant has a corresponding test in the consistency test suite.
///
/// These are compile-time documentation; runtime checks are in the WAL module.
pub mod wal_invariants {
    /// **WAL-1**: Every WAL entry has a globally unique, monotonically
    /// increasing LSN. `∀ e1, e2 ∈ WAL: e1.lsn < e2.lsn ⟹ e1 was written before e2`
    pub const WAL_1_UNIQUE_MONOTONIC_LSN: &str =
        "Every WAL entry has a unique, monotonically increasing LSN";

    /// **WAL-2**: Every WAL entry carries an idempotent `txn_id`.
    /// Replaying the same entry twice produces the same state as replaying once.
    /// `∀ r ∈ WAL: apply(apply(state, r), r) = apply(state, r)`
    pub const WAL_2_IDEMPOTENT_TXN_ID: &str =
        "WAL replay is idempotent: replaying the same record twice is a no-op";

    /// **WAL-3**: Commit records form a monotonically increasing sequence.
    /// `∀ c1, c2 ∈ CommitRecords: c1.lsn < c2.lsn ⟹ c1.commit_ts ≤ c2.commit_ts`
    pub const WAL_3_MONOTONIC_COMMIT_SEQUENCE: &str =
        "Commit timestamps are monotonically increasing in WAL order";

    /// **WAL-4**: WAL replay produces identical state regardless of how
    /// many times it is replayed (convergence).
    /// `replay(replay(empty, WAL)) = replay(empty, WAL)`
    pub const WAL_4_REPLAY_CONVERGENCE: &str =
        "Repeated WAL replay converges to the same state";

    /// **WAL-5**: After recovery, every committed transaction (i.e. has a
    /// CommitTxn record in WAL) MUST be visible. Every uncommitted
    /// transaction (no CommitTxn record) MUST NOT be visible.
    pub const WAL_5_RECOVERY_COMPLETENESS: &str =
        "Recovery makes all committed txns visible and all uncommitted txns invisible";

    /// **WAL-6**: CRC32 checksum on every WAL record detects corruption.
    /// A corrupted record MUST cause recovery to stop at the corruption
    /// point (not silently skip).
    pub const WAL_6_CHECKSUM_INTEGRITY: &str =
        "CRC32 checksum on every record; corruption halts recovery";
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: Crash Point Model
// ═══════════════════════════════════════════════════════════════════════════

/// Models the possible crash points during a transaction's lifecycle.
/// Used by fault injection tests to verify recovery correctness.
///
/// **Invariant CRASH-1**: For each crash point, the recovery outcome is
/// deterministic and documented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CrashPoint {
    /// Crash before any WAL write for this transaction.
    /// **Recovery**: Transaction does not exist. No trace in WAL.
    BeforeWalWrite,

    /// Crash after WAL write (Insert/Update/Delete) but before CommitTxn record.
    /// **Recovery**: Transaction is rolled back (uncommitted writes cleaned up).
    AfterWalWriteBeforeCommit,

    /// Crash after CommitTxn record written but before WAL fsync.
    /// **Recovery under LocalWalSync**: Transaction MAY or MAY NOT exist
    ///   (depends on whether the OS flushed the buffer).
    /// **Recovery under PrimaryWalOnly**: Transaction MAY be lost.
    AfterCommitBeforeSync,

    /// Crash after WAL fsync but before client ACK.
    /// **Recovery**: Transaction MUST exist (WAL is durable).
    /// Client receives `Indeterminate` if connection is lost.
    AfterSyncBeforeAck,

    /// Crash after client ACK but before replication.
    /// **Recovery**: Transaction exists on primary. Replica may be behind.
    /// **Failover risk**: If primary is permanently lost, transaction is lost
    ///   unless `PrimaryPlusReplicaAck` policy was used.
    AfterAckBeforeReplication,

    /// Crash after replication ACK.
    /// **Recovery**: Transaction exists on both primary and replica(s).
    AfterReplicationAck,
}

impl CrashPoint {
    /// Whether a transaction at this crash point MUST survive recovery.
    pub fn must_survive_recovery(&self, policy: &CommitPolicy) -> bool {
        match self {
            Self::BeforeWalWrite => false,
            Self::AfterWalWriteBeforeCommit => false,
            Self::AfterCommitBeforeSync => {
                // Only survives if policy doesn't require fsync
                matches!(policy, CommitPolicy::PrimaryWalOnly)
            }
            Self::AfterSyncBeforeAck => policy.requires_local_fsync(),
            Self::AfterAckBeforeReplication => true, // WAL is durable locally
            Self::AfterReplicationAck => true,
        }
    }

    /// Whether a transaction at this crash point is allowed to be lost.
    pub fn may_be_lost(&self, policy: &CommitPolicy) -> bool {
        !self.must_survive_recovery(policy)
    }

    /// Whether the client may have received an ACK for this crash point.
    pub fn client_may_have_ack(&self) -> bool {
        matches!(
            self,
            Self::AfterAckBeforeReplication | Self::AfterReplicationAck
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6: Replication Invariants
// ═══════════════════════════════════════════════════════════════════════════

/// Formal invariants for the replication subsystem.
pub mod replication_invariants {
    /// **REP-1 (Prefix Property)**: Replica state is always a prefix of
    /// primary state. `∀ t: replica_committed_set(t) ⊆ primary_committed_set(t)`
    pub const REP_1_PREFIX_PROPERTY: &str =
        "Replica committed set is always a prefix of primary committed set";

    /// **REP-2 (No Phantom Commits)**: A replica MUST NOT produce a commit
    /// that the primary has not produced.
    /// `∀ txn ∈ replica_committed: txn ∈ primary_committed`
    pub const REP_2_NO_PHANTOM_COMMITS: &str =
        "Replica never commits a transaction that primary has not committed";

    /// **REP-3 (Ordering)**: WAL entries are applied on the replica in
    /// the same order they were written on the primary.
    /// `∀ e1, e2: primary_order(e1) < primary_order(e2) ⟹ replica_apply_order(e1) < replica_apply_order(e2)`
    pub const REP_3_STRICT_ORDERING: &str =
        "Replica applies WAL entries in primary's write order";

    /// **REP-4 (ACK Semantics)**: A replica ACK means the WAL entry has
    /// been applied to the replica's storage engine (not just received).
    pub const REP_4_ACK_MEANS_APPLIED: &str =
        "Replica ACK means WAL entry has been applied, not just received";
}

/// Replication unit: what is shipped from primary to replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationUnit {
    /// Individual WAL entries are shipped one-by-one.
    WalEntry,
    /// Groups of WAL entries (e.g. all entries for a transaction) are shipped together.
    TransactionGroup,
}

/// What a replica ACK actually means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum ReplicaAckSemantics {
    /// WAL bytes received into memory buffer.
    Received,
    /// WAL bytes written to replica's local WAL (fsync'd).
    Persisted,
    /// WAL entries applied to replica's storage engine (visible to reads).
    #[default]
    Applied,
}


// ═══════════════════════════════════════════════════════════════════════════
// §7: Failover / Promote Invariants
// ═══════════════════════════════════════════════════════════════════════════

/// Formal invariants for failover and promotion.
pub mod failover_invariants {
    /// **FAIL-1 (Commit Set Containment)**: After promote, the new primary's
    /// committed set is a subset or equal to the old primary's committed set.
    /// `new_primary_committed ⊆ old_primary_committed`
    pub const FAIL_1_COMMIT_SET_CONTAINMENT: &str =
        "After promote, new primary's commits ⊆ old primary's commits";

    /// **FAIL-2 (No Rollback of ACK'd)**: Transactions that were ACK'd to
    /// the client under `PrimaryPlusReplicaAck(N)` MUST survive failover
    /// if at least one acked replica is promoted.
    pub const FAIL_2_NO_ROLLBACK_ACKED: &str =
        "ACK'd transactions under replica-ack policy must survive failover";

    /// **FAIL-3 (Fencing)**: The old primary MUST be fenced after promote.
    /// It MUST NOT accept new writes. Clients connecting to the old primary
    /// MUST receive an error.
    pub const FAIL_3_FENCING: &str =
        "Old primary must be fenced after promote (no new writes)";
}

/// Preconditions that MUST be satisfied before a replica can be promoted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotePreconditions {
    /// Minimum LSN the replica must have applied.
    pub min_applied_lsn: u64,
    /// Maximum allowed replication lag (in LSN units).
    pub max_promotion_lag: u64,
    /// Whether data-loss promotion is allowed (force promote even if behind).
    pub allow_data_loss: bool,
    /// Epoch number for fencing (monotonically increasing across promotes).
    pub epoch: u64,
}

impl Default for PromotePreconditions {
    fn default() -> Self {
        Self {
            min_applied_lsn: 0,
            max_promotion_lag: 1000,
            allow_data_loss: false,
            epoch: 0,
        }
    }
}

/// Result of a promote operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromoteResult {
    /// New epoch number assigned to the promoted node.
    pub new_epoch: u64,
    /// LSN at which the new primary starts accepting writes.
    pub start_lsn: u64,
    /// Number of transactions that may have been lost (0 if clean promote).
    pub potential_data_loss_txns: u64,
    /// Whether this was a forced (data-loss) promote.
    pub forced: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// §8: Cross-Shard Transaction Invariants
// ═══════════════════════════════════════════════════════════════════════════

/// Formal invariants for cross-shard (distributed) transactions.
pub mod cross_shard_invariants {
    /// **XS-1 (Atomicity)**: A cross-shard transaction either commits on
    /// ALL participating shards or aborts on ALL. No partial commits.
    pub const XS_1_ATOMICITY: &str =
        "Cross-shard txn commits on all shards or aborts on all";

    /// **XS-2 (At-Most-Once Commit)**: A cross-shard transaction MUST NOT
    /// be committed more than once, even under coordinator retries.
    pub const XS_2_AT_MOST_ONCE: &str =
        "Cross-shard txn commits at most once (no duplicate commits)";

    /// **XS-3 (Coordinator Crash Recovery)**: If the coordinator crashes
    /// after writing `CoordinatorCommit` to its WAL, recovery MUST
    /// complete the commit on all participants. If only `CoordinatorPrepare`
    /// exists, recovery MUST abort all participants.
    pub const XS_3_COORDINATOR_CRASH_RECOVERY: &str =
        "Coordinator crash recovery resolves in-doubt txns deterministically";

    /// **XS-4 (Participant Crash Recovery)**: A participant that crashes
    /// after PREPARE but before receiving the coordinator's decision MUST
    /// hold the transaction in PREPARED state until the coordinator resolves.
    pub const XS_4_PARTICIPANT_CRASH_RECOVERY: &str =
        "Participant holds PREPARED txn until coordinator resolves";
}

/// Cross-shard transaction model in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum CrossShardModel {
    /// Standard Two-Phase Commit (2PC) with coordinator WAL.
    TwoPhaseCommit,
    /// Hybrid: fast-path (single-shard) + slow-path (multi-shard 2PC).
    #[default]
    HybridFastSlow,
}


// ═══════════════════════════════════════════════════════════════════════════
// §9: Read Consistency Semantics
// ═══════════════════════════════════════════════════════════════════════════

/// Formal definition of supported read semantics.
pub mod read_invariants {
    /// **READ-1 (Read Committed)**: A read sees only data committed before
    /// the statement began. Different statements in the same transaction
    /// may see different snapshots.
    pub const READ_1_READ_COMMITTED: &str =
        "Read Committed: each statement sees data committed before it started";

    /// **READ-2 (Snapshot Isolation)**: All reads in a transaction see a
    /// consistent snapshot taken at transaction start.
    pub const READ_2_SNAPSHOT_ISOLATION: &str =
        "Snapshot Isolation: all reads in txn see snapshot at txn start";

    /// **READ-3 (Serializable)**: Transactions execute as if in some serial
    /// order. Write skew and phantom reads are prevented.
    pub const READ_3_SERIALIZABLE: &str =
        "Serializable: transactions are equivalent to some serial execution";

    /// **READ-4 (Replica Staleness Bound)**: Reads from a replica are
    /// bounded by the replica's applied LSN. The maximum staleness is
    /// defined by `max_replica_staleness`.
    pub const READ_4_REPLICA_STALENESS: &str =
        "Replica reads are bounded by the replica's applied LSN";
}

/// Configuration for replica read behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaReadConfig {
    /// Whether reads from replicas are enabled.
    pub enabled: bool,
    /// Maximum acceptable staleness for replica reads.
    pub max_staleness: Duration,
    /// Whether read-your-writes consistency is provided.
    /// If true, a read after a write on the same session routes to primary.
    pub read_your_writes: bool,
    /// Whether snapshot reads (at a specific timestamp) are supported.
    pub snapshot_reads: bool,
}

impl Default for ReplicaReadConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_staleness: Duration::from_secs(5),
            read_your_writes: true,
            snapshot_reads: false,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §10: Error Classification for Client Observability
// ═══════════════════════════════════════════════════════════════════════════

/// PG SQLSTATE codes for consistency-critical errors.
///
/// **Invariant ERR-1**: The client MUST be able to distinguish between
/// "committed", "aborted", and "unknown" outcomes from the error code alone.
///
/// **Invariant ERR-2**: No consistency-critical error may use a generic
/// internal error code (XX000). Each must map to a specific SQLSTATE.
pub mod pg_error_codes {
    /// Transaction committed successfully.
    pub const SUCCESSFUL_COMPLETION: &str = "00000";

    /// Serialization failure — client should retry.
    pub const SERIALIZATION_FAILURE: &str = "40001";

    /// Deadlock detected — client should retry.
    pub const DEADLOCK_DETECTED: &str = "40P01";

    /// Statement cancelled by user request.
    pub const QUERY_CANCELED: &str = "57014";

    /// Connection failure — commit status is INDETERMINATE.
    /// Client MUST check transaction status before retrying.
    pub const CONNECTION_FAILURE: &str = "08006";

    /// Crash recovery — commit status is INDETERMINATE.
    pub const CRASH_SHUTDOWN: &str = "57P01";

    /// Read-only transaction attempted a write.
    pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";

    /// Insufficient replicas for the configured commit policy.
    pub const INSUFFICIENT_REPLICAS: &str = "53400";

    /// WAL backlog exceeded — admission control rejected the txn.
    pub const WAL_BACKLOG_EXCEEDED: &str = "53300";

    /// Replication lag exceeded — admission control rejected the txn.
    pub const REPLICATION_LAG_EXCEEDED: &str = "53301";

    /// Foreign key constraint violation.
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";

    /// Unique constraint violation.
    pub const UNIQUE_VIOLATION: &str = "23505";

    /// CHECK constraint violation.
    pub const CHECK_VIOLATION: &str = "23514";

    /// Transaction in wrong state for this operation.
    pub const INVALID_TRANSACTION_STATE: &str = "25000";

    /// Node is fenced (old primary after failover).
    pub const NODE_FENCED: &str = "57P03";
}

/// Maps a `CommitOutcome` to the appropriate PG SQLSTATE code.
pub fn outcome_to_sqlstate(outcome: &CommitOutcome) -> &'static str {
    match outcome {
        CommitOutcome::Committed { .. } => pg_error_codes::SUCCESSFUL_COMPLETION,
        CommitOutcome::Aborted { reason, .. } => {
            if reason.contains("serialization") {
                pg_error_codes::SERIALIZATION_FAILURE
            } else if reason.contains("deadlock") {
                pg_error_codes::DEADLOCK_DETECTED
            } else {
                pg_error_codes::SERIALIZATION_FAILURE
            }
        }
        CommitOutcome::Indeterminate { .. } => pg_error_codes::CONNECTION_FAILURE,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §11: Consistency Validation Helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Validate that a commit timeline is consistent with the commit point model.
/// Returns Ok(()) if all invariants hold, or Err with violation description.
pub fn validate_commit_timeline(
    logical_ts: Option<u64>,
    durable_ts: Option<u64>,
    client_visible_ts: Option<u64>,
) -> Result<(), String> {
    // CP-1: strict ordering
    if let (Some(l), Some(d)) = (logical_ts, durable_ts) {
        if l > d {
            return Err(format!(
                "CP-1 violation: logical_ts({}) > durable_ts({})",
                l, d
            ));
        }
    }
    if let (Some(d), Some(c)) = (durable_ts, client_visible_ts) {
        if d > c {
            return Err(format!(
                "CP-1 violation: durable_ts({}) > client_visible_ts({})",
                d, c
            ));
        }
    }
    // CP-2: no client visibility without durability
    if client_visible_ts.is_some() && durable_ts.is_none() {
        return Err("CP-2 violation: client_visible without durable".into());
    }
    Ok(())
}

/// Validate the prefix property for replication.
/// `replica_commits` must be a subset of `primary_commits`.
pub fn validate_prefix_property(
    primary_commits: &[TxnId],
    replica_commits: &[TxnId],
) -> Result<(), String> {
    for rtxn in replica_commits {
        if !primary_commits.contains(rtxn) {
            return Err(format!(
                "REP-1/REP-2 violation: replica has txn {} not in primary",
                rtxn
            ));
        }
    }
    Ok(())
}

/// Validate that after promote, the new primary's commit set is contained
/// in the old primary's commit set.
pub fn validate_promote_commit_set(
    old_primary_commits: &[TxnId],
    new_primary_commits: &[TxnId],
) -> Result<(), String> {
    for ntxn in new_primary_commits {
        if !old_primary_commits.contains(ntxn) {
            return Err(format!(
                "FAIL-1 violation: new primary has txn {} not in old primary",
                ntxn
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_point_ordering() {
        assert!(CommitPoint::LogicalCommit < CommitPoint::DurableCommit);
        assert!(CommitPoint::DurableCommit < CommitPoint::ClientVisibleCommit);
    }

    #[test]
    fn test_commit_policy_display() {
        assert_eq!(CommitPolicy::LocalWalSync.to_string(), "LOCAL_WAL_SYNC");
        assert_eq!(CommitPolicy::PrimaryWalOnly.to_string(), "PRIMARY_WAL_ONLY");
        assert_eq!(
            CommitPolicy::PrimaryPlusReplicaAck { required_acks: 2 }.to_string(),
            "PRIMARY_PLUS_REPLICA_ACK(2)"
        );
        assert_eq!(CommitPolicy::RaftMajority.to_string(), "RAFT_MAJORITY");
    }

    #[test]
    fn test_commit_policy_properties() {
        assert!(CommitPolicy::LocalWalSync.requires_local_fsync());
        assert!(!CommitPolicy::PrimaryWalOnly.requires_local_fsync());
        assert!(CommitPolicy::PrimaryPlusReplicaAck { required_acks: 1 }.requires_replica_ack());
        assert!(!CommitPolicy::LocalWalSync.requires_replica_ack());
        assert!(CommitPolicy::RaftMajority.requires_replica_ack());
    }

    #[test]
    fn test_crash_point_survival() {
        let local = CommitPolicy::LocalWalSync;
        let async_p = CommitPolicy::PrimaryWalOnly;

        // Before WAL write: never survives
        assert!(!CrashPoint::BeforeWalWrite.must_survive_recovery(&local));
        assert!(!CrashPoint::BeforeWalWrite.must_survive_recovery(&async_p));

        // After sync before ACK: survives under local fsync
        assert!(CrashPoint::AfterSyncBeforeAck.must_survive_recovery(&local));
        assert!(!CrashPoint::AfterSyncBeforeAck.must_survive_recovery(&async_p));

        // After ACK: always survives (WAL is durable)
        assert!(CrashPoint::AfterAckBeforeReplication.must_survive_recovery(&local));
        assert!(CrashPoint::AfterAckBeforeReplication.must_survive_recovery(&async_p));
    }

    #[test]
    fn test_validate_commit_timeline_ok() {
        assert!(validate_commit_timeline(Some(1), Some(2), Some(3)).is_ok());
        assert!(validate_commit_timeline(Some(1), Some(1), Some(1)).is_ok());
        assert!(validate_commit_timeline(Some(1), Some(2), None).is_ok());
        assert!(validate_commit_timeline(None, None, None).is_ok());
    }

    #[test]
    fn test_validate_commit_timeline_violations() {
        // CP-1: logical > durable
        assert!(validate_commit_timeline(Some(5), Some(3), None).is_err());
        // CP-1: durable > client_visible
        assert!(validate_commit_timeline(Some(1), Some(5), Some(3)).is_err());
        // CP-2: client_visible without durable
        assert!(validate_commit_timeline(Some(1), None, Some(3)).is_err());
    }

    #[test]
    fn test_prefix_property_validation() {
        let primary = vec![TxnId(1), TxnId(2), TxnId(3)];
        let replica = vec![TxnId(1), TxnId(2)];
        assert!(validate_prefix_property(&primary, &replica).is_ok());

        let bad_replica = vec![TxnId(1), TxnId(99)];
        assert!(validate_prefix_property(&primary, &bad_replica).is_err());
    }

    #[test]
    fn test_promote_commit_set_validation() {
        let old = vec![TxnId(1), TxnId(2), TxnId(3)];
        let new_subset = vec![TxnId(1), TxnId(2)];
        assert!(validate_promote_commit_set(&old, &new_subset).is_ok());

        let new_equal = vec![TxnId(1), TxnId(2), TxnId(3)];
        assert!(validate_promote_commit_set(&old, &new_equal).is_ok());

        let new_extra = vec![TxnId(1), TxnId(99)];
        assert!(validate_promote_commit_set(&old, &new_extra).is_err());
    }

    #[test]
    fn test_commit_outcome_sqlstate() {
        let committed = CommitOutcome::Committed {
            txn_id: TxnId(1),
            commit_ts: Timestamp(100),
            policy: CommitPolicy::LocalWalSync,
            degraded: false,
        };
        assert_eq!(outcome_to_sqlstate(&committed), "00000");

        let indeterminate = CommitOutcome::Indeterminate {
            txn_id: TxnId(2),
            reason: "connection lost".into(),
        };
        assert_eq!(outcome_to_sqlstate(&indeterminate), "08006");
    }

    #[test]
    fn test_crash_point_client_ack() {
        assert!(!CrashPoint::BeforeWalWrite.client_may_have_ack());
        assert!(!CrashPoint::AfterWalWriteBeforeCommit.client_may_have_ack());
        assert!(!CrashPoint::AfterCommitBeforeSync.client_may_have_ack());
        assert!(!CrashPoint::AfterSyncBeforeAck.client_may_have_ack());
        assert!(CrashPoint::AfterAckBeforeReplication.client_may_have_ack());
        assert!(CrashPoint::AfterReplicationAck.client_may_have_ack());
    }

    #[test]
    fn test_policy_allows_duplicate_after_failover() {
        assert!(CommitPolicy::PrimaryWalOnly.allows_duplicate_after_failover());
        assert!(!CommitPolicy::LocalWalSync.allows_duplicate_after_failover());
        assert!(!CommitPolicy::PrimaryPlusReplicaAck { required_acks: 1 }
            .allows_duplicate_after_failover());
        assert!(!CommitPolicy::RaftMajority.allows_duplicate_after_failover());
    }
}
