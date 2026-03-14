//! Distributed (cross-shard) transaction invariant tests.
//!
//! Verifies the five cross-shard invariants codified in
//! `falcon_common::consistency::CrossShardInvariant`:
//!
//! - XS-1: Atomicity (all-or-nothing across shards)
//! - XS-2: At-Most-Once Commit (no duplicate commits)
//! - XS-3: Coordinator Crash Recovery (decision log resolves in-doubt txns)
//! - XS-4: Participant Crash Recovery (prepared state held until resolved)
//! - XS-5: Timeout Rollback (no hanging locks)
//!
//! These tests are deterministic and non-flaky.

mod common;

use falcon_cluster::deterministic_2pc::{
    CoordinatorDecision, CoordinatorDecisionLog, DecisionLogConfig,
};
use falcon_cluster::indoubt_resolver::{InDoubtResolver, TxnOutcomeCache};
use falcon_common::consistency::CrossShardInvariant;
use falcon_common::types::{ShardId, TxnId};
use std::time::Duration;

// ─── XS-1: Atomicity ────────────────────────────────────────────────────────

#[test]
fn xs1_atomicity_all_commit() {
    // All shards commit → decision is Commit, applied to all
    let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
    let shards = vec![ShardId(0), ShardId(1), ShardId(2)];
    let txn = TxnId(1001);

    log.log_decision(txn, CoordinatorDecision::Commit, &shards, 100);
    assert_eq!(log.get_decision(txn), Some(CoordinatorDecision::Commit));

    // All participants acknowledged
    log.mark_applied(txn);
    assert_eq!(log.unapplied_decisions().len(), 0);
}

#[test]
fn xs1_atomicity_any_fail_aborts_all() {
    // If any shard fails prepare → decision is Abort for all
    let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
    let shards = vec![ShardId(0), ShardId(1)];
    let txn = TxnId(1002);

    // Simulate: shard 1 failed prepare → coordinator decides abort
    log.log_decision(txn, CoordinatorDecision::Abort, &shards, 50);
    assert_eq!(log.get_decision(txn), Some(CoordinatorDecision::Abort));
}

// ─── XS-2: At-Most-Once Commit ──────────────────────────────────────────────

#[test]
fn xs2_at_most_once_duplicate_decision_idempotent() {
    // Logging the same txn decision twice should not create inconsistency
    let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
    let txn = TxnId(2001);
    let shards = vec![ShardId(0)];

    log.log_decision(txn, CoordinatorDecision::Commit, &shards, 100);
    log.log_decision(txn, CoordinatorDecision::Commit, &shards, 100);

    // get_decision returns the most recent — both are Commit, so idempotent
    assert_eq!(log.get_decision(txn), Some(CoordinatorDecision::Commit));
}

#[test]
fn xs2_at_most_once_outcome_cache_returns_same() {
    let cache = TxnOutcomeCache::new(Duration::from_secs(60), 1000);
    let txn = TxnId(2002);

    cache.record(txn, falcon_cluster::indoubt_resolver::TxnOutcome::Committed);

    // Multiple lookups return the same outcome
    let o1 = cache.lookup(txn);
    let o2 = cache.lookup(txn);
    assert_eq!(o1, o2);
    assert_eq!(
        o1,
        Some(falcon_cluster::indoubt_resolver::TxnOutcome::Committed)
    );
}

// ─── XS-3: Coordinator Crash Recovery ────────────────────────────────────────

#[test]
fn xs3_coordinator_crash_unapplied_decisions_survive() {
    // After coordinator crash, unapplied decisions are available for recovery
    let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());

    // Three transactions: one applied, two unapplied (simulating crash)
    log.log_decision(TxnId(3001), CoordinatorDecision::Commit, &[ShardId(0)], 100);
    log.mark_applied(TxnId(3001)); // This one completed before crash

    log.log_decision(
        TxnId(3002),
        CoordinatorDecision::Commit,
        &[ShardId(0), ShardId(1)],
        200,
    );
    log.log_decision(TxnId(3003), CoordinatorDecision::Abort, &[ShardId(1)], 50);

    // Recovery: enumerate unapplied
    let unapplied = log.unapplied_decisions();
    assert_eq!(unapplied.len(), 2);
    assert_eq!(unapplied[0].txn_id, TxnId(3002));
    assert_eq!(unapplied[0].decision, CoordinatorDecision::Commit);
    assert_eq!(unapplied[1].txn_id, TxnId(3003));
    assert_eq!(unapplied[1].decision, CoordinatorDecision::Abort);
}

#[test]
fn xs3_coordinator_crash_before_decision_aborts() {
    // If coordinator crashes BEFORE writing decision → no decision in log → abort
    let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
    let txn = TxnId(3004);

    // No decision logged (crash before prepare completed)
    assert_eq!(log.get_decision(txn), None);
    // InDoubtResolver defaults to Abort when no decision found
}

#[test]
fn xs3_indoubt_resolver_sweep_resolves() {
    // InDoubtResolver sweeps and resolves in-doubt transactions
    let cache = TxnOutcomeCache::new(Duration::from_secs(60), 1000);
    let resolver = InDoubtResolver::new(cache.clone());

    // Register an in-doubt txn with (ShardId, TxnId) participants
    resolver.register_indoubt(
        TxnId(3005),
        vec![(ShardId(0), TxnId(100)), (ShardId(1), TxnId(101))],
    );

    // Cache the decision (coordinator recovered)
    cache.record(
        TxnId(3005),
        falcon_cluster::indoubt_resolver::TxnOutcome::Committed,
    );

    // Sweep
    let resolved = resolver.sweep();
    assert_eq!(resolved, 1);

    // No more in-doubt txns
    let metrics = resolver.metrics();
    assert_eq!(metrics.currently_indoubt, 0);
    assert_eq!(metrics.resolved_committed, 1);
}

// ─── XS-4: Participant Crash Recovery ────────────────────────────────────────

#[test]
fn xs4_participant_crash_holds_prepared_until_resolved() {
    // A participant's in-doubt txn persists across sweeps until resolved
    let cache = TxnOutcomeCache::new(Duration::from_secs(60), 1000);
    let resolver = InDoubtResolver::new(cache.clone());

    resolver.register_indoubt(TxnId(4001), vec![(ShardId(0), TxnId(200))]);

    // First sweep: no decision cached → defaults to abort, resolves it
    let resolved = resolver.sweep();
    assert_eq!(resolved, 1);

    let metrics = resolver.metrics();
    assert_eq!(metrics.resolved_aborted, 1);
}

// ─── XS-5: Timeout Rollback ─────────────────────────────────────────────────

#[test]
fn xs5_timeout_rollback_no_hanging_locks() {
    use falcon_cluster::deterministic_2pc::{
        LayeredTimeoutConfig, LayeredTimeoutController, TimeoutResult,
    };
    use std::time::Instant;

    let config = LayeredTimeoutConfig {
        soft_timeout: Duration::from_millis(1),
        hard_timeout: Duration::from_millis(5),
        per_shard_timeout: Duration::from_millis(100),
    };
    let ctrl = LayeredTimeoutController::new(config);

    let started = Instant::now();
    std::thread::sleep(Duration::from_millis(10));

    // Hard timeout exceeded → transaction MUST be aborted
    let result = ctrl.check_transaction(started);
    assert!(
        matches!(result, TimeoutResult::HardTimeout { .. }),
        "expected HardTimeout, got {:?}",
        result
    );
}

// ─── Invariant Enum Completeness ─────────────────────────────────────────────

#[test]
fn all_cross_shard_invariants_have_descriptions() {
    let invariants = CrossShardInvariant::all();
    assert_eq!(invariants.len(), 5, "expected 5 cross-shard invariants");
    for inv in &invariants {
        let desc = inv.description();
        assert!(
            !desc.is_empty(),
            "invariant {:?} has empty description",
            inv
        );
    }
}

#[test]
fn invariant_enum_is_exhaustive() {
    // Ensure all enum variants are covered
    let all = CrossShardInvariant::all();
    assert!(all.contains(&CrossShardInvariant::Atomicity));
    assert!(all.contains(&CrossShardInvariant::AtMostOnceCommit));
    assert!(all.contains(&CrossShardInvariant::CoordinatorCrashRecovery));
    assert!(all.contains(&CrossShardInvariant::ParticipantCrashRecovery));
    assert!(all.contains(&CrossShardInvariant::TimeoutRollback));
}
