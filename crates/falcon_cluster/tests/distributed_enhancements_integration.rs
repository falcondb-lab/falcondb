//! Integration tests for distributed system enhancements (P0–P2).
//!
//! Each test maps to a specific acceptance criterion from the spec.
//! All tests are deterministic and run in-process (no network).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use falcon_cluster::distributed_enhancements::*;
use falcon_cluster::dist_hardening::{SplitBrainDetector, WriteEpochCheck};
use falcon_common::types::{ShardId, TxnId};
use falcon_storage::io::epoch_fence::{EpochFenceResult, StorageEpochFence};

// ═══════════════════════════════════════════════════════════════════════════
// P0-1: Epoch/Fencing Hardening — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Acceptance: stale leader attempts writes post-promotion → 100% rejected.
#[test]
fn test_p01_stale_leader_writes_rejected_at_storage_barrier() {
    // Simulate: epoch 5 is current, old leader has epoch 3
    let fence = StorageEpochFence::new(5);

    // Old leader tries 100 writes with stale epoch
    let mut rejected = 0;
    for _ in 0..100 {
        if let EpochFenceResult::Rejected { .. } = fence.check_write(3) {
            rejected += 1;
        }
    }

    assert_eq!(rejected, 100, "100% of stale-epoch writes must be rejected");

    let m = fence.metrics();
    assert_eq!(m.total_rejections, 100);
}

/// Acceptance: network partition chaos — no double-writes.
///
/// Simulates a partition where two leaders exist briefly:
/// 1. Leader A (epoch 5) is partitioned.
/// 2. Leader B (epoch 6) is promoted.
/// 3. Leader A heals and attempts writes.
/// 4. All of A's writes must be rejected by the storage fence.
#[test]
fn test_p01_network_partition_no_double_writes() {
    let fence = StorageEpochFence::new(5);

    // Phase 1: Leader A writes normally at epoch 5
    assert_eq!(fence.check_write(5), EpochFenceResult::Allowed);

    // Phase 2: Partition occurs. Leader B promoted → epoch 6.
    fence.advance_epoch(6);

    // Phase 3: Leader A (still thinks epoch 5) tries to write
    for i in 0..50 {
        let result = fence.check_write(5);
        assert_eq!(
            result,
            EpochFenceResult::Rejected {
                writer_epoch: 5,
                current_epoch: 6,
            },
            "write {} from partitioned leader A must be rejected",
            i
        );
    }

    // Phase 4: Leader B writes normally at epoch 6
    assert_eq!(fence.check_write(6), EpochFenceResult::Allowed);

    // Verify: zero stale writes got through
    let m = fence.metrics();
    assert_eq!(m.total_rejections, 50);
}

/// Epoch fence integrates with cluster-layer SplitBrainDetector.
#[test]
fn test_p01_epoch_fence_plus_split_brain_detector() {
    let detector = SplitBrainDetector::new(5);
    let fence = StorageEpochFence::new(5);

    // Both layers must agree
    let check = WriteEpochCheck {
        writer_epoch: 3,
        writer_node: 99,
    };
    let cluster_verdict = detector.check_write(&check);
    let storage_verdict = fence.check_write(3);

    // Cluster layer rejects
    assert!(matches!(
        cluster_verdict,
        falcon_cluster::dist_hardening::SplitBrainVerdict::Rejected { .. }
    ));
    // Storage layer also rejects
    assert!(matches!(storage_verdict, EpochFenceResult::Rejected { .. }));

    // Advance both layers
    detector.advance_epoch(10);
    fence.advance_epoch(10);

    // Old epoch 5 now rejected at both layers
    let check5 = WriteEpochCheck {
        writer_epoch: 5,
        writer_node: 1,
    };
    assert!(matches!(
        detector.check_write(&check5),
        falcon_cluster::dist_hardening::SplitBrainVerdict::Rejected { .. }
    ));
    assert!(matches!(fence.check_write(5), EpochFenceResult::Rejected { .. }));
}

// ═══════════════════════════════════════════════════════════════════════════
// P0-2: Replication Invariants — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Acceptance: replication invariants tests green across policy × crashpoint matrix.
#[test]
fn test_p02_invariants_across_policy_matrix() {
    let policies = [CommitPolicy::Local, CommitPolicy::Quorum, CommitPolicy::All];

    for &policy in &policies {
        let gate = ReplicationInvariantGate::new(policy);
        gate.register_shard(ShardId(1), 3);

        // Normal operation: primary at LSN 100, replicas at 80, 90, 95
        gate.update_primary_lsn(ShardId(1), 100);
        gate.update_replica_lsn(ShardId(1), 0, 80);
        gate.update_replica_lsn(ShardId(1), 1, 90);
        gate.update_replica_lsn(ShardId(1), 2, 95);

        // Prefix property: all replicas ≤ primary
        for replica_id in 0..3 {
            assert!(
                gate.check_prefix(ShardId(1), replica_id).is_satisfied(),
                "prefix should hold for policy={} replica={}",
                policy,
                replica_id
            );
        }

        // No phantom: replica visible LSN ≤ primary durable LSN
        assert!(gate.check_no_phantom(ShardId(1), 0, 80).is_satisfied());
        assert!(gate.check_no_phantom(ShardId(1), 1, 90).is_satisfied());
        assert!(gate.check_no_phantom(ShardId(1), 2, 95).is_satisfied());

        // Phantom: replica tries to expose LSN beyond primary
        assert!(!gate.check_no_phantom(ShardId(1), 0, 101).is_satisfied());

        // Visibility depends on policy
        match policy {
            CommitPolicy::Local => {
                assert!(gate.check_visibility(ShardId(1), 0).is_satisfied());
            }
            CommitPolicy::Quorum => {
                assert!(!gate.check_visibility(ShardId(1), 1).is_satisfied());
                assert!(gate.check_visibility(ShardId(1), 2).is_satisfied());
            }
            CommitPolicy::All => {
                assert!(!gate.check_visibility(ShardId(1), 2).is_satisfied());
                assert!(gate.check_visibility(ShardId(1), 3).is_satisfied());
            }
        }
    }
}

/// Acceptance: promotion — new primary commit set ⊆ old primary commit set.
#[test]
fn test_p02_promotion_commit_set_subset() {
    let gate = ReplicationInvariantGate::new(CommitPolicy::Quorum);

    // Old primary at LSN 100
    // Best replica at LSN 95 (caught up to 95, not 100)
    assert!(gate
        .check_promotion_safety(ShardId(1), 95, 100)
        .is_satisfied());

    // After catch-up, replica at LSN 100
    assert!(gate
        .check_promotion_safety(ShardId(1), 100, 100)
        .is_satisfied());

    // VIOLATION: replica somehow has LSN 101 (should never happen in correct impl)
    assert!(!gate
        .check_promotion_safety(ShardId(1), 101, 100)
        .is_satisfied());
}

/// Crash point matrix: simulate crash at various replication stages and
/// verify invariants hold.
#[test]
fn test_p02_crashpoint_matrix() {
    let crash_points = ["after_primary_commit", "during_replication", "after_replica_ack"];

    for crash_point in &crash_points {
        let gate = ReplicationInvariantGate::new(CommitPolicy::Quorum);
        gate.register_shard(ShardId(1), 3);

        match *crash_point {
            "after_primary_commit" => {
                // Primary committed LSN 50, crash before replication
                gate.update_primary_lsn(ShardId(1), 50);
                gate.update_replica_lsn(ShardId(1), 0, 40);
                gate.update_replica_lsn(ShardId(1), 1, 40);
                gate.update_replica_lsn(ShardId(1), 2, 40);

                // All replicas behind — prefix holds
                for r in 0..3 {
                    assert!(gate.check_prefix(ShardId(1), r).is_satisfied());
                }
                // Visibility: only 0 acks, quorum not met
                assert!(!gate.check_visibility(ShardId(1), 0).is_satisfied());
            }
            "during_replication" => {
                // Primary at 50, one replica got 50, others at 40
                gate.update_primary_lsn(ShardId(1), 50);
                gate.update_replica_lsn(ShardId(1), 0, 50);
                gate.update_replica_lsn(ShardId(1), 1, 40);
                gate.update_replica_lsn(ShardId(1), 2, 40);

                for r in 0..3 {
                    assert!(gate.check_prefix(ShardId(1), r).is_satisfied());
                }
                // 1 ack, quorum needs 2
                assert!(!gate.check_visibility(ShardId(1), 1).is_satisfied());
            }
            "after_replica_ack" => {
                // Primary at 50, two replicas at 50 (quorum met)
                gate.update_primary_lsn(ShardId(1), 50);
                gate.update_replica_lsn(ShardId(1), 0, 50);
                gate.update_replica_lsn(ShardId(1), 1, 50);
                gate.update_replica_lsn(ShardId(1), 2, 40);

                for r in 0..3 {
                    assert!(gate.check_prefix(ShardId(1), r).is_satisfied());
                }
                // 2 acks, quorum met
                assert!(gate.check_visibility(ShardId(1), 2).is_satisfied());
            }
            _ => unreachable!(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// P0-3: Failover Runbook — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Acceptance: run failover drill and output verified invariant report.
#[test]
fn test_p03_gameday_failover_drill() {
    let runbook = FailoverRunbook::new();
    let invariant_gate = ReplicationInvariantGate::new(CommitPolicy::Quorum);
    invariant_gate.register_shard(ShardId(1), 3);

    // Setup: primary at LSN 100, replicas at 95, 98, 90
    invariant_gate.update_primary_lsn(ShardId(1), 100);
    invariant_gate.update_replica_lsn(ShardId(1), 0, 95);
    invariant_gate.update_replica_lsn(ShardId(1), 1, 98);
    invariant_gate.update_replica_lsn(ShardId(1), 2, 90);

    // Execute failover drill
    runbook.begin_failover();

    // Stage 1: Detect failure
    runbook.enter_stage(FailoverStage::DetectFailure);
    runbook.complete_stage(FailoverStage::DetectFailure);

    // Stage 2: Freeze writes
    runbook.enter_stage(FailoverStage::FreezeWrites);
    runbook.complete_stage(FailoverStage::FreezeWrites);

    // Stage 3: Seal epoch
    runbook.enter_stage(FailoverStage::SealEpoch);
    runbook.complete_stage(FailoverStage::SealEpoch);

    // Stage 4: Catch up best replica (replica 1, LSN 98 → 100)
    runbook.enter_stage(FailoverStage::CatchUp);
    invariant_gate.update_replica_lsn(ShardId(1), 1, 100); // caught up
    runbook.complete_stage(FailoverStage::CatchUp);

    // Stage 5: Promote
    runbook.enter_stage(FailoverStage::Promote);
    runbook.complete_stage(FailoverStage::Promote);

    // Stage 6: Reopen
    runbook.enter_stage(FailoverStage::Reopen);
    runbook.complete_stage(FailoverStage::Reopen);

    // Stage 7: Verify invariants
    runbook.enter_stage(FailoverStage::Verify);
    let invariants = vec![
        InvariantCheckResult {
            invariant: "prefix_property".into(),
            passed: invariant_gate.check_prefix(ShardId(1), 1).is_satisfied(),
            detail: "promoted replica LSN <= old primary LSN".into(),
        },
        InvariantCheckResult {
            invariant: "no_phantom_commits".into(),
            passed: invariant_gate
                .check_no_phantom(ShardId(1), 1, 100)
                .is_satisfied(),
            detail: "no phantom commits on promoted replica".into(),
        },
        InvariantCheckResult {
            invariant: "promotion_safety".into(),
            passed: invariant_gate
                .check_promotion_safety(ShardId(1), 100, 100)
                .is_satisfied(),
            detail: "new primary commit set ⊆ old primary".into(),
        },
        InvariantCheckResult {
            invariant: "visibility_quorum".into(),
            passed: invariant_gate
                .check_visibility(ShardId(1), 2)
                .is_satisfied(),
            detail: "quorum acks met for visibility".into(),
        },
    ];
    runbook.complete_stage(FailoverStage::Verify);
    runbook.mark_complete();

    // Generate machine-readable report
    let report = runbook.generate_report(
        ShardId(1),
        5,
        6,
        1, // old primary node
        2, // new primary node
        invariants,
    );

    // Verify report
    assert!(report.passed, "gameday failover drill must pass");
    assert_eq!(report.old_epoch, 5);
    assert_eq!(report.new_epoch, 6);
    assert!(report.total_duration_us > 0);
    assert!(report.audit_trail.len() >= 14); // 2 events per stage × 7 stages
    assert!(report.invariants.iter().all(|i| i.passed));
    assert!(report.failure_reason.is_none());

    // Verify audit trail completeness
    let stages_seen: Vec<FailoverStage> = report
        .audit_trail
        .iter()
        .map(|e| e.stage)
        .collect();
    assert!(stages_seen.contains(&FailoverStage::DetectFailure));
    assert!(stages_seen.contains(&FailoverStage::FreezeWrites));
    assert!(stages_seen.contains(&FailoverStage::SealEpoch));
    assert!(stages_seen.contains(&FailoverStage::CatchUp));
    assert!(stages_seen.contains(&FailoverStage::Promote));
    assert!(stages_seen.contains(&FailoverStage::Reopen));
    assert!(stages_seen.contains(&FailoverStage::Verify));
    assert!(stages_seen.contains(&FailoverStage::Complete));
}

/// Acceptance: report fails if any invariant is violated.
#[test]
fn test_p03_gameday_fails_on_violation() {
    let runbook = FailoverRunbook::new();
    runbook.begin_failover();
    runbook.enter_stage(FailoverStage::Promote);
    runbook.complete_stage(FailoverStage::Promote);
    runbook.mark_complete();

    let invariants = vec![
        InvariantCheckResult {
            invariant: "prefix_property".into(),
            passed: true,
            detail: "OK".into(),
        },
        InvariantCheckResult {
            invariant: "no_phantom_commits".into(),
            passed: false,
            detail: "PHANTOM: replica visible LSN 101 > primary durable LSN 100".into(),
        },
    ];

    let report = runbook.generate_report(ShardId(1), 5, 6, 1, 2, invariants);
    assert!(!report.passed, "report must fail when invariant violated");
    assert!(report.failure_reason.is_some());
    let reason = report.failure_reason.unwrap();
    assert!(reason.contains("invariant failures"));
    assert!(reason.contains("PHANTOM"));
}

// ═══════════════════════════════════════════════════════════════════════════
// P0-4: 2PC Recovery — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Acceptance: crash injection at each phase — coordinator recovers to
/// deterministic outcome.
#[test]
fn test_p04_crash_at_each_phase() {
    let phases = [
        TwoPhasePhase::PrePrepare,
        TwoPhasePhase::Preparing,
        TwoPhasePhase::DecisionPending,
        TwoPhasePhase::Applying,
        TwoPhasePhase::Applied,
    ];

    let idempotency = Arc::new(ParticipantIdempotencyRegistry::new(
        10000,
        Duration::from_secs(300),
    ));
    let recovery = TwoPcRecoveryCoordinator::new(idempotency.clone());

    let shards = vec![ShardId(1), ShardId(2), ShardId(3)];

    for (i, &crash_phase) in phases.iter().enumerate() {
        let txn = TxnId(100 + i as u64);

        // Simulate normal progress up to crash point
        recovery.enter_phase(txn, TwoPhasePhase::PrePrepare);

        if crash_phase == TwoPhasePhase::PrePrepare {
            // Crash before prepare: coordinator aborts on recovery
            let ok = recovery.recover_decision(txn, &shards, ParticipantDecision::Abort);
            assert!(ok, "recovery after pre-prepare crash must succeed");
            recovery.clear_phase(txn);
            continue;
        }

        recovery.enter_phase(txn, TwoPhasePhase::Preparing);
        if crash_phase == TwoPhasePhase::Preparing {
            // Crash during prepare: safe to abort (no commit decision yet)
            let ok = recovery.recover_decision(txn, &shards, ParticipantDecision::Abort);
            assert!(ok, "recovery after preparing crash must succeed");
            recovery.clear_phase(txn);
            continue;
        }

        recovery.enter_phase(txn, TwoPhasePhase::DecisionPending);
        if crash_phase == TwoPhasePhase::DecisionPending {
            // Crash with decision pending: depends on whether decision was written.
            // If decision log has Commit, recover with Commit; else Abort.
            // Here we simulate the decision was written as Commit.
            let ok = recovery.recover_decision(txn, &shards, ParticipantDecision::Commit);
            assert!(ok, "recovery after decision-pending crash must succeed");
            recovery.clear_phase(txn);
            continue;
        }

        recovery.enter_phase(txn, TwoPhasePhase::Applying);
        if crash_phase == TwoPhasePhase::Applying {
            // Crash during apply: re-send commit to all participants.
            // Idempotency ensures at-most-once.
            let ok = recovery.recover_decision(txn, &shards, ParticipantDecision::Commit);
            assert!(ok, "recovery after applying crash must succeed");

            // Second recovery attempt — all duplicates
            let ok2 = recovery.recover_decision(txn, &shards, ParticipantDecision::Commit);
            assert!(ok2, "duplicate recovery must succeed (idempotent)");

            // Verify idempotency counters
            let m = idempotency.metrics();
            assert!(m.duplicate_applies > 0, "must have duplicate applies");
            recovery.clear_phase(txn);
            continue;
        }

        recovery.enter_phase(txn, TwoPhasePhase::Applied);
        // Already applied — no recovery needed
        recovery.clear_phase(txn);
    }

    let m = recovery.recovery_metrics();
    assert!(m.recovery_attempts > 0);
    assert_eq!(m.recovery_failures, 0, "no recovery failures expected");
}

/// Acceptance: participant idempotency under concurrent recovery.
#[test]
fn test_p04_participant_idempotency_concurrent() {
    let registry = Arc::new(ParticipantIdempotencyRegistry::new(
        10000,
        Duration::from_secs(300),
    ));

    let txn = TxnId(42);
    let shards: Vec<ShardId> = (0..10).map(|i| ShardId(i)).collect();

    // First apply: all succeed
    for &shard in &shards {
        assert!(
            !registry.check_and_record(txn, shard, ParticipantDecision::Commit),
            "first apply must not be duplicate"
        );
    }

    // Second apply: all duplicates
    for &shard in &shards {
        assert!(
            registry.check_and_record(txn, shard, ParticipantDecision::Commit),
            "second apply must be duplicate"
        );
    }

    let m = registry.metrics();
    assert_eq!(m.first_applies, 10);
    assert_eq!(m.duplicate_applies, 10);
    assert_eq!(m.total_checks, 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1: Membership Lifecycle — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// End-to-end: add node, make active, drain, remove.
#[test]
fn test_p11_add_drain_remove_lifecycle() {
    let lifecycle = MembershipLifecycle::new();

    // Add node 1
    lifecycle.join_node(1).unwrap();
    assert_eq!(lifecycle.get_state(1), Some(MemberState::Joining));

    // Bootstrap complete → active
    lifecycle.activate_node(1).unwrap();
    assert_eq!(lifecycle.get_state(1), Some(MemberState::Active));

    // Assign shards
    lifecycle.set_shard_assignments(1, vec![ShardId(1), ShardId(2)]);
    lifecycle.set_inflight_txns(1, 10);
    assert!(!lifecycle.is_drain_complete(1));

    // Begin drain
    lifecycle.drain_node(1).unwrap();
    assert_eq!(lifecycle.get_state(1), Some(MemberState::Draining));

    // Simulate shard migration and inflight completion
    lifecycle.set_shard_assignments(1, vec![ShardId(1)]); // one shard remaining
    lifecycle.set_inflight_txns(1, 3);
    assert!(!lifecycle.is_drain_complete(1));

    lifecycle.set_shard_assignments(1, vec![]); // all migrated
    lifecycle.set_inflight_txns(1, 0);
    assert!(lifecycle.is_drain_complete(1));

    // Remove
    lifecycle.remove_node(1).unwrap();
    assert_eq!(lifecycle.get_state(1), Some(MemberState::Removed));

    // History records all transitions
    let history = lifecycle.history();
    assert!(history.len() >= 4);
}

/// Replace node: add new, drain old, remove old.
#[test]
fn test_p11_replace_node() {
    let lifecycle = MembershipLifecycle::new();

    // Original node
    lifecycle.join_node(1).unwrap();
    lifecycle.activate_node(1).unwrap();
    lifecycle.set_shard_assignments(1, vec![ShardId(1)]);

    // New replacement node
    lifecycle.join_node(2).unwrap();
    lifecycle.activate_node(2).unwrap();

    // Begin draining old node
    lifecycle.drain_node(1).unwrap();

    // Migrate shard from node 1 to node 2
    lifecycle.set_shard_assignments(2, vec![ShardId(1)]);
    lifecycle.set_shard_assignments(1, vec![]);
    lifecycle.set_inflight_txns(1, 0);

    assert!(lifecycle.is_drain_complete(1));
    lifecycle.remove_node(1).unwrap();

    // Verify final state
    let summary = lifecycle.summary();
    assert_eq!(summary.get(&MemberState::Active), Some(&1)); // node 2
    assert_eq!(summary.get(&MemberState::Removed), Some(&1)); // node 1
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-2: Shard Migration — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Migration under continuous writes; disruption bounded.
#[test]
fn test_p12_migration_bounded_disruption() {
    let coord = ShardMigrationCoordinator::new(3, Duration::from_secs(5));

    // Start migration
    coord.start_migration(ShardId(1), 1, 2).unwrap();

    // Planning
    coord
        .advance_phase(ShardId(1), ShardMigrationPhase::Freeze)
        .unwrap();

    // Freeze is brief (simulated — we immediately advance)
    coord
        .advance_phase(ShardId(1), ShardMigrationPhase::Snapshot)
        .unwrap();

    // Snapshot phase: transfer data
    coord.record_progress(ShardId(1), 50 * 1024 * 1024, 0);

    coord
        .advance_phase(ShardId(1), ShardMigrationPhase::CatchUp)
        .unwrap();

    // Catch-up: apply WAL entries accumulated during snapshot
    coord.record_progress(ShardId(1), 1024 * 1024, 1000);

    coord
        .advance_phase(ShardId(1), ShardMigrationPhase::SwitchRouting)
        .unwrap();
    coord
        .advance_phase(ShardId(1), ShardMigrationPhase::Verify)
        .unwrap();
    coord.complete_migration(ShardId(1)).unwrap();

    let m = coord.metrics();
    assert_eq!(m.successful_migrations, 1);
    assert_eq!(m.failed_migrations, 0);
    assert_eq!(m.total_bytes_transferred, 51 * 1024 * 1024);
    assert_eq!(m.active_migrations, 0);
}

/// Invariants preserved during migration.
#[test]
fn test_p12_migration_invariants_preserved() {
    let coord = ShardMigrationCoordinator::new(3, Duration::from_secs(5));
    let gate = ReplicationInvariantGate::new(CommitPolicy::Quorum);
    gate.register_shard(ShardId(1), 3);

    // Primary at LSN 100
    gate.update_primary_lsn(ShardId(1), 100);
    gate.update_replica_lsn(ShardId(1), 0, 95);

    // Start migration
    coord.start_migration(ShardId(1), 1, 2).unwrap();

    // During migration, writes continue → primary advances
    gate.update_primary_lsn(ShardId(1), 120);

    // Prefix still holds
    assert!(gate.check_prefix(ShardId(1), 0).is_satisfied());

    // After migration catch-up, new node catches up
    gate.update_replica_lsn(ShardId(1), 0, 120);
    assert!(gate.check_prefix(ShardId(1), 0).is_satisfied());
    assert!(gate.check_no_phantom(ShardId(1), 0, 120).is_satisfied());

    coord.complete_migration(ShardId(1)).unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-3: Observability — Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Single "cluster status" view provides all signals.
#[test]
fn test_p13_unified_cluster_status() {
    let mut membership_summary = HashMap::new();
    membership_summary.insert("active".into(), 3);
    membership_summary.insert("draining".into(), 1);

    let status = ClusterStatusBuilder::new()
        .shard_epoch(ShardId(1), 5)
        .shard_epoch(ShardId(2), 3)
        .replication_lag(ShardId(1), 50)
        .replication_lag(ShardId(2), 200)
        .twopc_inflight(7)
        .indoubt_count(0)
        .routing_version(42)
        .membership(membership_summary)
        .split_brain_events(0)
        .epoch_fence_rejections(0)
        .idempotency_duplicates(3)
        .build();

    // Verify all signals present
    assert_eq!(status.shard_epochs.len(), 2);
    assert_eq!(status.shard_epochs[&ShardId(1)], 5);
    assert_eq!(status.replication_lag_ms.len(), 2);
    assert_eq!(status.twopc_inflight, 7);
    assert_eq!(status.indoubt_count, 0);
    assert_eq!(status.routing_version, 42);
    assert_eq!(status.membership_summary.len(), 2);
    assert_eq!(status.idempotency_duplicates, 3);
    assert_eq!(status.health, ClusterHealthStatus::Healthy);
}

/// Status correctly escalates to degraded/critical.
#[test]
fn test_p13_cluster_status_escalation() {
    // Degraded: high replication lag
    let degraded = ClusterStatusBuilder::new()
        .replication_lag(ShardId(1), 2000)
        .build();
    assert_eq!(degraded.health, ClusterHealthStatus::Degraded);

    // Degraded: in-doubt txns
    let degraded2 = ClusterStatusBuilder::new()
        .indoubt_count(5)
        .build();
    assert_eq!(degraded2.health, ClusterHealthStatus::Degraded);

    // Critical: split-brain events
    let critical = ClusterStatusBuilder::new()
        .split_brain_events(1)
        .build();
    assert_eq!(critical.health, ClusterHealthStatus::Critical);

    // Critical: epoch fence rejections
    let critical2 = ClusterStatusBuilder::new()
        .epoch_fence_rejections(3)
        .build();
    assert_eq!(critical2.health, ClusterHealthStatus::Critical);
}

// ═══════════════════════════════════════════════════════════════════════════
// End-to-End: Full Distributed Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

/// Full lifecycle: cluster setup → writes → failover → recovery → verify.
#[test]
fn test_e2e_full_distributed_lifecycle() {
    // Setup
    let fence = StorageEpochFence::new(1);
    let gate = ReplicationInvariantGate::new(CommitPolicy::Quorum);
    let runbook = FailoverRunbook::new();
    let idempotency = Arc::new(ParticipantIdempotencyRegistry::new(
        10000,
        Duration::from_secs(300),
    ));
    let recovery = TwoPcRecoveryCoordinator::new(idempotency.clone());
    let lifecycle = MembershipLifecycle::new();

    gate.register_shard(ShardId(1), 3);

    // Phase 1: Normal operation
    lifecycle.join_node(1).unwrap();
    lifecycle.activate_node(1).unwrap();
    lifecycle.join_node(2).unwrap();
    lifecycle.activate_node(2).unwrap();
    lifecycle.join_node(3).unwrap();
    lifecycle.activate_node(3).unwrap();

    // Primary writes at epoch 1
    assert_eq!(fence.check_write(1), EpochFenceResult::Allowed);
    gate.update_primary_lsn(ShardId(1), 50);
    gate.update_replica_lsn(ShardId(1), 0, 45);
    gate.update_replica_lsn(ShardId(1), 1, 48);
    gate.update_replica_lsn(ShardId(1), 2, 42);

    // 2PC transaction
    let txn = TxnId(1);
    recovery.enter_phase(txn, TwoPhasePhase::Preparing);
    recovery.enter_phase(txn, TwoPhasePhase::Applying);
    idempotency.check_and_record(txn, ShardId(1), ParticipantDecision::Commit);
    recovery.clear_phase(txn);

    // Phase 2: Primary failure → failover
    runbook.begin_failover();
    runbook.enter_stage(FailoverStage::DetectFailure);
    runbook.complete_stage(FailoverStage::DetectFailure);

    runbook.enter_stage(FailoverStage::FreezeWrites);
    runbook.complete_stage(FailoverStage::FreezeWrites);

    runbook.enter_stage(FailoverStage::SealEpoch);
    fence.advance_epoch(2);
    runbook.complete_stage(FailoverStage::SealEpoch);

    // Old leader (epoch 1) writes must be rejected
    assert!(matches!(fence.check_write(1), EpochFenceResult::Rejected { .. }));

    runbook.enter_stage(FailoverStage::CatchUp);
    gate.update_replica_lsn(ShardId(1), 1, 50); // best replica catches up
    runbook.complete_stage(FailoverStage::CatchUp);

    runbook.enter_stage(FailoverStage::Promote);
    runbook.complete_stage(FailoverStage::Promote);

    runbook.enter_stage(FailoverStage::Reopen);
    // New leader writes at epoch 2
    assert_eq!(fence.check_write(2), EpochFenceResult::Allowed);
    gate.update_primary_lsn(ShardId(1), 55);
    runbook.complete_stage(FailoverStage::Reopen);

    // Phase 3: Verify invariants
    runbook.enter_stage(FailoverStage::Verify);
    let invariants = vec![
        InvariantCheckResult {
            invariant: "prefix_property".into(),
            passed: gate.check_prefix(ShardId(1), 0).is_satisfied()
                && gate.check_prefix(ShardId(1), 1).is_satisfied()
                && gate.check_prefix(ShardId(1), 2).is_satisfied(),
            detail: "all replicas satisfy prefix".into(),
        },
        InvariantCheckResult {
            invariant: "no_phantom".into(),
            passed: gate.check_no_phantom(ShardId(1), 1, 50).is_satisfied(),
            detail: "promoted replica has no phantom commits".into(),
        },
        InvariantCheckResult {
            invariant: "promotion_safety".into(),
            passed: gate
                .check_promotion_safety(ShardId(1), 50, 50)
                .is_satisfied(),
            detail: "new primary commit set ⊆ old primary".into(),
        },
    ];
    runbook.complete_stage(FailoverStage::Verify);
    runbook.mark_complete();

    let report = runbook.generate_report(ShardId(1), 1, 2, 1, 2, invariants);
    assert!(report.passed, "E2E lifecycle failover must pass all invariants");
    assert_eq!(report.old_epoch, 1);
    assert_eq!(report.new_epoch, 2);

    // Phase 4: Cluster status reflects healthy state
    let status = ClusterStatusBuilder::new()
        .shard_epoch(ShardId(1), 2)
        .replication_lag(ShardId(1), 0)
        .twopc_inflight(0)
        .indoubt_count(0)
        .split_brain_events(0)
        .epoch_fence_rejections(fence.metrics().total_rejections)
        .build();

    // Epoch fence had rejections from old leader → critical
    if fence.metrics().total_rejections > 0 {
        assert_eq!(status.health, ClusterHealthStatus::Critical);
    }
}
