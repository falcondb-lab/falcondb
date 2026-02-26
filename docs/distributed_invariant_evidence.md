# FalconDB — Distributed Invariant Evidence Map

> Every claim about FalconDB's distributed correctness maps to a specific
> test, module, or gameday script. No hand-wavy behavior.

---

## Evidence Index

| ID | Invariant / Claim | Code Module | Test(s) | Gameday Drill |
|----|-------------------|-------------|---------|---------------|
| E-01 | Stale-epoch writes rejected at storage barrier | `falcon_storage::io::epoch_fence::StorageEpochFence` | `test_fence_*` (6 unit) + `test_p01_*` (3 integration) | GD-8 |
| E-02 | Stale-epoch writes rejected at cluster layer | `falcon_cluster::dist_hardening::SplitBrainDetector` | `test_split_brain_*` (unit) + `test_p01_epoch_fence_plus_split_brain_detector` | GD-8 |
| E-03 | Epoch monotonicity after promotion | `StorageEpochFence::advance_epoch` (fetch_max) | `test_fence_advance_is_monotonic` | GD-2, GD-8 |
| E-04 | No double-writes during network partition | `StorageEpochFence` + `SplitBrainDetector` | `test_p01_network_partition_no_double_writes` | GD-3, GD-8 |
| E-05 | Prefix property: committed(replica) ⊆ committed(primary) | `distributed_enhancements::ReplicationInvariantGate::check_prefix` | `test_prefix_property_*` (2 unit) + `test_p02_invariants_across_policy_matrix` | GD-9 |
| E-06 | No phantom commits on replicas | `ReplicationInvariantGate::check_no_phantom` | `test_no_phantom_commits` (unit) + `test_p02_*` | GD-9 |
| E-07 | Visibility bound to commit policy | `ReplicationInvariantGate::check_visibility` | `test_visibility_*_policy` (3 unit) + `test_p02_invariants_across_policy_matrix` | GD-9 |
| E-08 | Promotion safety: new primary ⊆ old primary | `ReplicationInvariantGate::check_promotion_safety` | `test_promotion_safety` (unit) + `test_p02_promotion_commit_set_subset` | GD-9 |
| E-09 | Crash-point matrix: invariants hold across all crash points | `ReplicationInvariantGate` | `test_p02_crashpoint_matrix` | GD-9 |
| E-10 | Failover stages formalized with audit trail | `distributed_enhancements::FailoverRunbook` | `test_failover_runbook_stages` + `test_p03_gameday_failover_drill` | GD-10 |
| E-11 | Machine-readable failover verification report | `FailoverRunbook::generate_report` | `test_failover_verification_report` + `test_p03_gameday_failover_drill` | GD-10 |
| E-12 | Report fails if any invariant violated | `FailoverVerificationReport::passed` | `test_failover_report_with_failure` + `test_p03_gameday_fails_on_violation` | GD-10 |
| E-13 | Coordinator decision log survives crash | `deterministic_2pc::CoordinatorDecisionLog` | `test_p04_crash_at_each_phase` | GD-11 |
| E-14 | Deterministic 2PC recovery at each phase | `TwoPcRecoveryCoordinator::recover_decision` | `test_p04_crash_at_each_phase` | GD-11 |
| E-15 | Participant idempotency (at-most-once) | `ParticipantIdempotencyRegistry::check_and_record` | `test_participant_idempotency_*` (3 unit) + `test_p04_participant_idempotency_concurrent` | GD-11 |
| E-16 | Node lifecycle: Joining→Active→Draining→Removed | `MembershipLifecycle` | `test_membership_*` (5 unit) + `test_p11_*` (2 integration) | GD-12 |
| E-17 | Invalid transitions rejected | `MembershipLifecycle::transition` | `test_membership_invalid_transition` | GD-12 |
| E-18 | Drain completes only when inflight=0 and shards=0 | `MembershipLifecycle::is_drain_complete` | `test_membership_drain_complete` + `test_p11_add_drain_remove_lifecycle` | GD-12 |
| E-19 | Shard migration bounded disruption | `ShardMigrationCoordinator` (max_freeze_duration) | `test_migration_lifecycle` + `test_p12_migration_bounded_disruption` | GD-12 |
| E-20 | Migration concurrency limited | `ShardMigrationCoordinator` (max_concurrent) | `test_migration_concurrency_limit` | GD-12 |
| E-21 | Invariants preserved during migration | `ReplicationInvariantGate` + `ShardMigrationCoordinator` | `test_p12_migration_invariants_preserved` | GD-12 |
| E-22 | Unified cluster status view | `ClusterStatusBuilder` | `test_cluster_status_*` (4 unit) + `test_p13_*` (2 integration) | — |
| E-23 | Health escalation: healthy → degraded → critical | `ClusterStatusBuilder::build` | `test_p13_cluster_status_escalation` | — |
| E-24 | E2E: setup → writes → failover → recovery → verify | All components | `test_e2e_full_distributed_lifecycle` | GD-2, GD-8–12 |
| E-25 | Failover outcome guard (at-most-once commit) | `stability_hardening::FailoverOutcomeGuard` | `test_failover_outcome_*` | GD-2 |

---

## Test Counts by Deliverable

| Deliverable | Unit Tests | Integration Tests | Total |
|-------------|-----------|-------------------|-------|
| P0-1: Epoch/Fencing | 6 (storage) + 2 (cluster) | 3 | 11 |
| P0-2: Replication Invariants | 8 | 3 | 11 |
| P0-3: Failover Runbook | 3 | 2 | 5 |
| P0-4: 2PC Recovery | 4 | 2 | 6 |
| P1-1: Membership Lifecycle | 5 | 2 | 7 |
| P1-2: Shard Migration | 3 | 2 | 5 |
| P1-3: Observability | 4 | 2 | 6 |
| E2E Lifecycle | — | 1 | 1 |
| **Total** | **35** | **17** | **52** |

---

## How to Run All Evidence

```bash
# Quick: all unit + integration tests
cargo test -p falcon_storage --lib -- epoch_fence
cargo test -p falcon_cluster --lib -- distributed_enhancements
cargo test -p falcon_cluster --test distributed_enhancements_integration

# Full: entire cluster crate
cargo test -p falcon_cluster

# CI: add to .github/workflows/ci.yml
# - name: Distributed invariant tests
#   run: |
#     cargo test -p falcon_storage --lib -- epoch_fence
#     cargo test -p falcon_cluster --test distributed_enhancements_integration
```

---

## Module → File Map

| Module | File | Lines |
|--------|------|-------|
| `StorageEpochFence` | `crates/falcon_storage/src/io/epoch_fence.rs` | ~200 |
| `ReplicationInvariantGate` | `crates/falcon_cluster/src/distributed_enhancements.rs` §1 | ~230 |
| `FailoverRunbook` | `crates/falcon_cluster/src/distributed_enhancements.rs` §2 | ~250 |
| `TwoPcRecoveryCoordinator` | `crates/falcon_cluster/src/distributed_enhancements.rs` §3 | ~300 |
| `MembershipLifecycle` | `crates/falcon_cluster/src/distributed_enhancements.rs` §4 | ~200 |
| `ShardMigrationCoordinator` | `crates/falcon_cluster/src/distributed_enhancements.rs` §5 | ~200 |
| `ClusterStatusBuilder` | `crates/falcon_cluster/src/distributed_enhancements.rs` §6 | ~150 |
| Integration tests | `crates/falcon_cluster/tests/distributed_enhancements_integration.rs` | ~500 |

---

## Existing Distributed Modules (Pre-Enhancement)

These modules provide the foundation that the enhancements build on:

| Module | File | Purpose |
|--------|------|---------|
| `SplitBrainDetector` | `dist_hardening.rs` | Cluster-layer epoch fencing |
| `HAReplicaGroup` | `ha.rs` | Epoch tracking, promotion orchestration |
| `FailoverOutcomeGuard` | `stability_hardening.rs` | At-most-once commit semantics |
| `CoordinatorDecisionLog` | `deterministic_2pc.rs` | Durable 2PC decision log |
| `HardenedTwoPhaseCoordinator` | `two_phase.rs` | 2PC with retry, circuit breaker, deadlock detection |
| `InDoubtResolver` | `indoubt_resolver.rs` | Background 2PC crash recovery |
| `EpochGuard` | `epoch.rs` | RAII epoch proof token |
| `LeaderLease` | `leader_lease.rs` | Quorum-based leader authority |
| `ShardMigration` | `migration.rs` | Migration state machine |
| `DistributedSupervisor` | `supervisor.rs` | Control-plane component health |
