//! v1.0.9 Chaos & Failure Tests — Self-Healing Integration Tests
//!
//! Tests cover:
//! - Leader kill + automatic re-election
//! - Follower kill + replica catch-up
//! - Network jitter simulation (suspect→alive flapping)
//! - WAL backlog pressure escalation
//! - Gateway overload + backpressure admission
//! - Rolling upgrade lifecycle
//! - Node drain + join lifecycle
//! - SLO metric accuracy under load
//! - Ops audit log completeness
//! - Full self-healing orchestration cycle

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use falcon_common::types::NodeId;
use falcon_cluster::self_healing::*;

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn setup_3node_cluster() -> (ClusterFailureDetector, LeaderElectionCoordinator) {
    let fd = ClusterFailureDetector::new(FailureDetectorConfig {
        suspect_threshold: Duration::from_millis(50),
        failure_timeout: Duration::from_millis(150),
        max_consecutive_misses: 3,
        ..Default::default()
    });
    for i in 1..=3 {
        fd.register_node(NodeId(i), "1.0.9".into());
        fd.record_heartbeat(NodeId(i), 0, 0);
    }

    let ec = LeaderElectionCoordinator::new(ElectionConfig {
        require_quorum: false, // simplified for testing
        ..Default::default()
    });
    ec.register_shard(0, Some(NodeId(1)), 3);
    ec.register_shard(1, Some(NodeId(2)), 3);

    (fd, ec)
}

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Leader Kill + Re-election
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_leader_kill_triggers_reelection() {
    let (fd, ec) = setup_3node_cluster();
    let audit = OpsAuditLog::new(100);

    // Leader is Node 1 for shard 0
    assert_eq!(ec.get_leader(0), Some(NodeId(1)));

    // Kill leader (stop heartbeats)
    thread::sleep(Duration::from_millis(200));
    let transitions = fd.evaluate();

    // Node 1 should be DEAD
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Dead));
    audit.record(OpsEventType::NodeDown(NodeId(1)), "failure_detector");

    // Start election for shard 0
    let term = ec.start_election(0).unwrap();
    audit.record(OpsEventType::LeaderLost {
        shard_id: 0,
        old_leader: NodeId(1),
    }, "election_coordinator");

    // Surviving nodes submit candidacy
    ec.submit_candidate(0, NodeId(2), 500, term);
    ec.submit_candidate(0, NodeId(3), 800, term);

    // Resolve — Node 3 has highest LSN
    let winner = ec.resolve_election(0).unwrap();
    assert_eq!(winner, NodeId(3));
    audit.record(OpsEventType::LeaderElected {
        shard_id: 0,
        leader: NodeId(3),
        term,
    }, "election_coordinator");

    // Verify audit trail
    let events = audit.all();
    assert_eq!(events.len(), 3);
    assert!(matches!(events[0].event_type, OpsEventType::NodeDown(NodeId(1))));
    assert!(matches!(events[2].event_type, OpsEventType::LeaderElected { leader: NodeId(3), .. }));
}

#[test]
fn test_leader_kill_no_quorum_degrades_gracefully() {
    let fd = ClusterFailureDetector::new(FailureDetectorConfig {
        suspect_threshold: Duration::from_millis(30),
        failure_timeout: Duration::from_millis(80),
        ..Default::default()
    });
    for i in 1..=3 {
        fd.register_node(NodeId(i), "1.0.9".into());
        fd.record_heartbeat(NodeId(i), 0, 0);
    }

    let ec = LeaderElectionCoordinator::new(ElectionConfig {
        require_quorum: true,
        ..Default::default()
    });
    ec.register_shard(0, Some(NodeId(1)), 3); // quorum = 2

    // Kill 2 of 3 nodes
    thread::sleep(Duration::from_millis(100));
    fd.evaluate();
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Dead));
    assert_eq!(fd.get_liveness(NodeId(2)), Some(NodeLiveness::Dead));

    // Start election
    let term = ec.start_election(0).unwrap();
    ec.submit_candidate(0, NodeId(3), 100, term);

    // Should fail — no quorum (1 of 2 needed)
    let result = ec.resolve_election(0);
    assert!(matches!(result, Err(ElectionError::NoQuorum { .. })));
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Follower Kill + Catch-up
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_follower_kill_and_wal_catchup() {
    let (fd, _ec) = setup_3node_cluster();
    let catchup = ReplicaCatchUpCoordinator::new(CatchUpConfig::default());
    let audit = OpsAuditLog::new(100);

    // Simulate follower Node 3 dying
    thread::sleep(Duration::from_millis(200));
    // Only Node 1 and 2 send heartbeats
    fd.record_heartbeat(NodeId(1), 5000, 300);
    fd.record_heartbeat(NodeId(2), 4900, 300);
    fd.evaluate();

    assert_eq!(fd.get_liveness(NodeId(3)), Some(NodeLiveness::Dead));
    audit.record(OpsEventType::NodeDown(NodeId(3)), "failure_detector");

    // Node 3 comes back, needs catch-up
    fd.record_heartbeat(NodeId(3), 3000, 10); // behind by 2000 LSN
    assert_eq!(fd.get_liveness(NodeId(3)), Some(NodeLiveness::Alive));

    let strategy = catchup.start_catch_up(NodeId(3), 0, 5000, 3000);
    assert_eq!(strategy, ReplicaLagClass::WalReplay);
    audit.record(OpsEventType::CatchUpStarted {
        node_id: NodeId(3),
        shard_id: 0,
        strategy,
    }, "catchup_coordinator");

    // Simulate progress
    catchup.update_progress(NodeId(3), 0, 4000, 1000);
    let state = catchup.get_state(NodeId(3), 0).unwrap();
    assert_eq!(state.phase, CatchUpPhase::StreamingWal);

    catchup.update_progress(NodeId(3), 0, 5000, 1000);
    let state = catchup.get_state(NodeId(3), 0).unwrap();
    assert_eq!(state.phase, CatchUpPhase::Complete);
    audit.record(OpsEventType::CatchUpCompleted {
        node_id: NodeId(3),
        shard_id: 0,
    }, "catchup_coordinator");

    assert_eq!(audit.total(), 3);
}

#[test]
fn test_follower_far_behind_needs_snapshot() {
    let catchup = ReplicaCatchUpCoordinator::new(CatchUpConfig {
        snapshot_threshold_lsn: 10_000,
        ..Default::default()
    });

    // Replica is 500K behind — needs snapshot
    let strategy = catchup.start_catch_up(NodeId(5), 0, 500_000, 0);
    assert_eq!(strategy, ReplicaLagClass::SnapshotRequired);

    let state = catchup.get_state(NodeId(5), 0).unwrap();
    assert_eq!(state.phase, CatchUpPhase::StreamingSnapshot);
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Network Jitter (Suspect Flapping)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_network_jitter_suspect_flapping() {
    let fd = ClusterFailureDetector::new(FailureDetectorConfig {
        suspect_threshold: Duration::from_millis(20),
        failure_timeout: Duration::from_millis(100),
        ..Default::default()
    });
    fd.register_node(NodeId(1), "1.0.9".into());
    fd.record_heartbeat(NodeId(1), 0, 0);

    // Simulate jitter: heartbeat comes late but before failure timeout
    thread::sleep(Duration::from_millis(30));
    fd.evaluate();
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Suspect));

    // Heartbeat arrives — back to alive
    fd.record_heartbeat(NodeId(1), 100, 30);
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Alive));

    // Repeat: another jitter cycle
    thread::sleep(Duration::from_millis(30));
    fd.evaluate();
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Suspect));
    fd.record_heartbeat(NodeId(1), 200, 60);
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Alive));

    // Should not have reached DEAD at any point
    assert!(fd.metrics.dead_transitions.load(std::sync::atomic::Ordering::Relaxed) == 0);
    assert!(fd.metrics.suspect_transitions.load(std::sync::atomic::Ordering::Relaxed) >= 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — WAL Backlog Pressure
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal_backlog_escalation() {
    let bp = BackpressureController::new(BackpressureConfig::default());

    // Normal
    bp.update_signal(PressureSignal::WalBacklog, 1_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::Normal);

    // Elevated
    bp.update_signal(PressureSignal::WalBacklog, 15_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::Elevated);
    assert_eq!(bp.current_rate(), 0.8);

    // High
    bp.update_signal(PressureSignal::WalBacklog, 60_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::High);
    assert_eq!(bp.current_rate(), 0.5);

    // Critical
    bp.update_signal(PressureSignal::WalBacklog, 150_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::Critical);
    assert_eq!(bp.current_rate(), 0.1);

    // Recovery
    bp.update_signal(PressureSignal::WalBacklog, 5_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::Normal);
    assert_eq!(bp.current_rate(), 1.0);
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Gateway Overload + Backpressure
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_gateway_overload_backpressure() {
    let bp = BackpressureController::new(BackpressureConfig::default());
    let slo = SloTracker::new(60);

    // Simulate overload: CPU high + memory high
    bp.update_signal(PressureSignal::Cpu, 0.92);
    bp.update_signal(PressureSignal::Memory, 0.88);
    bp.evaluate();

    // Should be critical (CPU > 0.85 threshold for HIGH, but 0.92 < 0.95 = HIGH)
    // Actually CPU 0.92 >= 0.85 (high) but < 0.95 (critical) → HIGH
    // Memory 0.88 >= 0.85 (high) but < 0.95 (critical) → HIGH
    assert_eq!(bp.current_level(), PressureLevel::High);

    // Under high pressure, ~50% of requests should be throttled
    let mut admitted = 0;
    let mut rejected = 0;
    for i in 0..1000u64 {
        if bp.should_admit(i) {
            admitted += 1;
            slo.record_write(100, true);
        } else {
            rejected += 1;
            slo.record_gateway_request(true);
        }
    }
    assert!(admitted > 400 && admitted < 600, "admitted={}", admitted);
    assert!(rejected > 400 && rejected < 600, "rejected={}", rejected);

    let snap = slo.snapshot();
    assert!(snap.gateway_reject_rate > 0.0);
}

#[test]
fn test_combined_pressure_max_wins() {
    let bp = BackpressureController::new(BackpressureConfig::default());

    // CPU normal, but replication lag critical
    bp.update_signal(PressureSignal::Cpu, 0.3);
    bp.update_signal(PressureSignal::ReplicationLag, 200_000.0);
    let level = bp.evaluate();

    // Critical wins (max of all signals)
    assert_eq!(level, PressureLevel::Critical);
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Rolling Upgrade Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_rolling_upgrade_full_lifecycle() {
    let audit = OpsAuditLog::new(100);
    let coord = RollingUpgradeCoordinator::new(
        "1.0.10".into(),
        ProtocolVersion::new(1, 9),
        ProtocolVersion::new(1, 10),
    );

    assert!(coord.check_compatibility());
    audit.record(OpsEventType::UpgradeStarted {
        target_version: "1.0.10".into(),
    }, "ops");

    // Plan: follower → gateway → leader
    coord.plan_upgrade(vec![
        (NodeId(3), UpgradeOrder::Leader, "1.0.9".into()),
        (NodeId(1), UpgradeOrder::Follower, "1.0.9".into()),
        (NodeId(2), UpgradeOrder::Gateway, "1.0.9".into()),
    ]);

    let plan = coord.plan_snapshot();
    assert_eq!(plan[0].node_id, NodeId(1)); // Follower first
    assert_eq!(plan[1].node_id, NodeId(2)); // Gateway second
    assert_eq!(plan[2].node_id, NodeId(3)); // Leader last

    // Upgrade each node sequentially
    for i in 0..3 {
        let node = coord.next_node().unwrap();
        coord.start_node_upgrade(node);
        coord.mark_upgrading(node);
        coord.mark_rejoining(node);
        coord.mark_complete(node);
        audit.record(OpsEventType::UpgradeNodeComplete {
            node_id: node,
            version: "1.0.10".into(),
        }, "ops");
    }

    assert!(coord.is_complete());
    assert_eq!(coord.progress(), (3, 3));
    audit.record(OpsEventType::UpgradeComplete {
        version: "1.0.10".into(),
    }, "ops");

    // Verify audit trail
    assert_eq!(audit.total(), 5); // 1 start + 3 node + 1 complete
}

#[test]
fn test_rolling_upgrade_incompatible_version() {
    let coord = RollingUpgradeCoordinator::new(
        "2.0.0".into(),
        ProtocolVersion::new(1, 9),
        ProtocolVersion::new(2, 0), // major version change
    );
    assert!(!coord.check_compatibility());
}

#[test]
fn test_rolling_upgrade_node_failure() {
    let coord = RollingUpgradeCoordinator::new(
        "1.0.10".into(),
        ProtocolVersion::new(1, 9),
        ProtocolVersion::new(1, 10),
    );
    coord.plan_upgrade(vec![
        (NodeId(1), UpgradeOrder::Follower, "1.0.9".into()),
        (NodeId(2), UpgradeOrder::Leader, "1.0.9".into()),
    ]);

    coord.start_node_upgrade(NodeId(1));
    coord.mark_failed(NodeId(1), "health check failed after restart".into());

    let plan = coord.plan_snapshot();
    assert!(matches!(plan[0].state, UpgradeNodeState::Failed(_)));
    assert!(!coord.is_complete());
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Node Drain + Join
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_drain_with_inflight_completion() {
    let lc = NodeLifecycleCoordinator::new();
    let fd = ClusterFailureDetector::new(FailureDetectorConfig::default());
    let audit = OpsAuditLog::new(100);

    fd.register_node(NodeId(1), "1.0.9".into());

    // Start drain
    lc.start_drain(NodeId(1), vec![0, 1]);
    fd.mark_draining(NodeId(1));
    audit.record(OpsEventType::DrainStarted(NodeId(1)), "ops");

    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Draining));

    // Simulate inflight completion
    lc.update_drain(NodeId(1), 10); // 10 inflight
    assert!(!lc.is_drained(NodeId(1)));

    lc.update_drain(NodeId(1), 0); // all drained
    // Still have shards to transfer
    lc.record_shard_transferred(NodeId(1), 0);
    lc.record_shard_transferred(NodeId(1), 1);

    assert!(lc.is_drained(NodeId(1)));
    audit.record(OpsEventType::DrainCompleted(NodeId(1)), "ops");
    assert_eq!(audit.total(), 2);
}

#[test]
fn test_join_with_shard_sync() {
    let lc = NodeLifecycleCoordinator::new();
    let fd = ClusterFailureDetector::new(FailureDetectorConfig::default());
    let catchup = ReplicaCatchUpCoordinator::new(CatchUpConfig::default());
    let audit = OpsAuditLog::new(100);

    fd.register_node(NodeId(5), "1.0.9".into());
    fd.mark_joining(NodeId(5));
    assert_eq!(fd.get_liveness(NodeId(5)), Some(NodeLiveness::Joining));

    lc.start_join(NodeId(5), vec![0, 1]);
    audit.record(OpsEventType::JoinStarted(NodeId(5)), "ops");

    // Start catch-up for each shard
    catchup.start_catch_up(NodeId(5), 0, 10_000, 0);
    catchup.start_catch_up(NodeId(5), 1, 8_000, 0);

    // Complete shard 0 catch-up
    catchup.update_progress(NodeId(5), 0, 10_000, 10_000);
    lc.record_shard_synced(NodeId(5), 0);

    // Complete shard 1 catch-up
    catchup.update_progress(NodeId(5), 1, 8_000, 8_000);
    lc.record_shard_synced(NodeId(5), 1);

    let join_state = lc.get_join(NodeId(5)).unwrap();
    assert_eq!(join_state.phase, JoinPhase::Ready);

    // Activate
    lc.activate_join(NodeId(5));
    audit.record(OpsEventType::JoinCompleted(NodeId(5)), "ops");

    let join_state = lc.get_join(NodeId(5)).unwrap();
    assert_eq!(join_state.phase, JoinPhase::Active);
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — SLO Accuracy Under Load
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_slo_accuracy_under_mixed_workload() {
    let slo = SloTracker::new(60);

    // 90% successful writes, 10% failures
    for i in 0..1000 {
        let success = i % 10 != 0;
        slo.record_write((i as u64) * 10, success);
    }

    // 99% successful reads
    for i in 0..1000 {
        let success = i % 100 != 0;
        slo.record_read((i as u64) * 5, success);
    }

    slo.update_replication_lag(500);

    // 5% gateway rejects
    for i in 0..1000 {
        slo.record_gateway_request(i % 20 == 0);
    }

    let snap = slo.snapshot();

    // Write availability: 900/1000 = 90%
    assert!((snap.write_availability - 0.9).abs() < 0.01,
        "write_availability={}", snap.write_availability);

    // Read availability: 990/1000 = 99%
    assert!((snap.read_availability - 0.99).abs() < 0.01,
        "read_availability={}", snap.read_availability);

    // Replication lag
    assert_eq!(snap.replication_lag_max_lsn, 500);

    // Gateway reject rate: 50/1000 = 5%
    assert!((snap.gateway_reject_rate - 0.05).abs() < 0.01,
        "gateway_reject_rate={}", snap.gateway_reject_rate);

    // p99 should be in upper range
    assert!(snap.write_p99_us > 0);
    assert!(snap.read_p99_us > 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — Concurrent Self-Healing Orchestration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_concurrent_failure_detection_and_election() {
    let fd = Arc::new(ClusterFailureDetector::new(FailureDetectorConfig {
        suspect_threshold: Duration::from_millis(20),
        failure_timeout: Duration::from_millis(60),
        ..Default::default()
    }));
    let ec = Arc::new(LeaderElectionCoordinator::new(ElectionConfig {
        require_quorum: false,
        ..Default::default()
    }));

    // Register 5 nodes
    for i in 1..=5 {
        fd.register_node(NodeId(i), "1.0.9".into());
        fd.record_heartbeat(NodeId(i), 0, 0);
    }
    ec.register_shard(0, Some(NodeId(1)), 5);

    // Concurrent heartbeats from 4 threads (simulating 4 healthy nodes)
    let mut handles = Vec::new();
    for node in 2..=5u64 {
        let fd_clone = Arc::clone(&fd);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                fd_clone.record_heartbeat(NodeId(node), i * 10, i);
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    // Meanwhile, Node 1 (leader) stops sending heartbeats
    thread::sleep(Duration::from_millis(80));
    fd.evaluate();

    // Wait for all heartbeat threads
    for h in handles {
        h.join().unwrap();
    }

    // Node 1 should be dead
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Dead));

    // Trigger election
    let term = ec.start_election(0).unwrap();
    for node in 2..=5u64 {
        let snap = fd.snapshot();
        let lsn = snap.iter()
            .find(|r| r.node_id == NodeId(node))
            .map(|r| r.last_lsn)
            .unwrap_or(0);
        ec.submit_candidate(0, NodeId(node), lsn, term);
    }
    let winner = ec.resolve_election(0).unwrap();

    // Winner should be one of the surviving nodes
    assert!(winner.0 >= 2 && winner.0 <= 5);
    assert_eq!(ec.get_state(0), Some(ElectionState::Stable));
}

#[test]
fn test_concurrent_slo_recording() {
    let slo = Arc::new(SloTracker::new(60));
    let mut handles = Vec::new();

    for t in 0..4 {
        let slo_clone = Arc::clone(&slo);
        handles.push(thread::spawn(move || {
            for i in 0..1000u64 {
                if t % 2 == 0 {
                    slo_clone.record_write(i * 10, i % 10 != 0);
                } else {
                    slo_clone.record_read(i * 5, i % 20 != 0);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let snap = slo.snapshot();
    assert!(snap.write_availability > 0.0);
    assert!(snap.read_availability > 0.0);
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Full Self-Healing Cycle
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_full_self_healing_cycle() {
    // Setup: 3-node cluster, shard 0 on Node 1 (leader), 2, 3
    let fd = ClusterFailureDetector::new(FailureDetectorConfig {
        suspect_threshold: Duration::from_millis(30),
        failure_timeout: Duration::from_millis(80),
        ..Default::default()
    });
    let ec = LeaderElectionCoordinator::new(ElectionConfig {
        require_quorum: false,
        ..Default::default()
    });
    let catchup = ReplicaCatchUpCoordinator::new(CatchUpConfig::default());
    let bp = BackpressureController::new(BackpressureConfig::default());
    let slo = SloTracker::new(60);
    let audit = OpsAuditLog::new(100);

    // Step 1: Register nodes and establish leader
    for i in 1..=3 {
        fd.register_node(NodeId(i), "1.0.9".into());
        fd.record_heartbeat(NodeId(i), 1000, 60);
    }
    ec.register_shard(0, Some(NodeId(1)), 3);
    audit.record(OpsEventType::NodeUp(NodeId(1)), "system");
    audit.record(OpsEventType::NodeUp(NodeId(2)), "system");
    audit.record(OpsEventType::NodeUp(NodeId(3)), "system");

    // Step 2: Normal operation
    for _ in 0..10 {
        slo.record_write(100, true);
        slo.record_read(50, true);
    }
    bp.update_signal(PressureSignal::Cpu, 0.3);
    assert_eq!(bp.evaluate(), PressureLevel::Normal);

    // Step 3: Leader Node 1 crashes
    thread::sleep(Duration::from_millis(100));
    fd.record_heartbeat(NodeId(2), 2000, 120);
    fd.record_heartbeat(NodeId(3), 1800, 120);
    fd.evaluate();
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Dead));
    audit.record(OpsEventType::NodeDown(NodeId(1)), "failure_detector");

    // Step 4: Election
    let term = ec.start_election(0).unwrap();
    audit.record(OpsEventType::LeaderLost {
        shard_id: 0,
        old_leader: NodeId(1),
    }, "election_coordinator");
    ec.submit_candidate(0, NodeId(2), 2000, term);
    ec.submit_candidate(0, NodeId(3), 1800, term);
    let winner = ec.resolve_election(0).unwrap();
    assert_eq!(winner, NodeId(2)); // highest LSN
    audit.record(OpsEventType::LeaderElected {
        shard_id: 0,
        leader: winner,
        term,
    }, "election_coordinator");

    // Step 5: Catch up Node 3 (behind by 200 LSN)
    let strategy = catchup.start_catch_up(NodeId(3), 0, 2000, 1800);
    assert_eq!(strategy, ReplicaLagClass::WalReplay);
    catchup.update_progress(NodeId(3), 0, 2000, 200);
    let state = catchup.get_state(NodeId(3), 0).unwrap();
    assert_eq!(state.phase, CatchUpPhase::Complete);

    // Step 6: Verify SLO is still reasonable
    let snap = slo.snapshot();
    assert!(snap.write_availability > 0.9);

    // Step 7: Node 1 comes back
    fd.record_heartbeat(NodeId(1), 500, 5);
    assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Alive));
    audit.record(OpsEventType::NodeUp(NodeId(1)), "failure_detector");

    // Step 8: Catch up Node 1
    let strategy = catchup.start_catch_up(NodeId(1), 0, 2000, 500);
    assert_eq!(strategy, ReplicaLagClass::WalReplay);

    // Verify audit trail tells the full story
    let events = audit.all();
    assert!(events.len() >= 7);
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 — Backpressure Under Multi-Signal Pressure
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_backpressure_multi_signal_interaction() {
    let bp = BackpressureController::new(BackpressureConfig::default());

    // Start normal
    bp.update_signal(PressureSignal::Cpu, 0.3);
    bp.update_signal(PressureSignal::Memory, 0.4);
    bp.update_signal(PressureSignal::WalBacklog, 1_000_000.0);
    bp.update_signal(PressureSignal::ReplicationLag, 100.0);
    assert_eq!(bp.evaluate(), PressureLevel::Normal);

    // CPU spikes → elevated
    bp.update_signal(PressureSignal::Cpu, 0.75);
    assert_eq!(bp.evaluate(), PressureLevel::Elevated);

    // WAL backlog also rises → high (max of elevated, high)
    bp.update_signal(PressureSignal::WalBacklog, 60_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::High);

    // Replication lag goes critical → critical
    bp.update_signal(PressureSignal::ReplicationLag, 200_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::Critical);

    // Fix replication lag → back to high (WAL still high)
    bp.update_signal(PressureSignal::ReplicationLag, 100.0);
    assert_eq!(bp.evaluate(), PressureLevel::High);

    // Fix WAL → elevated (CPU still elevated)
    bp.update_signal(PressureSignal::WalBacklog, 1_000_000.0);
    assert_eq!(bp.evaluate(), PressureLevel::Elevated);

    // Fix CPU → normal
    bp.update_signal(PressureSignal::Cpu, 0.3);
    assert_eq!(bp.evaluate(), PressureLevel::Normal);
}

// ═══════════════════════════════════════════════════════════════════════════
// §12 — Ops Audit Completeness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_ops_audit_covers_all_event_types() {
    let audit = OpsAuditLog::new(100);

    // Record every event type
    audit.record(OpsEventType::NodeUp(NodeId(1)), "system");
    audit.record(OpsEventType::NodeDown(NodeId(1)), "fd");
    audit.record(OpsEventType::NodeSuspect(NodeId(1)), "fd");
    audit.record(OpsEventType::LeaderElected {
        shard_id: 0, leader: NodeId(1), term: 1,
    }, "ec");
    audit.record(OpsEventType::LeaderLost {
        shard_id: 0, old_leader: NodeId(1),
    }, "ec");
    audit.record(OpsEventType::DrainStarted(NodeId(1)), "ops");
    audit.record(OpsEventType::DrainCompleted(NodeId(1)), "ops");
    audit.record(OpsEventType::JoinStarted(NodeId(2)), "ops");
    audit.record(OpsEventType::JoinCompleted(NodeId(2)), "ops");
    audit.record(OpsEventType::UpgradeStarted {
        target_version: "1.0.10".into(),
    }, "ops");
    audit.record(OpsEventType::UpgradeNodeComplete {
        node_id: NodeId(1), version: "1.0.10".into(),
    }, "ops");
    audit.record(OpsEventType::UpgradeComplete {
        version: "1.0.10".into(),
    }, "ops");
    audit.record(OpsEventType::ConfigChanged {
        key: "heartbeat_interval".into(),
        old_value: "1s".into(),
        new_value: "2s".into(),
    }, "admin");
    audit.record(OpsEventType::BackpressureChanged {
        old_level: PressureLevel::Normal,
        new_level: PressureLevel::High,
    }, "bp");
    audit.record(OpsEventType::CatchUpStarted {
        node_id: NodeId(3),
        shard_id: 0,
        strategy: ReplicaLagClass::WalReplay,
    }, "catchup");
    audit.record(OpsEventType::CatchUpCompleted {
        node_id: NodeId(3),
        shard_id: 0,
    }, "catchup");

    assert_eq!(audit.total(), 16);
    let all = audit.all();
    assert_eq!(all.len(), 16);

    // Each event has monotonic ID and timestamp
    for i in 1..all.len() {
        assert!(all[i].id > all[i - 1].id);
        assert!(all[i].timestamp >= all[i - 1].timestamp);
    }
}
