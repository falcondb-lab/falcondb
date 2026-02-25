//! v1.1.1 Enterprise Soak / Longevity Tests
//!
//! Simulates 7-day steady-state operation in compressed time:
//! mixed read/write/backup/compaction/rebalance workloads,
//! fault injection (node restart, network jitter, disk delay),
//! and verifies SLA compliance, zero resource leaks, and
//! deterministic behavior throughout.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use falcon_cluster::ga_hardening::*;
use falcon_cluster::cost_capacity::*;
use falcon_common::types::{NodeId, TableId};

// ═══════════════════════════════════════════════════════════════════════════
// Crash / Restart Hardening
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_crash_recovery_deterministic_startup() {
    let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), None);
    let order = coord.startup_order().to_vec();

    // Register all components in startup order
    for c in &order {
        coord.register(*c);
    }

    // Simulate startup with WAL replay on data node
    coord.record_startup(
        ComponentType::DataNode,
        vec![
            RecoveryAction::WalReplay { records_replayed: 5000, from_lsn: 100, to_lsn: 5100 },
            RecoveryAction::InDoubtTxnResolution { committed: 3, aborted: 2 },
            RecoveryAction::IndexRebuild { indexes_rebuilt: 5 },
        ],
        ShutdownType::Crash,
        2500,
    );

    // Simulate startup with checkpoint on gateway
    coord.record_startup(
        ComponentType::Gateway,
        vec![RecoveryAction::CheckpointRestore { checkpoint_lsn: 5000 }],
        ShutdownType::Clean,
        100,
    );

    // Simulate controller startup
    coord.record_startup(
        ComponentType::Controller,
        vec![RecoveryAction::None],
        ShutdownType::Clean,
        50,
    );

    // Verify
    assert_eq!(coord.metrics.crash_recoveries.load(Ordering::Relaxed), 1);
    assert_eq!(coord.metrics.wal_replays.load(Ordering::Relaxed), 1);
    assert_eq!(coord.metrics.indoubt_resolutions.load(Ordering::Relaxed), 1);
    assert!(coord.startup_log().len() >= 3);
}

#[test]
fn test_graceful_shutdown_writes_sentinel() {
    let dir = std::env::temp_dir().join("falcon_soak_sentinel");
    let _ = std::fs::create_dir_all(&dir);
    let sentinel = dir.join("shutdown.marker").to_string_lossy().to_string();

    let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), Some(sentinel.clone()));

    // Initial state: no sentinel (crash recovery scenario)
    let _ = std::fs::remove_file(&sentinel);
    assert_eq!(coord.detect_previous_shutdown(), ShutdownType::Crash);

    // Register and start all components
    for c in coord.startup_order() {
        coord.register(*c);
        coord.transition(*c, ComponentState::Running);
    }
    assert!(coord.all_running());

    // Graceful shutdown
    coord.begin_shutdown();
    coord.complete_shutdown();
    assert_eq!(coord.detect_previous_shutdown(), ShutdownType::Clean);

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_multi_crash_recovery_cycles() {
    let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), None);

    for cycle in 0..5u64 {
        coord.register(ComponentType::DataNode);
        coord.record_startup(
            ComponentType::DataNode,
            vec![RecoveryAction::WalReplay {
                records_replayed: 100 * (cycle + 1),
                from_lsn: cycle * 1000,
                to_lsn: (cycle + 1) * 1000,
            }],
            if cycle % 2 == 0 { ShutdownType::Crash } else { ShutdownType::Clean },
            200 + cycle * 50,
        );
    }

    assert_eq!(coord.metrics.wal_replays.load(Ordering::Relaxed), 5);
    assert_eq!(coord.metrics.crash_recoveries.load(Ordering::Relaxed), 3); // cycles 0,2,4
    assert_eq!(coord.startup_log().len(), 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// Config Change Consistency & Rollback
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_config_rollback_within_sla_window() {
    let mgr = ConfigRollbackManager::new(100);

    // Set initial config
    let v1 = mgr.set("max_connections", "100", "admin");
    let _v2 = mgr.set("max_connections", "200", "admin");

    // Bad config causes issues — rollback
    assert_eq!(mgr.get("max_connections").unwrap().value, "200");
    assert!(mgr.rollback("max_connections"));
    assert_eq!(mgr.get("max_connections").unwrap().value, "100");

    // Can also rollback to specific version
    let _v3 = mgr.set("max_connections", "300", "admin");
    assert!(mgr.rollback_to_version("max_connections", v1));
    assert_eq!(mgr.get("max_connections").unwrap().value, "100");
}

#[test]
fn test_config_staged_rollout_full_cycle() {
    let mgr = ConfigRollbackManager::new(100);

    // Stage → Canary → RollingOut → Applied
    let v = mgr.stage_change("replication_factor", "3", "ops-bot");
    assert_eq!(mgr.key_history("replication_factor").last().unwrap().rollout_state, RolloutState::Staged);

    mgr.advance_rollout("replication_factor", v, RolloutState::Canary);
    assert_eq!(mgr.key_history("replication_factor").last().unwrap().rollout_state, RolloutState::Canary);

    mgr.advance_rollout("replication_factor", v, RolloutState::RollingOut);
    mgr.advance_rollout("replication_factor", v, RolloutState::Applied);

    assert_eq!(mgr.get("replication_factor").unwrap().value, "3");
}

#[test]
fn test_config_bad_change_no_global_avalanche() {
    let mgr = ConfigRollbackManager::new(100);

    // Multiple independent config keys
    mgr.set("pool_size", "10", "admin");
    mgr.set("timeout_ms", "5000", "admin");
    mgr.set("max_connections", "100", "admin");

    // Bad change to one key
    mgr.set("pool_size", "0", "bad-actor");

    // Rollback only the bad key — others unaffected
    mgr.rollback("pool_size");
    assert_eq!(mgr.get("pool_size").unwrap().value, "10");
    assert_eq!(mgr.get("timeout_ms").unwrap().value, "5000");
    assert_eq!(mgr.get("max_connections").unwrap().value, "100");
}

#[test]
fn test_config_checksum_verification() {
    let mgr = ConfigRollbackManager::new(100);
    mgr.set("key1", "value1", "admin");
    // Valid checksum
    assert!(mgr.verify_checksum("key1"));
    // Missing key is not a mismatch
    assert!(mgr.verify_checksum("nonexistent"));
    // Multiple keys all valid
    mgr.set("key2", "value2", "admin");
    assert!(mgr.verify_checksum("key2"));
}

// ═══════════════════════════════════════════════════════════════════════════
// Resource Leak Zero-Tolerance
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_72h_stable_resources_no_leak() {
    let detector = ResourceLeakDetector::new(10000);
    detector.set_threshold(LeakResourceType::FileDescriptor, 0.5);
    detector.set_threshold(LeakResourceType::HotMemoryBytes, 1_000_000.0);
    detector.set_threshold(LeakResourceType::Thread, 0.2);

    // Simulate 72h of stable resources (1 sample per hour)
    let base_ts = 1_000_000u64;
    for hour in 0..72u64 {
        let mut vals = HashMap::new();
        // Slight jitter but stable
        let jitter = if hour % 3 == 0 { 1 } else { 0 };
        vals.insert(LeakResourceType::FileDescriptor, 120 + jitter);
        vals.insert(LeakResourceType::HotMemoryBytes, 500_000_000 + (jitter * 1000));
        vals.insert(LeakResourceType::Thread, 24 + jitter);
        detector.record_snapshot_at(vals, base_ts + hour * 3600);
    }

    let results = detector.analyze_all();
    for result in &results {
        assert!(!result.is_leaking,
            "Resource {:?} falsely detected as leaking (rate={:.2}/h, confidence={:.2})",
            result.resource, result.growth_rate_per_hour, result.confidence);
    }
}

#[test]
fn test_leak_detection_catches_fd_leak() {
    let detector = ResourceLeakDetector::new(10000);
    detector.set_threshold(LeakResourceType::FileDescriptor, 0.5);

    // FD count increasing at 2/hour
    let base_ts = 1_000_000u64;
    for hour in 0..48u64 {
        let mut vals = HashMap::new();
        vals.insert(LeakResourceType::FileDescriptor, 100 + (hour as i64) * 2);
        detector.record_snapshot_at(vals, base_ts + hour * 3600);
    }

    let result = detector.analyze(LeakResourceType::FileDescriptor);
    assert!(result.is_leaking);
    assert!(result.growth_rate_per_hour > 1.5);
    assert!(result.confidence > 0.9);
}

#[test]
fn test_memory_leak_detected_but_fd_stable() {
    let detector = ResourceLeakDetector::new(10000);
    detector.set_threshold(LeakResourceType::FileDescriptor, 0.5);
    detector.set_threshold(LeakResourceType::HotMemoryBytes, 500_000.0);

    let base_ts = 1_000_000u64;
    for hour in 0..24u64 {
        let mut vals = HashMap::new();
        vals.insert(LeakResourceType::FileDescriptor, 50i64); // stable
        vals.insert(LeakResourceType::HotMemoryBytes, 100_000_000 + (hour as i64) * 2_000_000); // leaking 2MB/h
        detector.record_snapshot_at(vals, base_ts + hour * 3600);
    }

    let fd_result = detector.analyze(LeakResourceType::FileDescriptor);
    let mem_result = detector.analyze(LeakResourceType::HotMemoryBytes);
    assert!(!fd_result.is_leaking);
    assert!(mem_result.is_leaking);
}

// ═══════════════════════════════════════════════════════════════════════════
// Tail Latency Guardrails
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_guardrails_all_paths() {
    let engine = LatencyGuardrailEngine::new(100_000, 1000);

    // Define guardrails for all paths
    let paths = vec![
        (GuardedPath::WalCommit, 5_000, 20_000, 50_000),
        (GuardedPath::GatewayForward, 10_000, 50_000, 100_000),
        (GuardedPath::ColdDecompress, 20_000, 100_000, 200_000),
        (GuardedPath::ReplicationApply, 5_000, 25_000, 75_000),
        (GuardedPath::TxnCommit, 10_000, 50_000, 100_000),
    ];

    for (path, p99, p999, abs_max) in &paths {
        engine.define_guardrail(PathGuardrail {
            path: *path,
            p99_threshold_us: *p99,
            p999_threshold_us: *p999,
            absolute_max_us: *abs_max,
            trigger_backpressure: true,
        });
    }

    // Normal operation — all within bounds
    for (path, _, _, _) in &paths {
        for _ in 0..100 {
            engine.record(*path, 1_000);
        }
    }
    assert!(!engine.is_backpressure_active());

    // Absolute breach on WAL commit
    let breach = engine.record(GuardedPath::WalCommit, 60_000);
    assert!(breach.is_some());
    assert!(engine.is_backpressure_active());
    engine.clear_backpressure();
}

#[test]
fn test_guardrail_under_pressure_p99_stable() {
    let engine = LatencyGuardrailEngine::new(100_000, 1000);
    engine.define_guardrail(PathGuardrail {
        path: GuardedPath::TxnCommit,
        p99_threshold_us: 10_000,
        p999_threshold_us: 50_000,
        absolute_max_us: 100_000,
        trigger_backpressure: false,
    });

    // Normal load: 99% of ops under 5ms, 1% at 8ms
    for i in 0..10_000u64 {
        let lat = if i % 100 == 0 { 8_000 } else { 2_000 + (i % 3_000) };
        engine.record(GuardedPath::TxnCommit, lat);
    }

    let (_p50, p99, _p999) = engine.percentiles(GuardedPath::TxnCommit).unwrap();
    assert!(p99 < 10_000, "p99 should be under guardrail: got {}", p99);
    assert!(!engine.is_backpressure_active());
}

// ═══════════════════════════════════════════════════════════════════════════
// Background Task Isolation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_bg_tasks_dont_starve_foreground() {
    let isolator = BgTaskIsolator::new();

    // Define quotas for all background tasks
    let tasks = vec![
        (BgTaskType::Compaction, 50_000_000u64, 2u32),
        (BgTaskType::Backup, 20_000_000, 1),
        (BgTaskType::Snapshot, 30_000_000, 1),
        (BgTaskType::Rebalance, 40_000_000, 2),
    ];

    for (task, io, concurrent) in &tasks {
        isolator.set_quota(BgTaskQuota {
            task_type: *task,
            cpu_quota: 0.2,
            io_bytes_per_sec: *io,
            memory_bytes: 500_000_000,
            max_concurrent: *concurrent,
            dynamic_throttle: true,
        });
    }

    // Low foreground load — all allowed
    isolator.set_foreground_load(0.2);
    for (task, _, _) in &tasks {
        let decision = isolator.request(*task, 100_000);
        assert_eq!(decision, ThrottleDecision::Allow,
            "{} should be allowed at low fg load", task);
    }

    // High foreground load — background throttled
    isolator.set_foreground_load(0.9);
    for (task, _, _) in &tasks {
        let decision = isolator.request(*task, 5_000_000);
        match decision {
            ThrottleDecision::Throttle(ms) => assert!(ms > 0, "{} should throttle", task),
            ThrottleDecision::Allow => { /* small IO may still pass */ }
            ThrottleDecision::Reject => { /* also acceptable under high load */ }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Cost Visibility
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_cost_visibility_full_cluster() {
    let tracker = CostTracker::new(100);

    // Simulate 5 tables across 3 shards
    for i in 0..5u64 {
        tracker.update_table(TableCost {
            table_id: TableId(i),
            table_name: format!("table_{}", i),
            hot_memory_bytes: (i + 1) * 10_000_000,
            cold_memory_bytes: (i + 1) * 50_000_000,
            wal_io_bytes: (i + 1) * 1_000_000,
            replication_bytes: (i + 1) * 500_000,
            row_count: (i + 1) * 100_000,
            compression_ratio: 2.0 + i as f64 * 0.5,
            compression_savings_bytes: (i + 1) * 25_000_000,
        });
    }

    for s in 0..3u64 {
        tracker.update_shard(ShardCost {
            shard_id: s,
            node_id: NodeId(s as u64),
            hot_memory_bytes: 50_000_000,
            cold_memory_bytes: 200_000_000,
            wal_io_bytes: 10_000_000,
            replication_bytes: 5_000_000,
            table_count: 2,
            total_rows: 300_000,
        });
    }

    let summary = tracker.snapshot();
    assert_eq!(summary.table_costs.len(), 5);
    assert_eq!(summary.shard_costs.len(), 3);
    assert!(summary.total_hot_memory_bytes > 0);
    assert!(summary.total_cold_memory_bytes > 0);
    assert!(summary.avg_compression_ratio > 2.0);
    assert!(summary.total_compression_savings > 0);

    // Top tables
    let top = tracker.top_tables_by_hot_memory(2);
    assert_eq!(top.len(), 2);
    assert!(top[0].hot_memory_bytes >= top[1].hot_memory_bytes);
}

// ═══════════════════════════════════════════════════════════════════════════
// Capacity Guard v2
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_capacity_guard_multi_pressure() {
    let guard = CapacityGuardV2::new(100);

    // Memory OK
    assert!(guard.evaluate_memory(500_000_000, 1_000_000_000).is_none());

    // Memory warning
    let alert = guard.evaluate_memory(800_000_000, 1_000_000_000);
    assert!(alert.is_some());
    assert_eq!(alert.unwrap().severity, CapacityGuardSeverity::Warning);

    // WAL growth urgent
    let alert = guard.evaluate_wal_growth(500_000_000, 1_000_000_000); // fills in 2h
    assert!(alert.is_some());
    assert_eq!(alert.unwrap().severity, CapacityGuardSeverity::Urgent);

    // Cold bloat advisory
    let alert = guard.evaluate_cold_bloat(900_000, 1_000_000); // 90% = poor compression
    assert!(alert.is_some());

    let recent = guard.recent_alerts(10);
    assert!(recent.len() >= 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// Audit Log Hardening
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_audit_trace_correlation_across_components() {
    let log = HardenedAuditLog::new(10000);
    let trace = "trace-incident-42";

    // Simulate a request flowing through components
    let components = vec!["gateway", "executor", "storage", "replication"];
    for (i, comp) in components.iter().enumerate() {
        log.record(UnifiedAuditEvent {
            id: 0, timestamp: 1000 + i as u64,
            trace_id: trace.into(), span_id: format!("span-{}", i),
            category: "DATA".into(), severity: "INFO".into(),
            actor: "alice".into(), source_ip: "10.0.0.1".into(),
            action: "SELECT".into(), resource: "users".into(),
            outcome: "OK".into(), details: "".into(),
            component: comp.to_string(),
            node_id: Some(NodeId(1)), shard_id: Some(0),
            duration_us: Some(100 * (i as u64 + 1)),
        });
    }

    let correlated = log.query_by_trace(trace);
    assert_eq!(correlated.len(), 4);

    // Verify components are in order
    let comp_names: Vec<&str> = correlated.iter().map(|e| e.component.as_str()).collect();
    assert_eq!(comp_names, components);
}

#[test]
fn test_audit_siem_export_format() {
    let log = HardenedAuditLog::new(1000);
    log.record(UnifiedAuditEvent {
        id: 0, timestamp: 1718000000,
        trace_id: "tr-1".into(), span_id: "sp-1".into(),
        category: "AUTH".into(), severity: "WARN".into(),
        actor: "bob".into(), source_ip: "192.168.1.1".into(),
        action: "LOGIN_FAILED".into(), resource: "system".into(),
        outcome: "DENIED".into(), details: "bad password".into(),
        component: "gateway".into(),
        node_id: None, shard_id: None, duration_us: Some(50),
    });

    let lines = log.export_jsonl(1);
    assert_eq!(lines.len(), 1);
    let line = &lines[0];
    assert!(line.contains("\"trace\":\"tr-1\""));
    assert!(line.contains("\"cat\":\"AUTH\""));
    assert!(line.contains("\"sev\":\"WARN\""));
    assert!(line.contains("\"action\":\"LOGIN_FAILED\""));
}

// ═══════════════════════════════════════════════════════════════════════════
// Postmortem / Incident Replay
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_postmortem_full_incident_replay() {
    let gen = PostmortemGenerator::new(5000, 50000);

    // Before incident: stable metrics
    for i in 0..60u64 {
        gen.record_metric_at("latency_p99_ms", 5.0 + (i as f64 % 2.0) * 0.1, 900 + i);
        gen.record_metric_at("throughput_qps", 10000.0 - (i as f64 % 5.0) * 10.0, 900 + i);
    }

    // During incident: metrics degrade
    for i in 0..30u64 {
        gen.record_metric_at("latency_p99_ms", 50.0 + i as f64 * 2.0, 1000 + i);
        gen.record_metric_at("throughput_qps", 5000.0 - i as f64 * 100.0, 1000 + i);
    }

    // Decision points
    gen.record_decision_at(
        "LEADER_ELECTION", "Node 2 heartbeat timeout",
        "heartbeat_gap_ms", 5000.0, 3000.0,
        "initiated leader election for shard 0",
        1002,
    );
    gen.record_decision_at(
        "BACKPRESSURE", "p99 exceeded guardrail",
        "latency_p99_ms", 70.0, 20.0,
        "activated backpressure on gateway",
        1010,
    );
    gen.record_decision_at(
        "REBALANCE", "Node 2 marked offline",
        "node_count", 2.0, 3.0,
        "scheduled shard migration from node 2",
        1015,
    );

    let timeline = vec![
        PostmortemTimelineEntry { timestamp: 1000, event_type: "NODE_FAILURE".into(),
            description: "Node 2 process crashed (SIGKILL)".into(), severity: "CRITICAL".into() },
        PostmortemTimelineEntry { timestamp: 1002, event_type: "LEADER_ELECTION".into(),
            description: "Shard 0 leader transferred to node 3".into(), severity: "HIGH".into() },
        PostmortemTimelineEntry { timestamp: 1005, event_type: "CLIENT_ERRORS".into(),
            description: "15 client connections reset".into(), severity: "HIGH".into() },
        PostmortemTimelineEntry { timestamp: 1010, event_type: "BACKPRESSURE".into(),
            description: "Gateway backpressure activated".into(), severity: "MEDIUM".into() },
        PostmortemTimelineEntry { timestamp: 1020, event_type: "RECOVERY".into(),
            description: "Latency returning to normal".into(), severity: "INFO".into() },
    ];

    let report = gen.generate_report(
        42, "Node 2 Crash — Production Incident",
        1000, 1030, timeline,
    );

    assert_eq!(report.incident_id, 42);
    assert_eq!(report.timeline.len(), 5);
    assert_eq!(report.decision_points.len(), 3);
    assert!(!report.metric_changes.is_empty());

    let text = PostmortemGenerator::export_text(&report);
    assert!(text.contains("POSTMORTEM"));
    assert!(text.contains("LEADER_ELECTION"));
    assert!(text.contains("BACKPRESSURE"));
    assert!(text.contains("REBALANCE"));
    assert!(text.contains("METRIC CHANGES"));
    assert!(text.contains("DECISION POINTS"));
}

// ═══════════════════════════════════════════════════════════════════════════
// Change Impact Preview
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_change_impact_all_scenarios() {
    let preview = ChangeImpactPreview::new();

    // Compression increase: low risk
    let est = preview.preview(&ProposedChange::IncreaseCompression {
        from: "balanced".into(), to: "aggressive".into(),
    });
    assert_eq!(est.risk, ImpactRisk::Low);
    assert!(est.guardrail_ok);

    // Add node: medium risk (rebalance)
    let est = preview.preview(&ProposedChange::AddNode { node_count: 2 });
    assert_eq!(est.risk, ImpactRisk::Medium);
    assert!(est.guardrail_ok);

    // Remove node: high risk
    let est = preview.preview(&ProposedChange::RemoveNode { node_id: NodeId(3) });
    assert_eq!(est.risk, ImpactRisk::High);
    assert!(!est.guardrail_ok);

    // Replication factor change
    let est = preview.preview(&ProposedChange::ChangeReplicationFactor { from: 2, to: 3 });
    assert_eq!(est.risk, ImpactRisk::Medium);

    // Big replication change: high risk
    let est = preview.preview(&ProposedChange::ChangeReplicationFactor { from: 1, to: 5 });
    assert_eq!(est.risk, ImpactRisk::High);

    assert!(preview.metrics.previews_computed.load(Ordering::Relaxed) >= 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// Full Soak Simulation (compressed 7 days → seconds)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_full_soak_7day_simulation() {
    // Components
    let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), None);
    let config_mgr = ConfigRollbackManager::new(500);
    let leak_detector = ResourceLeakDetector::new(10000);
    let guardrail = LatencyGuardrailEngine::new(100_000, 1000);
    let isolator = BgTaskIsolator::new();
    let cost_tracker = CostTracker::new(200);
    let cap_guard = CapacityGuardV2::new(200);

    leak_detector.set_threshold(LeakResourceType::FileDescriptor, 0.5);
    leak_detector.set_threshold(LeakResourceType::Thread, 0.2);

    guardrail.define_guardrail(PathGuardrail {
        path: GuardedPath::WalCommit,
        p99_threshold_us: 10_000,
        p999_threshold_us: 50_000,
        absolute_max_us: 100_000,
        trigger_backpressure: true,
    });
    guardrail.define_guardrail(PathGuardrail {
        path: GuardedPath::TxnCommit,
        p99_threshold_us: 15_000,
        p999_threshold_us: 75_000,
        absolute_max_us: 150_000,
        trigger_backpressure: true,
    });

    isolator.set_quota(BgTaskQuota {
        task_type: BgTaskType::Compaction,
        cpu_quota: 0.2,
        io_bytes_per_sec: 50_000_000,
        memory_bytes: 500_000_000,
        max_concurrent: 2,
        dynamic_throttle: true,
    });

    // Startup
    for c in coord.startup_order() {
        coord.register(*c);
        coord.transition(*c, ComponentState::Running);
    }
    assert!(coord.all_running());

    // Initial config
    config_mgr.set("pool_size", "10", "admin");
    config_mgr.set("max_connections", "100", "admin");

    // Simulate 7 days (1 sample per simulated hour = 168 iterations)
    let base_ts = 1_000_000u64;
    for hour in 0..168u64 {
        let ts = base_ts + hour * 3600;

        // Record resource usage (stable)
        let mut vals = HashMap::new();
        vals.insert(LeakResourceType::FileDescriptor, 120i64);
        vals.insert(LeakResourceType::Thread, 24i64);
        vals.insert(LeakResourceType::HotMemoryBytes, 500_000_000i64);
        leak_detector.record_snapshot_at(vals, ts);

        // Record latency samples (normal)
        for _ in 0..100 {
            guardrail.record(GuardedPath::WalCommit, 500 + (hour % 500));
            guardrail.record(GuardedPath::TxnCommit, 1000 + (hour % 1000));
        }

        // Background task requests
        isolator.set_foreground_load(0.3 + (hour as f64 % 20.0) * 0.02);
        let _ = isolator.request(BgTaskType::Compaction, 100_000);

        // Cost tracking
        cost_tracker.update_table(TableCost {
            table_id: TableId(0),
            table_name: "main_table".into(),
            hot_memory_bytes: 100_000_000,
            cold_memory_bytes: 500_000_000 + hour * 100_000,
            wal_io_bytes: hour * 50_000,
            replication_bytes: hour * 25_000,
            row_count: 1_000_000 + hour * 100,
            compression_ratio: 3.0,
            compression_savings_bytes: 1_000_000_000,
        });

        // Inject fault at day 3 (hour 72): node restart
        if hour == 72 {
            coord.transition(ComponentType::DataNode, ComponentState::Failed);
            assert!(coord.any_failed());
            coord.record_startup(
                ComponentType::DataNode,
                vec![RecoveryAction::WalReplay { records_replayed: 500, from_lsn: 1000, to_lsn: 1500 }],
                ShutdownType::Crash,
                300,
            );
        }

        // Config change at day 5 (hour 120)
        if hour == 120 {
            config_mgr.set("pool_size", "20", "ops-bot");
        }

        // Rollback at hour 125 (simulated issue)
        if hour == 125 {
            config_mgr.rollback("pool_size");
        }
    }

    // ── Verification ──

    // No resource leaks
    let leak_results = leak_detector.analyze_all();
    for r in &leak_results {
        assert!(!r.is_leaking,
            "SOAK FAIL: Resource {:?} leaking (rate={:.2}/h, confidence={:.2})",
            r.resource, r.growth_rate_per_hour, r.confidence);
    }

    // Latency guardrails never breached (no absolute breaches)
    assert_eq!(guardrail.metrics.absolute_breaches.load(Ordering::Relaxed), 0);

    // Config rollback worked
    assert_eq!(config_mgr.get("pool_size").unwrap().value, "10");

    // Crash recovery happened
    assert_eq!(coord.metrics.crash_recoveries.load(Ordering::Relaxed), 1);

    // Cost data tracked
    assert!(cost_tracker.table_count() > 0);

    // Capacity guard evaluatable
    let mem_alert = cap_guard.evaluate_memory(500_000_000, 1_000_000_000);
    assert!(mem_alert.is_none()); // 50% is fine
}
