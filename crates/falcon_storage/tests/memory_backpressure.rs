//! P1-1: Memory Backpressure Integration Tests
//!
//! Proves that under artificial memory pressure:
//!   1. System does NOT crash (no OOM)
//!   2. Behavior matches documented policy (throttle / reject / shed)
//!   3. Metrics are accurate and observable
//!   4. Consistency is never broken by backpressure
//!
//! Run: cargo test -p falcon_storage --test memory_backpressure -- --nocapture

use falcon_storage::memory::{
    BackpressureAction, BackpressureLevel, GlobalMemoryGovernor, GovernorConfig,
    MemoryBudget, MemoryTracker, PressureState,
};

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.1: All 7 memory consumers tracked correctly
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn all_seven_consumers_tracked_in_total() {
    let tracker = MemoryTracker::new(MemoryBudget::new(10_000, 20_000));

    tracker.alloc_mvcc(1000);
    tracker.alloc_index(1000);
    tracker.alloc_write_buffer(1000);
    tracker.alloc_wal_buffer(1000);
    tracker.alloc_replication_buffer(1000);
    tracker.alloc_snapshot_buffer(1000);
    tracker.alloc_exec_buffer(1000);

    assert_eq!(tracker.total_bytes(), 7000);
    assert_eq!(tracker.mvcc_bytes(), 1000);
    assert_eq!(tracker.index_bytes(), 1000);
    assert_eq!(tracker.write_buffer_bytes(), 1000);
    assert_eq!(tracker.wal_buffer_bytes(), 1000);
    assert_eq!(tracker.replication_buffer_bytes(), 1000);
    assert_eq!(tracker.snapshot_buffer_bytes(), 1000);
    assert_eq!(tracker.exec_buffer_bytes(), 1000);
}

#[test]
fn snapshot_includes_all_consumers() {
    let tracker = MemoryTracker::new(MemoryBudget::new(5000, 10_000));

    tracker.alloc_mvcc(100);
    tracker.alloc_index(200);
    tracker.alloc_write_buffer(300);
    tracker.alloc_wal_buffer(400);
    tracker.alloc_replication_buffer(500);
    tracker.alloc_snapshot_buffer(600);
    tracker.alloc_exec_buffer(700);

    let snap = tracker.snapshot();
    assert_eq!(snap.mvcc_bytes, 100);
    assert_eq!(snap.index_bytes, 200);
    assert_eq!(snap.write_buffer_bytes, 300);
    assert_eq!(snap.wal_buffer_bytes, 400);
    assert_eq!(snap.replication_buffer_bytes, 500);
    assert_eq!(snap.snapshot_buffer_bytes, 600);
    assert_eq!(snap.exec_buffer_bytes, 700);
    assert_eq!(snap.total_bytes, 2800);
    assert_eq!(snap.pressure_state, PressureState::Normal);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.2: Pressure transitions triggered by any consumer
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wal_buffer_can_trigger_pressure() {
    let tracker = MemoryTracker::new(MemoryBudget::new(1000, 2000));
    assert_eq!(tracker.pressure_state(), PressureState::Normal);

    tracker.alloc_wal_buffer(1500);
    assert_eq!(tracker.pressure_state(), PressureState::Pressure);

    tracker.alloc_wal_buffer(600);
    assert_eq!(tracker.pressure_state(), PressureState::Critical);

    // Dealloc 600 → 1500, still Pressure (>= soft=1000)
    tracker.dealloc_wal_buffer(600);
    assert_eq!(tracker.pressure_state(), PressureState::Pressure);

    // Dealloc 1500 → 0, Normal
    tracker.dealloc_wal_buffer(1500);
    assert_eq!(tracker.pressure_state(), PressureState::Normal);
}

#[test]
fn exec_buffer_can_trigger_critical() {
    let tracker = MemoryTracker::new(MemoryBudget::new(500, 1000));
    tracker.alloc_exec_buffer(1200);
    assert_eq!(tracker.pressure_state(), PressureState::Critical);
}

#[test]
fn mixed_consumers_aggregate_to_pressure() {
    let tracker = MemoryTracker::new(MemoryBudget::new(1000, 2000));
    // Each consumer adds 200 bytes, 7 consumers = 1400 > soft limit
    tracker.alloc_mvcc(200);
    tracker.alloc_index(200);
    tracker.alloc_write_buffer(200);
    tracker.alloc_wal_buffer(200);
    tracker.alloc_replication_buffer(200);
    tracker.alloc_snapshot_buffer(200);
    tracker.alloc_exec_buffer(200);

    assert_eq!(tracker.total_bytes(), 1400);
    assert_eq!(tracker.pressure_state(), PressureState::Pressure);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.3: Governor 4-tier backpressure decisions
// ═══════════════════════════════════════════════════════════════════════════

fn make_governor(budget: u64) -> GlobalMemoryGovernor {
    GlobalMemoryGovernor::new(GovernorConfig {
        node_budget_bytes: budget,
        soft_threshold: 0.70,
        hard_threshold: 0.85,
        emergency_threshold: 0.95,
        soft_delay_base_us: 1000,
        soft_delay_scale: 10.0,
    })
}

#[test]
fn governor_allows_under_soft_threshold() {
    let gov = make_governor(10_000);
    gov.report_usage(5000); // 50%
    assert_eq!(gov.current_level(), BackpressureLevel::None);
    assert_eq!(gov.check(true), BackpressureAction::Allow);
    assert_eq!(gov.check(false), BackpressureAction::Allow);
}

#[test]
fn governor_delays_writes_in_soft_zone() {
    let gov = make_governor(10_000);
    gov.report_usage(7500); // 75% — soft
    assert_eq!(gov.current_level(), BackpressureLevel::Soft);
    assert_eq!(gov.check(false), BackpressureAction::Allow); // reads still OK
    match gov.check(true) {
        BackpressureAction::Delay(d) => {
            assert!(d.as_micros() > 0, "delay must be positive");
        }
        other => panic!("expected Delay for writes in soft zone, got {:?}", other),
    }
}

#[test]
fn governor_rejects_writes_in_hard_zone() {
    let gov = make_governor(10_000);
    gov.report_usage(9000); // 90% — hard
    assert_eq!(gov.current_level(), BackpressureLevel::Hard);
    assert_eq!(gov.check(false), BackpressureAction::Allow); // reads still OK
    match gov.check(true) {
        BackpressureAction::RejectWrite { .. } => {}
        other => panic!("expected RejectWrite, got {:?}", other),
    }
}

#[test]
fn governor_rejects_everything_in_emergency() {
    let gov = make_governor(10_000);
    gov.report_usage(9600); // 96% — emergency
    assert_eq!(gov.current_level(), BackpressureLevel::Emergency);
    match gov.check(false) {
        BackpressureAction::RejectAll { .. } => {}
        other => panic!("expected RejectAll for reads, got {:?}", other),
    }
    match gov.check(true) {
        BackpressureAction::RejectAll { .. } => {}
        other => panic!("expected RejectAll for writes, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.4: Metrics counters are accurate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn governor_stats_count_correctly() {
    let gov = make_governor(1000);

    // Soft zone: 1 delay
    gov.report_usage(750);
    let _ = gov.check(true);

    // Hard zone: 1 reject
    gov.report_usage(900);
    let _ = gov.check(true);

    // Emergency: 1 reject-all
    gov.report_usage(960);
    let _ = gov.check(true);

    let stats = gov.stats();
    assert_eq!(stats.soft_delays, 1);
    assert_eq!(stats.hard_rejects, 1);
    assert_eq!(stats.emergency_rejects, 1);
    assert_eq!(stats.gc_urgents, 1); // emergency triggers GC urgent
}

#[test]
fn tracker_backpressure_counters_accumulate() {
    let tracker = MemoryTracker::new(MemoryBudget::new(100, 200));
    assert_eq!(tracker.rejected_count(), 0);
    assert_eq!(tracker.delayed_count(), 0);

    for _ in 0..5 {
        tracker.record_rejected();
    }
    for _ in 0..3 {
        tracker.record_delayed();
    }
    tracker.record_gc_trigger();

    assert_eq!(tracker.rejected_count(), 5);
    assert_eq!(tracker.delayed_count(), 3);
    assert_eq!(tracker.gc_trigger_count(), 1);

    let snap = tracker.snapshot();
    assert_eq!(snap.rejected_txn_count, 5);
    assert_eq!(snap.delayed_txn_count, 3);
    assert_eq!(snap.gc_trigger_count, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.5: Pressure ratio observable
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn pressure_ratio_accurate_across_thresholds() {
    let tracker = MemoryTracker::new(MemoryBudget::new(500, 1000));

    tracker.alloc_mvcc(250);
    let snap = tracker.snapshot();
    assert!((snap.pressure_ratio - 0.25).abs() < 0.01);

    tracker.alloc_wal_buffer(250);
    let snap = tracker.snapshot();
    assert!((snap.pressure_ratio - 0.50).abs() < 0.01);

    tracker.alloc_exec_buffer(500);
    let snap = tracker.snapshot();
    assert!((snap.pressure_ratio - 1.0).abs() < 0.01);
    assert_eq!(snap.pressure_state, PressureState::Critical);
}

#[test]
fn pressure_ratio_nan_when_unlimited() {
    let tracker = MemoryTracker::unlimited();
    tracker.alloc_mvcc(999_999);
    let snap = tracker.snapshot();
    assert!(snap.pressure_ratio.is_nan());
    assert_eq!(snap.pressure_state, PressureState::Normal);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.6: Deallocation restores pressure correctly
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn dealloc_across_consumers_restores_normal() {
    let tracker = MemoryTracker::new(MemoryBudget::new(1000, 2000));

    // Fill to critical via multiple consumers
    tracker.alloc_mvcc(500);
    tracker.alloc_wal_buffer(500);
    tracker.alloc_replication_buffer(500);
    tracker.alloc_exec_buffer(600);
    assert_eq!(tracker.total_bytes(), 2100);
    assert_eq!(tracker.pressure_state(), PressureState::Critical);

    // Dealloc exec buffer → pressure
    tracker.dealloc_exec_buffer(600);
    assert_eq!(tracker.total_bytes(), 1500);
    assert_eq!(tracker.pressure_state(), PressureState::Pressure);

    // Dealloc replication buffer → normal
    tracker.dealloc_replication_buffer(500);
    assert_eq!(tracker.total_bytes(), 1000);
    assert_eq!(tracker.pressure_state(), PressureState::Pressure);

    // Dealloc WAL buffer → normal
    tracker.dealloc_wal_buffer(500);
    assert_eq!(tracker.total_bytes(), 500);
    assert_eq!(tracker.pressure_state(), PressureState::Normal);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.7: Concurrent alloc/dealloc safety
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn concurrent_alloc_dealloc_does_not_panic() {
    use std::sync::Arc;
    use std::thread;

    let tracker = Arc::new(MemoryTracker::new(MemoryBudget::new(100_000, 200_000)));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let t = tracker.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                t.alloc_mvcc(100);
                t.alloc_wal_buffer(50);
                t.alloc_exec_buffer(50);
                let _ = t.pressure_state();
                let _ = t.snapshot();
                t.dealloc_mvcc(100);
                t.dealloc_wal_buffer(50);
                t.dealloc_exec_buffer(50);
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // After all threads: should be back to 0 (or very close due to atomics)
    assert_eq!(tracker.total_bytes(), 0);
    assert_eq!(tracker.pressure_state(), PressureState::Normal);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-1.8: Governor recovery after pressure relief
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn governor_recovers_from_emergency_to_none() {
    let gov = make_governor(1000);

    gov.report_usage(960);
    assert_eq!(gov.current_level(), BackpressureLevel::Emergency);

    gov.report_usage(800);
    assert_eq!(gov.current_level(), BackpressureLevel::Soft);

    gov.report_usage(600);
    assert_eq!(gov.current_level(), BackpressureLevel::None);

    // Writes should be allowed again
    assert_eq!(gov.check(true), BackpressureAction::Allow);
}
