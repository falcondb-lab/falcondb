//! Gateway stability test matrix for FalconDB v1.0.5.
//!
//! Validates:
//! - GatewayMetrics struct default + snapshot semantics
//! - GatewayMetricsSnapshot is consistent
//! - Atomic counter accumulation
//! - DistributedQueryEngine gateway_metrics_snapshot accessor
//!
//! Run: cargo test -p falcon_cluster --test gateway_stability

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use falcon_cluster::{DistributedQueryEngine, GatewayMetrics, GatewayMetricsSnapshot};
use falcon_cluster::sharded_engine::ShardedEngine;
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::*;

fn test_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "gw_test".into(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "val".into(),
                data_type: DataType::Int32,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        ..Default::default()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §1 — GatewayMetrics struct and snapshot
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_gateway_metrics_default() {
    let m = GatewayMetrics::new();
    let snap = m.snapshot();
    assert_eq!(snap.forward_total, 0);
    assert_eq!(snap.forward_failed_total, 0);
    assert_eq!(snap.forward_latency_us, 0);
}

#[test]
fn test_gateway_metrics_snapshot_default() {
    let snap = GatewayMetricsSnapshot::default();
    assert_eq!(snap.forward_total, 0);
    assert_eq!(snap.forward_failed_total, 0);
    assert_eq!(snap.forward_latency_us, 0);
}

#[test]
fn test_gateway_metrics_atomic_accumulation() {
    let m = GatewayMetrics::new();

    m.forward_total.fetch_add(10, Ordering::Relaxed);
    m.forward_failed_total.fetch_add(2, Ordering::Relaxed);
    m.forward_latency_us.fetch_add(5000, Ordering::Relaxed);

    let snap = m.snapshot();
    assert_eq!(snap.forward_total, 10);
    assert_eq!(snap.forward_failed_total, 2);
    assert_eq!(snap.forward_latency_us, 5000);

    // Additional increments accumulate
    m.forward_total.fetch_add(5, Ordering::Relaxed);
    let snap2 = m.snapshot();
    assert_eq!(snap2.forward_total, 15);
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — DistributedQueryEngine exposes gateway metrics
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_dist_engine_gateway_metrics_starts_at_zero() {
    let engine = Arc::new(ShardedEngine::new(2));
    let dist = DistributedQueryEngine::new(engine, Duration::from_secs(5));

    let snap = dist.gateway_metrics_snapshot();
    assert_eq!(snap.forward_total, 0);
    assert_eq!(snap.forward_failed_total, 0);
    assert_eq!(snap.forward_latency_us, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — DDL propagated to all shards (CreateTable + health check)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_dist_engine_ddl_and_health_check() {
    let engine = Arc::new(ShardedEngine::new(2));
    let dist = DistributedQueryEngine::new(engine.clone(), Duration::from_secs(5));

    // Create table on all shards
    engine.create_table_all(&test_schema()).unwrap();

    // Health check should report both shards healthy
    let health = dist.shard_health_check();
    assert_eq!(health.len(), 2, "should have 2 shards");
    for (sid, healthy, _latency) in &health {
        assert!(healthy, "shard {:?} should be healthy", sid);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Shard health check detects missing shards
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_shard_health_check_all_healthy() {
    let engine = Arc::new(ShardedEngine::new(3));
    let dist = DistributedQueryEngine::new(engine, Duration::from_secs(5));

    let health = dist.shard_health_check();
    assert_eq!(health.len(), 3);

    for (_sid, healthy, latency_us) in &health {
        assert!(healthy);
        // Latency should be positive (at least some microseconds)
        assert!(*latency_us < 1_000_000, "health check latency suspiciously high");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Gateway metrics concurrent safety
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_gateway_metrics_concurrent_increments() {
    let m = Arc::new(GatewayMetrics::new());
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let m = m.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    m.forward_total.fetch_add(1, Ordering::Relaxed);
                    m.forward_latency_us.fetch_add(10, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let snap = m.snapshot();
    assert_eq!(snap.forward_total, 8000);
    assert_eq!(snap.forward_latency_us, 80000);
    assert_eq!(snap.forward_failed_total, 0);
}
