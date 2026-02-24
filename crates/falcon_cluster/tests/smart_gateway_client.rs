//! v1.0.8 Client-Focused Tests — JDBC failover, gateway restart, leader switch, overload.
//!
//! These tests verify the client experience guarantees:
//! - JDBC never panics
//! - Errors carry explicit retry/fail-fast semantics
//! - No long-term hangs
//! - Leader changes are absorbed, not exposed

use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_cluster::smart_gateway::*;
use falcon_common::types::{NodeId, ShardId};

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn make_3node_gateway(local_node: u64) -> SmartGateway {
    let config = SmartGatewayConfig {
        node_id: NodeId(local_node),
        role: GatewayRole::SmartGateway,
        max_inflight: 10_000,
        max_forwarded: 5_000,
        forward_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let gw = SmartGateway::new(config);

    // Register 3 nodes
    for i in 1..=3u64 {
        gw.topology.register_node(
            NodeId(i),
            format!("node{}:5443", i),
            GatewayRole::SmartGateway,
        );
    }

    // Assign shards: 4 shards distributed across 3 nodes
    gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());
    gw.topology.update_leader(ShardId(1), NodeId(2), "node2:5443".into());
    gw.topology.update_leader(ShardId(2), NodeId(3), "node3:5443".into());
    gw.topology.update_leader(ShardId(3), NodeId(1), "node1:5443".into());

    gw
}

// ═══════════════════════════════════════════════════════════════════════════
// JDBC URL Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_jdbc_connect_to_any_seed() {
    let url = JdbcConnectionUrl::parse(
        "jdbc:falcondb://node1:5443,node2:5443,node3:5443/falcon",
    )
    .unwrap();

    let mut seeds = SeedGatewayList::from_url(&url);
    assert_eq!(seeds.seed_count(), 3);

    // Initial connection goes to node1
    assert_eq!(seeds.active().host, "node1");
    seeds.report_success();

    // After success, still on node1
    assert_eq!(seeds.active().host, "node1");
}

#[test]
fn test_jdbc_failover_on_gateway_unavailable() {
    let url = JdbcConnectionUrl::parse(
        "jdbc:falcondb://node1:5443,node2:5443,node3:5443/falcon",
    )
    .unwrap();

    let mut seeds = SeedGatewayList::from_url(&url);

    // node1 fails 3 times → failover to node2
    seeds.report_failure();
    seeds.report_failure();
    let switched = seeds.report_failure();
    assert!(switched, "should failover after max failures");
    assert_eq!(seeds.active().host, "node2");
}

#[test]
fn test_jdbc_no_hang_all_gateways_down() {
    let url = JdbcConnectionUrl::parse(
        "jdbc:falcondb://node1:5443,node2:5443/falcon",
    )
    .unwrap();

    let mut seeds = SeedGatewayList::from_url(&url);
    let start = Instant::now();

    let mut total_failures = 0u32;
    // Simulate all gateways failing
    for _ in 0..10 {
        seeds.report_failure();
        total_failures += 1;
        if seeds.all_seeds_exhausted(total_failures) {
            break;
        }
    }

    // Should complete quickly, not hang
    assert!(start.elapsed() < Duration::from_millis(100));
    assert!(seeds.all_seeds_exhausted(total_failures));
}

#[test]
fn test_jdbc_leader_change_no_client_reconnect() {
    let gw = make_3node_gateway(1);

    // Request to shard 0 succeeds locally
    let d1 = gw.classify_request(ShardId(0));
    assert_eq!(d1.classification, RequestClassification::LocalExec);

    // Leader for shard 0 changes to node 2
    let err = gw.handle_not_leader(
        ShardId(0),
        Some((NodeId(2), "node2:5443".into())),
    );
    assert_eq!(err.code, GatewayErrorCode::NotLeader);
    assert!(err.code.is_retryable());
    assert_eq!(err.retry_after_ms, Some(0)); // Immediate retry

    // Next request to shard 0 → forward to node 2 (no reconnect needed)
    let d2 = gw.classify_request(ShardId(0));
    assert_eq!(d2.classification, RequestClassification::ForwardToLeader);
    assert_eq!(d2.target_node, Some(NodeId(2)));
}

// ═══════════════════════════════════════════════════════════════════════════
// Error Code & Retry Contract Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_not_leader_error_is_retryable_with_hint() {
    let err = GatewayError::not_leader(0, 5, Some("node2:5443".into()));
    assert!(err.code.is_retryable());
    assert_eq!(err.retry_after_ms, Some(0));
    assert_eq!(err.leader_hint, Some("node2:5443".to_string()));
    assert_eq!(err.code.sqlstate(), "FD001");
}

#[test]
fn test_no_route_error_is_retryable_with_delay() {
    let err = GatewayError::no_route(5, 10);
    assert!(err.code.is_retryable());
    assert!(err.retry_after_ms.unwrap() > 0);
    assert_eq!(err.code.sqlstate(), "FD002");
}

#[test]
fn test_overloaded_error_is_retryable_with_backoff() {
    let err = GatewayError::overloaded("too many inflight");
    assert!(err.code.is_retryable());
    assert!(err.retry_after_ms.unwrap() >= 500);
    assert_eq!(err.code.sqlstate(), "FD003");
}

#[test]
fn test_fatal_error_is_not_retryable() {
    let err = GatewayError::fatal("bad SQL syntax".to_string());
    assert!(!err.code.is_retryable());
    assert_eq!(err.retry_after_ms, None);
    assert_eq!(err.code.sqlstate(), "FD000");
}

#[test]
fn test_timeout_error_retry_semantics() {
    let err = GatewayError::timeout(2, 5000);
    assert!(err.code.is_retryable());
    assert!(err.retry_after_ms.unwrap() > 0);
    assert_eq!(err.code.sqlstate(), "FD004");
}

#[test]
fn test_error_codes_no_infinite_retry_loop() {
    // Verify that a client following the retry contract won't loop forever.
    // Fatal errors stop immediately; retryable errors have bounded delays.
    let errors = vec![
        GatewayError::not_leader(0, 1, None),
        GatewayError::no_route(0, 1),
        GatewayError::overloaded("test"),
        GatewayError::timeout(0, 1000),
        GatewayError::fatal("bad".to_string()),
    ];

    let mut retryable_count = 0;
    let mut fatal_count = 0;
    for err in &errors {
        if err.code.is_retryable() {
            retryable_count += 1;
            assert!(err.code.retry_delay_ms().is_some());
        } else {
            fatal_count += 1;
            assert!(err.code.retry_delay_ms().is_none());
        }
    }

    assert_eq!(retryable_count, 4);
    assert_eq!(fatal_count, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Overload Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_gateway_overload_immediate_reject() {
    let config = SmartGatewayConfig {
        node_id: NodeId(1),
        max_inflight: 5,
        ..Default::default()
    };
    let gw = SmartGateway::new(config);
    gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
    gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

    // Fill inflight slots
    for _ in 0..5 {
        gw.acquire_local();
    }

    let start = Instant::now();
    let decision = gw.classify_request(ShardId(0));
    let elapsed = start.elapsed();

    assert_eq!(decision.classification, RequestClassification::RejectOverloaded);
    // Must be fast — no queuing
    assert!(elapsed < Duration::from_millis(10), "reject must be immediate, took {:?}", elapsed);
}

#[test]
fn test_gateway_forward_limit_reject() {
    let config = SmartGatewayConfig {
        node_id: NodeId(1),
        max_forwarded: 2,
        ..Default::default()
    };
    let gw = SmartGateway::new(config);
    gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
    gw.topology.register_node(NodeId(2), "node2:5443".into(), GatewayRole::SmartGateway);
    gw.topology.update_leader(ShardId(0), NodeId(2), "node2:5443".into());

    // Fill forwarded slots
    gw.metrics.forwarded.store(2, std::sync::atomic::Ordering::Relaxed);

    let decision = gw.classify_request(ShardId(0));
    assert_eq!(decision.classification, RequestClassification::RejectOverloaded);
}

// ═══════════════════════════════════════════════════════════════════════════
// Leader Failover Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_leader_switch_request_success_or_fast_fail() {
    let gw = make_3node_gateway(1);

    // Rapid leader switches on shard 0
    for new_leader in [2u64, 3, 1, 2, 3] {
        gw.topology.update_leader(
            ShardId(0),
            NodeId(new_leader),
            format!("node{}:5443", new_leader),
        );

        let decision = gw.classify_request(ShardId(0));
        // Must either succeed (local/forward) or fast-fail (reject) — never hang
        match decision.classification {
            RequestClassification::LocalExec
            | RequestClassification::ForwardToLeader
            | RequestClassification::RejectNoRoute
            | RequestClassification::RejectOverloaded => {}
        }
    }
}

#[test]
fn test_node_crash_invalidates_leader_entries() {
    let gw = make_3node_gateway(1);

    // Node 2 crashes
    gw.topology.mark_node_dead(NodeId(2));

    // Request to shard 1 (was on node 2) → should reject, not blind forward
    let decision = gw.classify_request(ShardId(1));
    assert_eq!(
        decision.classification,
        RequestClassification::RejectNoRoute,
        "must not forward to dead node"
    );
}

#[test]
fn test_node_recovery_restores_routing() {
    let gw = make_3node_gateway(1);

    // Node 2 crashes
    gw.topology.mark_node_dead(NodeId(2));

    // Node 2 recovers
    gw.topology.mark_node_alive(NodeId(2));

    // Request to shard 1 (on node 2) → should forward again
    let decision = gw.classify_request(ShardId(1));
    assert_eq!(decision.classification, RequestClassification::ForwardToLeader);
    assert_eq!(decision.target_node, Some(NodeId(2)));
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrent Client Scenario
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_concurrent_clients_no_panic() {
    let gw = Arc::new(make_3node_gateway(1));

    let mut handles = Vec::new();
    for t in 0..8 {
        let g = Arc::clone(&gw);
        handles.push(std::thread::spawn(move || {
            for i in 0..500 {
                let shard = ShardId(((t * 500 + i) % 4) as u64);
                let decision = g.classify_request(shard);

                // Simulate error handling per JDBC contract
                match decision.classification {
                    RequestClassification::LocalExec => {
                        g.acquire_local();
                        // simulate work
                        g.release_local();
                    }
                    RequestClassification::ForwardToLeader => {
                        if g.acquire_forward() {
                            g.release_forward(100, true);
                        }
                    }
                    RequestClassification::RejectNoRoute => {
                        // Client sees NO_ROUTE — retryable
                    }
                    RequestClassification::RejectOverloaded => {
                        // Client backs off
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("thread must not panic");
    }

    let snap = gw.metrics_snapshot();
    assert_eq!(snap.requests_total, 4000);
}

#[test]
fn test_leader_changes_during_concurrent_requests() {
    let gw = Arc::new(make_3node_gateway(1));

    // Writer thread: rapidly changes leaders
    let gw_writer = Arc::clone(&gw);
    let writer = std::thread::spawn(move || {
        for i in 0..200 {
            let shard = ShardId((i % 4) as u64);
            let leader = NodeId(((i % 3) + 1) as u64);
            gw_writer.topology.update_leader(
                shard,
                leader,
                format!("node{}:5443", leader.0),
            );
        }
    });

    // Reader threads: classify requests concurrently
    let mut readers = Vec::new();
    for t in 0..4 {
        let g = Arc::clone(&gw);
        readers.push(std::thread::spawn(move || {
            for i in 0..500 {
                let shard = ShardId(((t * 500 + i) % 4) as u64);
                let _ = g.classify_request(shard);
            }
        }));
    }

    writer.join().expect("writer must not panic");
    for r in readers {
        r.join().expect("reader must not panic");
    }
}
