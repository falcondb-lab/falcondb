//! v1.0.8 Gateway Scale Tests — single/multi gateway, crash/restart, client recovery.
//!
//! Verifies:
//! - Single gateway handles full request load
//! - Multiple gateways operate independently
//! - Gateway crash: remaining gateways continue serving
//! - Client auto-recovers after gateway restart
//! - Topology cache consistency under scale

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_cluster::smart_gateway::*;
use falcon_common::types::{NodeId, ShardId};

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn make_topology(num_nodes: u64, num_shards: u64) -> Arc<TopologyCache> {
    let topo = Arc::new(TopologyCache::new());
    for n in 1..=num_nodes {
        topo.register_node(
            NodeId(n),
            format!("node{}:5443", n),
            GatewayRole::SmartGateway,
        );
    }
    for s in 0..num_shards {
        let leader = NodeId((s % num_nodes) + 1);
        topo.update_leader(ShardId(s), leader, format!("node{}:5443", leader.0));
    }
    topo
}

fn make_gateway(node_id: u64, topo: Arc<TopologyCache>) -> SmartGateway {
    let config = SmartGatewayConfig {
        node_id: NodeId(node_id),
        role: GatewayRole::SmartGateway,
        max_inflight: 50_000,
        max_forwarded: 25_000,
        forward_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    SmartGateway::with_topology(config, topo)
}

// ═══════════════════════════════════════════════════════════════════════════
// Single Gateway Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_single_gateway_handles_full_load() {
    let topo = make_topology(3, 8);
    let gw = make_gateway(1, topo);

    let start = Instant::now();
    for i in 0..10_000u64 {
        let shard = ShardId(i % 8);
        let decision = gw.classify_request(shard);
        match decision.classification {
            RequestClassification::LocalExec
            | RequestClassification::ForwardToLeader => {}
            other => panic!("Unexpected classification: {:?} for shard {}", other, i % 8),
        }
    }
    let elapsed = start.elapsed();

    let snap = gw.metrics_snapshot();
    assert_eq!(snap.requests_total, 10_000);
    assert!(snap.local_exec_total > 0);
    assert!(snap.forward_total > 0);
    // Must complete within reasonable time
    assert!(elapsed < Duration::from_secs(2), "10K classifications took {:?}", elapsed);
}

#[test]
fn test_single_gateway_all_local() {
    // Single node cluster — everything is local
    let topo = make_topology(1, 4);
    let gw = make_gateway(1, topo);

    for s in 0..4u64 {
        let decision = gw.classify_request(ShardId(s));
        assert_eq!(decision.classification, RequestClassification::LocalExec);
    }

    let snap = gw.metrics_snapshot();
    assert_eq!(snap.local_exec_total, 4);
    assert_eq!(snap.forward_total, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi Gateway Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_multi_gateway_shared_topology() {
    let topo = make_topology(3, 6);
    let gw1 = Arc::new(make_gateway(1, Arc::clone(&topo)));
    let gw2 = Arc::new(make_gateway(2, Arc::clone(&topo)));
    let gw3 = Arc::new(make_gateway(3, Arc::clone(&topo)));

    // Each gateway routes the same shard differently based on its local node_id
    let d1 = gw1.classify_request(ShardId(0)); // shard 0 leader = node 1
    let d2 = gw2.classify_request(ShardId(0));
    let d3 = gw3.classify_request(ShardId(0));

    assert_eq!(d1.classification, RequestClassification::LocalExec);
    assert_eq!(d2.classification, RequestClassification::ForwardToLeader);
    assert_eq!(d3.classification, RequestClassification::ForwardToLeader);
}

#[test]
fn test_multi_gateway_independent_metrics() {
    let topo = make_topology(3, 6);
    let gw1 = make_gateway(1, Arc::clone(&topo));
    let gw2 = make_gateway(2, Arc::clone(&topo));

    // Route requests through different gateways
    for s in 0..6u64 {
        gw1.classify_request(ShardId(s));
    }
    for s in 0..3u64 {
        gw2.classify_request(ShardId(s));
    }

    let snap1 = gw1.metrics_snapshot();
    let snap2 = gw2.metrics_snapshot();

    assert_eq!(snap1.requests_total, 6);
    assert_eq!(snap2.requests_total, 3);
}

#[test]
fn test_multi_gateway_concurrent_operations() {
    let topo = make_topology(4, 16);
    let gateways: Vec<Arc<SmartGateway>> = (1..=4)
        .map(|i| Arc::new(make_gateway(i, Arc::clone(&topo))))
        .collect();

    let mut handles = Vec::new();
    for gw in &gateways {
        let g = Arc::clone(gw);
        handles.push(std::thread::spawn(move || {
            for i in 0..1000u64 {
                let shard = ShardId(i % 16);
                let _ = g.classify_request(shard);
            }
        }));
    }

    for h in handles {
        h.join().expect("gateway thread must not panic");
    }

    let total: u64 = gateways.iter().map(|g| g.metrics_snapshot().requests_total).sum();
    assert_eq!(total, 4000);
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Crash / Restart Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_gateway_crash_remaining_gateways_continue() {
    let topo = make_topology(3, 6);
    let gw1 = make_gateway(1, Arc::clone(&topo));
    let gw2 = make_gateway(2, Arc::clone(&topo));

    // Simulate gw1 "crash" by marking node 1 as dead
    topo.mark_node_dead(NodeId(1));

    // gw2 continues serving — shard 0 was on node 1, should reject
    let d = gw2.classify_request(ShardId(0));
    assert_eq!(d.classification, RequestClassification::RejectNoRoute);

    // Shards on other nodes still work from gw2
    let d = gw2.classify_request(ShardId(1)); // leader = node 2 = local for gw2
    assert_eq!(d.classification, RequestClassification::LocalExec);

    // gw1 (crashed) can't verify its own liveness check externally, but its
    // topology still works locally
    let _ = gw1.classify_request(ShardId(0));
}

#[test]
fn test_gateway_restart_client_recovery() {
    let topo = make_topology(3, 6);

    // Initial gateway
    let gw1 = make_gateway(1, Arc::clone(&topo));

    // Verify routing works
    let d = gw1.classify_request(ShardId(0));
    assert_eq!(d.classification, RequestClassification::LocalExec);

    // "Crash" node 1 (client will failover)
    topo.mark_node_dead(NodeId(1));

    // Client failover: create JDBC seed list and simulate failover
    let mut seeds = SeedGatewayList::new(vec![
        HostPort { host: "node1".into(), port: 5443 },
        HostPort { host: "node2".into(), port: 5443 },
        HostPort { host: "node3".into(), port: 5443 },
    ]);

    // node1 failures → switch to node2
    for _ in 0..3 {
        seeds.report_failure();
    }
    assert_eq!(seeds.active().host, "node2");

    // node2 gateway starts serving
    let gw2 = make_gateway(2, Arc::clone(&topo));
    gw2.record_client_failover();

    // shard 1 = node 2 = local for gw2
    let d = gw2.classify_request(ShardId(1));
    assert_eq!(d.classification, RequestClassification::LocalExec);

    // "Restart" node 1
    topo.mark_node_alive(NodeId(1));

    // Client can reconnect to node1 (or stay on node2 — both valid)
    seeds.report_success();
    assert_eq!(seeds.active().host, "node2"); // Stays on current until next failure cycle
}

#[test]
fn test_topology_update_after_node_crash_and_leader_election() {
    let topo = make_topology(3, 6);
    let gw = make_gateway(2, Arc::clone(&topo));

    // shard 0 was on node 1
    let d = gw.classify_request(ShardId(0));
    assert_eq!(d.classification, RequestClassification::ForwardToLeader);
    assert_eq!(d.target_node, Some(NodeId(1)));

    // Node 1 crashes
    topo.mark_node_dead(NodeId(1));

    // Shard 0 now rejects (no route to dead node)
    let d = gw.classify_request(ShardId(0));
    assert_eq!(d.classification, RequestClassification::RejectNoRoute);

    // New leader election: shard 0 → node 3
    topo.update_leader(ShardId(0), NodeId(3), "node3:5443".into());

    // Now routes to new leader
    let d = gw.classify_request(ShardId(0));
    assert_eq!(d.classification, RequestClassification::ForwardToLeader);
    assert_eq!(d.target_node, Some(NodeId(3)));
}

// ═══════════════════════════════════════════════════════════════════════════
// Topology Cache Consistency Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_topology_epoch_monotonicity() {
    let topo = make_topology(3, 4);
    let initial_epoch = topo.epoch();

    // Every leader change bumps epoch
    let mut last_epoch = initial_epoch;
    for i in 0..20 {
        let shard = ShardId(i % 4);
        let leader = NodeId((i % 3) + 1);
        let epoch = topo.update_leader(shard, leader, format!("node{}:5443", leader.0));
        assert!(epoch >= last_epoch, "epoch must be monotonic");
        last_epoch = epoch;
    }
}

#[test]
fn test_topology_invalidation_then_update() {
    let topo = make_topology(3, 4);

    // Shard 0 has a leader
    assert!(topo.get_leader(ShardId(0)).is_some());

    // Invalidate
    topo.invalidate(ShardId(0));
    assert!(topo.get_leader(ShardId(0)).is_none());

    // Re-establish
    topo.update_leader(ShardId(0), NodeId(3), "node3:5443".into());
    let entry = topo.get_leader(ShardId(0)).unwrap();
    assert_eq!(entry.leader_node, NodeId(3));
}

#[test]
fn test_topology_concurrent_updates_and_reads() {
    let topo = Arc::new(TopologyCache::new());
    for i in 1..=4u64 {
        topo.register_node(NodeId(i), format!("node{}:5443", i), GatewayRole::SmartGateway);
    }

    let mut handles = Vec::new();

    // Writers: update leaders rapidly
    for t in 0..4 {
        let tc = Arc::clone(&topo);
        handles.push(std::thread::spawn(move || {
            for i in 0..500u64 {
                let shard = ShardId((t * 500 + i) % 32);
                let leader = NodeId((i % 4) + 1);
                tc.update_leader(shard, leader, format!("node{}:5443", leader.0));
            }
        }));
    }

    // Readers: lookup leaders concurrently
    for _ in 0..4 {
        let tc = Arc::clone(&topo);
        handles.push(std::thread::spawn(move || {
            for i in 0..1000u64 {
                let shard = ShardId(i % 32);
                let _ = tc.get_leader(shard);
            }
        }));
    }

    for h in handles {
        h.join().expect("thread must not panic");
    }

    let snap = topo.metrics_snapshot();
    assert!(snap.cache_hits + snap.cache_misses > 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// Performance / Latency Guardrails
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_classification_latency_under_1us() {
    let topo = make_topology(3, 8);
    let gw = make_gateway(1, topo);

    // Warmup
    for s in 0..8u64 {
        gw.classify_request(ShardId(s));
    }

    let iterations = 10_000;
    let start = Instant::now();
    for i in 0..iterations as u64 {
        gw.classify_request(ShardId(i % 8));
    }
    let elapsed = start.elapsed();
    let avg_ns = elapsed.as_nanos() / iterations as u128;

    // Classification should be < 1µs average (it's just cache lookup + atomic ops)
    assert!(
        avg_ns < 1000,
        "avg classification latency {}ns exceeds 1µs",
        avg_ns
    );
}

#[test]
fn test_reject_latency_under_1us() {
    let config = SmartGatewayConfig {
        node_id: NodeId(1),
        max_inflight: 1,
        ..Default::default()
    };
    let gw = SmartGateway::new(config);
    gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
    gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

    // Saturate
    gw.metrics.inflight.store(1, Ordering::Relaxed);

    let iterations = 10_000;
    let start = Instant::now();
    for _ in 0..iterations {
        let d = gw.classify_request(ShardId(0));
        assert_eq!(d.classification, RequestClassification::RejectOverloaded);
    }
    let elapsed = start.elapsed();
    let avg_ns = elapsed.as_nanos() / iterations as u128;

    assert!(
        avg_ns < 1000,
        "avg reject latency {}ns exceeds 1µs",
        avg_ns
    );
}

#[test]
fn test_no_hang_under_concurrent_topology_churn() {
    let topo = Arc::new(TopologyCache::new());
    for i in 1..=4u64 {
        topo.register_node(NodeId(i), format!("node{}:5443", i), GatewayRole::SmartGateway);
    }
    let gw = Arc::new(SmartGateway::with_topology(
        SmartGatewayConfig { node_id: NodeId(1), ..Default::default() },
        Arc::clone(&topo),
    ));

    let start = Instant::now();
    let timeout = Duration::from_secs(5);

    let mut handles = Vec::new();

    // Topology churn thread
    let tc = Arc::clone(&topo);
    handles.push(std::thread::spawn(move || {
        for i in 0..1000u64 {
            let shard = ShardId(i % 16);
            let leader = NodeId((i % 4) + 1);
            tc.update_leader(shard, leader, format!("node{}:5443", leader.0));
            if i % 100 == 0 {
                tc.invalidate_node(NodeId((i / 100 % 4) + 1));
                // Re-register
                let n = (i / 100 % 4) + 1;
                tc.register_node(NodeId(n), format!("node{}:5443", n), GatewayRole::SmartGateway);
            }
        }
    }));

    // Classify threads
    for _ in 0..4 {
        let g = Arc::clone(&gw);
        handles.push(std::thread::spawn(move || {
            for i in 0..500u64 {
                let _ = g.classify_request(ShardId(i % 16));
            }
        }));
    }

    for h in handles {
        h.join().expect("thread must not panic");
    }

    assert!(start.elapsed() < timeout, "test must complete within {:?}", timeout);
}
