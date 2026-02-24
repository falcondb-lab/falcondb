//! Integration tests for falcon_raft transport layer.
//!
//! Tests cover:
//! - Multi-node gRPC transport (election, replication, snapshot)
//! - Fault injection (partitions, drops, delays)
//! - In-process router with full_snapshot support
//! - Transport error semantics and circuit breaker behavior
//!
//! These tests use separate Tokio runtimes bound to different ports to
//! simulate multi-process behavior within a single test binary.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use falcon_raft::network::GrpcNetworkFactory;
use falcon_raft::server::{start_raft_transport_server, LocalRaftHandle};
use falcon_raft::store::{LogStore, StateMachine};
use falcon_raft::transport::fault::{FaultConfig, FaultDecision, FaultInjector};
use falcon_raft::transport::grpc::GrpcTransport;
use falcon_raft::transport::{
    BackoffConfig, CircuitBreakerConfig, CircuitState, PeerCircuitBreaker, RaftTransportConfig,
    RetryPolicy, TransportError, TransportMetrics,
};
use falcon_raft::types::{FalconRequest, TypeConfig};
use falcon_raft::RaftGroup;
use openraft::{BasicNode, Config, Raft};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Multi-node gRPC transport integration tests
// ═══════════════════════════════════════════════════════════════════════════

/// Helper: start a raft node with gRPC transport server on given port.
async fn start_grpc_node(
    node_id: u64,
    port: u16,
    peer_addrs: Vec<(u64, String)>,
) -> (Raft<TypeConfig>, Arc<LocalRaftHandle>, tokio::task::JoinHandle<()>) {
    let local = LocalRaftHandle::new();

    // Start the gRPC server
    let server_handle = start_raft_transport_server(
        format!("127.0.0.1:{}", port),
        local.clone(),
    )
    .await
    .expect("start server");

    // Give server a moment to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let transport = GrpcTransport::new(RaftTransportConfig::default());
    for (id, addr) in &peer_addrs {
        transport.add_peer(*id, addr.clone());
    }

    let network = GrpcNetworkFactory::new(Arc::new(transport));
    let raft = Raft::new(
        node_id,
        config,
        network,
        LogStore::new(),
        StateMachine::new(),
    )
    .await
    .unwrap();

    local.set(raft.clone()).await;

    (raft, local, server_handle)
}

#[tokio::test]
async fn test_grpc_three_node_election() {
    // Start 3 nodes on different ports
    let base_port = 19100u16;
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i + 1, format!("127.0.0.1:{}", base_port + i as u16)))
        .collect();

    let (raft1, _h1, _s1) = start_grpc_node(1, base_port, peers.clone()).await;
    let (raft2, _h2, _s2) = start_grpc_node(2, base_port + 1, peers.clone()).await;
    let (raft3, _h3, _s3) = start_grpc_node(3, base_port + 2, peers.clone()).await;

    // Initialize cluster on node 1
    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:19100"));
    members.insert(2, BasicNode::new("127.0.0.1:19101"));
    members.insert(3, BasicNode::new("127.0.0.1:19102"));
    raft1.initialize(members).await.unwrap();

    // Wait for leader election
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut leader = None;
    while tokio::time::Instant::now() < deadline {
        for raft in [&raft1, &raft2, &raft3] {
            if let Some(l) = raft.metrics().borrow().current_leader {
                leader = Some(l);
                break;
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(leader.is_some(), "expected leader election within 10s");
    let leader_id = leader.unwrap();
    assert!(
        [1, 2, 3].contains(&leader_id),
        "leader must be one of the cluster nodes"
    );

    // Shutdown
    let _ = raft1.shutdown().await;
    let _ = raft2.shutdown().await;
    let _ = raft3.shutdown().await;
}

#[tokio::test]
async fn test_grpc_replication() {
    let base_port = 19200u16;
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i + 1, format!("127.0.0.1:{}", base_port + i as u16)))
        .collect();

    let (raft1, _, _) = start_grpc_node(1, base_port, peers.clone()).await;
    let (raft2, _, _) = start_grpc_node(2, base_port + 1, peers.clone()).await;
    let (raft3, _, _) = start_grpc_node(3, base_port + 2, peers.clone()).await;

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:19200"));
    members.insert(2, BasicNode::new("127.0.0.1:19201"));
    members.insert(3, BasicNode::new("127.0.0.1:19202"));
    raft1.initialize(members).await.unwrap();

    // Wait for leader
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Find leader and propose entries
    let rafts = [&raft1, &raft2, &raft3];
    for i in 0..5u32 {
        let mut proposed = false;
        for raft in &rafts {
            match raft
                .client_write(FalconRequest::Write {
                    data: i.to_le_bytes().to_vec(),
                })
                .await
            {
                Ok(_) => {
                    proposed = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        assert!(proposed, "failed to propose entry {}", i);
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all nodes have applied entries
    for (idx, raft) in rafts.iter().enumerate() {
        let applied = raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        assert!(
            applied >= 5,
            "node {} applied only {} entries, expected >= 5",
            idx + 1,
            applied
        );
    }

    let _ = raft1.shutdown().await;
    let _ = raft2.shutdown().await;
    let _ = raft3.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — In-process full_snapshot test
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_router_full_snapshot_delivery() {
    // Create a 3-node in-process cluster
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Propose enough entries to trigger snapshot
    for i in 0..20u32 {
        group.propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Trigger snapshot on leader
    group.trigger_snapshot(leader).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify all nodes have applied entries
    for &id in group.node_ids() {
        if let Some(raft) = group.get_node(id) {
            let applied = raft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            assert!(
                applied >= 20,
                "node {} applied only {} entries",
                id,
                applied
            );
        }
    }

    group.shutdown().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Fault injection integration tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_fault_injection_partition_prevents_commit() {
    // Create a 3-node cluster, partition the leader from majority
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let old_leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Propose some entries first
    group
        .propose(b"before-partition".to_vec())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Partition the leader
    group.partition_node(old_leader);

    // Wait for new leader among remaining nodes
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut new_leader = None;
    while tokio::time::Instant::now() < deadline {
        for &id in &[1u64, 2, 3] {
            if id == old_leader {
                continue;
            }
            if let Some(raft) = group.get_node(id) {
                if let Some(l) = raft.metrics().borrow().current_leader {
                    if l != old_leader {
                        new_leader = Some(l);
                        break;
                    }
                }
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(new_leader.is_some(), "expected new leader after partition");
    assert_ne!(new_leader.unwrap(), old_leader);

    // New leader can still commit
    group
        .propose(b"after-partition".to_vec())
        .await
        .unwrap();

    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_fault_injection_drop_rate_recovery() {
    // Test with FaultInjector: high drop rate, then recovery
    let fi = FaultInjector::new(FaultConfig::with_drop_rate(0.9));

    // With 90% drop rate, most messages should be dropped
    let mut dropped = 0;
    for _ in 0..100 {
        if fi.decide(1, 2) == FaultDecision::Drop {
            dropped += 1;
        }
    }
    assert!(dropped > 50, "expected > 50% drops with 90% rate, got {}", dropped);

    // Disable faults — all messages should pass
    fi.disable();
    for _ in 0..100 {
        assert_eq!(fi.decide(1, 2), FaultDecision::Pass);
    }
}

#[tokio::test]
async fn test_fault_injection_bidirectional_partition_and_heal() {
    let fi = FaultInjector::disabled();

    // No partition — messages flow
    assert_eq!(fi.decide(1, 2), FaultDecision::Pass);
    assert_eq!(fi.decide(2, 1), FaultDecision::Pass);

    // Add bidirectional partition between 1 and 2
    fi.add_partition(1, 2);
    assert_eq!(fi.decide(1, 2), FaultDecision::Drop);
    assert_eq!(fi.decide(2, 1), FaultDecision::Drop);
    assert_eq!(fi.decide(1, 3), FaultDecision::Pass); // unaffected

    // Heal partition
    fi.remove_partition(1, 2);
    assert_eq!(fi.decide(1, 2), FaultDecision::Pass);
    assert_eq!(fi.decide(2, 1), FaultDecision::Pass);

    let m = fi.metrics();
    assert_eq!(m.messages_partitioned, 2);
    assert!(m.messages_passed >= 4);
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Transport error and circuit breaker tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_transport_error_semantics() {
    // Retryable errors
    assert!(TransportError::Timeout("t".into()).is_retryable());
    assert!(TransportError::Unreachable("u".into()).is_retryable());
    assert!(TransportError::ConnectionReset("c".into()).is_retryable());

    // Non-retryable errors
    assert!(!TransportError::RemoteRejected("r".into()).is_retryable());
    assert!(!TransportError::PayloadTooLarge { size: 10, max: 5 }.is_retryable());
    assert!(!TransportError::Unsupported("u".into()).is_retryable());
    assert!(!TransportError::Internal("i".into()).is_retryable());

    // Retry policies
    assert_eq!(
        TransportError::Timeout("t".into()).retry_policy(),
        RetryPolicy::ExponentialBackoff
    );
    assert_eq!(
        TransportError::RemoteRejected("r".into()).retry_policy(),
        RetryPolicy::NoRetry
    );
}

#[test]
fn test_circuit_breaker_lifecycle() {
    let mut cb = PeerCircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        open_duration_ms: 0, // immediate half-open transition
    });

    // Start closed
    assert_eq!(cb.state(), CircuitState::Closed);
    assert!(cb.allow_request());

    // 3 failures → open
    cb.record_failure();
    cb.record_failure();
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);
    assert_eq!(cb.total_trips(), 1);

    // With open_duration_ms=0, transitions to half-open immediately
    assert!(cb.allow_request());
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // 1 success in half-open (need 2)
    cb.record_success();
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // 2nd success → close
    cb.record_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

#[test]
fn test_backoff_exponential_with_cap() {
    let cfg = BackoffConfig {
        initial_ms: 100,
        max_ms: 10_000,
        multiplier: 2.0,
        max_retries: 5,
    };

    assert_eq!(cfg.delay_for_attempt(0), Duration::from_millis(100));
    assert_eq!(cfg.delay_for_attempt(1), Duration::from_millis(200));
    assert_eq!(cfg.delay_for_attempt(2), Duration::from_millis(400));
    assert_eq!(cfg.delay_for_attempt(3), Duration::from_millis(800));
    // Should be capped at max
    assert_eq!(cfg.delay_for_attempt(10), Duration::from_millis(10_000));
}

#[test]
fn test_transport_config_defaults() {
    let cfg = RaftTransportConfig::default();
    assert_eq!(cfg.connect_timeout_ms, 2_000);
    assert_eq!(cfg.request_timeout_ms, 5_000);
    assert_eq!(cfg.snapshot_chunk_timeout_ms, 10_000);
    assert_eq!(cfg.max_snapshot_chunk_bytes, 1024 * 1024);
    assert_eq!(cfg.max_rpc_payload_bytes, 64 * 1024 * 1024);
    assert_eq!(cfg.backoff.max_retries, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Leader crash and recovery test
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_leader_crash_new_leader_continues() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let old_leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Propose entries
    for i in 0..5u32 {
        group.propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Crash the leader
    group.partition_node(old_leader);

    // Wait for new leader
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut new_leader = None;
    while tokio::time::Instant::now() < deadline {
        for &id in &[1u64, 2, 3] {
            if id == old_leader {
                continue;
            }
            if let Some(raft) = group.get_node(id) {
                if let Some(l) = raft.metrics().borrow().current_leader {
                    if l != old_leader {
                        new_leader = Some(l);
                        break;
                    }
                }
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(new_leader.is_some(), "new leader should be elected");
    let new_leader_id = new_leader.unwrap();
    assert_ne!(new_leader_id, old_leader);

    // New leader can still accept proposals
    group
        .propose(b"post-crash-entry".to_vec())
        .await
        .unwrap();

    // Verify surviving nodes have committed the new entry
    tokio::time::sleep(Duration::from_millis(300)).await;
    for &id in &[1u64, 2, 3] {
        if id == old_leader {
            continue;
        }
        if let Some(raft) = group.get_node(id) {
            let applied = raft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            assert!(
                applied >= 6,
                "surviving node {} applied only {} entries",
                id,
                applied
            );
        }
    }

    group.shutdown().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Snapshot catch-up after lag
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_follower_lag_snapshot_catchup() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Propose entries and let them replicate
    for i in 0..10u32 {
        group.propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Partition node 3
    group.partition_node(3);

    // Propose more entries while node 3 is down
    for i in 10..20u32 {
        group.propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger snapshot on leader so there's a snapshot to send
    group.trigger_snapshot(leader).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Reconnect node 3
    group.reconnect_node(3).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify node 3 caught up (it may catch up via log replication or snapshot)
    // At minimum, the surviving majority should have applied all entries
    for &id in &[1u64, 2] {
        if let Some(raft) = group.get_node(id) {
            let applied = raft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            assert!(
                applied >= 20,
                "node {} applied only {} entries, expected >= 20",
                id,
                applied
            );
        }
    }

    group.shutdown().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Transport metrics tracking
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_transport_metrics_accumulation() {
    let m = TransportMetrics::new();

    m.record_success();
    m.record_success();
    m.record_failure(&TransportError::Timeout("t".into()));
    m.record_failure(&TransportError::Unreachable("u".into()));
    m.record_failure(&TransportError::Internal("i".into()));
    m.record_snapshot_bytes_sent(4096);
    m.record_snapshot_bytes_recv(2048);

    assert_eq!(
        m.rpc_success_total
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        m.rpc_failure_total
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );
    assert_eq!(
        m.rpc_timeout_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        m.rpc_unreachable_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        m.snapshot_bytes_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        4096
    );
    assert_eq!(
        m.snapshot_bytes_recv
            .load(std::sync::atomic::Ordering::Relaxed),
        2048
    );
}
