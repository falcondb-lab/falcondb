//! Integration tests for `falcon_raft` core consensus.
//!
//! These complement the unit tests in `lib.rs` by exercising:
//! - Membership changes (remove_voter)
//! - Reconnection after network partitions
//! - Concurrent proposal stress
//! - Multi-shard `RaftConsensus` coordination
//! - Error / edge-case paths

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use falcon_common::types::ShardId;
use falcon_raft::store::ApplyFn;
use falcon_raft::{Consensus, ConsensusError, LogEntry, RaftConsensus, RaftGroup};

// ─── Membership Changes ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_remove_voter_shrinks_cluster() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    let victim = group
        .node_ids()
        .iter()
        .find(|&&id| id != leader)
        .copied()
        .unwrap();
    group.remove_voter(victim).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    group.propose(b"after-remove".to_vec()).await.unwrap();
    group.shutdown().await.unwrap();
}

// ─── Partition & Reconnection ───────────────────────────────────────────────

#[tokio::test]
async fn test_partition_and_reconnect() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    group.partition_node(leader);
    tokio::time::sleep(Duration::from_millis(500)).await;

    group.reconnect_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    group.propose(b"after-reconnect".to_vec()).await.unwrap();
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_minority_cannot_propose() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Partition two nodes so the remaining one has no quorum.
    group.partition_node(2);
    group.partition_node(3);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // propose may block forever without quorum, so apply a timeout.
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        group.propose(b"should-fail".to_vec()),
    )
    .await;
    // Either the propose itself errors or the timeout fires — both are acceptable.
    match result {
        Ok(Ok(())) => panic!("proposal should not succeed without quorum"),
        _ => {} // Err (timeout) or Ok(Err(..)) — expected
    }
    group.shutdown().await.unwrap();
}

// ─── Concurrent Proposals ───────────────────────────────────────────────────

#[tokio::test]
async fn test_rapid_sequential_proposals() {
    let counter = Arc::new(AtomicUsize::new(0));
    let cb = {
        let c = counter.clone();
        let f: ApplyFn = Arc::new(move |_data: &[u8]| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        f
    };
    let group = RaftGroup::new_cluster_with_callback(vec![1, 2, 3], Some(cb))
        .await
        .unwrap();
    group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    for i in 0u32..50 {
        let _ = group.propose(i.to_le_bytes().to_vec()).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let applied = counter.load(Ordering::SeqCst);
    assert!(applied >= 50, "expected >= 50 apply calls, got {applied}");
    group.shutdown().await.unwrap();
}

// ─── Multi-shard RaftConsensus ──────────────────────────────────────────────

#[tokio::test]
async fn test_multi_shard_consensus() {
    let consensus = RaftConsensus::new();

    let group_a = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    group_a
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();
    let group_b = RaftGroup::new_cluster(vec![10, 20, 30]).await.unwrap();
    group_b
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    consensus.register_shard(ShardId(0), group_a);
    consensus.register_shard(ShardId(1), group_b);

    assert!(consensus.is_leader(ShardId(0)).await);
    assert!(consensus.is_leader(ShardId(1)).await);

    consensus
        .propose(
            ShardId(0),
            LogEntry {
                data: b"shard-0".to_vec(),
            },
        )
        .await
        .unwrap();
    consensus
        .propose(
            ShardId(1),
            LogEntry {
                data: b"shard-1".to_vec(),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_consensus_unknown_shard_returns_error() {
    let consensus = RaftConsensus::new();
    let result = consensus
        .propose(
            ShardId(999),
            LogEntry {
                data: b"nope".to_vec(),
            },
        )
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ConsensusError::NotLeader(shard) => assert_eq!(shard, ShardId(999)),
        other => panic!("expected NotLeader, got {:?}", other),
    }
}

#[tokio::test]
async fn test_consensus_is_leader_unknown_shard() {
    let consensus = RaftConsensus::new();
    assert!(!consensus.is_leader(ShardId(42)).await);
}

// ─── Edge Cases ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_empty_cluster_rejected() {
    let result = RaftGroup::new_cluster(vec![]).await;
    assert!(result.is_err(), "empty node list should be rejected");
}

#[tokio::test]
async fn test_single_node_raft_group() {
    let group = RaftGroup::new_cluster(vec![1]).await.unwrap();
    let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    assert_eq!(leader, 1);
    group.propose(b"solo".to_vec()).await.unwrap();
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_large_payload_proposal() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    let big = vec![0xABu8; 1024 * 1024];
    group.propose(big).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    for &id in group.node_ids() {
        if let Some(raft) = group.get_node(id) {
            let applied = raft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            assert!(
                applied >= 1,
                "node {id} should have applied the large entry"
            );
        }
    }
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_snapshot_after_many_entries() {
    let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
    let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    for i in 0u32..100 {
        group.propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    group.trigger_snapshot(leader).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    group.propose(b"post-snapshot".to_vec()).await.unwrap();
    group.shutdown().await.unwrap();
}
