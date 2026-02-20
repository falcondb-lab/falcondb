//! Raft consensus layer for FalconDB.
//!
//! - `RaftNode`: single-node Raft (backward compat)
//! - `RaftGroup`: multi-node in-process Raft cluster
//! - `RaftConsensus`: `Consensus` trait backed by per-shard `RaftGroup`
//! - `SingleNodeConsensus`: no-op for single-node deployments

pub mod network;
pub mod store;
pub mod types;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use falcon_common::types::{NodeId, ShardId};
use openraft::{BasicNode, Config, Raft};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::network::{NetworkFactory, RaftRouter, RouterNetworkFactory};
use crate::store::{ApplyFn, LogStore, StateMachine};
use crate::types::{CedarRequest, TypeConfig};

/// A log entry proposed to the Raft group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub data: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("Not leader for shard {0}")]
    NotLeader(ShardId),
    #[error("Proposal failed: {0}")]
    ProposalFailed(String),
    #[error("Membership change failed: {0}")]
    MembershipFailed(String),
    #[error("Node {0} not found")]
    NodeNotFound(u64),
}

#[async_trait]
pub trait Consensus: Send + Sync + 'static {
    async fn propose(&self, shard: ShardId, entry: LogEntry) -> Result<(), ConsensusError>;
    async fn is_leader(&self, shard: ShardId) -> bool;
    async fn add_member(&self, shard: ShardId, node: NodeId) -> Result<(), ConsensusError>;
    async fn remove_member(&self, shard: ShardId, node: NodeId) -> Result<(), ConsensusError>;
}

pub struct SingleNodeConsensus;

#[async_trait]
impl Consensus for SingleNodeConsensus {
    async fn propose(&self, _shard: ShardId, _entry: LogEntry) -> Result<(), ConsensusError> {
        Ok(())
    }

    async fn is_leader(&self, _shard: ShardId) -> bool {
        true
    }

    async fn add_member(&self, _shard: ShardId, _node: NodeId) -> Result<(), ConsensusError> {
        Ok(())
    }

    async fn remove_member(&self, _shard: ShardId, _node: NodeId) -> Result<(), ConsensusError> {
        Ok(())
    }
}

/// Single-node Raft cluster (backward compat).
pub struct RaftNode {
    raft: Raft<TypeConfig>,
}

impl RaftNode {
    pub async fn new_single_node(node_id: u64) -> Result<Self, ConsensusError> {
        let config = Arc::new(
            Config::default()
                .validate()
                .map_err(|e| ConsensusError::ProposalFailed(format!("config: {}", e)))?,
        );
        let raft = Raft::new(node_id, config, NetworkFactory, LogStore::new(), StateMachine::new())
            .await
            .map_err(|e| ConsensusError::ProposalFailed(format!("init: {}", e)))?;
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new("127.0.0.1"));
        raft.initialize(members)
            .await
            .map_err(|e| ConsensusError::ProposalFailed(format!("bootstrap: {}", e)))?;
        Ok(Self { raft })
    }

    pub async fn propose(&self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.raft
            .client_write(CedarRequest::Write { data })
            .await
            .map_err(|e| ConsensusError::ProposalFailed(format!("{}", e)))?;
        Ok(())
    }

    pub fn inner(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ConsensusError::ProposalFailed(format!("shutdown: {}", e)))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RaftGroup — multi-node in-process Raft cluster
// ---------------------------------------------------------------------------

/// Multi-node Raft cluster using in-process `RaftRouter` for RPCs.
/// All nodes run in the same tokio runtime; RPCs are zero-copy function calls.
pub struct RaftGroup {
    router: Arc<RaftRouter>,
    node_ids: Vec<u64>,
    config: Arc<Config>,
    apply_fn: Option<ApplyFn>,
}

impl RaftGroup {
    pub async fn new_cluster(node_ids: Vec<u64>) -> Result<Self, ConsensusError> {
        Self::new_cluster_with_callback(node_ids, None).await
    }

    pub async fn new_cluster_with_callback(
        node_ids: Vec<u64>,
        apply_fn: Option<ApplyFn>,
    ) -> Result<Self, ConsensusError> {
        if node_ids.is_empty() {
            return Err(ConsensusError::ProposalFailed("node_ids must not be empty".into()));
        }
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .map_err(|e| ConsensusError::ProposalFailed(format!("config: {}", e)))?,
        );
        let router = RaftRouter::new();
        let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
        for &id in &node_ids {
            members.insert(id, BasicNode::new(format!("node-{}", id).as_str()));
        }
        for &node_id in &node_ids {
            let sm = match &apply_fn {
                Some(cb) => StateMachine::with_apply_fn(cb.clone()),
                None => StateMachine::new(),
            };
            let raft = Raft::new(
                node_id,
                config.clone(),
                RouterNetworkFactory::new(router.clone()),
                LogStore::new(),
                sm,
            )
            .await
            .map_err(|e| ConsensusError::ProposalFailed(format!("node {} init: {}", node_id, e)))?;
            router.add_node(node_id, raft);
        }
        if let Some(raft) = router.get_node(node_ids[0]) {
            raft.initialize(members)
                .await
                .map_err(|e| ConsensusError::ProposalFailed(format!("bootstrap: {}", e)))?;
        }
        Ok(Self { router, node_ids, config, apply_fn })
    }

    /// Wait for a leader to be elected, polling up to `timeout`.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<u64, ConsensusError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for &id in &self.node_ids {
                if let Some(raft) = self.router.get_node(id) {
                    if let Some(leader) = raft.metrics().borrow().current_leader {
                        return Ok(leader);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ConsensusError::ProposalFailed("leader election timed out".into()));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Current leader node ID, if any.
    pub async fn current_leader(&self) -> Option<u64> {
        for &id in &self.node_ids {
            if let Some(raft) = self.router.get_node(id) {
                if let Some(leader) = raft.metrics().borrow().current_leader {
                    return Some(leader);
                }
            }
        }
        None
    }

    /// Propose a write entry (routes to leader automatically).
    pub async fn propose(&self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.propose_req(CedarRequest::Write { data }).await
    }

    /// Propose a no-op entry (leader confirmation without side effects).
    pub async fn propose_noop(&self) -> Result<(), ConsensusError> {
        self.propose_req(CedarRequest::Noop).await
    }

    async fn propose_req(&self, req: CedarRequest) -> Result<(), ConsensusError> {
        let mut last_err = ConsensusError::ProposalFailed("no nodes available".into());
        for &id in &self.node_ids {
            if let Some(raft) = self.router.get_node(id) {
                match raft.client_write(req.clone()).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        last_err = ConsensusError::ProposalFailed(format!("node {}: {}", id, e));
                    }
                }
            }
        }
        Err(last_err)
    }

    pub fn get_node(&self, node_id: u64) -> Option<Raft<TypeConfig>> {
        self.router.get_node(node_id)
    }

    /// Simulate a network partition: node stops receiving RPCs.
    pub fn partition_node(&self, node_id: u64) {
        self.router.remove_node(node_id);
    }

    /// Reconnect a previously partitioned node.
    pub async fn reconnect_node(&self, node_id: u64) -> Result<(), ConsensusError> {
        if self.router.get_node(node_id).is_some() {
            return Ok(());
        }
        let sm = match &self.apply_fn {
            Some(cb) => StateMachine::with_apply_fn(cb.clone()),
            None => StateMachine::new(),
        };
        let raft = Raft::new(
            node_id,
            self.config.clone(),
            RouterNetworkFactory::new(self.router.clone()),
            LogStore::new(),
            sm,
        )
        .await
        .map_err(|e| ConsensusError::ProposalFailed(format!("reconnect: {}", e)))?;
        self.router.add_node(node_id, raft);
        Ok(())
    }

    /// Add a new voting member to the cluster.
    pub async fn add_voter(&self, node_id: u64, addr: &str) -> Result<(), ConsensusError> {
        let leader_id = self.wait_for_leader(Duration::from_secs(2)).await?;
        let raft = self.router.get_node(leader_id).ok_or(ConsensusError::NodeNotFound(leader_id))?;
        raft.add_learner(node_id, BasicNode::new(addr), true)
            .await
            .map_err(|e| ConsensusError::MembershipFailed(format!("{}", e)))?;
        let members: Vec<u64> = self.node_ids.iter().copied()
            .chain(std::iter::once(node_id)).collect();
        raft.change_membership(members, false)
            .await
            .map_err(|e| ConsensusError::MembershipFailed(format!("{}", e)))?;
        Ok(())
    }

    /// Remove a voting member from the cluster.
    pub async fn remove_voter(&self, node_id: u64) -> Result<(), ConsensusError> {
        let leader_id = self.wait_for_leader(Duration::from_secs(2)).await?;
        let raft = self.router.get_node(leader_id).ok_or(ConsensusError::NodeNotFound(leader_id))?;
        let remaining: Vec<u64> = self.node_ids.iter().copied()
            .filter(|&id| id != node_id).collect();
        raft.change_membership(remaining, false)
            .await
            .map_err(|e| ConsensusError::MembershipFailed(format!("{}", e)))?;
        Ok(())
    }

    /// Trigger a snapshot on a specific node.
    pub async fn trigger_snapshot(&self, node_id: u64) -> Result<(), ConsensusError> {
        let raft = self.router.get_node(node_id).ok_or(ConsensusError::NodeNotFound(node_id))?;
        raft.trigger().snapshot().await
            .map_err(|e| ConsensusError::ProposalFailed(format!("snapshot: {}", e)))?;
        Ok(())
    }

    /// Shut down all nodes gracefully.
    pub async fn shutdown(self) -> Result<(), ConsensusError> {
        for id in self.node_ids.clone() {
            if let Some(raft) = self.router.get_node(id) {
                raft.shutdown().await
                    .map_err(|e| ConsensusError::ProposalFailed(format!("shutdown {}: {}", id, e)))?;
            }
        }
        Ok(())
    }

    pub fn node_count(&self) -> usize { self.node_ids.len() }
    pub fn node_ids(&self) -> &[u64] { &self.node_ids }
}

// ---------------------------------------------------------------------------
// RaftConsensus — Consensus trait backed by per-shard RaftGroups
// ---------------------------------------------------------------------------

struct RaftGroupHandle {
    group: tokio::sync::Mutex<RaftGroup>,
}

/// Multi-shard Raft consensus: each `ShardId` maps to a `RaftGroup`.
pub struct RaftConsensus {
    groups: RwLock<BTreeMap<u64, Arc<RaftGroupHandle>>>,
}

impl RaftConsensus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { groups: RwLock::new(BTreeMap::new()) })
    }

    pub fn register_shard(&self, shard: ShardId, group: RaftGroup) {
        self.groups.write().insert(
            shard.0,
            Arc::new(RaftGroupHandle { group: tokio::sync::Mutex::new(group) }),
        );
    }

    fn get_group(&self, shard: ShardId) -> Result<Arc<RaftGroupHandle>, ConsensusError> {
        self.groups.read().get(&shard.0).cloned()
            .ok_or(ConsensusError::NotLeader(shard))
    }
}

impl Default for RaftConsensus {
    fn default() -> Self {
        Self { groups: RwLock::new(BTreeMap::new()) }
    }
}

#[async_trait]
impl Consensus for RaftConsensus {
    async fn propose(&self, shard: ShardId, entry: LogEntry) -> Result<(), ConsensusError> {
        let h = self.get_group(shard)?;
        let g = h.group.lock().await;
        g.propose(entry.data).await
    }
    async fn is_leader(&self, shard: ShardId) -> bool {
        let Ok(h) = self.get_group(shard) else { return false; };
        let g = h.group.lock().await;
        g.current_leader().await.is_some()
    }
    async fn add_member(&self, shard: ShardId, node: NodeId) -> Result<(), ConsensusError> {
        let h = self.get_group(shard)?;
        let g = h.group.lock().await;
        g.add_voter(node.0, &format!("node-{}", node.0)).await
    }
    async fn remove_member(&self, shard: ShardId, node: NodeId) -> Result<(), ConsensusError> {
        let h = self.get_group(shard)?;
        let g = h.group.lock().await;
        g.remove_voter(node.0).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

    use super::*;

    // ── Single-node (backward compat) ────────────────────────────────────────

    #[tokio::test]
    async fn test_single_node_init() {
        let node = RaftNode::new_single_node(1).await.unwrap();
        let m = node.inner().metrics().borrow().clone();
        assert_eq!(m.id, 1);
        assert!(m.current_leader.is_some(), "single node should elect itself leader");
        assert_eq!(m.current_leader.unwrap(), 1);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_propose_and_apply() {
        let node = RaftNode::new_single_node(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        node.propose(b"hello".to_vec()).await.unwrap();
        node.propose(b"world".to_vec()).await.unwrap();
        node.propose(b"falcon".to_vec()).await.unwrap();
        let m = node.inner().metrics().borrow().clone();
        assert!(m.last_applied.is_some());
        assert!(m.last_applied.unwrap().index >= 4, "expected >= 4 applied entries");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_propose_empty_data() {
        let node = RaftNode::new_single_node(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        node.propose(vec![]).await.unwrap();
        assert!(node.inner().metrics().borrow().last_applied.is_some());
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_proposals_ordering() {
        let node = RaftNode::new_single_node(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        for i in 0..10u32 {
            node.propose(i.to_le_bytes().to_vec()).await.unwrap();
        }
        let m = node.inner().metrics().borrow().clone();
        assert!(m.last_applied.unwrap().index >= 11, "expected >= 11 applied entries");
        node.shutdown().await.unwrap();
    }

    // ── Multi-node RaftGroup ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_three_node_leader_election() {
        let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
        let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        assert!(
            group.node_ids().contains(&leader),
            "leader {} must be one of the cluster nodes",
            leader
        );
        assert_eq!(group.node_count(), 3);
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_three_node_propose_replication() {
        let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
        group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        for i in 0..5u32 {
            group.propose(i.to_le_bytes().to_vec()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        for &id in group.node_ids() {
            if let Some(raft) = group.get_node(id) {
                let applied = raft.metrics().borrow().last_applied.map(|l| l.index).unwrap_or(0);
                assert!(applied >= 5, "node {} applied only {} entries", id, applied);
            }
        }
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_callback_invoked() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = counter.clone();
        let cb: ApplyFn = Arc::new(move |_data: &[u8]| {
            c2.fetch_add(1, AOrdering::SeqCst);
            Ok(())
        });
        let group = RaftGroup::new_cluster_with_callback(vec![1, 2, 3], Some(cb))
            .await
            .unwrap();
        group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        group.propose(b"entry-1".to_vec()).await.unwrap();
        group.propose(b"entry-2".to_vec()).await.unwrap();
        group.propose(b"entry-3".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        // Each Write entry is applied on all 3 nodes → at least 3 calls.
        let count = counter.load(AOrdering::SeqCst);
        assert!(count >= 3, "expected >= 3 apply callbacks, got {}", count);
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_leader_failover() {
        let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
        let old_leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        group.propose(b"before-failover".to_vec()).await.unwrap();
        // Partition the current leader — remove it from the router.
        group.partition_node(old_leader);
        // Wait for the remaining nodes to elect a NEW leader (≠ old_leader).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let new_leader = loop {
            // Only query nodes still reachable (i.e. still in the router).
            let mut found = None;
            for &id in &[1u64, 2, 3] {
                if id == old_leader { continue; }
                if let Some(raft) = group.get_node(id) {
                    let leader = raft.metrics().borrow().current_leader;
                    if let Some(l) = leader {
                        if l != old_leader {
                            found = Some(l);
                            break;
                        }
                    }
                }
            }
            if let Some(l) = found { break l; }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for new leader after failover"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_ne!(new_leader, old_leader, "new leader must differ from old leader");
        // Propose after failover — route to surviving nodes.
        group.propose(b"after-failover".to_vec()).await.unwrap();
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_snapshot_trigger() {
        let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
        let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        for i in 0..5u32 {
            group.propose(i.to_le_bytes().to_vec()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        group.trigger_snapshot(leader).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_noop_proposal() {
        let group = RaftGroup::new_cluster(vec![1, 2, 3]).await.unwrap();
        group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        group.propose_noop().await.unwrap();
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_raft_consensus_trait() {
        let consensus = RaftConsensus::new();
        let shard = ShardId(42);
        let group = RaftGroup::new_cluster(vec![10, 20, 30]).await.unwrap();
        group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        consensus.register_shard(shard, group);
        assert!(consensus.is_leader(shard).await);
        consensus
            .propose(shard, LogEntry { data: b"via-trait".to_vec() })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_five_node_cluster() {
        let group = RaftGroup::new_cluster(vec![1, 2, 3, 4, 5]).await.unwrap();
        let leader = group.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        assert!(group.node_ids().contains(&leader));
        for i in 0..10u32 {
            group.propose(i.to_le_bytes().to_vec()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        for &id in group.node_ids() {
            if let Some(raft) = group.get_node(id) {
                let applied = raft.metrics().borrow().last_applied.map(|l| l.index).unwrap_or(0);
                assert!(applied >= 10, "node {} applied only {} entries", id, applied);
            }
        }
        group.shutdown().await.unwrap();
    }
}
