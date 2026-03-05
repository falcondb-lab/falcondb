//! High-level routing: decides local vs remote execution based on shard map.
//!
//! `ShardRouterClient` forwards SQL queries and serialized subplans to remote
//! shard leaders over gRPC. Channels are lazily created and cached per node.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use falcon_common::types::NodeId;

use super::shard_map::ShardMap;
use crate::distributed_exec::serializable_plan::SerializableSubPlan;
use crate::distributed_exec::SubPlanResult;

/// Client for forwarding queries and subplans to remote shard leaders via gRPC.
///
/// Maintains a cached tonic channel per node for connection reuse.
/// Thread-safe — all internal state is behind `DashMap` or atomics.
pub struct ShardRouterClient {
    /// Map of node_id -> gRPC endpoint address.
    endpoints: dashmap::DashMap<u64, String>,
    /// Cached tonic channels per node_id (lazily created).
    channels: dashmap::DashMap<u64, tonic::transport::Channel>,
    /// Connect timeout for new channels.
    connect_timeout: Duration,
    /// Monotonic request ID counter for subplan correlation.
    next_request_id: AtomicU64,
}

impl Default for ShardRouterClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardRouterClient {
    pub fn new() -> Self {
        Self {
            endpoints: dashmap::DashMap::new(),
            channels: dashmap::DashMap::new(),
            connect_timeout: Duration::from_secs(5),
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Create a client with a custom connect timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            endpoints: dashmap::DashMap::new(),
            channels: dashmap::DashMap::new(),
            connect_timeout: timeout,
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Register a remote node's gRPC endpoint.
    pub fn add_endpoint(&self, node_id: u64, addr: String) {
        // Invalidate cached channel if endpoint changed
        if let Some(old) = self.endpoints.get(&node_id) {
            if *old.value() != addr {
                self.channels.remove(&node_id);
            }
        }
        self.endpoints.insert(node_id, addr);
    }

    /// Remove a remote node's endpoint and cached channel.
    pub fn remove_endpoint(&self, node_id: u64) {
        self.endpoints.remove(&node_id);
        self.channels.remove(&node_id);
    }

    /// Get or create a cached tonic channel for a node.
    async fn get_channel(&self, node_id: u64) -> Result<tonic::transport::Channel, String> {
        // Fast path: cached channel
        if let Some(ch) = self.channels.get(&node_id) {
            return Ok(ch.value().clone());
        }

        // Slow path: create new channel
        let endpoint_addr = self
            .endpoints
            .get(&node_id)
            .map(|e| e.value().clone())
            .ok_or_else(|| format!("No endpoint registered for node {node_id}"))?;

        let channel = tonic::transport::Endpoint::from_shared(endpoint_addr.clone())
            .map_err(|e| format!("Invalid endpoint '{}': {}", endpoint_addr, e))?
            .connect_timeout(self.connect_timeout)
            .connect()
            .await
            .map_err(|e| format!("gRPC connect to node {} ({}): {}", node_id, endpoint_addr, e))?;

        self.channels.insert(node_id, channel.clone());
        Ok(channel)
    }

    /// Invalidate the cached channel for a node (e.g. after a transport error).
    pub fn invalidate_channel(&self, node_id: u64) {
        self.channels.remove(&node_id);
    }

    /// Forward a SQL query to the leader of the given shard.
    ///
    /// Locates the shard leader from the shard map, establishes (or reuses)
    /// a gRPC channel, and sends a `ForwardQuery` RPC.
    pub async fn forward_query(
        &self,
        shard_map: &ShardMap,
        pk_bytes: &[u8],
        sql: &str,
        txn_id: u64,
    ) -> Result<super::ForwardQueryResponse, String> {
        let shard = shard_map.locate_shard(pk_bytes);
        let leader_id = shard.leader.0;
        let shard_id = shard.id.0;

        tracing::debug!(
            shard_id,
            leader = leader_id,
            "Forwarding SQL query to shard leader"
        );

        let channel = self.get_channel(leader_id).await?;
        let mut client =
            crate::proto::wal_replication_client::WalReplicationClient::new(channel);

        let request = crate::proto::ForwardQueryRpc {
            shard_id,
            sql: sql.to_owned(),
            txn_id,
        };

        match client.forward_query(request).await {
            Ok(response) => {
                let r = response.into_inner();
                Ok(super::ForwardQueryResponse {
                    success: r.success,
                    error: r.error,
                    result_json: r.result_json,
                    rows_affected: r.rows_affected,
                })
            }
            Err(status) => {
                self.invalidate_channel(leader_id);
                Err(format!(
                    "ForwardQuery RPC to node {} failed: {}",
                    leader_id, status
                ))
            }
        }
    }

    /// Execute a serialized subplan on a remote shard via gRPC.
    ///
    /// This is the cross-node scatter path: the coordinator serializes a
    /// `SerializableSubPlan`, sends it to the shard leader, and receives
    /// the result rows back.
    ///
    /// Returns `(columns, rows, execution_us)` on success.
    pub async fn execute_subplan_remote(
        &self,
        node_id: u64,
        shard_id: u64,
        plan: &SerializableSubPlan,
    ) -> Result<(SubPlanResult, u64), String> {
        let start = Instant::now();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        // Serialize the subplan to bincode
        let payload = plan
            .to_bytes()
            .map_err(|e| format!("SubPlan serialization: {e}"))?;

        tracing::debug!(
            node_id,
            shard_id,
            request_id,
            payload_bytes = payload.len(),
            "Sending subplan to remote shard"
        );

        let channel = self.get_channel(node_id).await?;
        let mut client =
            crate::proto::wal_replication_client::WalReplicationClient::new(channel);

        let request = crate::proto::ExecuteSubPlanRequest {
            shard_id,
            subplan_payload: payload,
            request_id,
        };

        let response = match client.execute_sub_plan(request).await {
            Ok(r) => r.into_inner(),
            Err(status) => {
                self.invalidate_channel(node_id);
                return Err(format!(
                    "ExecuteSubPlan RPC to node {} shard {} failed: {}",
                    node_id, shard_id, status
                ));
            }
        };

        if !response.success {
            return Err(format!(
                "Remote subplan execution failed on node {} shard {}: {}",
                node_id, shard_id, response.error
            ));
        }

        // Deserialize the SubPlanResult from the single bincode blob
        let result: SubPlanResult = bincode::deserialize(&response.result_payload)
            .map_err(|e| format!("SubPlanResult deserialization: {e}"))?;

        let (columns, rows) = result;
        let rpc_latency_us = start.elapsed().as_micros() as u64;
        let remote_exec_us = response.execution_us;

        tracing::debug!(
            node_id,
            shard_id,
            request_id,
            rows = rows.len(),
            remote_exec_us,
            rpc_latency_us,
            "Received subplan result from remote shard"
        );

        Ok(((columns, rows), remote_exec_us))
    }

    /// Send Prepare2pc RPC to a remote node. Returns rows_affected on success.
    pub async fn prepare_2pc_remote(
        &self,
        node_id: u64,
        txn_id: u64,
        shard_id: u64,
        sql: &str,
    ) -> Result<u64, String> {
        let channel = self.get_channel(node_id).await?;
        let mut client = crate::proto::wal_replication_client::WalReplicationClient::new(channel);
        let req = crate::proto::Prepare2pcRequest {
            txn_id, shard_id, sql: sql.to_owned(),
        };
        match client.prepare2pc(req).await {
            Ok(r) => {
                let r = r.into_inner();
                if r.success { Ok(r.rows_affected) }
                else { Err(format!("Prepare2pc failed on node {node_id}: {}", r.error)) }
            }
            Err(s) => {
                self.invalidate_channel(node_id);
                Err(format!("Prepare2pc RPC to node {node_id}: {s}"))
            }
        }
    }

    /// Send Commit2pc RPC to a remote node.
    pub async fn commit_2pc_remote(&self, node_id: u64, txn_id: u64, shard_id: u64) -> Result<(), String> {
        let channel = self.get_channel(node_id).await?;
        let mut client = crate::proto::wal_replication_client::WalReplicationClient::new(channel);
        let req = crate::proto::Commit2pcRequest { txn_id, shard_id };
        match client.commit2pc(req).await {
            Ok(r) => {
                let r = r.into_inner();
                if r.success { Ok(()) }
                else { Err(format!("Commit2pc failed on node {node_id}: {}", r.error)) }
            }
            Err(s) => {
                self.invalidate_channel(node_id);
                Err(format!("Commit2pc RPC to node {node_id}: {s}"))
            }
        }
    }

    /// Send Abort2pc RPC to a remote node.
    pub async fn abort_2pc_remote(&self, node_id: u64, txn_id: u64, shard_id: u64) -> Result<(), String> {
        let channel = self.get_channel(node_id).await?;
        let mut client = crate::proto::wal_replication_client::WalReplicationClient::new(channel);
        let req = crate::proto::Abort2pcRequest { txn_id, shard_id };
        match client.abort2pc(req).await {
            Ok(_) => Ok(()),
            Err(s) => {
                self.invalidate_channel(node_id);
                Err(format!("Abort2pc RPC to node {node_id}: {s}"))
            }
        }
    }

    /// Check how many nodes have registered endpoints.
    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    /// Check how many cached channels are alive.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

/// High-level router that decides whether to execute locally or forward to a remote node.
pub struct Router {
    local_node_id: NodeId,
    shard_map: ShardMap,
    client: ShardRouterClient,
}

impl Router {
    pub fn new(local_node_id: NodeId, shard_map: ShardMap) -> Self {
        Self {
            local_node_id,
            shard_map,
            client: ShardRouterClient::new(),
        }
    }

    /// Check if a key's shard leader is the local node.
    pub fn is_local(&self, pk_bytes: &[u8]) -> bool {
        let shard = self.shard_map.locate_shard(pk_bytes);
        shard.leader == self.local_node_id
    }

    /// Get the shard map.
    pub const fn shard_map(&self) -> &ShardMap {
        &self.shard_map
    }

    /// Get the client for remote forwarding.
    pub const fn client(&self) -> &ShardRouterClient {
        &self.client
    }

    /// Register a remote node endpoint.
    pub fn add_node(&self, node_id: u64, addr: String) {
        self.client.add_endpoint(node_id, addr);
    }

    /// Route a query: returns Ok(None) if local, Ok(Some(response)) if forwarded.
    pub async fn route_query(
        &self,
        pk_bytes: &[u8],
        sql: &str,
        txn_id: u64,
    ) -> Result<Option<super::ForwardQueryResponse>, String> {
        if self.is_local(pk_bytes) {
            Ok(None) // Execute locally
        } else {
            let response = self
                .client
                .forward_query(&self.shard_map, pk_bytes, sql, txn_id)
                .await?;
            Ok(Some(response))
        }
    }
}
