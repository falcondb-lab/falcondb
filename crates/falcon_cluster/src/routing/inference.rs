//! High-level routing: decides local vs remote execution based on shard map.

use falcon_common::types::NodeId;

use super::shard_map::ShardMap;

/// Client for forwarding queries to remote shard leaders.
pub struct ShardRouterClient {
    /// Map of node_id -> gRPC endpoint address
    endpoints: dashmap::DashMap<u64, String>,
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
        }
    }

    /// Register a remote node's gRPC endpoint.
    pub fn add_endpoint(&self, node_id: u64, addr: String) {
        self.endpoints.insert(node_id, addr);
    }

    /// Remove a remote node's endpoint.
    pub fn remove_endpoint(&self, node_id: u64) {
        self.endpoints.remove(&node_id);
    }

    /// Forward a query to the leader of the given shard.
    pub async fn forward_query(
        &self,
        shard_map: &ShardMap,
        pk_bytes: &[u8],
        _sql: &str,
        _txn_id: u64,
    ) -> Result<super::ForwardQueryResponse, String> {
        let shard = shard_map.locate_shard(pk_bytes);
        let leader_id = shard.leader.0;

        let endpoint = self
            .endpoints
            .get(&leader_id)
            .map(|e| e.value().clone())
            .ok_or_else(|| format!("No endpoint for node {}", leader_id))?;

        tracing::debug!(
            shard_id = shard.id.0,
            leader = leader_id,
            endpoint = %endpoint,
            "Forwarding query to shard leader"
        );

        // In a real implementation, this would use tonic::transport::Channel
        // to make a gRPC call. For MVP, we return an error indicating remote
        // execution is not yet wired (single-node mode handles everything locally).
        Err(format!(
            "Remote execution not yet implemented (would forward to {} at {})",
            leader_id, endpoint
        ))
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
    pub fn shard_map(&self) -> &ShardMap {
        &self.shard_map
    }

    /// Get the client for remote forwarding.
    pub fn client(&self) -> &ShardRouterClient {
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
