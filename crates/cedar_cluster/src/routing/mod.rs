//! gRPC-based shard routing for distributed query execution.
//!
//! Provides a `ShardRouter` tonic service for forwarding SQL queries to
//! the correct shard leader, plus a client for making those calls.

pub mod inference;
pub mod shard_map;

// Re-export all public types at the module level for backward compatibility.
pub use inference::{Router, ShardRouterClient};
pub use shard_map::{ShardInfo, ShardMap};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tonic::Status;

use cedar_common::types::NodeId;

// ---------------------------------------------------------------------------
// Message types (hand-written, no protoc needed)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardQueryRequest {
    pub shard_id: u64,
    pub sql: String,
    pub txn_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardQueryResponse {
    pub success: bool,
    pub error: String,
    pub result_json: String,
    pub rows_affected: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub node_id: u64,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Service trait
// ---------------------------------------------------------------------------

/// Trait for handling shard-routed queries.
/// Implementations execute the SQL on the local shard and return results.
#[async_trait]
pub trait QueryExecutor: Send + Sync + 'static {
    async fn execute_forwarded(
        &self,
        shard_id: u64,
        sql: &str,
        txn_id: u64,
    ) -> Result<ForwardQueryResponse, String>;
}

// ---------------------------------------------------------------------------
// gRPC Server
// ---------------------------------------------------------------------------

/// gRPC service that receives forwarded queries from other nodes.
pub struct ShardRouterServer<E: QueryExecutor> {
    executor: E,
    node_id: NodeId,
}

impl<E: QueryExecutor> ShardRouterServer<E> {
    pub fn new(executor: E, node_id: NodeId) -> Self {
        Self { executor, node_id }
    }

    pub async fn handle_forward_query(
        &self,
        request: ForwardQueryRequest,
    ) -> Result<ForwardQueryResponse, Status> {
        tracing::debug!(
            shard_id = request.shard_id,
            sql = %request.sql,
            "Received forwarded query"
        );

        self.executor
            .execute_forwarded(request.shard_id, &request.sql, request.txn_id)
            .await
            .map_err(Status::internal)
    }

    pub async fn handle_heartbeat(
        &self,
        _request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, Status> {
        Ok(HeartbeatResponse {
            node_id: self.node_id.0,
            status: "active".to_string(),
        })
    }
}
