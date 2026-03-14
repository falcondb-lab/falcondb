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

use falcon_common::types::NodeId;

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
// Remote SubPlan execution types (scatter/gather over gRPC)
// ---------------------------------------------------------------------------

/// Request to execute a serialized subplan on a remote shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteSubPlanRequest {
    /// Target shard ID on the remote node.
    pub shard_id: u64,
    /// Bincode-serialized `SerializableSubPlan`.
    pub subplan_payload: Vec<u8>,
    /// Caller-assigned request ID for correlation / tracing.
    pub request_id: u64,
}

/// Column metadata in a remote subplan result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteColumnMeta {
    pub name: String,
    /// DataType ordinal.
    pub type_id: u32,
}

/// Response from a remote subplan execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteSubPlanResponse {
    pub success: bool,
    pub error: String,
    /// Column metadata.
    pub columns: Vec<RemoteColumnMeta>,
    /// Bincode-serialized rows (each element is one `OwnedRow`).
    pub row_payloads: Vec<Vec<u8>>,
    /// Execution latency on the remote shard (microseconds).
    pub execution_us: u64,
}

// ---------------------------------------------------------------------------
// Service trait
// ---------------------------------------------------------------------------

/// Trait for handling shard-routed SQL queries.
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

/// Trait for executing serialized subplans on a local shard.
/// Implemented by the shard node to support remote scatter/gather.
#[async_trait]
pub trait SubPlanExecutor: Send + Sync + 'static {
    async fn execute_subplan(
        &self,
        shard_id: u64,
        subplan_payload: &[u8],
    ) -> Result<ExecuteSubPlanResponse, String>;
}

// ---------------------------------------------------------------------------
// gRPC Server
// ---------------------------------------------------------------------------

/// gRPC service that receives forwarded queries and subplans from other nodes.
pub struct ShardRouterServer<E: QueryExecutor, S: SubPlanExecutor> {
    executor: E,
    subplan_executor: S,
    node_id: NodeId,
}

impl<E: QueryExecutor, S: SubPlanExecutor> ShardRouterServer<E, S> {
    pub fn new(executor: E, subplan_executor: S, node_id: NodeId) -> Self {
        Self {
            executor,
            subplan_executor,
            node_id,
        }
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

    pub async fn handle_execute_subplan(
        &self,
        request: ExecuteSubPlanRequest,
    ) -> Result<ExecuteSubPlanResponse, Status> {
        tracing::debug!(
            shard_id = request.shard_id,
            request_id = request.request_id,
            payload_bytes = request.subplan_payload.len(),
            "Received remote subplan execution request"
        );

        self.subplan_executor
            .execute_subplan(request.shard_id, &request.subplan_payload)
            .await
            .map_err(Status::internal)
    }

    pub async fn handle_heartbeat(
        &self,
        _request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, Status> {
        Ok(HeartbeatResponse {
            node_id: self.node_id.0,
            status: "active".to_owned(),
        })
    }
}
