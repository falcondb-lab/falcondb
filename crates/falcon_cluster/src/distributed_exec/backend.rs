//! Shard execution backend abstraction for transparent local/gRPC dispatch.
//!
//! `ShardExecBackend` decouples the scatter phase from the transport layer:
//! - `LocalShardBackend`: in-process execution via `ShardedEngine` (tests, single-binary)
//! - `GrpcShardBackend`: remote execution via `ShardRouterClient` (multi-node clusters)
//!
//! # Usage
//!
//! ```ignore
//! // In-process (tests / single-binary cluster):
//! let backend = LocalShardBackend::new(engine.clone());
//! executor.scatter_gather_serializable(&plan, &shards, &strategy, &backend)?;
//!
//! // Multi-node cluster:
//! let backend = GrpcShardBackend::new(client, shard_map, rt_handle);
//! executor.scatter_gather_serializable(&plan, &shards, &strategy, &backend)?;
//! ```

use std::sync::Arc;
use std::time::Instant;

use falcon_common::error::FalconError;
use falcon_common::types::ShardId;

use super::serializable_plan::SerializableSubPlan;
use super::ShardResult;
use crate::sharded_engine::ShardedEngine;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Backend Trait
// ═══════════════════════════════════════════════════════════════════════════

/// Trait abstracting shard-local execution for the scatter phase.
///
/// Implementations handle the transport (in-process vs gRPC) transparently.
/// The scatter phase calls `execute_on_shard` for each target shard in parallel
/// from OS threads spawned by `std::thread::scope`.
pub trait ShardExecBackend: Send + Sync {
    /// Execute a serializable subplan on the given shard.
    ///
    /// Returns the shard result including column metadata, rows, and latency.
    /// Implementations MUST be safe to call from multiple OS threads concurrently.
    fn execute_on_shard(
        &self,
        shard_id: ShardId,
        plan: &SerializableSubPlan,
    ) -> Result<ShardResult, FalconError>;
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Local (in-process) Backend
// ═══════════════════════════════════════════════════════════════════════════

/// In-process backend that executes subplans directly on `ShardedEngine` shards.
///
/// Used for single-binary clusters and integration tests where all shards
/// are co-located in the same process. Converts `SerializableSubPlan` into
/// a closure-based `SubPlan` via `into_subplan()` and executes it against
/// the shard's local `StorageEngine` + `TxnManager`.
pub struct LocalShardBackend {
    engine: Arc<ShardedEngine>,
}

impl LocalShardBackend {
    pub fn new(engine: Arc<ShardedEngine>) -> Self {
        Self { engine }
    }

    /// Access the underlying engine (for validation / testing).
    pub fn engine(&self) -> &ShardedEngine {
        &self.engine
    }
}

impl ShardExecBackend for LocalShardBackend {
    fn execute_on_shard(
        &self,
        shard_id: ShardId,
        plan: &SerializableSubPlan,
    ) -> Result<ShardResult, FalconError> {
        let shard = self.engine.shard(shard_id).ok_or_else(|| {
            FalconError::internal_bug(
                "E-BACKEND-001",
                format!("Shard {shard_id:?} not found in LocalShardBackend"),
                "ShardedEngine does not contain the requested shard",
            )
        })?;

        let start = Instant::now();
        let subplan = plan.clone().into_subplan();
        let (columns, rows) = subplan.execute(&shard.storage, &shard.txn_mgr)?;
        let latency_us = start.elapsed().as_micros() as u64;

        Ok(ShardResult {
            shard_id,
            columns,
            rows,
            latency_us,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — gRPC (remote) Backend
// ═══════════════════════════════════════════════════════════════════════════

/// Remote backend that sends serialized subplans to shard leaders via gRPC.
///
/// Used for multi-node clusters where shards are distributed across processes.
/// Requires a running tokio runtime — the stored `rt_handle` bridges sync
/// scatter threads (spawned by `std::thread::scope`) to async gRPC calls.
///
/// # Thread Safety
///
/// `ShardRouterClient` uses `DashMap` internally and is fully thread-safe.
/// `rt_handle.block_on()` is safe from OS threads (not from within a tokio task).
pub struct GrpcShardBackend {
    client: Arc<crate::routing::ShardRouterClient>,
    shard_map: Arc<parking_lot::RwLock<crate::routing::ShardMap>>,
    rt_handle: tokio::runtime::Handle,
}

impl GrpcShardBackend {
    pub fn new(
        client: Arc<crate::routing::ShardRouterClient>,
        shard_map: Arc<parking_lot::RwLock<crate::routing::ShardMap>>,
        rt_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            shard_map,
            rt_handle,
        }
    }
}

impl ShardExecBackend for GrpcShardBackend {
    fn execute_on_shard(
        &self,
        shard_id: ShardId,
        plan: &SerializableSubPlan,
    ) -> Result<ShardResult, FalconError> {
        // Look up the shard leader from the cluster topology.
        let node_id = {
            let map = self.shard_map.read();
            let shard_info = map.get_shard(shard_id).ok_or_else(|| {
                FalconError::internal_bug(
                    "E-BACKEND-002",
                    format!("Shard {shard_id:?} not found in ShardMap"),
                    "GrpcShardBackend: shard not registered in cluster topology",
                )
            })?;
            shard_info.leader.0
        };

        let start = Instant::now();

        // Bridge from sync thread to async gRPC call.
        let result = self.rt_handle.block_on(
            self.client
                .execute_subplan_remote(node_id, shard_id.0, plan),
        );

        let rpc_latency_us = start.elapsed().as_micros() as u64;

        match result {
            Ok(((columns, rows), remote_exec_us)) => {
                tracing::debug!(
                    shard = shard_id.0,
                    node = node_id,
                    rpc_us = rpc_latency_us,
                    exec_us = remote_exec_us,
                    rows = rows.len(),
                    "gRPC subplan execution complete"
                );
                Ok(ShardResult {
                    shard_id,
                    columns,
                    rows,
                    latency_us: rpc_latency_us,
                })
            }
            Err(e) => Err(FalconError::Internal(format!(
                "gRPC subplan execution failed on shard {shard_id:?} (node {node_id}): {e}"
            ))),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::DataType;

    /// Verify that `LocalShardBackend` correctly wraps `ShardedEngine`.
    #[test]
    fn test_local_backend_shard_not_found() {
        let engine = Arc::new(ShardedEngine::new(2));
        let backend = LocalShardBackend::new(engine);

        let plan = SerializableSubPlan::Scan {
            table_id: 999,
            projection: vec![],
            columns: vec![("id".into(), DataType::Int64)],
        };

        // ShardId(99) doesn't exist in a 2-shard engine
        let result = backend.execute_on_shard(ShardId(99), &plan);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("not found"), "Error: {err_msg}");
    }
}
