//! Cluster metadata service — shard map, node directory, DDL coordination,
//! and gRPC-based shard routing for distributed query execution.
//!
//! MVP: single-node, single-shard. All data lives on one node.
//! P1: hash-based sharding with shard routing via gRPC (tonic).

pub mod cluster;
pub mod distributed_exec;
pub mod grpc_transport;
pub mod ha;
pub mod query_engine;
pub mod rebalancer;
pub mod replication;
pub mod routing;
pub mod sharded_engine;
pub mod sharding;
pub mod two_phase;
pub mod cross_shard;
pub mod fault_injection;

/// Protobuf types and tonic client/server for WAL replication.
/// Re-exported from the `falcon_proto` crate (generated at build time).
pub use falcon_proto::falcon_replication as proto;

#[cfg(test)]
mod tests;

pub use cluster::{NodeInfo, NodeStatus};
pub use distributed_exec::{DistributedExecutor, GatherStrategy, SubPlan, AggMerge, ScatterGatherMetrics, FailurePolicy, GatherLimits};
pub use query_engine::DistributedQueryEngine;
pub use replication::{
    ReplicaNode, ReplicaRole, ReplicationLog, ShardReplicaGroup,
    WalChunk, LsnWalRecord, ReplicationTransport, AsyncReplicationTransport,
    InProcessTransport, ChannelTransport,
    ReplicationMetrics, ReplicationMetricsSnapshot,
    ReplicaRunner, ReplicaRunnerConfig, ReplicaRunnerHandle,
    ReplicaRunnerMetrics, ReplicaRunnerMetricsSnapshot,
    WriteOp, apply_wal_record_to_engine,
};
pub use rebalancer::{
    RebalanceRunner, RebalanceRunnerConfig, RebalanceRunnerHandle,
    ShardRebalancer, RebalancerConfig, RebalancerStatus,
    ShardLoadSnapshot, ShardLoadDetailed, TableLoad,
    MigrationPlan, MigrationTask, MigrationPhase, MigrationStatus,
};
pub use routing::{Router, ShardRouterClient, ShardRouterServer, ShardMap, ShardInfo};
pub use sharded_engine::ShardedEngine;
pub use sharding::{
    compute_shard_hash, compute_shard_hash_from_datums,
    target_shard_for_row, target_shard_from_datums, all_shards_for_table,
};
pub use ha::{
    HAConfig, HAReplicaGroup, HAStatus, HAReplicaStatus,
    SyncMode, FailureDetector, ReplicaHealth, ReplicaHealthStatus, PrimaryHealth,
    FailoverOrchestrator, FailoverOrchestratorConfig, FailoverOrchestratorHandle,
    FailoverOrchestratorMetrics, SyncReplicationWaiter,
};
pub use two_phase::TwoPhaseCoordinator;
