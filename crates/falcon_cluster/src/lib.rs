//! Cluster metadata service — shard map, node directory, DDL coordination,
//! and gRPC-based shard routing for distributed query execution.
//!
//! MVP: single-node, single-shard. All data lives on one node.
//! P1: hash-based sharding with shard routing via gRPC (tonic).

pub mod admission;
pub mod bg_supervisor;
pub mod circuit_breaker;
pub mod cluster;
pub mod cluster_ops;
pub mod cross_shard;
pub mod deterministic_2pc;
pub mod distributed_exec;
pub mod failover_txn_hardening;
pub mod fault_injection;
pub mod grpc_transport;
pub mod ha;
pub mod indoubt_resolver;
pub mod query_engine;
pub mod rebalancer;
pub mod replication;
pub mod routing;
pub mod security_hardening;
pub mod sharded_engine;
pub mod stability_hardening;
pub mod sharding;
pub mod token_bucket;
pub mod two_phase;

/// Protobuf types and tonic client/server for WAL replication.
/// Re-exported from the `falcon_proto` crate (generated at build time).
pub use falcon_proto::falcon_replication as proto;

#[cfg(test)]
mod failover_txn_tests;
#[cfg(test)]
mod stability_stress_tests;
#[cfg(test)]
mod tests;

pub use bg_supervisor::{
    BgTaskCriticality, BgTaskInfo, BgTaskSnapshot, BgTaskState, BgTaskSupervisor, NodeHealth,
};
pub use cluster::{NodeInfo, NodeStatus};
pub use cluster_ops::{
    ClusterAdmin, ClusterEvent, ClusterEventLog, EventCategory, EventSeverity, NodeModeController,
    NodeOperationalMode, ScaleInLifecycle, ScaleInState, ScaleOutLifecycle, ScaleOutState,
};
pub use deterministic_2pc::{
    CoordinatorDecision, CoordinatorDecisionLog, DecisionLogConfig, DecisionLogSnapshot,
    DecisionRecord, LayeredTimeoutConfig, LayeredTimeoutController, LayeredTimeoutSnapshot,
    SlowShardAction, SlowShardConfig, SlowShardEvent, SlowShardPolicy, SlowShardSnapshot,
    SlowShardTracker, TimeoutResult,
};
pub use distributed_exec::{
    AggMerge, DistributedExecutor, FailurePolicy, GatherLimits, GatherStrategy,
    ScatterGatherMetrics, SubPlan,
};
pub use ha::{
    FailoverOrchestrator, FailoverOrchestratorConfig, FailoverOrchestratorHandle,
    FailoverOrchestratorMetrics, FailureDetector, HAConfig, HAReplicaGroup, HAReplicaStatus,
    HAStatus, PrimaryHealth, ReplicaHealth, ReplicaHealthStatus, SyncMode, SyncReplicationWaiter,
};
pub use query_engine::DistributedQueryEngine;
pub use rebalancer::{
    MigrationPhase, MigrationPlan, MigrationStatus, MigrationTask, RebalanceRunner,
    RebalanceRunnerConfig, RebalanceRunnerHandle, RebalancerConfig, RebalancerStatus,
    ShardLoadDetailed, ShardLoadSnapshot, ShardRebalancer, TableLoad,
};
pub use replication::{
    apply_wal_record_to_engine, AsyncReplicationTransport, ChannelTransport, InProcessTransport,
    LsnWalRecord, ReplicaNode, ReplicaRole, ReplicaRunner, ReplicaRunnerConfig,
    ReplicaRunnerHandle, ReplicaRunnerMetrics, ReplicaRunnerMetricsSnapshot, ReplicationLog,
    ReplicationMetrics, ReplicationMetricsSnapshot, ReplicationTransport, ShardReplicaGroup,
    WalChunk, WriteOp,
};
pub use routing::{Router, ShardInfo, ShardMap, ShardRouterClient, ShardRouterServer};
pub use security_hardening::{
    AuthRateLimiter, AuthRateLimiterConfig, AuthRateLimiterSnapshot, AuthRateResult,
    PasswordPolicy, PasswordPolicyConfig, PasswordPolicySnapshot, PasswordValidation, SqlFirewall,
    SqlFirewallConfig, SqlFirewallResult, SqlFirewallSnapshot,
};
pub use failover_txn_hardening::{
    FailoverBlockedTxnConfig, FailoverBlockedTxnGuard, FailoverBlockedTxnMetrics,
    FailoverDamper, FailoverDamperConfig, FailoverDamperMetrics, FailoverEvent,
    FailoverTxnCoordinator, FailoverTxnConfig, FailoverTxnMetrics, FailoverTxnPhase,
    FailoverTxnResolution, InDoubtTtlConfig, InDoubtTtlEnforcer, InDoubtTtlMetrics,
};
pub use stability_hardening::{
    CommitPhase, CommitPhaseMetrics, CommitPhaseTracker, DefensiveValidator,
    DefensiveValidatorMetrics, ErrorClassStabilizer, EscalationOutcome, EscalationRecord,
    FailoverOutcomeGuard, FailoverOutcomeGuardMetrics, InDoubtEscalator, InDoubtEscalatorMetrics,
    InDoubtReason, ProtocolPhase, ResolutionMethod, RetryGuard, RetryGuardMetrics,
    StateOrdinal, TxnOutcomeEntry, TxnOutcomeJournal, TxnStateGuard, TxnStateGuardMetrics,
};
pub use sharded_engine::ShardedEngine;
pub use sharding::{
    all_shards_for_table, compute_shard_hash, compute_shard_hash_from_datums, target_shard_for_row,
    target_shard_from_datums,
};
pub use token_bucket::{TokenBucket, TokenBucketConfig, TokenBucketError, TokenBucketSnapshot};
pub use two_phase::TwoPhaseCoordinator;
