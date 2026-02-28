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
pub mod gateway;
pub mod query_engine;
pub mod raft_integration;
pub mod raft_rebalance;
pub mod rebalancer;
pub mod replication;
pub mod routing;
pub mod security_hardening;
pub mod sharded_engine;
pub mod stability_hardening;
pub mod determinism_hardening;
pub mod sharding;
pub mod sla_admission;
pub mod token_bucket;
pub use falcon_enterprise::control_plane;
pub use falcon_enterprise::enterprise_ops;
pub use falcon_enterprise::enterprise_security;
pub mod ga_hardening;
pub mod cost_capacity;
pub mod segment_streaming;
pub mod self_healing;
pub mod smart_gateway;
pub mod two_phase;
pub mod client_discovery;
pub mod dist_hardening;
pub mod distributed_enhancements;

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
pub use cluster::{
    MemberInfo, MemberSummary, MembershipConfig, MembershipError, MembershipEvent,
    MembershipEventType, MembershipManager, MembershipMetrics, MembershipView, NodeInfo,
    NodeMetrics, NodeRole, NodeState, NodeStatus,
};
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
pub use query_engine::{
    DistributedQueryEngine, GatewayAdmissionConfig, GatewayAdmissionControl,
    GatewayDisposition, GatewayMetrics, GatewayMetricsSnapshot,
};
pub use rebalancer::{
    MigrationPhase, MigrationPlan, MigrationStatus, MigrationTask, RebalanceRunner,
    RebalanceRunnerConfig, RebalanceRunnerHandle, RebalancerConfig, RebalancerMetrics,
    RebalancerStatus, ShardLoadDetailed, ShardLoadSnapshot, ShardRebalancer, TableLoad,
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
pub use determinism_hardening::{
    AbortReason, CommitPhase as DeterminismCommitPhase, DeterministicRejectPolicy,
    FailoverCrashRecord, FailoverExpectedOutcome, IdempotentReplayValidator,
    QueueDepthGuard, QueueDepthSnapshot, QueueSlot, RejectReason, ResourceExhaustionContract,
    RetryPolicy, TxnTerminalState,
};
pub use sharded_engine::ShardedEngine;
pub use sharding::{
    all_shards_for_table, compute_shard_hash, compute_shard_hash_from_datums, target_shard_for_row,
    target_shard_from_datums,
};
pub use sla_admission::{
    AdmissionDecision, LatencyPercentiles, LatencyTracker, RejectionExplanation, RejectionSignal,
    SlaAdmissionController, SlaAdmissionMetrics, SlaConfig, SlaPermit, TxnClass,
    TxnClassificationHints, classify_txn,
};
pub use token_bucket::{TokenBucket, TokenBucketConfig, TokenBucketError, TokenBucketSnapshot};
pub use smart_gateway::{
    ClusterTopology, CompressionProfile, GatewayError, GatewayErrorCode, GatewayRole,
    HostPort, JdbcConnectionUrl, RequestClassification, RouteDecision, SeedGatewayList,
    SmartGateway, SmartGatewayConfig, SmartGatewayMetrics, SmartGatewayMetricsSnapshot,
    TopologyCache, TopologyCacheMetrics, TopologyCacheMetricsSnapshot, TopologyEntry,
    WalMode,
};
pub use self_healing::{
    BackpressureConfig, BackpressureController, BackpressureMetrics,
    CatchUpConfig, CatchUpMetrics, CatchUpPhase, ClusterFailureDetector,
    ClusterHealthLevel, ClusterHealthResponse, ClusterStatusResponse,
    DrainPhase, ElectionConfig, ElectionError, ElectionMetrics, ElectionState,
    FailureDetectorConfig, FailureDetectorMetrics, JoinPhase,
    LeaderElectionCoordinator, LifecycleMetrics, NodeDrainState,
    NodeHealthRecord, NodeJoinState, NodeLiveness, NodeLifecycleCoordinator,
    OpsAuditEvent, OpsAuditLog, OpsCommand, OpsEventType,
    PressureLevel, PressureSignal, ProtocolVersion,
    ReplicaCatchUpCoordinator, ReplicaCatchUpState, ReplicaLagClass,
    RollingUpgradeCoordinator, ShardElection, SloSnapshot, SloTracker,
    UpgradeMetrics, UpgradeNodeRecord, UpgradeNodeState, UpgradeOrder,
};
pub use falcon_enterprise::control_plane::{
    CommandDispatcher, CommandResult, ConfigEntry, ConfigStore,
    ConsistentMetadataStore, ControlPlaneCommand, ControllerHAGroup,
    ControllerHAMetrics, ControllerNode, ControllerRole,
    DataNodeRecord, DataNodeState, MetadataCommand, MetadataDomain,
    MetadataOperation, MetadataStoreMetrics, MetadataWriteResult,
    NodeCapabilities, NodeRegistry, NodeRegistryMetrics,
    PlacementMetrics, ReadConsistency, ShardPlacement,
    ShardPlacementManager, ShardPlacementState,
};
pub use falcon_enterprise::enterprise_security::{
    AuditCategory, AuditSeverity, AuthnManager, AuthnMetrics,
    AuthnRequest, AuthnResult, BackupJob, BackupJobStatus,
    BackupOrchestrator, BackupOrchestratorMetrics, BackupTarget,
    CertMetrics, CertRotationEvent, CertificateManager,
    CertificateRecord, CredentialType, EnterpriseAuditEvent,
    EnterpriseAuditLog, EnterpriseAuditMetrics, EnterpriseBackupType,
    EnterprisePermission, EnterpriseRbac, RbacCheckResult,
    RbacGrant, RbacMetrics, RbacScope, RestoreJob, RestoreType,
    StoredCredential, TlsLinkType, UserRecord,
};
pub use falcon_enterprise::enterprise_ops::{
    AdminApiRouter, AdminEndpoint, AutoRebalancer, CapacityAlert,
    CapacityAlertLevel, CapacityMetrics, CapacityPlanner,
    ClusterOverviewResponse, ForecastResult, Incident,
    IncidentSeverity, IncidentTimeline, MigrationState,
    MigrationTask as EnterpriseMigrationTask, NodeDetailResponse,
    RebalanceConfig, RebalanceMetrics, RebalanceTrigger,
    ResourceSample, ResourceType, ShardDetailResponse,
    SloDefinition, SloEngine, SloEngineMetrics, SloEvaluation,
    SloMetricType, TimelineEvent, TimelineEventType, TimelineMetrics,
};
pub use ga_hardening::{
    BgIsolatorMetrics, BgTaskIsolator, BgTaskQuota, BgTaskType, BgTaskUsage,
    BreachType, ComponentState, ComponentType, ConfigRollbackManager,
    ConfigRollbackMetrics, CrashHardeningCoordinator, CrashHardeningMetrics,
    GuardedPath, GuardrailBreach, GuardrailMetrics, LatencyGuardrailEngine,
    LeakDetectionResult, LeakDetectorMetrics, LeakResourceType, LifecycleOrder,
    PathGuardrail, RecoveryAction, ResourceLeakDetector, ResourceSnapshot,
    RolloutState, ShutdownType, StartupRecord, ThrottleDecision,
    VersionedConfig,
};
pub use cost_capacity::{
    AdminConsoleV2, AdminV2Endpoint, CapacityGuardAlert, CapacityGuardMetrics,
    CapacityGuardSeverity, CapacityGuardV2, ChangeImpactMetrics,
    ChangeImpactPreview, ClusterCostSummary, CostTracker, CostTrackerMetrics,
    DecisionPoint, HardenedAuditLog, HardenedAuditMetrics, ImpactEstimate,
    ImpactRisk, MetricChange, PostmortemGenerator, PostmortemMetrics,
    PostmortemReport, PostmortemTimelineEntry, PressureType, ProposedChange,
    Recommendation, ShardCost, TableCost, UnifiedAuditEvent,
};
pub use raft_integration::{
    RaftCoordinatorMetrics, RaftFailoverWatcher, RaftFailoverWatcherHandle,
    RaftFailoverWatcherMetrics, RaftShardCoordinator, RaftStatRow, RaftWalGroup,
    RaftWalGroupMetrics, collect_raft_stats,
};
pub use raft_rebalance::{
    RebalanceTriggerPolicy, SmartRebalanceConfig, SmartRebalanceMetrics,
    SmartRebalanceMetricsSnapshot, SmartRebalanceRunner, SmartRebalanceRunnerHandle,
};
pub use two_phase::TwoPhaseCoordinator;
pub use dist_hardening::{
    AutoRestartConfig, AutoRestartMetrics, AutoRestartSupervisor, DebouncedHealth,
    FailoverPreFlight, HealthCheckHysteresis, HysteresisConfig, HysteresisMetrics,
    NodeHysteresisState, PreFlightConfig, PreFlightInput, PreFlightMetrics,
    PreFlightRejectReason, PromotionSafetyGuard, PromotionSafetyMetrics, PromotionStep,
    RestartableTaskState, SplitBrainDetector, SplitBrainEvent, SplitBrainMetrics,
    SplitBrainVerdict, WriteEpochCheck,
};
pub use client_discovery::{
    ClientConnectionManager, ClientRoutingMetrics, ClientRoutingTable,
    ConnectionManagerConfig, ConnectionManagerMetrics, ConnectionState,
    NodeDirectoryEntry, NotLeaderRedirector, ProviderMetrics, RedirectOutcome,
    RedirectorConfig, RedirectorMetrics, ShardRouteEntry, SubscriptionId,
    SubscriptionMetrics, TopologyChangeEvent, TopologyChangeType,
    TopologyProvider, TopologySnapshot, TopologySubscriptionManager,
};
pub use distributed_enhancements::{
    ClusterHealthStatus, ClusterStatusBuilder, ClusterStatusView, CommitPolicy,
    FailoverAuditEvent, FailoverRunbook, FailoverStage, FailoverVerificationReport,
    IdempotencyMetrics, InvariantCheckResult, InvariantResult, MemberState,
    MemberTransition, MembershipLifecycle, MigrationMetrics as ShardMigrationMetrics,
    ParticipantDecision, ParticipantIdempotencyRegistry, ReplicationInvariantGate,
    ReplicationInvariantMetrics, ShardMigrationCoordinator, ShardMigrationSummary,
    ShardMigrationPhase, ShardMigrationTask, TwoPcRecoveryCoordinator, TwoPcRecoveryMetrics,
    TwoPhasePhase,
};
