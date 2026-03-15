//! Cluster metadata service — shard map, node directory, DDL coordination,
//! and gRPC-based shard routing for distributed query execution.
//!
//! MVP: single-node, single-shard. All data lives on one node.
//! P1: hash-based sharding with shard routing via gRPC (tonic).

// ── Core production modules ──────────────────────────────────────────────
pub mod admission;
pub mod bg_supervisor;
pub mod cluster;
pub mod cluster_ops;
pub mod cross_shard;
pub mod determinism_hardening;
pub mod deterministic_2pc;
pub mod distributed_exec;
pub mod failover_txn_hardening;
pub mod gateway;
pub mod grpc_transport;
pub mod ha;
pub mod indoubt_resolver;
pub mod query_engine;
pub mod raft_integration;
pub mod raft_rebalance;
pub mod rebalancer;
pub mod replication;
pub mod routing;
pub mod sharded_engine;
pub mod sharding;
pub mod stability_hardening;
pub mod two_phase;

// ── CockroachDB gap-fill modules (C1-C8) ────────────────────────────────
/// C3: Centralized Timestamp Oracle — global monotonic timestamp source.
pub mod tso;
/// C1: Follower Read — route stale reads to replicas via closed_ts.
pub mod follower_read;
/// C4: Parallel 2PC — concurrent prepare/commit across shards.
pub mod parallel_2pc;
/// C2: Range/Tablet automatic split — hot-spot driven shard splitting.
pub mod range_split;
/// C5: Distributed SSI — cross-shard rw-antidependency detection.
pub mod distributed_ssi;
/// C6: Read Restart — uncertainty-interval retry protocol.
pub mod read_restart;
/// C8: Node Liveness — epoch-fenced write admission to prevent split-brain.
pub mod node_liveness;

// ── Hardening & safety modules ──────────────────────────────────────────
pub mod circuit_breaker;
pub mod dist_hardening;
pub mod fault_injection;
pub mod security_hardening;
pub mod sla_admission;
pub mod token_bucket;

// ── AI operations module ────────────────────────────────────────────────
pub mod ai_ops;

// ── Operational & enterprise modules ────────────────────────────────────
pub mod client_discovery;
pub mod cluster_lifecycle;
pub mod cost_capacity;
pub mod distributed_enhancements;
pub mod ga_hardening;
pub mod segment_streaming;
pub mod self_healing;
pub mod smart_gateway;
pub use falcon_enterprise::control_plane;
pub use falcon_enterprise::enterprise_ops;
pub use falcon_enterprise::enterprise_security;

/// Protobuf types and tonic client/server for WAL replication.
/// Re-exported from the `falcon_proto` crate (generated at build time).
pub use falcon_proto::falcon_replication as proto;

#[cfg(test)]
mod failover_txn_tests;
#[cfg(test)]
mod stability_stress_tests;
#[cfg(test)]
mod tests;

pub use ai_ops::{
    AiOpsConfig, AiOpsEngine, AiOpsMetrics, AiOpsSnapshot, AlertKind, AlertSeverity,
    AnomalyDetectorConfig, Diagnosis, ExponentialSmoother, ForecastResult as AiForecastResult,
    HoltSmoother, MetricSnapshot as AiMetricSnapshot, OpsAdvisor, OpsAlert, OpsRecommendation,
    RecommendationPriority, RootCause, RootCauseAnalyzer, WorkloadForecaster,
};
pub use bg_supervisor::{
    BgTaskCriticality, BgTaskInfo, BgTaskSnapshot, BgTaskState, BgTaskSupervisor, NodeHealth,
};
pub use client_discovery::{
    ClientConnectionManager, ClientRoutingMetrics, ClientRoutingTable, ConnectionManagerConfig,
    ConnectionManagerMetrics, ConnectionState, NodeDirectoryEntry, NotLeaderRedirector,
    ProviderMetrics, RedirectOutcome, RedirectorConfig, RedirectorMetrics, ShardRouteEntry,
    SubscriptionEvictorHandle, SubscriptionId, SubscriptionMetrics, TopologyChangeEvent,
    TopologyChangeType, TopologyProvider, TopologySnapshot, TopologySubscriptionManager,
};
pub use cluster::{
    MemberInfo, MemberSummary, MembershipConfig, MembershipError, MembershipEvent,
    MembershipEventType, MembershipManager, MembershipMetrics, MembershipView, NodeInfo,
    NodeMetrics, NodeRole, NodeState, NodeStatus,
};
pub use cluster_lifecycle::{
    ClusterLifecycleConfig, ClusterLifecycleCoordinator, LifecycleCoordinatorMetrics,
};
pub use cluster_ops::{
    ClusterAdmin, ClusterEvent, ClusterEventLog, EventCategory, EventSeverity, NodeModeController,
    NodeOperationalMode, ScaleInLifecycle, ScaleInState, ScaleOutLifecycle, ScaleOutState,
};
pub use cost_capacity::{
    AdminConsoleV2, AdminV2Endpoint, CapacityGuardAlert, CapacityGuardMetrics,
    CapacityGuardSeverity, CapacityGuardV2, ChangeImpactMetrics, ChangeImpactPreview,
    ClusterCostSummary, CostTracker, CostTrackerMetrics, DecisionPoint, HardenedAuditLog,
    HardenedAuditMetrics, ImpactEstimate, ImpactRisk, MetricChange, PostmortemGenerator,
    PostmortemMetrics, PostmortemReport, PostmortemTimelineEntry, PressureType, ProposedChange,
    Recommendation, ShardCost, TableCost, UnifiedAuditEvent,
};
pub use determinism_hardening::{
    AbortReason, DeterministicRejectPolicy, FailoverCrashRecord, IdempotentReplayValidator,
    QueueDepthGuard, QueueDepthSnapshot, QueueSlot, RejectReason, ResourceExhaustionContract,
    RetryPolicy, TxnTerminalState,
};
pub use deterministic_2pc::{
    CoordinatorDecision, CoordinatorDecisionLog, DecisionLogConfig, DecisionLogSnapshot,
    DecisionRecord, LayeredTimeoutConfig, LayeredTimeoutController, LayeredTimeoutSnapshot,
    SlowShardAction, SlowShardConfig, SlowShardEvent, SlowShardPolicy, SlowShardSnapshot,
    SlowShardTracker, TimeoutResult,
};
pub use dist_hardening::{
    AutoRestartConfig, AutoRestartMetrics, AutoRestartSupervisor, DebouncedHealth,
    FailoverPreFlight, HealthCheckHysteresis, HysteresisConfig, HysteresisMetrics,
    NodeHysteresisState, PreFlightConfig, PreFlightInput, PreFlightMetrics, PreFlightRejectReason,
    PromotionSafetyGuard, PromotionSafetyMetrics, PromotionStep, RestartableTaskState,
    SplitBrainDetector, SplitBrainEvent, SplitBrainMetrics, SplitBrainVerdict, WriteEpochCheck,
};
pub use distributed_enhancements::{
    ClusterHealthStatus, ClusterStatusBuilder, ClusterStatusView, CommitPolicy, FailoverAuditEvent,
    FailoverRunbook, FailoverStage, FailoverVerificationReport, IdempotencyMetrics,
    InvariantCheckResult, InvariantResult, MemberState, MemberTransition, MembershipLifecycle,
    MigrationMetrics as ShardMigrationMetrics, ParticipantDecision, ParticipantIdempotencyRegistry,
    ReplicationInvariantGate, ReplicationInvariantMetrics, ShardMigrationCoordinator,
    ShardMigrationPhase, ShardMigrationSummary, ShardMigrationTask, TwoPcRecoveryCoordinator,
    TwoPcRecoveryMetrics, TwoPhasePhase,
};
pub use distributed_exec::{
    AggMerge, DistributedExecutor, FailurePolicy, GatherLimits, GatherStrategy,
    ScatterGatherMetrics, StreamingMergeSort, SubPlan,
};
pub use failover_txn_hardening::{
    FailoverBlockedTxnConfig, FailoverBlockedTxnGuard, FailoverBlockedTxnMetrics, FailoverDamper,
    FailoverDamperConfig, FailoverDamperMetrics, FailoverEvent, FailoverTxnConfig,
    FailoverTxnCoordinator, FailoverTxnMetrics, FailoverTxnPhase, FailoverTxnResolution,
    InDoubtTtlConfig, InDoubtTtlEnforcer, InDoubtTtlMetrics,
};
pub use falcon_enterprise::control_plane::{
    CommandDispatcher, CommandResult, ConfigEntry, ConfigStore, ConsistentMetadataStore,
    ControlPlaneCommand, ControllerHAGroup, ControllerHAMetrics, ControllerNode, ControllerRole,
    DataNodeRecord, DataNodeState, MetadataCommand, MetadataDomain, MetadataOperation,
    MetadataStoreMetrics, MetadataWriteResult, NodeCapabilities, NodeRegistry, NodeRegistryMetrics,
    PlacementMetrics, ReadConsistency, ShardPlacement, ShardPlacementManager, ShardPlacementState,
};
pub use falcon_enterprise::enterprise_ops::{
    AdminApiRouter, AdminEndpoint, AutoRebalancer, CapacityAlert, CapacityAlertLevel,
    CapacityMetrics, CapacityPlanner, ClusterOverviewResponse, ForecastResult, Incident,
    IncidentSeverity, IncidentTimeline, MigrationState, MigrationTask as EnterpriseMigrationTask,
    NodeDetailResponse, RebalanceConfig, RebalanceMetrics, RebalanceTrigger, ResourceSample,
    ResourceType, ShardDetailResponse, SloDefinition, SloEngine, SloEngineMetrics, SloEvaluation,
    SloMetricType, TimelineEvent, TimelineEventType, TimelineMetrics,
};
pub use falcon_enterprise::enterprise_security::{
    AuditCategory, AuditSeverity, AuthnManager, AuthnMetrics, AuthnRequest, AuthnResult, BackupJob,
    BackupJobStatus, BackupOrchestrator, BackupOrchestratorMetrics, BackupTarget, CertMetrics,
    CertRotationEvent, CertificateManager, CertificateRecord, CredentialType, EnterpriseAuditEvent,
    EnterpriseAuditLog, EnterpriseAuditMetrics, EnterpriseBackupType, EnterprisePermission,
    EnterpriseRbac, RbacCheckResult, RbacGrant, RbacMetrics, RbacScope, RestoreJob, RestoreType,
    StoredCredential, TlsLinkType, UserRecord,
};
pub use ga_hardening::{
    BgIsolatorMetrics, BgTaskIsolator, BgTaskQuota, BgTaskType, BgTaskUsage, BreachType,
    ComponentState, ComponentType, ConfigRollbackManager, ConfigRollbackMetrics,
    CrashHardeningCoordinator, CrashHardeningMetrics, GuardedPath, GuardrailBreach,
    GuardrailMetrics, LatencyGuardrailEngine, LeakDetectionResult, LeakDetectorMetrics,
    LeakResourceType, LifecycleOrder, PathGuardrail, RecoveryAction, ResourceLeakDetector,
    ResourceSnapshot, RolloutState, ShutdownType, StartupRecord, ThrottleDecision, VersionedConfig,
};
pub use ha::{
    FailoverOrchestrator, FailoverOrchestratorConfig, FailoverOrchestratorHandle,
    FailoverOrchestratorMetrics, FailureDetector, HAConfig, HAReplicaGroup, HAReplicaStatus,
    HAStatus, PrimaryHealth, ReplicaHealth, ReplicaHealthStatus, SyncMode, SyncReplicationWaiter,
};
pub use query_engine::{
    DistributedQueryEngine, GatewayAdmissionConfig, GatewayAdmissionControl, GatewayDisposition,
    GatewayMetrics, GatewayMetricsSnapshot,
};
pub use raft_integration::{
    collect_raft_stats, RaftCoordinatorMetrics, RaftFailoverWatcher, RaftFailoverWatcherHandle,
    RaftFailoverWatcherMetrics, RaftShardCoordinator, RaftStatRow, RaftWalGroup,
    RaftWalGroupMetrics,
};
pub use raft_rebalance::{
    RebalanceTriggerPolicy, SmartRebalanceConfig, SmartRebalanceMetrics,
    SmartRebalanceMetricsSnapshot, SmartRebalanceRunner, SmartRebalanceRunnerHandle,
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
pub use self_healing::{
    BackpressureConfig, BackpressureController, BackpressureMetrics, CatchUpConfig, CatchUpMetrics,
    CatchUpPhase, ClusterFailureDetector, ClusterHealthLevel, ClusterHealthResponse,
    ClusterStatusResponse, DrainPhase, ElectionConfig, ElectionError, ElectionMetrics,
    ElectionState, FailureDetectorConfig, FailureDetectorHandle, FailureDetectorMetrics, JoinPhase,
    LeaderElectionCoordinator, LifecycleMetrics, NodeDrainState, NodeHealthRecord, NodeJoinState,
    NodeLifecycleCoordinator, NodeLiveness, OpsAuditEvent, OpsAuditLog, OpsCommand, OpsEventType,
    PressureLevel, PressureSignal, ProtocolVersion, ReplicaCatchUpCoordinator, ReplicaCatchUpState,
    ReplicaLagClass, RollingUpgradeCoordinator, ShardElection, SloSnapshot, SloTracker,
    UpgradeMetrics, UpgradeNodeRecord, UpgradeNodeState, UpgradeOrder,
};
pub use sharded_engine::ShardedEngine;
pub use sharding::{
    all_shards_for_table, cmp_datum_for_range, compute_shard_hash, compute_shard_hash_from_datums,
    shards_for_range_query, target_shard_for_row, target_shard_from_datums,
};
pub use sla_admission::{
    classify_txn, AdmissionDecision, LatencyPercentiles, LatencyTracker, RejectionExplanation,
    RejectionSignal, SlaAdmissionController, SlaAdmissionMetrics, SlaConfig, SlaPermit, TxnClass,
    TxnClassificationHints,
};
pub use smart_gateway::{
    ClusterTopology, CompressionProfile, GatewayError, GatewayErrorCode, GatewayRole, HostPort,
    JdbcConnectionUrl, RequestClassification, RouteDecision, SeedGatewayList, SmartGateway,
    SmartGatewayConfig, SmartGatewayMetrics, SmartGatewayMetricsSnapshot, TopologyCache,
    TopologyCacheMetrics, TopologyCacheMetricsSnapshot, TopologyEntry, WalMode,
};

// ── C1-C8 CockroachDB gap-fill exports ──────────────────────────────────────
pub use tso::{TsoBatch, TsoClient, TsoClientMetrics, TsoServer, TsoServerMetrics, TsoStatsSnapshot};
pub use follower_read::{
    FollowerReadConfig, FollowerReadMetrics, FollowerReadMetricsSnapshot, FollowerReadRouter,
    FollowerReadReject, ReadRoute, ReplicaEntry, ReplicaRegistry,
};
pub use parallel_2pc::{
    Parallel2PcCoordinator, Parallel2PcMetrics, Parallel2PcResult, ParallelShardResult,
};
pub use range_split::{
    QpsCounter, ShardHotspotMetrics, ShardSplitter, SplitConfig, SplitDecision, SplitMetrics,
    SplitMetricsSnapshot, SplitMonitor, SplitPolicy, SplitResult, SplitTrigger,
};
pub use distributed_ssi::{
    DistPredicate, DistSsiMetrics, DistSsiMetricsSnapshot, DistWriteIntent,
    DistributedSsiRegistry,
};
pub use read_restart::{
    ReadAttemptOutcome, ReadRestartConfig, ReadRestartError, ReadRestartExhausted,
    ReadRestartGuard, ReadRestartMetrics, ReadRestartMetricsSnapshot, UncertaintyChecker,
};
pub use node_liveness::{
    LivenessConfig, LivenessRecord, LivenessStore, NodeLivenessManager, WriteEpochGate,
    WriteEpochMetrics, WriteEpochMetricsSnapshot, WriteRejectionReason,
};
pub use stability_hardening::{
    CommitPhase, CommitPhaseMetrics, CommitPhaseTracker, DefensiveValidator,
    DefensiveValidatorMetrics, ErrorClassStabilizer, EscalationOutcome, EscalationRecord,
    FailoverExpectedOutcome, FailoverOutcomeGuard, FailoverOutcomeGuardMetrics, InDoubtEscalator,
    InDoubtEscalatorMetrics, InDoubtReason, ProtocolPhase, ResolutionMethod, RetryGuard,
    RetryGuardMetrics, StateOrdinal, TxnOutcomeEntry, TxnOutcomeJournal, TxnStateGuard,
    TxnStateGuardMetrics,
};
pub use token_bucket::{TokenBucket, TokenBucketConfig, TokenBucketError, TokenBucketSnapshot};
pub use two_phase::TwoPhaseCoordinator;
