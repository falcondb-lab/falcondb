//! Deterministic Low-Latency Transaction Kernel — foundational types.
//!
//! Covers requirements §1–§9 of the DK specification:
//! - §1 Latency decomposition (per-phase timing)
//! - §2 Fast-path state machine observability
//! - §3 Latency explainability (waterfall breakdown)
//! - §4 Admission control & backpressure
//! - §5 Hotspot detection & mitigation
//! - §6 GC determinism policy
//! - §7 Durability semantics
//! - §8 Cross-shard cost model
//! - §9 Self-verification & audit

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Instant;

// ═══════════════════════════════════════════════════════════════════════════
// §1 / §3: Latency Decomposition
// ═══════════════════════════════════════════════════════════════════════════

/// Per-phase latency breakdown for a single transaction.
/// Every field is in **microseconds** (0 = phase not entered).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TxnLatencyBreakdown {
    /// SQL parse + bind time.
    pub parse_bind_us: u64,
    /// Shard routing / classification.
    pub routing_us: u64,
    /// Read/write execution time.
    pub execution_us: u64,
    /// OCC conflict / lock wait time.
    pub conflict_wait_us: u64,
    /// Read-set / write-set validation.
    pub validate_us: u64,
    /// Replication RTT (quorum ack).
    pub replication_us: u64,
    /// Durability: WAL fsync or quorum ack.
    pub durability_us: u64,
    /// Final commit bookkeeping.
    pub commit_finalize_us: u64,
    /// Queue / admission wait time.
    pub queue_wait_us: u64,
}

impl TxnLatencyBreakdown {
    /// Total end-to-end latency (sum of all phases).
    pub fn total_us(&self) -> u64 {
        self.parse_bind_us
            + self.routing_us
            + self.execution_us
            + self.conflict_wait_us
            + self.validate_us
            + self.replication_us
            + self.durability_us
            + self.commit_finalize_us
            + self.queue_wait_us
    }

    /// The dominant phase (largest contributor).
    pub fn dominant_phase(&self) -> &'static str {
        let phases: [(&str, u64); 9] = [
            ("parse_bind", self.parse_bind_us),
            ("routing", self.routing_us),
            ("execution", self.execution_us),
            ("conflict_wait", self.conflict_wait_us),
            ("validate", self.validate_us),
            ("replication", self.replication_us),
            ("durability", self.durability_us),
            ("commit_finalize", self.commit_finalize_us),
            ("queue_wait", self.queue_wait_us),
        ];
        phases.iter().max_by_key(|p| p.1).map(|p| p.0).unwrap_or("unknown")
    }

    /// Format as a compact waterfall string: `phase=Xus|phase=Yus|...`
    pub fn waterfall(&self) -> String {
        format!(
            "parse={}|route={}|exec={}|conflict={}|validate={}|repl={}|dur={}|commit={}|queue={}",
            self.parse_bind_us, self.routing_us, self.execution_us,
            self.conflict_wait_us, self.validate_us, self.replication_us,
            self.durability_us, self.commit_finalize_us, self.queue_wait_us,
        )
    }
}

/// In-flight latency timer — accumulates phase durations during transaction execution.
#[derive(Debug, Clone)]
pub struct LatencyTimer {
    current_phase: Option<(&'static str, Instant)>,
    pub breakdown: TxnLatencyBreakdown,
}

impl LatencyTimer {
    pub fn new() -> Self {
        Self {
            current_phase: None,
            breakdown: TxnLatencyBreakdown::default(),
        }
    }

    /// Start timing a phase. Automatically ends any in-flight phase.
    pub fn start_phase(&mut self, phase: &'static str) {
        self.end_phase();
        self.current_phase = Some((phase, Instant::now()));
    }

    /// End the current phase and accumulate its duration.
    pub fn end_phase(&mut self) {
        if let Some((name, start)) = self.current_phase.take() {
            let elapsed_us = start.elapsed().as_micros() as u64;
            match name {
                "parse_bind" => self.breakdown.parse_bind_us += elapsed_us,
                "routing" => self.breakdown.routing_us += elapsed_us,
                "execution" => self.breakdown.execution_us += elapsed_us,
                "conflict_wait" => self.breakdown.conflict_wait_us += elapsed_us,
                "validate" => self.breakdown.validate_us += elapsed_us,
                "replication" => self.breakdown.replication_us += elapsed_us,
                "durability" => self.breakdown.durability_us += elapsed_us,
                "commit_finalize" => self.breakdown.commit_finalize_us += elapsed_us,
                "queue_wait" => self.breakdown.queue_wait_us += elapsed_us,
                _ => {}
            }
        }
    }

    /// Finalize and return the breakdown.
    pub fn finalize(mut self) -> TxnLatencyBreakdown {
        self.end_phase();
        self.breakdown
    }
}

impl Default for LatencyTimer {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: Admission Control & Backpressure
// ═══════════════════════════════════════════════════════════════════════════

/// Decision from the admission controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionDecision {
    /// Admit immediately.
    Admit,
    /// Queue the request (with estimated wait in microseconds).
    Queue { estimated_wait_us: u64 },
    /// Degrade: admit but with reduced guarantees (e.g. async durability).
    Degrade,
    /// Reject: system is overloaded.
    Reject { reason: AdmissionRejectReason },
}

/// Reason for admission rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionRejectReason {
    QueueFull,
    MemoryPressure,
    WalBacklog,
    ReplicationLag,
    TenantQuotaExceeded,
    ShardOverloaded,
}

impl fmt::Display for AdmissionRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => write!(f, "queue_full"),
            Self::MemoryPressure => write!(f, "memory_pressure"),
            Self::WalBacklog => write!(f, "wal_backlog"),
            Self::ReplicationLag => write!(f, "replication_lag"),
            Self::TenantQuotaExceeded => write!(f, "tenant_quota_exceeded"),
            Self::ShardOverloaded => write!(f, "shard_overloaded"),
        }
    }
}

/// Inputs to the admission controller.
#[derive(Debug, Clone, Default)]
pub struct AdmissionInputs {
    /// Current pending queue length.
    pub queue_length: usize,
    /// Maximum queue length before rejection.
    pub max_queue_length: usize,
    /// Current memory usage ratio (0.0–1.0).
    pub memory_pressure: f64,
    /// Memory pressure threshold for rejection.
    pub memory_reject_threshold: f64,
    /// WAL backlog in bytes.
    pub wal_backlog_bytes: u64,
    /// WAL backlog threshold for rejection.
    pub wal_backlog_reject_bytes: u64,
    /// Replication lag in microseconds.
    pub replication_lag_us: u64,
    /// Replication lag threshold for rejection.
    pub replication_lag_reject_us: u64,
    /// Whether the requesting tenant has exceeded their quota.
    pub tenant_quota_exceeded: bool,
}

/// Stateless admission control evaluator.
pub struct AdmissionController;

impl AdmissionController {
    /// Evaluate whether a transaction should be admitted.
    pub fn evaluate(inputs: &AdmissionInputs) -> AdmissionDecision {
        // Hard rejections first (priority order)
        if inputs.tenant_quota_exceeded {
            return AdmissionDecision::Reject { reason: AdmissionRejectReason::TenantQuotaExceeded };
        }
        if inputs.max_queue_length > 0 && inputs.queue_length >= inputs.max_queue_length {
            return AdmissionDecision::Reject { reason: AdmissionRejectReason::QueueFull };
        }
        if inputs.memory_reject_threshold > 0.0 && inputs.memory_pressure >= inputs.memory_reject_threshold {
            return AdmissionDecision::Reject { reason: AdmissionRejectReason::MemoryPressure };
        }
        if inputs.wal_backlog_reject_bytes > 0 && inputs.wal_backlog_bytes >= inputs.wal_backlog_reject_bytes {
            return AdmissionDecision::Reject { reason: AdmissionRejectReason::WalBacklog };
        }
        if inputs.replication_lag_reject_us > 0 && inputs.replication_lag_us >= inputs.replication_lag_reject_us {
            return AdmissionDecision::Reject { reason: AdmissionRejectReason::ReplicationLag };
        }

        // Soft degradation: high memory or WAL pressure → async durability
        let mem_degrade = inputs.memory_reject_threshold > 0.0
            && inputs.memory_pressure >= inputs.memory_reject_threshold * 0.8;
        let wal_degrade = inputs.wal_backlog_reject_bytes > 0
            && inputs.wal_backlog_bytes >= (inputs.wal_backlog_reject_bytes as f64 * 0.8) as u64;
        if mem_degrade || wal_degrade {
            return AdmissionDecision::Degrade;
        }

        // Queue if approaching limits
        if inputs.max_queue_length > 0 && inputs.queue_length > inputs.max_queue_length / 2 {
            let pct = inputs.queue_length as f64 / inputs.max_queue_length as f64;
            let est_wait = (pct * 10_000.0) as u64; // rough estimate
            return AdmissionDecision::Queue { estimated_wait_us: est_wait };
        }

        AdmissionDecision::Admit
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: Hotspot Detection
// ═══════════════════════════════════════════════════════════════════════════

/// A detected hotspot with severity and mitigation suggestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotspotAlert {
    /// What kind of hotspot.
    pub kind: HotspotKind,
    /// Identifier (shard id, table name, or key range).
    pub target: String,
    /// Access count in the detection window.
    pub access_count: u64,
    /// Severity (0.0 = cold, 1.0 = extreme hot).
    pub severity: f64,
    /// Suggested mitigation.
    pub mitigation: HotspotMitigation,
}

/// Kind of hotspot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HotspotKind {
    Shard,
    Table,
    KeyRange,
}

impl fmt::Display for HotspotKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shard => write!(f, "shard"),
            Self::Table => write!(f, "table"),
            Self::KeyRange => write!(f, "key_range"),
        }
    }
}

/// Suggested mitigation for a hotspot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HotspotMitigation {
    /// Throttle writes to the hot target.
    WriteThrottle,
    /// Queue requests locally to smooth bursts.
    LocalQueue,
    /// Suggest retrying with exponential backoff + jitter.
    RetryWithBackoff,
    /// Suggest re-sharding or changing the shard key.
    ReshardAdvice,
    /// No action needed (informational).
    None,
}

impl fmt::Display for HotspotMitigation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteThrottle => write!(f, "write_throttle"),
            Self::LocalQueue => write!(f, "local_queue"),
            Self::RetryWithBackoff => write!(f, "retry_with_backoff"),
            Self::ReshardAdvice => write!(f, "reshard_advice"),
            Self::None => write!(f, "none"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6: GC Determinism Policy
// ═══════════════════════════════════════════════════════════════════════════

/// GC policy ensuring MVCC reclamation does not hurt fast-path p99.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcPolicy {
    /// Maximum versions per key before triggering throttle/reclaim.
    pub max_versions_per_key: u32,
    /// Maximum total version chains before memory-pressure GC kicks in.
    pub max_total_chains: u64,
    /// GC rate limit: max keys reclaimed per millisecond.
    pub reclaim_rate_per_ms: u32,
    /// Memory usage ratio threshold that triggers aggressive GC.
    pub memory_pressure_threshold: f64,
    /// Whether GC is allowed to run during fast-path execution windows.
    pub allow_during_fast_path: bool,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            max_versions_per_key: 64,
            max_total_chains: 10_000_000,
            reclaim_rate_per_ms: 1000,
            memory_pressure_threshold: 0.85,
            allow_during_fast_path: false,
        }
    }
}

/// Status of GC determinism enforcement.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcDeterminismStatus {
    /// Whether GC is currently rate-limited.
    pub rate_limited: bool,
    /// Whether GC is currently paused (fast-path window).
    pub paused_for_fast_path: bool,
    /// Current max version chain length observed.
    pub max_chain_length: u32,
    /// Total keys pending reclamation.
    pub pending_reclaim_keys: u64,
    /// Keys reclaimed in the last period.
    pub keys_reclaimed_last_period: u64,
    /// Estimated GC impact on p99 (microseconds added).
    pub estimated_p99_impact_us: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §7: Durability Semantics
// ═══════════════════════════════════════════════════════════════════════════

/// Durability level for a transaction's commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum DurabilityLevel {
    /// Quorum ack from replicas (strong durability, higher latency).
    QuorumAck,
    /// Local WAL fsync + quorum ack (strongest).
    LocalFsyncPlusQuorum,
    /// Local WAL fsync only (no replica ack).
    #[default]
    LocalFsyncOnly,
    /// Async durability — commit returns before any fsync/ack (lowest latency, weakest).
    AsyncDurability,
}


impl fmt::Display for DurabilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QuorumAck => write!(f, "quorum_ack"),
            Self::LocalFsyncPlusQuorum => write!(f, "local_fsync+quorum"),
            Self::LocalFsyncOnly => write!(f, "local_fsync"),
            Self::AsyncDurability => write!(f, "async"),
        }
    }
}

/// Per-transaction durability annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityAnnotation {
    /// Requested durability level.
    pub level: DurabilityLevel,
    /// Actual ack path taken (may differ under degradation).
    pub actual_path: DurabilityLevel,
    /// Whether the durability was degraded from requested.
    pub degraded: bool,
    /// Ack latency (microseconds).
    pub ack_latency_us: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §8: Cross-Shard Cost Model
// ═══════════════════════════════════════════════════════════════════════════

/// Cost estimate for a transaction before execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnCostEstimate {
    /// Number of shards involved.
    pub shard_count: u32,
    /// Estimated network RTT rounds.
    pub estimated_rtt_count: u32,
    /// Estimated total commit cost (microseconds).
    pub estimated_commit_cost_us: u64,
    /// Whether 2PC is required.
    pub requires_2pc: bool,
    /// Whether the estimate exceeds the budget.
    pub exceeds_budget: bool,
}

/// Transaction budget — maximum resource allocation for a single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnBudget {
    /// Maximum number of shards a transaction may touch.
    pub max_shards: u32,
    /// Maximum total latency budget (microseconds).
    pub max_latency_us: u64,
    /// Maximum number of network RTTs.
    pub max_rtts: u32,
}

impl Default for TxnBudget {
    fn default() -> Self {
        Self {
            max_shards: 8,
            max_latency_us: 100_000, // 100ms
            max_rtts: 4,
        }
    }
}

impl TxnBudget {
    /// Check if a cost estimate exceeds this budget.
    pub fn check(&self, estimate: &TxnCostEstimate) -> TxnBudgetCheck {
        let mut violations = Vec::new();
        if estimate.shard_count > self.max_shards {
            violations.push(format!(
                "shards: {} > max {}", estimate.shard_count, self.max_shards
            ));
        }
        if estimate.estimated_commit_cost_us > self.max_latency_us {
            violations.push(format!(
                "latency: {}us > max {}us", estimate.estimated_commit_cost_us, self.max_latency_us
            ));
        }
        if estimate.estimated_rtt_count > self.max_rtts {
            violations.push(format!(
                "rtts: {} > max {}", estimate.estimated_rtt_count, self.max_rtts
            ));
        }
        TxnBudgetCheck {
            within_budget: violations.is_empty(),
            violations,
        }
    }
}

/// Result of a budget check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnBudgetCheck {
    pub within_budget: bool,
    pub violations: Vec<String>,
}

// ═══════════════════════════════════════════════════════════════════════════
// §9: Self-Verification & Consistency Audit
// ═══════════════════════════════════════════════════════════════════════════

/// Type of consistency check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerificationType {
    /// State hash comparison across replicas.
    StateHash,
    /// WAL replay checksum verification.
    WalReplayChecksum,
    /// Range hash (partial data verification).
    RangeHash,
    /// Transaction replay (sampled re-execution).
    TxnReplay,
}

impl fmt::Display for VerificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StateHash => write!(f, "state_hash"),
            Self::WalReplayChecksum => write!(f, "wal_replay_checksum"),
            Self::RangeHash => write!(f, "range_hash"),
            Self::TxnReplay => write!(f, "txn_replay"),
        }
    }
}

/// Result of a single verification check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    pub check_type: VerificationType,
    /// Whether the check passed.
    pub passed: bool,
    /// Scope (e.g. shard id, table name, key range).
    pub scope: String,
    /// Computed checksum / hash.
    pub computed_hash: String,
    /// Expected checksum / hash (if comparing).
    pub expected_hash: Option<String>,
    /// Timestamp of the check (unix seconds).
    pub checked_at: u64,
    /// Error detail if the check failed.
    pub error: Option<String>,
}

/// Audit report aggregating multiple verification results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationAuditReport {
    /// Total checks performed.
    pub total_checks: u64,
    /// Checks that passed.
    pub passed: u64,
    /// Checks that failed.
    pub failed: u64,
    /// Coverage ratio (0.0–1.0): fraction of data verified.
    pub coverage_ratio: f64,
    /// Time of report generation (unix seconds).
    pub generated_at: u64,
    /// Individual results (most recent).
    pub results: Vec<VerificationResult>,
    /// Whether any anomalies were detected.
    pub anomalies_detected: bool,
}

impl VerificationAuditReport {
    /// Summary line for display.
    pub fn summary(&self) -> String {
        format!(
            "checks={} passed={} failed={} coverage={:.1}% anomalies={}",
            self.total_checks, self.passed, self.failed,
            self.coverage_ratio * 100.0, self.anomalies_detected,
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 continued: Latency Stability Contract
// ═══════════════════════════════════════════════════════════════════════════

/// A latency stability contract — defines the SLA promise for a workload class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyContract {
    /// Name of the contract (e.g. "single_shard_write").
    pub name: String,
    /// p99 target (microseconds).
    pub p99_target_us: u64,
    /// p999 target (microseconds).
    pub p999_target_us: u64,
    /// Maximum allowed jitter: (p99_observed - p99_target) / p99_target.
    /// E.g. 0.10 means 10% jitter allowed.
    pub max_jitter_ratio: f64,
    /// Evaluation window (seconds).
    pub window_secs: u64,
}

impl Default for LatencyContract {
    fn default() -> Self {
        Self {
            name: "default".into(),
            p99_target_us: 10_000,  // 10ms
            p999_target_us: 50_000, // 50ms
            max_jitter_ratio: 0.15, // 15%
            window_secs: 60,
        }
    }
}

/// Result of evaluating a latency contract against observed metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractEvaluation {
    /// Contract name.
    pub contract_name: String,
    /// Observed p99 (microseconds).
    pub observed_p99_us: u64,
    /// Observed p999 (microseconds).
    pub observed_p999_us: u64,
    /// Whether the p99 target was met.
    pub p99_met: bool,
    /// Whether the p999 target was met.
    pub p999_met: bool,
    /// Observed jitter ratio.
    pub jitter_ratio: f64,
    /// Whether jitter is within bounds.
    pub jitter_within_bounds: bool,
    /// Overall pass/fail.
    pub passed: bool,
    /// Attribution: which phase contributed most to violations.
    pub dominant_violation_phase: Option<String>,
}

impl LatencyContract {
    /// Evaluate the contract against observed latencies.
    pub fn evaluate(&self, observed_p99_us: u64, observed_p999_us: u64) -> ContractEvaluation {
        let p99_met = observed_p99_us <= self.p99_target_us;
        let p999_met = observed_p999_us <= self.p999_target_us;
        let jitter = if self.p99_target_us > 0 && observed_p99_us > self.p99_target_us {
            (observed_p99_us - self.p99_target_us) as f64 / self.p99_target_us as f64
        } else {
            0.0
        };
        let jitter_ok = jitter <= self.max_jitter_ratio;
        ContractEvaluation {
            contract_name: self.name.clone(),
            observed_p99_us,
            observed_p999_us,
            p99_met,
            p999_met,
            jitter_ratio: jitter,
            jitter_within_bounds: jitter_ok,
            passed: p99_met && p999_met && jitter_ok,
            dominant_violation_phase: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── §1/§3: Latency Decomposition ──

    #[test]
    fn test_breakdown_total() {
        let b = TxnLatencyBreakdown {
            parse_bind_us: 100,
            routing_us: 50,
            execution_us: 500,
            conflict_wait_us: 0,
            validate_us: 80,
            replication_us: 200,
            durability_us: 300,
            commit_finalize_us: 20,
            queue_wait_us: 10,
        };
        assert_eq!(b.total_us(), 1260);
    }

    #[test]
    fn test_breakdown_dominant_phase() {
        let b = TxnLatencyBreakdown {
            execution_us: 1000,
            durability_us: 200,
            ..Default::default()
        };
        assert_eq!(b.dominant_phase(), "execution");
    }

    #[test]
    fn test_breakdown_waterfall_format() {
        let b = TxnLatencyBreakdown {
            parse_bind_us: 10,
            routing_us: 5,
            ..Default::default()
        };
        let w = b.waterfall();
        assert!(w.contains("parse=10"));
        assert!(w.contains("route=5"));
    }

    #[test]
    fn test_latency_timer_phases() {
        let mut timer = LatencyTimer::new();
        timer.start_phase("execution");
        // Simulate some work
        std::thread::sleep(std::time::Duration::from_micros(100));
        timer.end_phase();
        let breakdown = timer.finalize();
        assert!(breakdown.execution_us > 0);
        assert_eq!(breakdown.routing_us, 0);
    }

    #[test]
    fn test_latency_timer_auto_end_previous() {
        let mut timer = LatencyTimer::new();
        timer.start_phase("routing");
        std::thread::sleep(std::time::Duration::from_micros(50));
        timer.start_phase("execution"); // should auto-end routing
        std::thread::sleep(std::time::Duration::from_micros(50));
        let breakdown = timer.finalize();
        assert!(breakdown.routing_us > 0);
        assert!(breakdown.execution_us > 0);
    }

    // ── §4: Admission Control ──

    #[test]
    fn test_admission_admit() {
        let inputs = AdmissionInputs {
            queue_length: 0,
            max_queue_length: 100,
            memory_pressure: 0.3,
            memory_reject_threshold: 0.95,
            ..Default::default()
        };
        assert_eq!(AdmissionController::evaluate(&inputs), AdmissionDecision::Admit);
    }

    #[test]
    fn test_admission_reject_queue_full() {
        let inputs = AdmissionInputs {
            queue_length: 100,
            max_queue_length: 100,
            ..Default::default()
        };
        assert_eq!(
            AdmissionController::evaluate(&inputs),
            AdmissionDecision::Reject { reason: AdmissionRejectReason::QueueFull }
        );
    }

    #[test]
    fn test_admission_reject_memory() {
        let inputs = AdmissionInputs {
            memory_pressure: 0.96,
            memory_reject_threshold: 0.95,
            max_queue_length: 100,
            ..Default::default()
        };
        assert_eq!(
            AdmissionController::evaluate(&inputs),
            AdmissionDecision::Reject { reason: AdmissionRejectReason::MemoryPressure }
        );
    }

    #[test]
    fn test_admission_reject_wal_backlog() {
        let inputs = AdmissionInputs {
            wal_backlog_bytes: 500_000_000,
            wal_backlog_reject_bytes: 100_000_000,
            max_queue_length: 100,
            ..Default::default()
        };
        assert_eq!(
            AdmissionController::evaluate(&inputs),
            AdmissionDecision::Reject { reason: AdmissionRejectReason::WalBacklog }
        );
    }

    #[test]
    fn test_admission_reject_replication_lag() {
        let inputs = AdmissionInputs {
            replication_lag_us: 10_000_000,
            replication_lag_reject_us: 5_000_000,
            max_queue_length: 100,
            ..Default::default()
        };
        assert_eq!(
            AdmissionController::evaluate(&inputs),
            AdmissionDecision::Reject { reason: AdmissionRejectReason::ReplicationLag }
        );
    }

    #[test]
    fn test_admission_reject_tenant_quota() {
        let inputs = AdmissionInputs {
            tenant_quota_exceeded: true,
            ..Default::default()
        };
        assert_eq!(
            AdmissionController::evaluate(&inputs),
            AdmissionDecision::Reject { reason: AdmissionRejectReason::TenantQuotaExceeded }
        );
    }

    #[test]
    fn test_admission_degrade_high_memory() {
        let inputs = AdmissionInputs {
            memory_pressure: 0.80,
            memory_reject_threshold: 0.95,
            max_queue_length: 100,
            ..Default::default()
        };
        assert_eq!(AdmissionController::evaluate(&inputs), AdmissionDecision::Degrade);
    }

    #[test]
    fn test_admission_queue_approaching_limit() {
        let inputs = AdmissionInputs {
            queue_length: 60,
            max_queue_length: 100,
            ..Default::default()
        };
        match AdmissionController::evaluate(&inputs) {
            AdmissionDecision::Queue { estimated_wait_us } => assert!(estimated_wait_us > 0),
            other => panic!("Expected Queue, got {:?}", other),
        }
    }

    // ── §7: Durability ──

    #[test]
    fn test_durability_level_display() {
        assert_eq!(DurabilityLevel::QuorumAck.to_string(), "quorum_ack");
        assert_eq!(DurabilityLevel::AsyncDurability.to_string(), "async");
        assert_eq!(DurabilityLevel::LocalFsyncPlusQuorum.to_string(), "local_fsync+quorum");
    }

    // ── §8: Cost Model & Budget ──

    #[test]
    fn test_budget_within() {
        let budget = TxnBudget::default();
        let estimate = TxnCostEstimate {
            shard_count: 2,
            estimated_rtt_count: 2,
            estimated_commit_cost_us: 5000,
            requires_2pc: true,
            exceeds_budget: false,
        };
        let check = budget.check(&estimate);
        assert!(check.within_budget);
        assert!(check.violations.is_empty());
    }

    #[test]
    fn test_budget_exceeded_shards() {
        let budget = TxnBudget { max_shards: 4, ..Default::default() };
        let estimate = TxnCostEstimate {
            shard_count: 10,
            estimated_rtt_count: 2,
            estimated_commit_cost_us: 5000,
            requires_2pc: true,
            exceeds_budget: false,
        };
        let check = budget.check(&estimate);
        assert!(!check.within_budget);
        assert!(check.violations[0].contains("shards"));
    }

    #[test]
    fn test_budget_exceeded_latency() {
        let budget = TxnBudget { max_latency_us: 10_000, ..Default::default() };
        let estimate = TxnCostEstimate {
            shard_count: 2,
            estimated_rtt_count: 2,
            estimated_commit_cost_us: 50_000,
            requires_2pc: true,
            exceeds_budget: false,
        };
        let check = budget.check(&estimate);
        assert!(!check.within_budget);
        assert!(check.violations[0].contains("latency"));
    }

    // ── §9: Self-Verification ──

    #[test]
    fn test_verification_report_summary() {
        let report = VerificationAuditReport {
            total_checks: 100,
            passed: 98,
            failed: 2,
            coverage_ratio: 0.75,
            generated_at: 1700000000,
            results: vec![],
            anomalies_detected: true,
        };
        let s = report.summary();
        assert!(s.contains("checks=100"));
        assert!(s.contains("failed=2"));
        assert!(s.contains("anomalies=true"));
    }

    #[test]
    fn test_verification_type_display() {
        assert_eq!(VerificationType::StateHash.to_string(), "state_hash");
        assert_eq!(VerificationType::WalReplayChecksum.to_string(), "wal_replay_checksum");
    }

    // ── §9: Latency Contract ──

    #[test]
    fn test_contract_pass() {
        let contract = LatencyContract {
            name: "single_shard_write".into(),
            p99_target_us: 10_000,
            p999_target_us: 50_000,
            max_jitter_ratio: 0.15,
            window_secs: 60,
        };
        let eval = contract.evaluate(8_000, 40_000);
        assert!(eval.passed);
        assert!(eval.p99_met);
        assert!(eval.p999_met);
        assert!(eval.jitter_within_bounds);
    }

    #[test]
    fn test_contract_fail_p99() {
        let contract = LatencyContract {
            name: "test".into(),
            p99_target_us: 10_000,
            p999_target_us: 50_000,
            max_jitter_ratio: 0.15,
            window_secs: 60,
        };
        let eval = contract.evaluate(15_000, 40_000);
        assert!(!eval.passed);
        assert!(!eval.p99_met);
        assert!(eval.p999_met);
    }

    #[test]
    fn test_contract_fail_jitter() {
        let contract = LatencyContract {
            name: "strict".into(),
            p99_target_us: 10_000,
            p999_target_us: 50_000,
            max_jitter_ratio: 0.05, // 5% — very strict
            window_secs: 60,
        };
        // 10800 exceeds target by 8% → p99 not met AND jitter 8% > 5%
        let eval = contract.evaluate(10_800, 40_000);
        assert!(!eval.passed);
        assert!(!eval.p99_met);
        assert!(!eval.jitter_within_bounds);
        assert!((eval.jitter_ratio - 0.08).abs() < 0.01);
    }

    #[test]
    fn test_contract_within_target_no_jitter() {
        let contract = LatencyContract {
            name: "relaxed".into(),
            p99_target_us: 10_000,
            p999_target_us: 50_000,
            max_jitter_ratio: 0.20,
            window_secs: 60,
        };
        // Under target → jitter = 0, everything passes
        let eval = contract.evaluate(9_500, 45_000);
        assert!(eval.passed);
        assert!(eval.p99_met);
        assert_eq!(eval.jitter_ratio, 0.0);
    }

    // ── §5: Hotspot ──

    #[test]
    fn test_hotspot_alert_display() {
        assert_eq!(HotspotKind::Shard.to_string(), "shard");
        assert_eq!(HotspotMitigation::WriteThrottle.to_string(), "write_throttle");
        assert_eq!(HotspotMitigation::ReshardAdvice.to_string(), "reshard_advice");
    }

    // ── §6: GC Policy ──

    #[test]
    fn test_gc_policy_defaults() {
        let policy = GcPolicy::default();
        assert_eq!(policy.max_versions_per_key, 64);
        assert!(!policy.allow_during_fast_path);
        assert!((policy.memory_pressure_threshold - 0.85).abs() < 0.01);
    }
}
