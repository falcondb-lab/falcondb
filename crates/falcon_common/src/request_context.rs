//! Per-request context for tracing and observability.
//!
//! Every SQL request carries a `RequestContext` that propagates through
//! all processing stages (parse → bind → plan → execute → commit).
//!
//! Fields:
//! - `request_id`: unique per TCP connection request (monotonic)
//! - `session_id`: unique per client session (connection lifetime)
//! - `query_id`: unique per SQL query within a session
//! - `txn_id`: transaction ID (0 for auto-commit)
//! - `shard_id`: target shard (0 for single-shard)
//!
//! Stage latency breakdown is recorded in `StageLatency`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Global monotonic request ID counter.
static GLOBAL_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
/// Global monotonic session ID counter.
static GLOBAL_SESSION_ID: AtomicU64 = AtomicU64::new(1);
/// Global monotonic query ID counter.
static GLOBAL_QUERY_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a new unique request ID.
pub fn next_request_id() -> u64 {
    GLOBAL_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

/// Allocate a new unique session ID.
pub fn next_session_id() -> u64 {
    GLOBAL_SESSION_ID.fetch_add(1, Ordering::Relaxed)
}

/// Allocate a new unique query ID.
pub fn next_query_id() -> u64 {
    GLOBAL_QUERY_ID.fetch_add(1, Ordering::Relaxed)
}

/// Per-request context propagated through all processing stages.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Unique per TCP connection request (monotonic).
    pub request_id: u64,
    /// Unique per client session (connection lifetime).
    pub session_id: u64,
    /// Unique per SQL query within a session.
    pub query_id: u64,
    /// Transaction ID (0 for auto-commit or not yet assigned).
    pub txn_id: u64,
    /// Target shard (0 for single-shard or not yet routed).
    pub shard_id: u64,
    /// When this request was received.
    pub started_at: Instant,
}

impl RequestContext {
    /// Create a new request context with fresh IDs.
    pub fn new(session_id: u64) -> Self {
        Self {
            request_id: next_request_id(),
            session_id,
            query_id: next_query_id(),
            txn_id: 0,
            shard_id: 0,
            started_at: Instant::now(),
        }
    }

    /// Create with explicit IDs (for testing).
    pub fn with_ids(request_id: u64, session_id: u64, query_id: u64) -> Self {
        Self {
            request_id,
            session_id,
            query_id,
            txn_id: 0,
            shard_id: 0,
            started_at: Instant::now(),
        }
    }

    /// Set the transaction ID.
    pub fn with_txn_id(mut self, txn_id: u64) -> Self {
        self.txn_id = txn_id;
        self
    }

    /// Set the shard ID.
    pub fn with_shard_id(mut self, shard_id: u64) -> Self {
        self.shard_id = shard_id;
        self
    }

    /// Elapsed time since request start in microseconds.
    pub fn elapsed_us(&self) -> u64 {
        self.started_at.elapsed().as_micros() as u64
    }

    /// Elapsed time since request start in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// Format as a structured context string for log/error messages.
    /// Example: `"request_id=42, session_id=1, query_id=7, txn_id=100, shard_id=0"`
    pub fn as_context_str(&self) -> String {
        format!(
            "request_id={}, session_id={}, query_id={}, txn_id={}, shard_id={}",
            self.request_id, self.session_id, self.query_id, self.txn_id, self.shard_id
        )
    }
}

/// Stage latency breakdown for a single query execution.
///
/// All values are in microseconds. 0 means the stage was not executed.
#[derive(Debug, Clone, Default)]
pub struct StageLatency {
    /// SQL text → AST parsing.
    pub parse_us: u64,
    /// AST → BoundStatement (name resolution, type checking).
    pub bind_us: u64,
    /// BoundStatement → PhysicalPlan.
    pub plan_us: u64,
    /// PhysicalPlan → ExecutionResult.
    pub execute_us: u64,
    /// Transaction commit (WAL flush + replication ack).
    pub commit_us: u64,
    /// WAL record serialization + append.
    pub wal_append_us: u64,
    /// WAL fdatasync.
    pub wal_flush_us: u64,
    /// Time waiting for replica ack (sync mode only).
    pub replication_ack_us: u64,
    /// Total end-to-end wall time.
    pub total_us: u64,
}

impl StageLatency {
    /// Record a stage duration from a start `Instant`.
    pub fn record_parse(&mut self, start: Instant) {
        self.parse_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_bind(&mut self, start: Instant) {
        self.bind_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_plan(&mut self, start: Instant) {
        self.plan_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_execute(&mut self, start: Instant) {
        self.execute_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_commit(&mut self, start: Instant) {
        self.commit_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_wal_append(&mut self, start: Instant) {
        self.wal_append_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_wal_flush(&mut self, start: Instant) {
        self.wal_flush_us = start.elapsed().as_micros() as u64;
    }

    pub fn record_replication_ack(&mut self, start: Instant) {
        self.replication_ack_us = start.elapsed().as_micros() as u64;
    }

    pub fn finalize(&mut self, request_start: Instant) {
        self.total_us = request_start.elapsed().as_micros() as u64;
    }

    /// Format as a structured breakdown string for slow query log.
    pub fn as_breakdown_str(&self) -> String {
        format!(
            "parse={}µs bind={}µs plan={}µs execute={}µs commit={}µs wal_append={}µs wal_flush={}µs repl_ack={}µs total={}µs",
            self.parse_us, self.bind_us, self.plan_us, self.execute_us,
            self.commit_us, self.wal_append_us, self.wal_flush_us,
            self.replication_ack_us, self.total_us
        )
    }

    /// Returns true if this query is "slow" (total > threshold_ms).
    pub fn is_slow(&self, threshold_ms: u64) -> bool {
        self.total_us > threshold_ms * 1000
    }
}

/// Recovery phase tracking for structured startup logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    LoadMetadata,
    ReplayWal,
    RebuildIndexes,
    OpenForReads,
    OpenForWrites,
    Complete,
}

impl std::fmt::Display for RecoveryPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryPhase::LoadMetadata => write!(f, "load_metadata"),
            RecoveryPhase::ReplayWal => write!(f, "replay_wal"),
            RecoveryPhase::RebuildIndexes => write!(f, "rebuild_indexes"),
            RecoveryPhase::OpenForReads => write!(f, "open_for_reads"),
            RecoveryPhase::OpenForWrites => write!(f, "open_for_writes"),
            RecoveryPhase::Complete => write!(f, "complete"),
        }
    }
}

/// Structured recovery progress tracker.
pub struct RecoveryTracker {
    started_at: Instant,
    phase: RecoveryPhase,
    phase_started_at: Instant,
}

impl RecoveryTracker {
    pub fn new() -> Self {
        let now = Instant::now();
        tracing::info!(phase = "load_metadata", step = "1/5", "recovery: starting");
        Self {
            started_at: now,
            phase: RecoveryPhase::LoadMetadata,
            phase_started_at: now,
        }
    }

    pub fn advance(&mut self, phase: RecoveryPhase) {
        let phase_elapsed_ms = self.phase_started_at.elapsed().as_millis() as u64;
        let total_elapsed_ms = self.started_at.elapsed().as_millis() as u64;

        let step = match phase {
            RecoveryPhase::LoadMetadata => "1/5",
            RecoveryPhase::ReplayWal => "2/5",
            RecoveryPhase::RebuildIndexes => "3/5",
            RecoveryPhase::OpenForReads => "4/5",
            RecoveryPhase::OpenForWrites => "5/5",
            RecoveryPhase::Complete => "done",
        };

        tracing::info!(
            phase = %phase,
            step = step,
            phase_elapsed_ms = phase_elapsed_ms,
            total_elapsed_ms = total_elapsed_ms,
            "recovery: phase complete, advancing"
        );

        self.phase = phase;
        self.phase_started_at = Instant::now();
    }

    pub fn complete(self) {
        let total_ms = self.started_at.elapsed().as_millis() as u64;
        tracing::info!(
            phase = "complete",
            total_elapsed_ms = total_ms,
            "recovery: complete"
        );
    }

    pub fn current_phase(&self) -> RecoveryPhase {
        self.phase
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }
}

impl Default for RecoveryTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_context_new() {
        let ctx = RequestContext::new(42);
        assert_eq!(ctx.session_id, 42);
        assert!(ctx.request_id > 0);
        assert!(ctx.query_id > 0);
        assert_eq!(ctx.txn_id, 0);
        assert_eq!(ctx.shard_id, 0);
    }

    #[test]
    fn test_request_ids_are_unique() {
        let ctx1 = RequestContext::new(1);
        let ctx2 = RequestContext::new(1);
        assert_ne!(ctx1.request_id, ctx2.request_id);
        assert_ne!(ctx1.query_id, ctx2.query_id);
    }

    #[test]
    fn test_with_txn_id() {
        let ctx = RequestContext::new(1).with_txn_id(99);
        assert_eq!(ctx.txn_id, 99);
    }

    #[test]
    fn test_with_shard_id() {
        let ctx = RequestContext::new(1).with_shard_id(3);
        assert_eq!(ctx.shard_id, 3);
    }

    #[test]
    fn test_elapsed_us_increases() {
        let ctx = RequestContext::new(1);
        let t0 = ctx.elapsed_us();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t1 = ctx.elapsed_us();
        assert!(t1 >= t0);
    }

    #[test]
    fn test_as_context_str_format() {
        let ctx = RequestContext::with_ids(1, 2, 3)
            .with_txn_id(4)
            .with_shard_id(5);
        let s = ctx.as_context_str();
        assert!(s.contains("request_id=1"));
        assert!(s.contains("session_id=2"));
        assert!(s.contains("query_id=3"));
        assert!(s.contains("txn_id=4"));
        assert!(s.contains("shard_id=5"));
    }

    #[test]
    fn test_stage_latency_default_zeros() {
        let lat = StageLatency::default();
        assert_eq!(lat.parse_us, 0);
        assert_eq!(lat.total_us, 0);
    }

    #[test]
    fn test_stage_latency_record() {
        let mut lat = StageLatency::default();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        lat.record_parse(start);
        assert!(lat.parse_us >= 1000, "parse_us should be >= 1ms");
    }

    #[test]
    fn test_stage_latency_is_slow() {
        let mut lat = StageLatency::default();
        lat.total_us = 200_000; // 200ms
        assert!(lat.is_slow(100));
        assert!(!lat.is_slow(300));
    }

    #[test]
    fn test_stage_latency_breakdown_str() {
        let mut lat = StageLatency::default();
        lat.parse_us = 100;
        lat.execute_us = 5000;
        lat.total_us = 6000;
        let s = lat.as_breakdown_str();
        assert!(s.contains("parse=100µs"));
        assert!(s.contains("execute=5000µs"));
        assert!(s.contains("total=6000µs"));
    }

    #[test]
    fn test_recovery_phase_display() {
        assert_eq!(RecoveryPhase::LoadMetadata.to_string(), "load_metadata");
        assert_eq!(RecoveryPhase::ReplayWal.to_string(), "replay_wal");
        assert_eq!(RecoveryPhase::Complete.to_string(), "complete");
    }

    #[test]
    fn test_recovery_tracker_phases() {
        let mut tracker = RecoveryTracker::new();
        assert_eq!(tracker.current_phase(), RecoveryPhase::LoadMetadata);
        tracker.advance(RecoveryPhase::ReplayWal);
        assert_eq!(tracker.current_phase(), RecoveryPhase::ReplayWal);
        tracker.advance(RecoveryPhase::RebuildIndexes);
        tracker.advance(RecoveryPhase::OpenForReads);
        tracker.advance(RecoveryPhase::OpenForWrites);
        tracker.complete();
    }

    #[test]
    fn test_next_ids_monotonic() {
        let r1 = next_request_id();
        let r2 = next_request_id();
        let r3 = next_request_id();
        assert!(r2 > r1);
        assert!(r3 > r2);
    }

    #[test]
    fn test_session_id_monotonic() {
        let s1 = next_session_id();
        let s2 = next_session_id();
        assert!(s2 > s1);
    }
}
