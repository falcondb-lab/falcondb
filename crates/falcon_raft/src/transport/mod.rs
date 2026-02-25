//! Transport abstraction for Raft inter-node RPC.
//!
//! Provides:
//! - `RaftTransport` trait: async RPC send + snapshot streaming
//! - `TransportError`: typed error model with retry policy mapping
//! - `RetryPolicy` + `BackoffConfig`: exponential backoff with jitter
//! - `PeerCircuitBreaker`: per-peer circuit breaker to avoid overload
//! - `RaftTransportConfig`: timeouts, limits, backoff parameters
//!
//! Implementations:
//! - `grpc::GrpcTransport` — production gRPC transport
//! - `fault::FaultyTransport` — fault-injection wrapper for testing

pub mod fault;
pub mod grpc;

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Transport Error Model
// ═══════════════════════════════════════════════════════════════════════════

/// Transport-level errors. Each variant maps to a retry policy.
#[derive(Debug, Clone)]
pub enum TransportError {
    /// Peer is unreachable (DNS failure, connection refused, etc.).
    Unreachable(String),
    /// RPC timed out (connect or request).
    Timeout(String),
    /// Connection was reset mid-RPC.
    ConnectionReset(String),
    /// Remote peer explicitly rejected the RPC (e.g. wrong term, not leader).
    RemoteRejected(String),
    /// Payload exceeds configured max size.
    PayloadTooLarge { size: usize, max: usize },
    /// Operation not supported by this transport.
    Unsupported(String),
    /// Internal transport error (serialization, etc.).
    Internal(String),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable(s) => write!(f, "unreachable: {s}"),
            Self::Timeout(s) => write!(f, "timeout: {s}"),
            Self::ConnectionReset(s) => write!(f, "connection_reset: {s}"),
            Self::RemoteRejected(s) => write!(f, "remote_rejected: {s}"),
            Self::PayloadTooLarge { size, max } => {
                write!(f, "payload_too_large: {size} > {max}")
            }
            Self::Unsupported(s) => write!(f, "unsupported: {s}"),
            Self::Internal(s) => write!(f, "internal: {s}"),
        }
    }
}

impl std::error::Error for TransportError {}

impl TransportError {
    /// Whether this error is retryable.
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Unreachable(_) | Self::Timeout(_) | Self::ConnectionReset(_)
        )
    }

    /// Suggested retry policy for this error.
    pub const fn retry_policy(&self) -> RetryPolicy {
        match self {
            Self::Timeout(_)
            | Self::ConnectionReset(_)
            | Self::Unreachable(_) => RetryPolicy::ExponentialBackoff,
            Self::RemoteRejected(_)
            | Self::PayloadTooLarge { .. }
            | Self::Unsupported(_)
            | Self::Internal(_) => RetryPolicy::NoRetry,
        }
    }

    /// Metric label for this error category.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Unreachable(_) => "unreachable",
            Self::Timeout(_) => "timeout",
            Self::ConnectionReset(_) => "connection_reset",
            Self::RemoteRejected(_) => "remote_rejected",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::Unsupported(_) => "unsupported",
            Self::Internal(_) => "internal",
        }
    }
}

/// Retry policy for transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Do not retry.
    NoRetry,
    /// Retry with exponential backoff.
    ExponentialBackoff,
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Transport Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the Raft transport layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftTransportConfig {
    /// TCP connect timeout.
    pub connect_timeout_ms: u64,
    /// Per-RPC request timeout.
    pub request_timeout_ms: u64,
    /// Snapshot stream per-chunk timeout.
    pub snapshot_chunk_timeout_ms: u64,
    /// Maximum snapshot chunk size (bytes).
    pub max_snapshot_chunk_bytes: usize,
    /// Maximum total snapshot size (bytes). 0 = unlimited.
    pub max_snapshot_total_bytes: u64,
    /// Maximum RPC payload size (bytes).
    pub max_rpc_payload_bytes: usize,
    /// Backoff configuration for retries.
    pub backoff: BackoffConfig,
    /// Circuit breaker configuration.
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for RaftTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 2_000,
            request_timeout_ms: 5_000,
            snapshot_chunk_timeout_ms: 10_000,
            max_snapshot_chunk_bytes: 1024 * 1024, // 1 MB
            max_snapshot_total_bytes: 0,            // unlimited
            max_rpc_payload_bytes: 64 * 1024 * 1024, // 64 MB
            backoff: BackoffConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

impl RaftTransportConfig {
    pub const fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
    pub const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
    pub const fn snapshot_chunk_timeout(&self) -> Duration {
        Duration::from_millis(self.snapshot_chunk_timeout_ms)
    }
}

/// Backoff configuration for retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    /// Initial backoff duration (ms).
    pub initial_ms: u64,
    /// Maximum backoff duration (ms).
    pub max_ms: u64,
    /// Backoff multiplier.
    pub multiplier: f64,
    /// Maximum number of retries (0 = no retries).
    pub max_retries: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_ms: 50,
            max_ms: 5_000,
            multiplier: 2.0,
            max_retries: 3,
        }
    }
}

impl BackoffConfig {
    /// Compute the backoff duration for the given attempt (0-indexed).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base = self.initial_ms as f64 * self.multiplier.powi(attempt as i32);
        let clamped = base.min(self.max_ms as f64);
        Duration::from_millis(clamped as u64)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Circuit Breaker (per-peer)
// ═══════════════════════════════════════════════════════════════════════════

/// Circuit breaker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures to open the circuit.
    pub failure_threshold: u32,
    /// Number of consecutive successes in half-open to close the circuit.
    pub success_threshold: u32,
    /// Duration the circuit stays open before transitioning to half-open.
    pub open_duration_ms: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            open_duration_ms: 10_000,
        }
    }
}

/// Per-peer circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Per-peer circuit breaker.
pub struct PeerCircuitBreaker {
    config: CircuitBreakerConfig,
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    opened_at: Option<std::time::Instant>,
    total_trips: u64,
}

impl PeerCircuitBreaker {
    pub const fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            opened_at: None,
            total_trips: 0,
        }
    }

    /// Check if the circuit allows a request.
    pub fn allow_request(&mut self) -> bool {
        match self.state {
            CircuitState::Closed | CircuitState::HalfOpen => true,
            CircuitState::Open => {
                if let Some(opened) = self.opened_at {
                    if opened.elapsed() >= Duration::from_millis(self.config.open_duration_ms) {
                        self.state = CircuitState::HalfOpen;
                        self.consecutive_successes = 0;
                        true // allow probe
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful RPC.
    pub const fn record_success(&mut self) {
        self.consecutive_failures = 0;
        match self.state {
            CircuitState::HalfOpen => {
                self.consecutive_successes += 1;
                if self.consecutive_successes >= self.config.success_threshold {
                    self.state = CircuitState::Closed;
                    self.opened_at = None;
                }
            }
            _ => {
                self.state = CircuitState::Closed;
            }
        }
    }

    /// Record a failed RPC.
    pub fn record_failure(&mut self) {
        self.consecutive_successes = 0;
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.config.failure_threshold
            && self.state != CircuitState::Open {
                self.state = CircuitState::Open;
                self.opened_at = Some(std::time::Instant::now());
                self.total_trips += 1;
            }
    }

    pub const fn state(&self) -> CircuitState {
        self.state
    }

    pub const fn total_trips(&self) -> u64 {
        self.total_trips
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Transport Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Cumulative transport metrics.
#[derive(Debug, Default)]
pub struct TransportMetrics {
    pub rpc_success_total: std::sync::atomic::AtomicU64,
    pub rpc_failure_total: std::sync::atomic::AtomicU64,
    pub rpc_timeout_total: std::sync::atomic::AtomicU64,
    pub rpc_unreachable_total: std::sync::atomic::AtomicU64,
    pub snapshot_bytes_sent: std::sync::atomic::AtomicU64,
    pub snapshot_bytes_recv: std::sync::atomic::AtomicU64,
    pub snapshot_success_total: std::sync::atomic::AtomicU64,
    pub snapshot_failure_total: std::sync::atomic::AtomicU64,
}

impl TransportMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self) {
        self.rpc_success_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_failure(&self, err: &TransportError) {
        self.rpc_failure_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match err {
            TransportError::Timeout(_) => {
                self.rpc_timeout_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            TransportError::Unreachable(_) => {
                self.rpc_unreachable_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn record_snapshot_bytes_sent(&self, bytes: u64) {
        self.snapshot_bytes_sent
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_snapshot_bytes_recv(&self, bytes: u64) {
        self.snapshot_bytes_recv
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Snapshot Metadata
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot metadata for streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub snapshot_id: String,
    pub last_included_index: u64,
    pub last_included_term: u64,
    /// Serialized StoredMembership.
    pub membership_data: Vec<u8>,
    /// Total snapshot size in bytes (known after build).
    pub total_bytes: u64,
    /// CRC32 of the full snapshot data.
    pub checksum: u32,
}

/// A single chunk of a snapshot stream.
#[derive(Debug, Clone)]
pub struct SnapshotChunkData {
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
    pub chunk_index: u32,
    pub chunk_checksum: u32,
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_error_retryable() {
        assert!(TransportError::Timeout("t".into()).is_retryable());
        assert!(TransportError::Unreachable("u".into()).is_retryable());
        assert!(TransportError::ConnectionReset("c".into()).is_retryable());
        assert!(!TransportError::RemoteRejected("r".into()).is_retryable());
        assert!(!TransportError::PayloadTooLarge { size: 10, max: 5 }.is_retryable());
        assert!(!TransportError::Unsupported("u".into()).is_retryable());
        assert!(!TransportError::Internal("i".into()).is_retryable());
    }

    #[test]
    fn test_transport_error_retry_policy() {
        assert_eq!(
            TransportError::Timeout("t".into()).retry_policy(),
            RetryPolicy::ExponentialBackoff
        );
        assert_eq!(
            TransportError::RemoteRejected("r".into()).retry_policy(),
            RetryPolicy::NoRetry
        );
    }

    #[test]
    fn test_transport_error_display() {
        let e = TransportError::Unreachable("node 3".into());
        assert_eq!(format!("{}", e), "unreachable: node 3");
    }

    #[test]
    fn test_backoff_delay() {
        let cfg = BackoffConfig::default();
        let d0 = cfg.delay_for_attempt(0);
        assert_eq!(d0, Duration::from_millis(50));
        let d1 = cfg.delay_for_attempt(1);
        assert_eq!(d1, Duration::from_millis(100));
        let d2 = cfg.delay_for_attempt(2);
        assert_eq!(d2, Duration::from_millis(200));
        // Should be capped at max
        let d10 = cfg.delay_for_attempt(10);
        assert_eq!(d10, Duration::from_millis(5000));
    }

    #[test]
    fn test_circuit_breaker_closed() {
        let mut cb = PeerCircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let mut cb = PeerCircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 1,
            open_duration_ms: 10_000,
        });
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request()); // circuit is open
        assert_eq!(cb.total_trips(), 1);
    }

    #[test]
    fn test_circuit_breaker_half_open_probe() {
        let mut cb = PeerCircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 1,
            open_duration_ms: 0, // immediate transition to half-open
        });
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        // With open_duration_ms=0, should transition to half-open immediately
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        // Success in half-open → close
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_resets_on_success() {
        let mut cb = PeerCircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets consecutive failures
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed); // still closed (only 1 consecutive)
    }

    #[test]
    fn test_config_defaults() {
        let cfg = RaftTransportConfig::default();
        assert_eq!(cfg.connect_timeout_ms, 2_000);
        assert_eq!(cfg.request_timeout_ms, 5_000);
        assert_eq!(cfg.max_snapshot_chunk_bytes, 1024 * 1024);
    }

    #[test]
    fn test_transport_metrics() {
        let m = TransportMetrics::new();
        m.record_success();
        m.record_success();
        m.record_failure(&TransportError::Timeout("t".into()));
        m.record_failure(&TransportError::Unreachable("u".into()));
        assert_eq!(
            m.rpc_success_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            m.rpc_failure_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            m.rpc_timeout_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.rpc_unreachable_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_snapshot_metadata() {
        let meta = SnapshotMetadata {
            snapshot_id: "snap-1".into(),
            last_included_index: 100,
            last_included_term: 5,
            membership_data: vec![1, 2, 3],
            total_bytes: 4096,
            checksum: 0xDEADBEEF,
        };
        assert_eq!(meta.snapshot_id, "snap-1");
        assert_eq!(meta.total_bytes, 4096);
    }
}
