//! Policy-Driven Automatic Shard Rebalance Integration.
//!
//! Extends the existing `RebalanceRunner` (in `rebalancer.rs`) with richer
//! trigger conditions, exponential backoff, and detailed metrics.
//!
//! Uses `SmartRebalanceRunner` / `SmartRebalanceRunnerHandle` / `SmartRebalanceConfig`
//! to avoid name collisions with the existing types in `rebalancer.rs`.
//!
//! # Architecture
//!
//! ```text
//!   SmartRebalanceRunner (background std::thread)
//!       │
//!       ├── poll every check_interval
//!       │       │
//!       │       ▼
//!       │   ShardLoadSnapshot::collect(engine)
//!       │       │
//!       │       ▼
//!       │   RebalanceTriggerPolicy::should_trigger()
//!       │       │  yes
//!       │       ▼
//!       │   ShardRebalancer::check_and_rebalance(engine)
//!       │
//!       └── SmartRebalanceMetricsSnapshot for SHOW falcon.*
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use falcon_common::error::FalconError;
use falcon_common::shutdown::ShutdownSignal;

use crate::rebalancer::{RebalancerConfig, ShardLoadSnapshot, ShardRebalancer};
use crate::sharded_engine::ShardedEngine;

// ---------------------------------------------------------------------------
// RebalanceTriggerPolicy
// ---------------------------------------------------------------------------

/// Policy that guards *when* automatic rebalancing fires.
///
/// More powerful than the raw `imbalance_threshold` in `RebalancerConfig`:
/// also requires a minimum row count and minimum shard count before any
/// migration is attempted.
#[derive(Debug, Clone)]
pub struct RebalanceTriggerPolicy {
    /// Imbalance ratio above which rebalancing is triggered.
    /// E.g. 1.25 = trigger when max_shard_rows > 1.25 × avg_shard_rows.
    pub imbalance_threshold: f64,
    /// Minimum total row count before rebalancing is allowed.
    /// Prevents thrashing on tiny or empty clusters.
    pub min_rows_to_trigger: u64,
    /// Minimum number of shards required before rebalancing can occur.
    pub min_shards: usize,
}

impl Default for RebalanceTriggerPolicy {
    fn default() -> Self {
        Self {
            imbalance_threshold: 1.25,
            min_rows_to_trigger: 1_000,
            min_shards: 2,
        }
    }
}

impl RebalanceTriggerPolicy {
    /// Returns `true` if the given load snapshot warrants a rebalance run.
    pub fn should_trigger(&self, snapshot: &ShardLoadSnapshot) -> bool {
        if snapshot.loads.len() < self.min_shards {
            return false;
        }
        if snapshot.total_rows() < self.min_rows_to_trigger {
            return false;
        }
        snapshot.imbalance_ratio() > self.imbalance_threshold
    }
}

// ---------------------------------------------------------------------------
// SmartRebalanceConfig
// ---------------------------------------------------------------------------

/// Configuration for `SmartRebalanceRunner`.
#[derive(Debug, Clone)]
pub struct SmartRebalanceConfig {
    /// How often to evaluate shard loads.
    pub check_interval: Duration,
    /// Underlying rebalancer config (batch size, cooldown, etc.).
    pub rebalancer: RebalancerConfig,
    /// Trigger policy.
    pub policy: RebalanceTriggerPolicy,
    /// Initial backoff duration after a failed/empty rebalance run.
    pub initial_backoff: Duration,
    /// Maximum backoff (exponential cap).
    pub max_backoff: Duration,
    /// Whether the runner starts in paused state (requires manual `resume()`).
    pub start_paused: bool,
}

impl Default for SmartRebalanceConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            rebalancer: RebalancerConfig::default(),
            policy: RebalanceTriggerPolicy::default(),
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(120),
            start_paused: false,
        }
    }
}

// ---------------------------------------------------------------------------
// SmartRebalanceMetrics / Snapshot
// ---------------------------------------------------------------------------

/// Live atomic counters for `SmartRebalanceRunner`.
pub struct SmartRebalanceMetrics {
    pub checks_run: AtomicU64,
    pub rebalances_triggered: AtomicU64,
    pub rebalances_succeeded: AtomicU64,
    pub rebalances_failed: AtomicU64,
    pub total_rows_migrated: AtomicU64,
    pub consecutive_failures: AtomicU64,
}

impl Default for SmartRebalanceMetrics {
    fn default() -> Self {
        Self {
            checks_run: AtomicU64::new(0),
            rebalances_triggered: AtomicU64::new(0),
            rebalances_succeeded: AtomicU64::new(0),
            rebalances_failed: AtomicU64::new(0),
            total_rows_migrated: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
        }
    }
}

/// Point-in-time snapshot for observability (`SHOW falcon.rebalance_stats`).
#[derive(Debug, Clone)]
pub struct SmartRebalanceMetricsSnapshot {
    pub checks_run: u64,
    pub rebalances_triggered: u64,
    pub rebalances_succeeded: u64,
    pub rebalances_failed: u64,
    pub total_rows_migrated: u64,
    pub consecutive_failures: u64,
    pub is_paused: bool,
    pub is_active: bool,
}

// ---------------------------------------------------------------------------
// SmartRebalanceRunner
// ---------------------------------------------------------------------------

/// Policy-driven background shard rebalancer with backoff and pause support.
///
/// Start with [`SmartRebalanceRunner::start`]; stop by dropping or calling
/// `stop_and_join()` on the returned [`SmartRebalanceRunnerHandle`].
pub struct SmartRebalanceRunner {
    config: SmartRebalanceConfig,
    rebalancer: Arc<ShardRebalancer>,
    pub metrics: Arc<SmartRebalanceMetrics>,
    paused: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
}

impl SmartRebalanceRunner {
    /// Create a new runner (not yet started).
    pub fn new(config: SmartRebalanceConfig) -> Arc<Self> {
        let paused = Arc::new(AtomicBool::new(config.start_paused));
        Arc::new(Self {
            rebalancer: Arc::new(ShardRebalancer::new(config.rebalancer.clone())),
            metrics: Arc::new(SmartRebalanceMetrics::default()),
            paused,
            active: Arc::new(AtomicBool::new(false)),
            config,
        })
    }

    /// Pause: future checks are skipped; in-progress migration completes.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        tracing::info!("SmartRebalanceRunner: paused");
    }

    /// Resume after a pause.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        tracing::info!("SmartRebalanceRunner: resumed");
    }

    /// Current metrics snapshot.
    pub fn metrics_snapshot(&self) -> SmartRebalanceMetricsSnapshot {
        let m = &*self.metrics;
        SmartRebalanceMetricsSnapshot {
            checks_run: m.checks_run.load(Ordering::Relaxed),
            rebalances_triggered: m.rebalances_triggered.load(Ordering::Relaxed),
            rebalances_succeeded: m.rebalances_succeeded.load(Ordering::Relaxed),
            rebalances_failed: m.rebalances_failed.load(Ordering::Relaxed),
            total_rows_migrated: m.total_rows_migrated.load(Ordering::Relaxed),
            consecutive_failures: m.consecutive_failures.load(Ordering::Relaxed),
            is_paused: self.paused.load(Ordering::Relaxed),
            is_active: self.active.load(Ordering::Relaxed),
        }
    }

    /// Start the background rebalance thread.
    ///
    /// Returns `Err` if the OS fails to spawn the thread (resource exhaustion).
    /// The caller must handle this as a degraded condition — no panic.
    pub fn start(
        self: Arc<Self>,
        engine: Arc<ShardedEngine>,
    ) -> Result<SmartRebalanceRunnerHandle, FalconError> {
        let signal = ShutdownSignal::new();
        let signal_clone = signal.clone();

        let config = self.config.clone();
        let rebalancer = self.rebalancer.clone();
        let metrics = self.metrics.clone();
        let paused = self.paused.clone();
        let active = self.active.clone();

        let join_handle = std::thread::Builder::new()
            .name("falcon-smart-rebalancer".to_owned())
            .spawn(move || {
                tracing::info!(
                    check_interval_ms = config.check_interval.as_millis(),
                    imbalance_threshold = config.policy.imbalance_threshold,
                    min_rows = config.policy.min_rows_to_trigger,
                    "SmartRebalanceRunner started"
                );

                let mut backoff = config.initial_backoff;

                while !signal_clone.is_shutdown() {
                    if signal_clone.wait_timeout(config.check_interval) {
                        break;
                    }

                    if paused.load(Ordering::Relaxed) {
                        tracing::debug!("SmartRebalanceRunner: paused, skipping");
                        continue;
                    }

                    metrics.checks_run.fetch_add(1, Ordering::Relaxed);

                    let snapshot = ShardLoadSnapshot::collect(&engine);

                    if !config.policy.should_trigger(&snapshot) {
                        tracing::debug!(
                            imbalance_ratio = snapshot.imbalance_ratio(),
                            total_rows = snapshot.total_rows(),
                            "SmartRebalanceRunner: balanced, no action"
                        );
                        backoff = config.initial_backoff;
                        metrics.consecutive_failures.store(0, Ordering::Relaxed);
                        continue;
                    }

                    metrics.rebalances_triggered.fetch_add(1, Ordering::Relaxed);
                    active.store(true, Ordering::SeqCst);

                    tracing::info!(
                        imbalance_ratio = snapshot.imbalance_ratio(),
                        total_rows = snapshot.total_rows(),
                        "SmartRebalanceRunner: triggering rebalance"
                    );

                    let rows_migrated = rebalancer.check_and_rebalance(&engine);

                    active.store(false, Ordering::SeqCst);

                    if rows_migrated > 0 {
                        metrics.rebalances_succeeded.fetch_add(1, Ordering::Relaxed);
                        metrics.total_rows_migrated.fetch_add(rows_migrated, Ordering::Relaxed);
                        metrics.consecutive_failures.store(0, Ordering::Relaxed);
                        backoff = config.initial_backoff;
                        tracing::info!(rows_migrated, "SmartRebalanceRunner: succeeded");
                    } else {
                        metrics.rebalances_failed.fetch_add(1, Ordering::Relaxed);
                        let fails =
                            metrics.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                        tracing::warn!(
                            consecutive_failures = fails,
                            backoff_ms = backoff.as_millis(),
                            "SmartRebalanceRunner: no migrations, backing off"
                        );
                        if signal_clone.wait_timeout(backoff) {
                            break;
                        }
                        backoff = (backoff * 2).min(config.max_backoff);
                    }
                }

                tracing::info!("SmartRebalanceRunner stopped");
            })
            .map_err(|e| {
                FalconError::Internal(format!(
                    "failed to spawn smart-rebalancer thread: {e}"
                ))
            })?;

        Ok(SmartRebalanceRunnerHandle {
            signal,
            join_handle: Mutex::new(Some(join_handle)),
        })
    }

    /// Run exactly one check synchronously (useful for testing / manual trigger).
    pub fn check_once(&self, engine: &ShardedEngine) -> u64 {
        self.metrics.checks_run.fetch_add(1, Ordering::Relaxed);
        let snapshot = ShardLoadSnapshot::collect(engine);
        if !self.config.policy.should_trigger(&snapshot) {
            return 0;
        }
        self.metrics.rebalances_triggered.fetch_add(1, Ordering::Relaxed);
        let rows = self.rebalancer.check_and_rebalance(engine);
        if rows > 0 {
            self.metrics.rebalances_succeeded.fetch_add(1, Ordering::Relaxed);
            self.metrics.total_rows_migrated.fetch_add(rows, Ordering::Relaxed);
        } else {
            self.metrics.rebalances_failed.fetch_add(1, Ordering::Relaxed);
        }
        rows
    }

    /// Direct access to the underlying `ShardRebalancer` for diagnostics.
    pub fn inner_rebalancer(&self) -> &ShardRebalancer {
        &self.rebalancer
    }
}

// ---------------------------------------------------------------------------
// SmartRebalanceRunnerHandle
// ---------------------------------------------------------------------------

/// Stop handle for a running `SmartRebalanceRunner` background thread.
pub struct SmartRebalanceRunnerHandle {
    signal: ShutdownSignal,
    join_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SmartRebalanceRunnerHandle {
    /// Signal the runner to stop (non-blocking).
    pub fn stop(&self) {
        self.signal.shutdown();
    }

    /// Signal the runner to stop and wait for it to finish.
    pub fn stop_and_join(&self) {
        self.signal.shutdown();
        if let Some(h) = self.join_handle.lock().take() {
            let _ = h.join();
        }
    }

    /// Whether the background thread is still alive.
    pub fn is_running(&self) -> bool {
        !self.signal.is_shutdown()
    }
}

impl Drop for SmartRebalanceRunnerHandle {
    fn drop(&mut self) {
        self.signal.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::*;

    fn two_shard_engine() -> Arc<ShardedEngine> {
        let schema = TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            ..Default::default()
        };
        let engine = Arc::new(ShardedEngine::new(2));
        engine.create_table_all(&schema).unwrap();
        engine
    }

    #[test]
    fn test_policy_rejects_single_shard() {
        let policy = RebalanceTriggerPolicy { min_shards: 2, ..Default::default() };
        let engine = Arc::new(ShardedEngine::new(1));
        let schema = TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            ..Default::default()
        };
        engine.create_table_all(&schema).unwrap();
        let snap = ShardLoadSnapshot::collect(&engine);
        assert!(!policy.should_trigger(&snap));
    }

    #[test]
    fn test_policy_rejects_below_min_rows() {
        let policy = RebalanceTriggerPolicy {
            min_shards: 2,
            min_rows_to_trigger: 10_000,
            imbalance_threshold: 1.0,
        };
        let engine = two_shard_engine(); // empty
        let snap = ShardLoadSnapshot::collect(&engine);
        assert!(!policy.should_trigger(&snap));
    }

    #[test]
    fn test_policy_rejects_balanced_cluster() {
        let policy = RebalanceTriggerPolicy {
            min_shards: 2,
            min_rows_to_trigger: 0,
            imbalance_threshold: 2.0, // very high threshold
        };
        let engine = two_shard_engine();
        let snap = ShardLoadSnapshot::collect(&engine);
        assert!(!policy.should_trigger(&snap));
    }

    #[test]
    fn test_check_once_no_trigger_on_empty() {
        let config = SmartRebalanceConfig {
            policy: RebalanceTriggerPolicy {
                min_rows_to_trigger: 1_000,
                ..Default::default()
            },
            ..Default::default()
        };
        let runner = SmartRebalanceRunner::new(config);
        let engine = two_shard_engine();
        let migrated = runner.check_once(&engine);
        assert_eq!(migrated, 0);
        let snap = runner.metrics_snapshot();
        assert_eq!(snap.checks_run, 1);
        assert_eq!(snap.rebalances_triggered, 0);
    }

    #[test]
    fn test_pause_resume() {
        let runner = SmartRebalanceRunner::new(SmartRebalanceConfig::default());
        assert!(!runner.paused.load(Ordering::Relaxed));
        runner.pause();
        assert!(runner.paused.load(Ordering::Relaxed));
        runner.resume();
        assert!(!runner.paused.load(Ordering::Relaxed));
    }

    #[test]
    fn test_start_paused() {
        let config = SmartRebalanceConfig { start_paused: true, ..Default::default() };
        let runner = SmartRebalanceRunner::new(config);
        assert!(runner.paused.load(Ordering::Relaxed));
        let snap = runner.metrics_snapshot();
        assert!(snap.is_paused);
    }

    #[test]
    fn test_metrics_defaults() {
        let runner = SmartRebalanceRunner::new(SmartRebalanceConfig::default());
        let snap = runner.metrics_snapshot();
        assert_eq!(snap.checks_run, 0);
        assert_eq!(snap.rebalances_triggered, 0);
        assert_eq!(snap.rebalances_succeeded, 0);
        assert_eq!(snap.rebalances_failed, 0);
        assert_eq!(snap.total_rows_migrated, 0);
        assert!(!snap.is_paused);
        assert!(!snap.is_active);
    }

    #[test]
    fn test_start_and_stop() {
        let config = SmartRebalanceConfig {
            check_interval: Duration::from_secs(3600), // never fires
            ..Default::default()
        };
        let runner = SmartRebalanceRunner::new(config);
        let engine = two_shard_engine();
        let handle = runner.start(engine).unwrap();
        assert!(handle.is_running());
        handle.stop_and_join();
        assert!(!handle.is_running());
    }

    #[test]
    fn test_check_once_triggers_when_skewed() {
        use falcon_common::datum::{Datum, OwnedRow};
        use falcon_storage::memtable::encode_pk_from_datums;

        let engine = two_shard_engine();

        // Insert 200 rows into shard 0 only
        let shard0 = engine.shard(ShardId(0)).unwrap();
        for i in 0i32..200 {
            let row = OwnedRow::new(vec![Datum::Int32(i)]);
            let pk = encode_pk_from_datums(&[&Datum::Int32(i)]);
            shard0.storage.insert_row(TableId(1), pk, row).unwrap();
        }

        let snap = ShardLoadSnapshot::collect(&engine);
        assert!(snap.imbalance_ratio() > 1.25, "setup: cluster should be skewed");

        let config = SmartRebalanceConfig {
            policy: RebalanceTriggerPolicy {
                imbalance_threshold: 1.25,
                min_rows_to_trigger: 10,
                min_shards: 2,
            },
            rebalancer: crate::rebalancer::RebalancerConfig {
                imbalance_threshold: 1.25,
                min_donor_rows: 10,
                batch_size: 50,
                cooldown_ms: 0,
            },
            ..Default::default()
        };
        let runner = SmartRebalanceRunner::new(config);
        // check_once should trigger a rebalance
        let _ = runner.check_once(&engine);
        let m = runner.metrics_snapshot();
        assert_eq!(m.rebalances_triggered, 1, "should have triggered one rebalance");
    }
}
