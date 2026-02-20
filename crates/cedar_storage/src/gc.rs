//! MVCC Garbage Collection.
//!
//! Design principles:
//! - **No global lock**: GC operates per-key with fine-grained version chain locks.
//! - **No stop-the-world**: GC runs concurrently with reads and writes.
//! - **WAL-aware**: never reclaims uncommitted versions (commit_ts == 0).
//! - **Replication-safe**: safepoint respects replica applied LSN (via timestamp mapping).
//! - **Correctness invariant**: a version is only reclaimed if no active or future
//!   transaction can ever read it.
//!
//! Safepoint computation:
//!   gc_safepoint = min(min_active_ts, replica_safe_ts) - 1
//!
//! Any committed version with commit_ts <= gc_safepoint that is NOT the newest
//! such version for its key can be safely reclaimed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cedar_common::types::Timestamp;

/// GC configuration.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Whether GC is enabled.
    pub enabled: bool,
    /// Interval between GC sweeps (milliseconds).
    pub interval_ms: u64,
    /// Maximum number of keys to process per sweep (0 = unlimited).
    pub batch_size: usize,
    /// Minimum number of versions in a chain before GC considers it (avoids
    /// overhead on short chains). 0 = always process.
    pub min_chain_length: usize,
    /// Maximum allowed version chain length per key.
    /// When a chain exceeds this length, GC will aggressively prune it even
    /// if the safepoint would not normally allow it (best-effort, never removes
    /// uncommitted or still-visible versions).
    /// 0 = no cap (default).
    pub max_chain_length: usize,
    /// Minimum wall-clock time between consecutive GC sweeps (milliseconds).
    /// Acts as a rate-limiter: even under memory pressure, GC will not sweep
    /// more frequently than this. 0 = no rate limit (default).
    pub min_sweep_interval_ms: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: 1000,       // 1 second
            batch_size: 0,           // unlimited
            min_chain_length: 2,     // only GC chains with >= 2 versions
            max_chain_length: 0,     // no cap
            min_sweep_interval_ms: 0, // no rate limit
        }
    }
}

/// Result of a single GC sweep across all tables.
#[derive(Debug, Clone, Default)]
pub struct GcSweepResult {
    /// Number of version chains inspected.
    pub chains_inspected: u64,
    /// Number of version chains that had versions reclaimed.
    pub chains_pruned: u64,
    /// Total number of old versions reclaimed.
    pub reclaimed_versions: u64,
    /// Estimated bytes reclaimed.
    pub reclaimed_bytes: u64,
    /// The safepoint timestamp used for this sweep.
    pub safepoint_ts: Timestamp,
    /// Wall-clock duration of the sweep (microseconds).
    pub sweep_duration_us: u64,
    /// Number of keys skipped (batch limit or min_chain_length).
    pub keys_skipped: u64,
}

/// Cumulative GC statistics (atomic, lock-free).
#[derive(Debug)]
pub struct GcStats {
    pub total_sweeps: AtomicU64,
    pub total_reclaimed_versions: AtomicU64,
    pub total_reclaimed_bytes: AtomicU64,
    pub total_chains_inspected: AtomicU64,
    pub total_chains_pruned: AtomicU64,
    pub last_safepoint_ts: AtomicU64,
    pub last_sweep_duration_us: AtomicU64,
    /// Max version chain length observed across all sweeps.
    pub max_chain_length_observed: AtomicU64,
    /// Sum of all chain lengths observed (for computing average).
    pub sum_chain_lengths: AtomicU64,
    pub sum_chain_count: AtomicU64,
}

impl Default for GcStats {
    fn default() -> Self {
        Self::new()
    }
}

impl GcStats {
    pub fn new() -> Self {
        Self {
            total_sweeps: AtomicU64::new(0),
            total_reclaimed_versions: AtomicU64::new(0),
            total_reclaimed_bytes: AtomicU64::new(0),
            total_chains_inspected: AtomicU64::new(0),
            total_chains_pruned: AtomicU64::new(0),
            last_safepoint_ts: AtomicU64::new(0),
            last_sweep_duration_us: AtomicU64::new(0),
            max_chain_length_observed: AtomicU64::new(0),
            sum_chain_lengths: AtomicU64::new(0),
            sum_chain_count: AtomicU64::new(0),
        }
    }

    /// Record the result of a GC sweep.
    pub fn record_sweep(&self, result: &GcSweepResult) {
        self.total_sweeps.fetch_add(1, Ordering::Relaxed);
        self.total_reclaimed_versions
            .fetch_add(result.reclaimed_versions, Ordering::Relaxed);
        self.total_reclaimed_bytes
            .fetch_add(result.reclaimed_bytes, Ordering::Relaxed);
        self.total_chains_inspected
            .fetch_add(result.chains_inspected, Ordering::Relaxed);
        self.total_chains_pruned
            .fetch_add(result.chains_pruned, Ordering::Relaxed);
        self.last_safepoint_ts
            .store(result.safepoint_ts.0, Ordering::Relaxed);
        self.last_sweep_duration_us
            .store(result.sweep_duration_us, Ordering::Relaxed);
    }

    /// Record a chain length observation (for avg/max tracking).
    pub fn observe_chain_length(&self, len: u64) {
        self.sum_chain_lengths.fetch_add(len, Ordering::Relaxed);
        self.sum_chain_count.fetch_add(1, Ordering::Relaxed);
        // CAS loop to update max
        let mut current = self.max_chain_length_observed.load(Ordering::Relaxed);
        while len > current {
            match self.max_chain_length_observed.compare_exchange_weak(
                current, len, Ordering::Relaxed, Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Take a snapshot for reporting.
    pub fn snapshot(&self) -> GcStatsSnapshot {
        let count = self.sum_chain_count.load(Ordering::Relaxed);
        let avg = if count > 0 {
            self.sum_chain_lengths.load(Ordering::Relaxed) as f64 / count as f64
        } else {
            0.0
        };
        GcStatsSnapshot {
            total_sweeps: self.total_sweeps.load(Ordering::Relaxed),
            total_reclaimed_versions: self.total_reclaimed_versions.load(Ordering::Relaxed),
            total_reclaimed_bytes: self.total_reclaimed_bytes.load(Ordering::Relaxed),
            total_chains_inspected: self.total_chains_inspected.load(Ordering::Relaxed),
            total_chains_pruned: self.total_chains_pruned.load(Ordering::Relaxed),
            last_safepoint_ts: Timestamp(self.last_safepoint_ts.load(Ordering::Relaxed)),
            last_sweep_duration_us: self.last_sweep_duration_us.load(Ordering::Relaxed),
            max_chain_length_observed: self.max_chain_length_observed.load(Ordering::Relaxed),
            avg_chain_length: avg,
        }
    }
}

/// Immutable snapshot of GC statistics for reporting.
#[derive(Debug, Clone, Default)]
pub struct GcStatsSnapshot {
    pub total_sweeps: u64,
    pub total_reclaimed_versions: u64,
    pub total_reclaimed_bytes: u64,
    pub total_chains_inspected: u64,
    pub total_chains_pruned: u64,
    pub last_safepoint_ts: Timestamp,
    pub last_sweep_duration_us: u64,
    pub max_chain_length_observed: u64,
    pub avg_chain_length: f64,
}

/// Compute the GC safepoint timestamp.
///
/// The safepoint is the highest timestamp at which we are guaranteed no
/// active transaction will ever read. Versions committed at or before
/// the safepoint (and not the newest such version for a key) can be reclaimed.
///
/// Formula: safepoint = min(min_active_ts, replica_safe_ts) - 1
///
/// If there are no active transactions, `min_active_ts` = current_ts,
/// meaning GC can reclaim everything committed before current_ts.
///
/// `replica_safe_ts` is the minimum applied timestamp across all replicas.
/// If no replicas exist, it is Timestamp::MAX (no constraint).
pub fn compute_safepoint(
    min_active_ts: Timestamp,
    replica_safe_ts: Timestamp,
) -> Timestamp {
    let effective = std::cmp::min(min_active_ts, replica_safe_ts);
    // safepoint = effective - 1, so that versions at exactly effective are preserved
    Timestamp(effective.0.saturating_sub(1))
}

/// Run a single GC sweep over a MemTable.
///
/// Iterates all keys, runs per-chain GC with the given watermark.
/// Respects `config.batch_size` and `config.min_chain_length`.
/// This function does NOT hold any table-wide lock — it iterates the
/// DashMap entry-by-entry, each chain lock is acquired independently.
pub fn sweep_memtable(
    table: &crate::memtable::MemTable,
    watermark: Timestamp,
    config: &GcConfig,
    stats: &GcStats,
) -> GcSweepResult {
    let start = Instant::now();
    let mut result = GcSweepResult {
        safepoint_ts: watermark,
        ..Default::default()
    };

    let mut processed = 0u64;

    for entry in table.data.iter() {
        // Batch limit
        if config.batch_size > 0 && processed >= config.batch_size as u64 {
            result.keys_skipped += table.data.len() as u64 - processed;
            break;
        }

        let chain = entry.value();
        let chain_len = chain.version_chain_len();
        stats.observe_chain_length(chain_len as u64);

        // Skip short chains
        if chain_len < config.min_chain_length {
            result.keys_skipped += 1;
            processed += 1;
            continue;
        }

        result.chains_inspected += 1;
        let chain_result = chain.gc(watermark);

        if chain_result.reclaimed_versions > 0 {
            result.chains_pruned += 1;
            result.reclaimed_versions += chain_result.reclaimed_versions;
            result.reclaimed_bytes += chain_result.reclaimed_bytes;
        }

        processed += 1;
    }

    result.sweep_duration_us = start.elapsed().as_micros() as u64;
    stats.record_sweep(&result);
    result
}

/// Trait for providing the GC safepoint inputs.
/// Implemented by TxnManager (or a test stub) to avoid circular crate deps.
pub trait SafepointProvider: Send + Sync {
    /// Minimum start_ts of all active transactions.
    /// If no active txns, returns "current" timestamp.
    fn min_active_ts(&self) -> Timestamp;

    /// Minimum applied timestamp across all replicas.
    /// Return Timestamp::MAX if no replicas exist.
    fn replica_safe_ts(&self) -> Timestamp;
}

/// Background GC runner.
///
/// Spawns a thread that periodically:
/// 1. Queries the `SafepointProvider` for min_active_ts and replica_safe_ts.
/// 2. Computes `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`.
/// 3. Calls `StorageEngine::gc_sweep(safepoint)`.
/// 4. Logs results via tracing.
///
/// The runner does NOT hold any global lock. It is safe to run concurrently
/// with all read/write operations.
pub struct GcRunner {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl GcRunner {
    /// Start the background GC runner.
    pub fn start(
        engine: Arc<crate::engine::StorageEngine>,
        provider: Arc<dyn SafepointProvider>,
        config: GcConfig,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let interval = Duration::from_millis(config.interval_ms);

        let handle = std::thread::Builder::new()
            .name("cedar-gc".into())
            .spawn(move || {
                tracing::info!(
                    "GC runner started (interval={}ms, min_sweep_interval={}ms, max_chain={})",
                    config.interval_ms,
                    config.min_sweep_interval_ms,
                    config.max_chain_length,
                );
                // Pre-compute escalated intervals for memory pressure.
                let pressure_interval = Duration::from_millis((config.interval_ms / 4).max(10));
                let critical_interval = Duration::from_millis((config.interval_ms / 10).max(5));
                // Rate-limit floor: never sweep faster than min_sweep_interval_ms.
                let min_sweep_interval = if config.min_sweep_interval_ms > 0 {
                    Some(Duration::from_millis(config.min_sweep_interval_ms))
                } else {
                    None
                };
                let mut last_sweep = Instant::now() - interval; // allow first sweep immediately

                while !stop_clone.load(Ordering::Relaxed) {
                    // Adapt sleep interval based on memory pressure.
                    let pressure = engine.pressure_state();
                    let sleep_dur = match pressure {
                        crate::memory::PressureState::Critical => critical_interval,
                        crate::memory::PressureState::Pressure => pressure_interval,
                        crate::memory::PressureState::Normal => interval,
                    };
                    std::thread::sleep(sleep_dur);
                    if stop_clone.load(Ordering::Relaxed) {
                        break;
                    }

                    // Rate-limit: enforce minimum inter-sweep interval.
                    if let Some(min_interval) = min_sweep_interval {
                        let elapsed = last_sweep.elapsed();
                        if elapsed < min_interval {
                            tracing::trace!(
                                "GC rate-limit: skipping sweep (elapsed={}ms < min={}ms)",
                                elapsed.as_millis(),
                                min_interval.as_millis(),
                            );
                            continue;
                        }
                    }

                    let min_ts = provider.min_active_ts();
                    let replica_ts = provider.replica_safe_ts();
                    let safepoint = compute_safepoint(min_ts, replica_ts);

                    if safepoint.0 == 0 {
                        continue; // nothing safe to GC
                    }

                    // Under pressure, use a more aggressive config (relax min_chain_length).
                    let pressure = engine.pressure_state();
                    let effective_config = match pressure {
                        crate::memory::PressureState::Critical => {
                            tracing::warn!("GC escalation: CRITICAL pressure, aggressive sweep");
                            GcConfig {
                                min_chain_length: 0,
                                batch_size: 0, // unlimited
                                ..config.clone()
                            }
                        }
                        crate::memory::PressureState::Pressure => {
                            tracing::info!("GC escalation: PRESSURE state, relaxed sweep");
                            GcConfig {
                                min_chain_length: 0,
                                ..config.clone()
                            }
                        }
                        crate::memory::PressureState::Normal => config.clone(),
                    };

                    last_sweep = Instant::now();
                    let result = engine.run_gc_with_config(
                        safepoint,
                        &effective_config,
                        engine.gc_stats_snapshot_ref(),
                    );

                    if result.reclaimed_versions > 0 {
                        tracing::debug!(
                            "GC sweep: safepoint={}, reclaimed={} versions, {}B, {}us, pressure={}",
                            safepoint.0,
                            result.reclaimed_versions,
                            result.reclaimed_bytes,
                            result.sweep_duration_us,
                            pressure,
                        );
                    }
                }
                tracing::info!("GC runner stopped");
            })
            .expect("failed to spawn GC thread");

        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Signal the GC runner to stop and wait for it to finish.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    /// Check if the runner is still alive.
    pub fn is_running(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }
}

impl Drop for GcRunner {
    fn drop(&mut self) {
        self.stop();
    }
}
