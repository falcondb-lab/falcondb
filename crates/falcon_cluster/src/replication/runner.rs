//! ReplicaRunner — continuous gRPC replication loop with reconnect and backoff.
//!
//! Connects to the primary via `GrpcTransport`, subscribes to WAL from
//! `last_applied_lsn`, applies chunks to the local `StorageEngine`, acks
//! back to primary, and reconnects with exponential backoff on failure.
//!
//! Usage:
//! ```ignore
//! let runner = ReplicaRunner::new(config);
//! let handle = runner.start();
//! // ... later ...
//! handle.stop().await;
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_storage::engine::StorageEngine;

use crate::grpc_transport::GrpcTransport;
use crate::replication::catchup::{apply_wal_record_to_engine, WriteOp};
use crate::replication::wal_stream::AsyncReplicationTransport;
use falcon_common::types::TxnId;

/// Configuration for a ReplicaRunner.
#[derive(Debug, Clone)]
pub struct ReplicaRunnerConfig {
    /// gRPC endpoint of the primary (e.g. "http://primary:50051").
    pub primary_endpoint: String,
    /// Shard ID this runner replicates.
    pub shard_id: ShardId,
    /// Replica ID (used for ack tracking on the primary side).
    pub replica_id: usize,
    /// Max records per WAL chunk request.
    pub max_records_per_chunk: usize,
    /// How often to ack applied_lsn back to primary (in chunks received).
    pub ack_interval_chunks: u64,
    /// Initial backoff on reconnect (doubles up to max_backoff).
    pub initial_backoff: Duration,
    /// Maximum backoff between reconnect attempts.
    pub max_backoff: Duration,
    /// Connect timeout for gRPC channel.
    pub connect_timeout: Duration,
}

impl Default for ReplicaRunnerConfig {
    fn default() -> Self {
        Self {
            primary_endpoint: "http://127.0.0.1:50051".into(),
            shard_id: ShardId(0),
            replica_id: 0,
            max_records_per_chunk: 1000,
            ack_interval_chunks: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// Metrics exposed by a running ReplicaRunner.
#[derive(Debug)]
pub struct ReplicaRunnerMetrics {
    /// Total WAL chunks applied since start.
    pub chunks_applied: AtomicU64,
    /// Total WAL records applied since start.
    pub records_applied: AtomicU64,
    /// Current applied LSN on the replica.
    pub applied_lsn: AtomicU64,
    /// Last known primary LSN (from chunk end_lsn). Used to compute lag.
    /// 0 = unknown (no chunk received yet).
    pub primary_lsn: AtomicU64,
    /// Number of reconnect attempts since start.
    pub reconnect_count: AtomicU64,
    /// Number of successful acks sent to primary.
    pub acks_sent: AtomicU64,
    /// Whether the runner is currently connected and streaming.
    pub connected: AtomicBool,
}

impl Default for ReplicaRunnerMetrics {
    fn default() -> Self {
        Self {
            chunks_applied: AtomicU64::new(0),
            records_applied: AtomicU64::new(0),
            applied_lsn: AtomicU64::new(0),
            primary_lsn: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            acks_sent: AtomicU64::new(0),
            connected: AtomicBool::new(false),
        }
    }
}

impl ReplicaRunnerMetrics {
    pub fn snapshot(&self) -> ReplicaRunnerMetricsSnapshot {
        let applied = self.applied_lsn.load(Ordering::Relaxed);
        let primary = self.primary_lsn.load(Ordering::Relaxed);
        ReplicaRunnerMetricsSnapshot {
            chunks_applied: self.chunks_applied.load(Ordering::Relaxed),
            records_applied: self.records_applied.load(Ordering::Relaxed),
            applied_lsn: applied,
            primary_lsn: primary,
            lag_lsn: primary.saturating_sub(applied),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            acks_sent: self.acks_sent.load(Ordering::Relaxed),
            connected: self.connected.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of ReplicaRunner metrics for reporting.
#[derive(Debug, Clone)]
pub struct ReplicaRunnerMetricsSnapshot {
    pub chunks_applied: u64,
    pub records_applied: u64,
    pub applied_lsn: u64,
    /// Last known primary LSN (from chunk end_lsn). 0 = unknown.
    pub primary_lsn: u64,
    /// Replication lag in LSN units (primary_lsn - applied_lsn).
    /// 0 means fully caught up or no primary LSN known yet.
    pub lag_lsn: u64,
    pub reconnect_count: u64,
    pub acks_sent: u64,
    pub connected: bool,
}

/// Handle returned by `ReplicaRunner::start()`. Use `stop()` to gracefully
/// shut down the replication loop.
pub struct ReplicaRunnerHandle {
    stop: Arc<AtomicBool>,
    metrics: Arc<ReplicaRunnerMetrics>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ReplicaRunnerHandle {
    /// Signal the runner to stop and wait for the task to finish.
    pub async fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.join_handle.take() {
            let _ = h.await;
        }
    }

    /// Signal the runner to stop (non-blocking).
    pub fn signal_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Get a reference to the live metrics.
    pub fn metrics(&self) -> &ReplicaRunnerMetrics {
        &self.metrics
    }

    /// Get a shared Arc to the live metrics (for passing to other components).
    pub fn metrics_arc(&self) -> Arc<ReplicaRunnerMetrics> {
        self.metrics.clone()
    }

    /// Check if the runner is still alive.
    pub fn is_running(&self) -> bool {
        self.join_handle.as_ref().is_some_and(|h| !h.is_finished())
    }
}

/// Continuous replication runner for a single shard.
///
/// Connects to the primary via gRPC, subscribes to WAL from the replica's
/// `applied_lsn`, applies chunks to the local StorageEngine, and acks
/// back to primary. On disconnect, reconnects with exponential backoff.
pub struct ReplicaRunner {
    config: ReplicaRunnerConfig,
    storage: Arc<StorageEngine>,
}

impl ReplicaRunner {
    pub fn new(config: ReplicaRunnerConfig, storage: Arc<StorageEngine>) -> Self {
        Self { config, storage }
    }

    /// Start the replication loop as a tokio task. Returns a handle for
    /// stopping and observing the runner.
    pub fn start(self) -> ReplicaRunnerHandle {
        let stop = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(ReplicaRunnerMetrics::default());

        let stop_clone = stop.clone();
        let metrics_clone = metrics.clone();

        let join_handle = tokio::spawn(async move {
            self.run_loop(stop_clone, metrics_clone).await;
        });

        ReplicaRunnerHandle {
            stop,
            metrics,
            join_handle: Some(join_handle),
        }
    }

    /// The main replication loop. Reconnects with exponential backoff.
    async fn run_loop(self, stop: Arc<AtomicBool>, metrics: Arc<ReplicaRunnerMetrics>) {
        let mut backoff = self.config.initial_backoff;

        tracing::info!(
            shard_id = self.config.shard_id.0,
            replica_id = self.config.replica_id,
            endpoint = %self.config.primary_endpoint,
            "ReplicaRunner starting",
        );

        while !stop.load(Ordering::SeqCst) {
            let transport = GrpcTransport::with_timeout(
                self.config.primary_endpoint.clone(),
                self.config.shard_id,
                self.config.connect_timeout,
            );

            match self.run_stream(&transport, &stop, &metrics).await {
                Ok(()) => {
                    // Clean shutdown (stop signaled while streaming).
                    tracing::info!(
                        shard_id = self.config.shard_id.0,
                        "ReplicaRunner stopped cleanly",
                    );
                    break;
                }
                Err(e) => {
                    metrics.connected.store(false, Ordering::Relaxed);
                    metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);

                    tracing::warn!(
                        shard_id = self.config.shard_id.0,
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        "ReplicaRunner disconnected, will reconnect",
                    );

                    // Exponential backoff with jitter (stop-aware sleep).
                    let sleep_end = tokio::time::Instant::now() + backoff;
                    while tokio::time::Instant::now() < sleep_end {
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    backoff = (backoff * 2).min(self.config.max_backoff);
                }
            }
        }
    }

    /// Bootstrap a new replica by downloading a checkpoint from the primary.
    ///
    /// Called when `applied_lsn == 0` (fresh replica with no data).
    /// After success, sets `metrics.applied_lsn` to the checkpoint's WAL LSN
    /// so the subsequent WAL subscription starts from the right position.
    async fn bootstrap_from_checkpoint(
        &self,
        transport: &GrpcTransport,
        metrics: &ReplicaRunnerMetrics,
    ) -> Result<(), FalconError> {
        tracing::info!(
            shard_id = self.config.shard_id.0,
            "Bootstrapping replica from primary checkpoint",
        );

        let (ckpt_data, ckpt_lsn) = transport.download_checkpoint(self.config.shard_id).await?;

        self.storage
            .apply_checkpoint_data(&ckpt_data)
            .map_err(|e| FalconError::Internal(format!("apply_checkpoint_data: {}", e)))?;

        metrics.applied_lsn.store(ckpt_lsn, Ordering::SeqCst);

        tracing::info!(
            shard_id = self.config.shard_id.0,
            ckpt_lsn,
            "Checkpoint bootstrap complete, will subscribe WAL from lsn={}",
            ckpt_lsn,
        );
        Ok(())
    }

    /// Subscribe to WAL stream and apply chunks until error or stop.
    async fn run_stream(
        &self,
        transport: &GrpcTransport,
        stop: &AtomicBool,
        metrics: &ReplicaRunnerMetrics,
    ) -> Result<(), FalconError> {
        // On first connect (applied_lsn == 0), bootstrap from checkpoint so we
        // don't have to replay the entire WAL history from the beginning.
        if metrics.applied_lsn.load(Ordering::SeqCst) == 0 {
            self.bootstrap_from_checkpoint(transport, metrics).await?;
        }

        let from_lsn = metrics.applied_lsn.load(Ordering::SeqCst);

        tracing::info!(
            shard_id = self.config.shard_id.0,
            from_lsn = from_lsn,
            "Subscribing to WAL stream",
        );

        let mut rx = transport
            .subscribe_wal_stream(
                self.config.shard_id,
                from_lsn,
                self.config.max_records_per_chunk,
            )
            .await?;

        metrics.connected.store(true, Ordering::Relaxed);
        let mut chunks_since_ack = 0u64;

        loop {
            if stop.load(Ordering::SeqCst) {
                return Ok(());
            }

            // Wait for next chunk with a timeout so we can check stop flag.
            let chunk_result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;

            match chunk_result {
                Ok(Some(Ok(chunk))) => {
                    if chunk.is_empty() {
                        continue;
                    }

                    // Apply chunk to local storage.
                    let mut write_sets: HashMap<TxnId, Vec<WriteOp>> = HashMap::new();
                    let mut max_lsn = metrics.applied_lsn.load(Ordering::SeqCst);
                    let mut applied = 0u64;

                    for lsn_record in &chunk.records {
                        if lsn_record.lsn <= max_lsn {
                            continue; // idempotent skip
                        }
                        apply_wal_record_to_engine(
                            &self.storage,
                            &lsn_record.record,
                            &mut write_sets,
                        )?;
                        max_lsn = lsn_record.lsn;
                        applied += 1;
                    }

                    metrics.applied_lsn.store(max_lsn, Ordering::SeqCst);
                    // Track primary's end_lsn so lag_lsn = primary_lsn - applied_lsn is accurate.
                    if chunk.end_lsn > metrics.primary_lsn.load(Ordering::Relaxed) {
                        metrics.primary_lsn.store(chunk.end_lsn, Ordering::Relaxed);
                    }
                    metrics.chunks_applied.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .records_applied
                        .fetch_add(applied, Ordering::Relaxed);
                    chunks_since_ack += 1;

                    // Periodic ack.
                    if chunks_since_ack >= self.config.ack_interval_chunks {
                        if let Err(e) = transport
                            .ack_wal(self.config.shard_id, self.config.replica_id, max_lsn)
                            .await
                        {
                            tracing::warn!(
                                shard_id = self.config.shard_id.0,
                                error = %e,
                                "Failed to ack applied_lsn to primary",
                            );
                            // Don't fail the stream — ack is best-effort.
                        } else {
                            metrics.acks_sent.fetch_add(1, Ordering::Relaxed);
                        }
                        chunks_since_ack = 0;
                    }
                }
                Ok(Some(Err(e))) => {
                    // Stream error — reconnect.
                    return Err(e);
                }
                Ok(None) => {
                    // Stream ended (primary closed connection) — reconnect.
                    return Err(FalconError::Internal(
                        "WAL stream ended (primary disconnected)".into(),
                    ));
                }
                Err(_) => {
                    // Timeout — no new data, just loop to check stop flag.
                    continue;
                }
            }
        }
    }
}
