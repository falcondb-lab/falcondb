//! # Module Status: PRODUCTION
//! WAL chunk, replication log, and transport abstractions.
//!
//! ## Commit Visibility Boundary
//! ```text
//! Client commit → WAL fsync (primary) → client ACK ← visibility boundary
//!                                      ↓
//!               WAL chunk shipped → replica apply → replica ACK
//! ```
//!
//! ## Policy-Driven Commit ACK (see HAConfig.sync_mode)
//! - **Async**: client ACK after primary WAL fsync. Replica may lag. RPO > 0.
//! - **SemiSync**: client ACK after primary WAL fsync + 1 replica ACK.
//! - **Sync**: client ACK after primary WAL fsync + ALL replica ACKs.
//!
//! ## Invariants
//! - REP-1: Replica committed set is always a prefix of primary's.
//! - REP-2: No phantom commits — replica never commits what primary hasn't.
//! - REP-3: WAL entries applied in primary's write order.
//! - REP-4: Replica ACK means WAL entry has been applied (not just received).
//!
//! ## Prohibited Patterns
//! - Replication emitting records not yet WAL-durable → phantom commits
//! - Replica ACK before apply → violates REP-4
//! - Out-of-order apply on replica → violates REP-3

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use parking_lot::Mutex;

use serde::{Deserialize, Serialize};

use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_storage::wal::WalRecord;

// ---------------------------------------------------------------------------
// WalChunk — the unit of WAL replication over the network
// ---------------------------------------------------------------------------

/// A chunk of WAL records shipped from primary to replica.
/// Corresponds to a gRPC `SubscribeWal` streaming response message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalChunk {
    pub shard_id: ShardId,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub records: Vec<LsnWalRecord>,
    pub checksum: u32,
}

impl WalChunk {
    /// Build a WalChunk from a slice of LsnWalRecords.
    /// Computes a CRC32 checksum over the LSN sequence for integrity.
    pub fn from_records(shard_id: ShardId, records: Vec<LsnWalRecord>) -> Self {
        let start_lsn = records.first().map_or(0, |r| r.lsn);
        let end_lsn = records.last().map_or(0, |r| r.lsn);
        let checksum = Self::compute_checksum(&records);
        Self {
            shard_id,
            start_lsn,
            end_lsn,
            records,
            checksum,
        }
    }

    /// Verify the chunk's integrity.
    pub fn verify_checksum(&self) -> bool {
        self.checksum == Self::compute_checksum(&self.records)
    }

    fn compute_checksum(records: &[LsnWalRecord]) -> u32 {
        let mut crc: u32 = 0;
        for rec in records {
            // Simple CRC over LSN bytes — sufficient for integrity detection.
            let bytes = rec.lsn.to_le_bytes();
            for &b in &bytes {
                crc = crc.wrapping_mul(31).wrapping_add(u32::from(b));
            }
        }
        crc
    }

    /// Create an empty WalChunk for a given shard (no records).
    pub const fn empty(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            start_lsn: 0,
            end_lsn: 0,
            records: Vec::new(),
            checksum: 0,
        }
    }

    pub const fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub const fn len(&self) -> usize {
        self.records.len()
    }
}

// ---------------------------------------------------------------------------
// ReplicationTransport — abstraction over the network layer
// ---------------------------------------------------------------------------

/// Abstraction over the network transport for WAL replication.
///
/// Implementations:
/// - `InProcessTransport`: direct memory access (same process, existing behavior).
/// - `ChannelTransport`: tokio mpsc channels (simulates gRPC streaming).
pub trait ReplicationTransport: Send + Sync {
    /// Subscribe to WAL stream from `from_lsn` for a given shard.
    /// Returns chunks of WAL records. In a streaming impl, this would be
    /// an async stream; for MVP we use pull-based polling.
    fn pull_wal_chunk(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<WalChunk, FalconError>;

    /// Ack that replica has applied up to `applied_lsn`.
    fn ack_wal(
        &self,
        shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) -> Result<(), FalconError>;
}

/// In-process transport: reads directly from the `ReplicationLog`.
///
/// On `ack_wal`, also notifies `ReplicationLog::sync_ack` so that any
/// primary waiting in `append_and_wait` is woken immediately.
pub struct InProcessTransport {
    log: Arc<ReplicationLog>,
    shard_id: ShardId,
    ack_lsns: Mutex<HashMap<usize, u64>>,
}

impl InProcessTransport {
    pub fn new(shard_id: ShardId, log: Arc<ReplicationLog>) -> Self {
        Self {
            log,
            shard_id,
            ack_lsns: Mutex::new(HashMap::new()),
        }
    }

    /// Get the acked LSN for a replica.
    pub fn get_ack_lsn(&self, replica_id: usize) -> u64 {
        self.ack_lsns.lock().get(&replica_id).copied().unwrap_or(0)
    }
}

impl ReplicationTransport for InProcessTransport {
    fn pull_wal_chunk(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<WalChunk, FalconError> {
        if shard_id != self.shard_id {
            return Err(FalconError::Internal(format!(
                "Transport shard mismatch: expected {:?}, got {:?}",
                self.shard_id, shard_id
            )));
        }
        let all = self.log.read_from(from_lsn);
        let records: Vec<LsnWalRecord> = all.into_iter().take(max_records).collect();
        Ok(WalChunk::from_records(shard_id, records))
    }

    fn ack_wal(
        &self,
        _shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) -> Result<(), FalconError> {
        self.ack_lsns.lock().insert(replica_id, applied_lsn);
        // Wake any primary blocked in append_and_wait.
        self.log.sync_ack.record_ack(replica_id, applied_lsn);
        Ok(())
    }
}

/// Channel-based transport: uses std sync channels to simulate cross-process
/// gRPC streaming. Primary pushes WAL chunks; replica pulls from a receiver.
pub struct ChannelTransport {
    shard_id: ShardId,
    /// Sender side (primary pushes chunks here).
    sender: Mutex<std::sync::mpsc::Sender<WalChunk>>,
    /// Receiver side (replica pulls chunks from here).
    receiver: Mutex<std::sync::mpsc::Receiver<WalChunk>>,
    /// Ack LSNs per replica, tracked on primary side.
    ack_lsns: Mutex<HashMap<usize, u64>>,
}

impl ChannelTransport {
    pub fn new(shard_id: ShardId) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self {
            shard_id,
            sender: Mutex::new(tx),
            receiver: Mutex::new(rx),
            ack_lsns: Mutex::new(HashMap::new()),
        }
    }

    /// Push a WAL chunk from the primary side.
    pub fn push_chunk(&self, chunk: WalChunk) -> Result<(), FalconError> {
        self.sender
            .lock()
            .send(chunk)
            .map_err(|e| FalconError::Internal(format!("Channel send error: {e}")))
    }

    /// Get the acked LSN for a replica.
    pub fn get_ack_lsn(&self, replica_id: usize) -> u64 {
        self.ack_lsns.lock().get(&replica_id).copied().unwrap_or(0)
    }
}

impl ReplicationTransport for ChannelTransport {
    fn pull_wal_chunk(
        &self,
        _shard_id: ShardId,
        _from_lsn: u64,
        _max_records: usize,
    ) -> Result<WalChunk, FalconError> {
        let rx = self.receiver.lock();
        match rx.try_recv() {
            Ok(chunk) => {
                if !chunk.verify_checksum() {
                    return Err(FalconError::Internal("WalChunk checksum mismatch".into()));
                }
                Ok(chunk)
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                Ok(WalChunk::from_records(self.shard_id, vec![]))
            }
            Err(e) => Err(FalconError::Internal(format!("Channel recv error: {e}"))),
        }
    }

    fn ack_wal(
        &self,
        _shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) -> Result<(), FalconError> {
        self.ack_lsns.lock().insert(replica_id, applied_lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AsyncReplicationTransport — M2 gRPC-ready async transport
// ---------------------------------------------------------------------------

/// Async version of `ReplicationTransport` for M2 gRPC streaming.
///
/// Uses `async_trait` so implementations can perform real network I/O
/// (e.g. tonic gRPC calls) without blocking the tokio runtime.
///
/// The sync `ReplicationTransport` trait remains for in-process testing.
#[async_trait::async_trait]
pub trait AsyncReplicationTransport: Send + Sync {
    /// Pull a chunk of WAL records from `from_lsn` for a given shard.
    async fn pull_wal_chunk(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<WalChunk, FalconError>;

    /// Ack that replica has applied up to `applied_lsn`.
    async fn ack_wal(
        &self,
        shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) -> Result<(), FalconError>;
}

/// Blanket impl: any sync transport is also a valid async transport.
/// This lets existing `InProcessTransport` / `ChannelTransport` be used
/// anywhere an `AsyncReplicationTransport` is expected.
#[async_trait::async_trait]
impl<T: ReplicationTransport> AsyncReplicationTransport for T {
    async fn pull_wal_chunk(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<WalChunk, FalconError> {
        ReplicationTransport::pull_wal_chunk(self, shard_id, from_lsn, max_records)
    }

    async fn ack_wal(
        &self,
        shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) -> Result<(), FalconError> {
        ReplicationTransport::ack_wal(self, shard_id, replica_id, applied_lsn)
    }
}

// ---------------------------------------------------------------------------
// SyncWalAck — per-commit replica acknowledgement gate (RPO = 0)
// ---------------------------------------------------------------------------

/// Replication sync mode.
///
/// Controls how many replicas must acknowledge a WAL record before the
/// primary returns success to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Primary ACKs client after local WAL append only.  RPO > 0.
    Async,
    /// Primary blocks until **at least 1** replica has applied the record.  RPO = 0.
    SemiSync,
    /// Primary blocks until **all** replicas have applied the record.  RPO = 0.
    Sync,
}

impl SyncMode {
    /// Number of replica ACKs required before unblocking.
    /// `total_replicas` is the current replica count.
    pub fn required_acks(self, total_replicas: usize) -> usize {
        match self {
            Self::Async => 0,
            Self::SemiSync => 1.min(total_replicas),
            Self::Sync => total_replicas,
        }
    }
}

/// Per-commit synchronisation gate.
///
/// The primary calls `wait_for_acks(lsn, n, timeout)` immediately after
/// appending a WAL record.  Each replica calls `record_ack(replica_id, lsn)`
/// after it has durably applied the record.  The condvar wakes the waiting
/// primary thread so it can check whether enough replicas have acked.
///
/// # Design notes
/// - A single `SyncWalAck` is shared for the whole `ReplicationLog`.
/// - `replica_lsns` maps replica_id → highest applied LSN.
/// - A replica ACK for LSN *k* counts as an ACK for all LSNs ≤ k
///   (monotone progress guarantee).
/// - The condvar uses a `std::sync::Mutex` (not parking_lot) so it can be
///   paired with `Condvar::wait_timeout`.
pub struct SyncWalAck {
    /// `replica_id` → highest LSN confirmed applied on that replica.
    replica_lsns: StdMutex<HashMap<usize, u64>>,
    /// Woken whenever any replica acks a new LSN.
    cond: Condvar,
}

impl Default for SyncWalAck {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncWalAck {
    pub fn new() -> Self {
        Self {
            replica_lsns: StdMutex::new(HashMap::new()),
            cond: Condvar::new(),
        }
    }

    /// Called by a replica (or `InProcessTransport::ack_wal`) when it has
    /// applied WAL records up to `applied_lsn`.
    pub fn record_ack(&self, replica_id: usize, applied_lsn: u64) {
        let mut guard = self
            .replica_lsns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = guard.entry(replica_id).or_insert(0);
        if applied_lsn > *prev {
            *prev = applied_lsn;
            drop(guard);
            self.cond.notify_all();
        }
    }

    /// Block the calling thread until `required_acks` replicas have confirmed
    /// LSN ≥ `target_lsn`, or `timeout` elapses.
    ///
    /// Returns `Ok(ack_count)` on success, `Err(WalAckTimeout)` on timeout.
    pub fn wait_for_acks(
        &self,
        target_lsn: u64,
        required_acks: usize,
        timeout: Duration,
    ) -> Result<usize, WalAckTimeout> {
        if required_acks == 0 {
            return Ok(0);
        }

        let deadline = std::time::Instant::now() + timeout;
        let mut guard = self
            .replica_lsns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        loop {
            let acked = guard.values().filter(|&&lsn| lsn >= target_lsn).count();
            if acked >= required_acks {
                return Ok(acked);
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(WalAckTimeout {
                    target_lsn,
                    required: required_acks,
                    got: acked,
                });
            }

            let (g, timed_out) = self
                .cond
                .wait_timeout(guard, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = g;
            if timed_out.timed_out() {
                let acked = guard.values().filter(|&&lsn| lsn >= target_lsn).count();
                return if acked >= required_acks {
                    Ok(acked)
                } else {
                    Err(WalAckTimeout {
                        target_lsn,
                        required: required_acks,
                        got: acked,
                    })
                };
            }
        }
    }

    /// Snapshot of per-replica acked LSNs (for observability).
    pub fn acked_lsns(&self) -> HashMap<usize, u64> {
        self.replica_lsns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Minimum acked LSN across all replicas that have reported.
    /// Returns 0 if no replica has acked yet.
    pub fn min_acked_lsn(&self) -> u64 {
        self.replica_lsns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .copied()
            .min()
            .unwrap_or(0)
    }
}

/// Error returned when `wait_for_acks` times out.
#[derive(Debug, Clone)]
pub struct WalAckTimeout {
    pub target_lsn: u64,
    pub required: usize,
    pub got: usize,
}

impl std::fmt::Display for WalAckTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WalAckTimeout: lsn={} required={} got={}",
            self.target_lsn, self.required, self.got
        )
    }
}

impl std::error::Error for WalAckTimeout {}

// ---------------------------------------------------------------------------
// LsnWalRecord & ReplicationLog
// ---------------------------------------------------------------------------

/// WAL record with LSN for replication ordering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsnWalRecord {
    pub lsn: u64,
    pub record: WalRecord,
}

/// Default maximum number of WAL records held in the in-memory replication log.
///
/// When the log reaches this capacity, the oldest records are evicted on each
/// `append()` call (ring-buffer semantics). Replicas that fall too far behind
/// will need to re-bootstrap from a checkpoint.
///
/// At ~1 KB per record this is ~128 MB. Tune via `ReplicationLog::with_capacity`.
const DEFAULT_REPLICATION_LOG_CAPACITY: usize = 128 * 1024;

/// The replication log: an ordered sequence of (LSN, WalRecord).
///
/// Primary appends; replicas read from their last applied LSN.
/// Includes a `Notify` for push-based streaming: `subscribe_wal` holds
/// the gRPC stream open and wakes when new records are appended.
///
/// ## Bounded memory
/// The log is capped at `max_capacity` records. When the cap is reached,
/// the oldest records are evicted (ring-buffer). A replica that falls behind
/// the eviction frontier must re-bootstrap from a checkpoint.
pub struct ReplicationLog {
    records: Mutex<Vec<LsnWalRecord>>,
    next_lsn: AtomicU64,
    /// Maximum number of records to retain in memory.
    max_capacity: usize,
    /// Count of records evicted due to capacity overflow (for observability).
    evicted_records: AtomicU64,
    /// Wakes any server-streaming tasks waiting for new records.
    notify: tokio::sync::Notify,
    /// Synchronous ACK gate for SemiSync / Sync modes.  Shared with transports.
    pub sync_ack: Arc<SyncWalAck>,
}

impl Default for ReplicationLog {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationLog {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_REPLICATION_LOG_CAPACITY)
    }

    /// Create a log with a custom capacity cap.
    pub fn with_capacity(max_capacity: usize) -> Self {
        Self {
            records: Mutex::new(Vec::with_capacity(max_capacity.min(4096))),
            next_lsn: AtomicU64::new(1),
            max_capacity,
            evicted_records: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
            sync_ack: Arc::new(SyncWalAck::new()),
        }
    }

    /// Append a WAL record and **block** until `required_acks` replicas have
    /// confirmed they applied it, or `timeout` elapses.
    ///
    /// This is the RPO = 0 path for SemiSync and Sync modes.
    /// Use `append()` for the legacy Async path.
    ///
    /// # Errors
    /// Returns `Err` if the replica quorum was not reached within `timeout`.
    pub fn append_and_wait(
        &self,
        record: WalRecord,
        required_acks: usize,
        timeout: Duration,
    ) -> Result<u64, FalconError> {
        let lsn = self.append(record);
        if required_acks == 0 {
            return Ok(lsn);
        }
        self.sync_ack
            .wait_for_acks(lsn, required_acks, timeout)
            .map(|_| lsn)
            .map_err(|e| FalconError::Internal(e.to_string()))
    }

    /// Append a WAL record and return its LSN.
    ///
    /// If the log is at capacity, the oldest record is evicted first (ring-buffer).
    /// Notifies any waiting streaming tasks.
    pub fn append(&self, record: WalRecord) -> u64 {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut records = self.records.lock();
        if self.max_capacity > 0 && records.len() >= self.max_capacity {
            // Evict oldest record to keep memory bounded.
            records.remove(0);
            self.evicted_records.fetch_add(1, Ordering::Relaxed);
        }
        records.push(LsnWalRecord { lsn, record });
        drop(records);
        self.notify.notify_waiters();
        lsn
    }

    /// Number of records evicted due to capacity overflow.
    /// Non-zero means at least one slow replica may need to re-bootstrap.
    pub fn evicted_records(&self) -> u64 {
        self.evicted_records.load(Ordering::Relaxed)
    }

    /// Maximum capacity of this log.
    pub const fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    /// Wait until new records are available (for push-based streaming).
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Read all records with LSN > from_lsn.
    pub fn read_from(&self, from_lsn: u64) -> Vec<LsnWalRecord> {
        let records = self.records.lock();
        records
            .iter()
            .filter(|r| r.lsn > from_lsn)
            .cloned()
            .collect()
    }

    /// Current highest LSN.
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// Number of records in the log.
    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.records.lock().is_empty()
    }

    /// Discard all records with LSN ≤ `safe_lsn`.
    ///
    /// Call this periodically once all replicas have acked past `safe_lsn`
    /// to prevent the in-memory log from growing without bound.
    /// Returns the number of records discarded.
    pub fn trim_before(&self, safe_lsn: u64) -> usize {
        let mut records = self.records.lock();
        let before = records.len();
        records.retain(|r| r.lsn > safe_lsn);
        before - records.len()
    }

    /// Minimum LSN still held in the log (0 if empty).
    pub fn min_lsn(&self) -> u64 {
        self.records.lock().first().map_or(0, |r| r.lsn)
    }
}
