//! # CSN (Commit Sequence Number) — Decoupled from LSN
//!
//! **LSN is a physical address; CSN is a logical visibility clock.**
//! Never conflate them.
//!
//! ## Design (Scheme A: Per-Shard CSN)
//!
//! - Each shard maintains an independent, monotonically increasing CSN
//! - Every committed transaction gets exactly one CSN
//! - MVCC visibility is determined by CSN, **never** by LSN
//! - LSN is used exclusively for WAL positioning / flush / replication / recovery
//! - Cross-shard reads use a CSN vector: `{ shard_id → csn }`
//!
//! ## Commit path ordering
//! 1. WAL append + flush → `commit_lsn` is known
//! 2. Assign CSN (atomic increment)
//! 3. Publish `commit_csn` to MVCC version headers (atomic store, Release)
//! 4. Any crash between 1–3 is recoverable: WAL replay rebuilds commit table
//!
//! ## Prohibited patterns
//! - ❌ Using LSN for visibility checks
//! - ❌ Inferring commit order from WAL position
//! - ❌ Skipping CSN assignment for any committed transaction

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use falcon_common::types::{ShardId, TxnId};

use crate::structured_lsn::StructuredLsn;

// ═══════════════════════════════════════════════════════════════════════════
// §A1 — CSN Type
// ═══════════════════════════════════════════════════════════════════════════

/// Commit Sequence Number — a logical, monotonically increasing counter
/// that defines transaction visibility order.
///
/// **CSN is NOT a physical address.** It is a pure logical clock.
///
/// Properties:
/// - Per-shard monotonically increasing
/// - Every committed txn has exactly one CSN
/// - `row.commit_csn <= snapshot.csn` ⟹ row is visible
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Csn(pub u64);

impl Csn {
    /// Zero CSN — "before any commit".
    pub const ZERO: Self = Self(0);
    /// Invalid/sentinel CSN.
    pub const INVALID: Self = Self(u64::MAX);
    /// Maximum valid CSN.
    pub const MAX: Self = Self(u64::MAX - 1);

    #[inline]
    pub const fn new(val: u64) -> Self {
        Self(val)
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Check if this CSN represents a committed transaction.
    #[inline]
    pub const fn is_committed(self) -> bool {
        self.0 > 0 && self.0 < u64::MAX
    }
}

impl fmt::Debug for Csn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CSN({})", self.0)
    }
}

impl fmt::Display for Csn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A2 — Per-Shard CSN Generator
// ═══════════════════════════════════════════════════════════════════════════

/// Per-shard CSN allocator.
///
/// Each shard has its own generator. CSNs are monotonically increasing
/// within a shard but independent across shards.
///
/// Thread-safe: uses `AtomicU64` for lock-free increment.
pub struct CsnGenerator {
    /// Shard this generator belongs to.
    shard_id: ShardId,
    /// Next CSN to allocate.
    next_csn: AtomicU64,
}

impl CsnGenerator {
    /// Create a new generator starting at CSN 1.
    pub const fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            next_csn: AtomicU64::new(1),
        }
    }

    /// Create a generator resuming from a recovered CSN.
    pub const fn from_recovered(shard_id: ShardId, last_committed_csn: Csn) -> Self {
        Self {
            shard_id,
            next_csn: AtomicU64::new(last_committed_csn.0 + 1),
        }
    }

    /// Allocate the next CSN. This is called exactly once per committing transaction.
    ///
    /// **Ordering:** SeqCst to ensure CSN assignment is globally visible
    /// before the commit_csn is published to MVCC version headers.
    #[inline]
    pub fn next(&self) -> Csn {
        Csn(self.next_csn.fetch_add(1, Ordering::SeqCst))
    }

    /// Get the current (next-to-allocate) CSN without incrementing.
    #[inline]
    pub fn current(&self) -> Csn {
        Csn(self.next_csn.load(Ordering::SeqCst))
    }

    /// Get the last committed CSN (current - 1).
    #[inline]
    pub fn last_committed(&self) -> Csn {
        let cur = self.next_csn.load(Ordering::SeqCst);
        if cur <= 1 { Csn::ZERO } else { Csn(cur - 1) }
    }

    /// Shard ID.
    #[inline]
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Reset to a specific CSN (for recovery).
    pub fn reset_to(&self, csn: Csn) {
        self.next_csn.store(csn.0 + 1, Ordering::SeqCst);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A3 — Commit Record (binds TxnId + CSN + LSN)
// ═══════════════════════════════════════════════════════════════════════════

/// A commit record binding together the three coordinates of a committed transaction.
///
/// - `txn_id`: which transaction
/// - `commit_csn`: logical visibility order (MVCC)
/// - `commit_lsn`: physical WAL position (durability / replication)
///
/// This record is written into the WAL at commit time and is the basis
/// for crash recovery of the CSN state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRecord {
    pub txn_id: TxnId,
    pub commit_csn: Csn,
    pub commit_lsn: StructuredLsn,
}

impl CommitRecord {
    pub const fn new(txn_id: TxnId, commit_csn: Csn, commit_lsn: StructuredLsn) -> Self {
        Self { txn_id, commit_csn, commit_lsn }
    }

    /// Serialize to bytes (for WAL embedding).
    /// Format: [txn_id: u64][csn: u64][lsn: u64] = 24 bytes
    pub fn to_bytes(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.txn_id.0.to_le_bytes());
        buf[8..16].copy_from_slice(&self.commit_csn.0.to_le_bytes());
        buf[16..24].copy_from_slice(&self.commit_lsn.raw().to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 24 { return None; }
        let txn_id = TxnId(u64::from_le_bytes(data[0..8].try_into().ok()?));
        let csn = Csn(u64::from_le_bytes(data[8..16].try_into().ok()?));
        let lsn = StructuredLsn::from_raw(u64::from_le_bytes(data[16..24].try_into().ok()?));
        Some(Self { txn_id, commit_csn: csn, commit_lsn: lsn })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A4 — Commit Table (txn_id → CSN mapping for recovery & visibility)
// ═══════════════════════════════════════════════════════════════════════════

/// In-memory mapping of committed transactions to their CSN and LSN.
///
/// **Populated during:**
/// - Normal operation: on each txn commit
/// - Recovery: by scanning WAL commit records
///
/// **Used for:**
/// - Looking up a transaction's CSN (e.g., for late-arriving reads)
/// - Rebuilding `last_committed_csn` after crash
/// - Determining the CSN generator's resume point
pub struct CommitTable {
    /// txn_id → (csn, lsn)
    table: RwLock<HashMap<TxnId, (Csn, StructuredLsn)>>,
    /// The highest CSN in the table.
    max_csn: AtomicU64,
    /// Total entries (for observability).
    entry_count: AtomicU64,
}

impl CommitTable {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
            max_csn: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
        }
    }

    /// Record a committed transaction.
    pub fn insert(&self, record: CommitRecord) {
        let mut t = self.table.write();
        t.insert(record.txn_id, (record.commit_csn, record.commit_lsn));
        // Update max CSN
        let mut current_max = self.max_csn.load(Ordering::Relaxed);
        while record.commit_csn.0 > current_max {
            match self.max_csn.compare_exchange_weak(
                current_max,
                record.commit_csn.0,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up a transaction's commit CSN.
    pub fn get_csn(&self, txn_id: TxnId) -> Option<Csn> {
        self.table.read().get(&txn_id).map(|(csn, _)| *csn)
    }

    /// Look up a transaction's commit LSN.
    pub fn get_lsn(&self, txn_id: TxnId) -> Option<StructuredLsn> {
        self.table.read().get(&txn_id).map(|(_, lsn)| *lsn)
    }

    /// Look up both CSN and LSN.
    pub fn get(&self, txn_id: TxnId) -> Option<(Csn, StructuredLsn)> {
        self.table.read().get(&txn_id).copied()
    }

    /// Check if a transaction is committed.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.table.read().contains_key(&txn_id)
    }

    /// Get the highest committed CSN.
    pub fn max_csn(&self) -> Csn {
        Csn(self.max_csn.load(Ordering::SeqCst))
    }

    /// Number of entries.
    pub fn len(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove entries for transactions with CSN below the watermark.
    /// Used for GC — once all active snapshots are past a CSN, those entries
    /// are no longer needed.
    pub fn gc_before(&self, watermark: Csn) -> u64 {
        let mut t = self.table.write();
        let before = t.len();
        t.retain(|_, (csn, _)| csn.0 >= watermark.0);
        let after = t.len();
        let removed = (before - after) as u64;
        // Adjust counter (approximate — concurrent inserts may have happened)
        if removed > 0 {
            self.entry_count.fetch_sub(removed, Ordering::Relaxed);
        }
        removed
    }

    /// Get all entries (for snapshot/debug).
    pub fn entries(&self) -> Vec<CommitRecord> {
        self.table.read().iter().map(|(txn_id, (csn, lsn))| {
            CommitRecord { txn_id: *txn_id, commit_csn: *csn, commit_lsn: *lsn }
        }).collect()
    }
}

impl Default for CommitTable {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A5 — CSN Snapshot (Scheme A: Per-Shard CSN Vector)
// ═══════════════════════════════════════════════════════════════════════════

/// A snapshot for read operations.
///
/// **Scheme A (Per-Shard CSN):** Each shard has its own CSN space.
/// A cross-shard snapshot is a vector of per-shard CSNs.
///
/// Visibility rule: `row.commit_csn <= snapshot.csn_for_shard(row_shard)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsnSnapshot {
    /// Per-shard CSN values. A row on shard S is visible iff
    /// `row.commit_csn <= csn_map[S]`.
    csn_map: HashMap<ShardId, Csn>,
    /// Timestamp when this snapshot was taken (wall clock, for diagnostics).
    pub taken_at_ns: u64,
}

impl CsnSnapshot {
    /// Create a single-shard snapshot.
    pub fn single(shard_id: ShardId, csn: Csn) -> Self {
        let mut map = HashMap::new();
        map.insert(shard_id, csn);
        Self {
            csn_map: map,
            taken_at_ns: 0,
        }
    }

    /// Create a multi-shard snapshot (CSN vector).
    pub fn multi(entries: Vec<(ShardId, Csn)>) -> Self {
        let map: HashMap<ShardId, Csn> = entries.into_iter().collect();
        Self {
            csn_map: map,
            taken_at_ns: 0,
        }
    }

    /// Get the CSN for a specific shard.
    pub fn csn_for_shard(&self, shard_id: ShardId) -> Option<Csn> {
        self.csn_map.get(&shard_id).copied()
    }

    /// Check if a row is visible under this snapshot.
    ///
    /// **This is THE visibility check.** It uses CSN, never LSN.
    ///
    /// Returns `true` iff `row_csn <= snapshot.csn_for_shard(shard_id)`.
    #[inline]
    pub fn is_visible(&self, shard_id: ShardId, row_csn: Csn) -> bool {
        if !row_csn.is_committed() {
            return false;
        }
        self.csn_map.get(&shard_id).is_some_and(|snap_csn| row_csn <= *snap_csn)
    }

    /// Number of shards in this snapshot.
    pub fn shard_count(&self) -> usize {
        self.csn_map.len()
    }

    /// All shard entries.
    pub fn entries(&self) -> Vec<(ShardId, Csn)> {
        self.csn_map.iter().map(|(s, c)| (*s, *c)).collect()
    }

    /// Advance the CSN for a shard (used in long-running transactions
    /// that need to see their own commits).
    pub fn advance_shard(&mut self, shard_id: ShardId, new_csn: Csn) {
        self.csn_map.insert(shard_id, new_csn);
    }

    /// Serialize to bytes.
    /// Format: [shard_count: u32][taken_at_ns: u64][(shard_id: u64, csn: u64)...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let count = self.csn_map.len() as u32;
        let mut buf = Vec::with_capacity(12 + count as usize * 16);
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&self.taken_at_ns.to_le_bytes());
        for (shard, csn) in &self.csn_map {
            buf.extend_from_slice(&shard.0.to_le_bytes());
            buf.extend_from_slice(&csn.0.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 12 { return None; }
        let count = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let taken_at_ns = u64::from_le_bytes(data[4..12].try_into().ok()?);
        if data.len() < 12 + count * 16 { return None; }
        let mut map = HashMap::with_capacity(count);
        for i in 0..count {
            let off = 12 + i * 16;
            let shard = ShardId(u64::from_le_bytes(data[off..off+8].try_into().ok()?));
            let csn = Csn(u64::from_le_bytes(data[off+8..off+16].try_into().ok()?));
            map.insert(shard, csn);
        }
        Some(Self { csn_map: map, taken_at_ns })
    }
}

impl fmt::Display for CsnSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CsnSnapshot{{")?;
        let mut first = true;
        for (shard, csn) in &self.csn_map {
            if !first { write!(f, ", ")?; }
            write!(f, "shard{}={}", shard.0, csn.0)?;
            first = false;
        }
        write!(f, "}}")
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A6 — WAL Recovery of CSN State
// ═══════════════════════════════════════════════════════════════════════════

/// Result of recovering CSN state from WAL commit records.
#[derive(Debug, Clone)]
pub struct CsnRecoveryState {
    /// Recovered commit table entries.
    pub commit_records: Vec<CommitRecord>,
    /// The highest CSN found during recovery (per shard).
    pub last_committed_csn: HashMap<ShardId, Csn>,
    /// Total commit records scanned.
    pub records_scanned: u64,
}

/// Recover CSN state from a sequence of commit records (extracted from WAL).
///
/// This is called during crash recovery after WAL replay. The input is the
/// sequence of commit records found in WAL commit entries.
///
/// **Invariant:** After recovery, `CsnGenerator` resumes from `max_csn + 1`.
pub fn recover_csn_state(
    shard_id: ShardId,
    commit_records: &[CommitRecord],
) -> CsnRecoveryState {
    let mut last_csn_per_shard: HashMap<ShardId, Csn> = HashMap::new();

    for rec in commit_records {
        let entry = last_csn_per_shard.entry(shard_id).or_insert(Csn::ZERO);
        if rec.commit_csn > *entry {
            *entry = rec.commit_csn;
        }
    }

    CsnRecoveryState {
        commit_records: commit_records.to_vec(),
        last_committed_csn: last_csn_per_shard,
        records_scanned: commit_records.len() as u64,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A7 — Commit Coordinator (orchestrates the commit path)
// ═══════════════════════════════════════════════════════════════════════════

/// Orchestrates the commit path ensuring correct ordering:
/// 1. WAL append + flush → commit_lsn known
/// 2. Assign CSN (atomic increment)
/// 3. Record in commit table
/// 4. Publish commit_csn to MVCC headers
///
/// This coordinator does NOT own the WAL or MVCC — it coordinates the
/// sequencing between them.
pub struct CommitCoordinator {
    /// Per-shard CSN generator.
    csn_gen: CsnGenerator,
    /// Commit table for this shard.
    commit_table: CommitTable,
    /// Metrics.
    pub metrics: CommitMetrics,
}

/// Metrics for commit path observability.
#[derive(Debug, Default)]
pub struct CommitMetrics {
    /// Total transactions committed.
    pub commits_total: AtomicU64,
    /// Total CSN assignments.
    pub csn_assigned_total: AtomicU64,
    /// Total commit table lookups.
    pub lookups_total: AtomicU64,
    /// Total GC'd entries.
    pub gc_entries_total: AtomicU64,
}

impl CommitCoordinator {
    /// Create a new coordinator for a shard.
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            csn_gen: CsnGenerator::new(shard_id),
            commit_table: CommitTable::new(),
            metrics: CommitMetrics::default(),
        }
    }

    /// Create from recovered state.
    pub fn from_recovered(shard_id: ShardId, recovery: &CsnRecoveryState) -> Self {
        let last_csn = recovery.last_committed_csn
            .get(&shard_id)
            .copied()
            .unwrap_or(Csn::ZERO);
        let gen = CsnGenerator::from_recovered(shard_id, last_csn);
        let table = CommitTable::new();
        for rec in &recovery.commit_records {
            table.insert(*rec);
        }
        Self {
            csn_gen: gen,
            commit_table: table,
            metrics: CommitMetrics::default(),
        }
    }

    /// Phase 2 of commit: assign CSN and record in commit table.
    ///
    /// **Must be called AFTER WAL flush** (commit_lsn is known and durable).
    ///
    /// Returns the assigned CSN.
    pub fn assign_csn(&self, txn_id: TxnId, commit_lsn: StructuredLsn) -> Csn {
        let csn = self.csn_gen.next();
        let record = CommitRecord::new(txn_id, csn, commit_lsn);
        self.commit_table.insert(record);
        self.metrics.commits_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.csn_assigned_total.fetch_add(1, Ordering::Relaxed);
        csn
    }

    /// Take a snapshot at the current CSN (for this shard).
    pub fn take_snapshot(&self) -> CsnSnapshot {
        let csn = self.csn_gen.last_committed();
        CsnSnapshot::single(self.csn_gen.shard_id(), csn)
    }

    /// Look up a transaction's CSN.
    pub fn get_csn(&self, txn_id: TxnId) -> Option<Csn> {
        self.metrics.lookups_total.fetch_add(1, Ordering::Relaxed);
        self.commit_table.get_csn(txn_id)
    }

    /// Get the underlying commit table.
    pub const fn commit_table(&self) -> &CommitTable {
        &self.commit_table
    }

    /// Get the CSN generator.
    pub const fn csn_generator(&self) -> &CsnGenerator {
        &self.csn_gen
    }

    /// The last committed CSN for this shard.
    pub fn last_committed_csn(&self) -> Csn {
        self.csn_gen.last_committed()
    }

    /// GC commit table entries below watermark.
    pub fn gc_before(&self, watermark: Csn) -> u64 {
        let removed = self.commit_table.gc_before(watermark);
        self.metrics.gc_entries_total.fetch_add(removed, Ordering::Relaxed);
        removed
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A8 — Cross-Shard Snapshot Coordinator
// ═══════════════════════════════════════════════════════════════════════════

/// Coordinates cross-shard snapshots by collecting per-shard CSNs.
///
/// In Scheme A, each shard has an independent CSN. A cross-shard read
/// needs a consistent vector snapshot: `{ shard_id → csn }`.
///
/// The coordinator collects the current CSN from each shard's generator
/// to produce a globally consistent read point.
pub struct CrossShardSnapshotCoordinator {
    /// Registered shard generators.
    shards: RwLock<HashMap<ShardId, Csn>>,
}

impl CrossShardSnapshotCoordinator {
    pub fn new() -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
        }
    }

    /// Register/update a shard's current CSN.
    pub fn update_shard(&self, shard_id: ShardId, csn: Csn) {
        self.shards.write().insert(shard_id, csn);
    }

    /// Take a cross-shard snapshot (CSN vector).
    pub fn take_snapshot(&self) -> CsnSnapshot {
        let shards = self.shards.read();
        let entries: Vec<(ShardId, Csn)> = shards.iter().map(|(s, c)| (*s, *c)).collect();
        CsnSnapshot::multi(entries)
    }

    /// Number of registered shards.
    pub fn shard_count(&self) -> usize {
        self.shards.read().len()
    }
}

impl Default for CrossShardSnapshotCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A9 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- CSN Type --

    #[test]
    fn test_csn_basics() {
        let a = Csn::new(1);
        let b = Csn::new(2);
        assert!(a < b);
        assert!(a.is_committed());
        assert!(!Csn::ZERO.is_committed());
        assert!(!Csn::INVALID.is_committed());
    }

    #[test]
    fn test_csn_display() {
        assert_eq!(format!("{}", Csn::new(42)), "42");
        assert_eq!(format!("{:?}", Csn::new(42)), "CSN(42)");
    }

    // -- CSN Generator --

    #[test]
    fn test_csn_generator_monotonic() {
        let gen = CsnGenerator::new(ShardId(0));
        let c1 = gen.next();
        let c2 = gen.next();
        let c3 = gen.next();
        assert_eq!(c1, Csn::new(1));
        assert_eq!(c2, Csn::new(2));
        assert_eq!(c3, Csn::new(3));
        assert_eq!(gen.last_committed(), Csn::new(3));
    }

    #[test]
    fn test_csn_generator_from_recovered() {
        let gen = CsnGenerator::from_recovered(ShardId(1), Csn::new(100));
        let c = gen.next();
        assert_eq!(c, Csn::new(101));
    }

    #[test]
    fn test_csn_generator_concurrent() {
        use std::sync::Arc;
        let gen = Arc::new(CsnGenerator::new(ShardId(0)));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let g = Arc::clone(&gen);
            handles.push(std::thread::spawn(move || {
                let mut csns = Vec::new();
                for _ in 0..1000 {
                    csns.push(g.next().raw());
                }
                csns
            }));
        }
        let mut all_csns: Vec<u64> = Vec::new();
        for h in handles {
            all_csns.extend(h.join().unwrap());
        }
        all_csns.sort();
        // All unique
        for i in 1..all_csns.len() {
            assert_ne!(all_csns[i], all_csns[i - 1], "duplicate CSN at index {}", i);
        }
        assert_eq!(all_csns.len(), 8000);
        assert_eq!(all_csns[0], 1);
        assert_eq!(all_csns[7999], 8000);
    }

    // -- Commit Record --

    #[test]
    fn test_commit_record_roundtrip() {
        let rec = CommitRecord::new(
            TxnId(42),
            Csn::new(100),
            StructuredLsn::new(5, 2048),
        );
        let bytes = rec.to_bytes();
        let recovered = CommitRecord::from_bytes(&bytes).unwrap();
        assert_eq!(recovered, rec);
    }

    // -- Commit Table --

    #[test]
    fn test_commit_table_insert_lookup() {
        let table = CommitTable::new();
        let rec = CommitRecord::new(TxnId(1), Csn::new(10), StructuredLsn::new(0, 100));
        table.insert(rec);

        assert_eq!(table.get_csn(TxnId(1)), Some(Csn::new(10)));
        assert_eq!(table.get_lsn(TxnId(1)), Some(StructuredLsn::new(0, 100)));
        assert!(table.is_committed(TxnId(1)));
        assert!(!table.is_committed(TxnId(999)));
        assert_eq!(table.max_csn(), Csn::new(10));
    }

    #[test]
    fn test_commit_table_gc() {
        let table = CommitTable::new();
        for i in 1..=10u64 {
            table.insert(CommitRecord::new(
                TxnId(i), Csn::new(i), StructuredLsn::ZERO,
            ));
        }
        assert_eq!(table.len(), 10);

        let removed = table.gc_before(Csn::new(6)); // remove csn < 6
        assert_eq!(removed, 5);
        assert_eq!(table.len(), 5);
        // CSNs 1-5 should be gone
        assert!(!table.is_committed(TxnId(1)));
        assert!(!table.is_committed(TxnId(5)));
        // CSNs 6-10 should remain
        assert!(table.is_committed(TxnId(6)));
        assert!(table.is_committed(TxnId(10)));
    }

    // -- CSN Snapshot --

    #[test]
    fn test_snapshot_single_shard() {
        let snap = CsnSnapshot::single(ShardId(0), Csn::new(50));
        assert!(snap.is_visible(ShardId(0), Csn::new(50)));
        assert!(snap.is_visible(ShardId(0), Csn::new(1)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(51)));
        assert!(!snap.is_visible(ShardId(0), Csn::ZERO));
        assert!(!snap.is_visible(ShardId(1), Csn::new(1))); // wrong shard
    }

    #[test]
    fn test_snapshot_multi_shard() {
        let snap = CsnSnapshot::multi(vec![
            (ShardId(0), Csn::new(100)),
            (ShardId(1), Csn::new(50)),
            (ShardId(2), Csn::new(200)),
        ]);
        assert!(snap.is_visible(ShardId(0), Csn::new(100)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(101)));
        assert!(snap.is_visible(ShardId(1), Csn::new(50)));
        assert!(!snap.is_visible(ShardId(1), Csn::new(51)));
        assert!(snap.is_visible(ShardId(2), Csn::new(200)));
        assert_eq!(snap.shard_count(), 3);
    }

    #[test]
    fn test_snapshot_serialization() {
        let snap = CsnSnapshot::multi(vec![
            (ShardId(0), Csn::new(100)),
            (ShardId(1), Csn::new(200)),
        ]);
        let bytes = snap.to_bytes();
        let recovered = CsnSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.shard_count(), 2);
        assert_eq!(recovered.csn_for_shard(ShardId(0)), Some(Csn::new(100)));
        assert_eq!(recovered.csn_for_shard(ShardId(1)), Some(Csn::new(200)));
    }

    #[test]
    fn test_snapshot_display() {
        let snap = CsnSnapshot::single(ShardId(0), Csn::new(42));
        let s = format!("{}", snap);
        assert!(s.contains("shard0=42"));
    }

    // -- Commit Coordinator --

    #[test]
    fn test_commit_coordinator_basic() {
        let coord = CommitCoordinator::new(ShardId(0));

        // Simulate: WAL flushed at some LSN, now assign CSN
        let csn1 = coord.assign_csn(TxnId(1), StructuredLsn::new(0, 100));
        let csn2 = coord.assign_csn(TxnId(2), StructuredLsn::new(0, 200));
        let csn3 = coord.assign_csn(TxnId(3), StructuredLsn::new(0, 300));

        assert_eq!(csn1, Csn::new(1));
        assert_eq!(csn2, Csn::new(2));
        assert_eq!(csn3, Csn::new(3));

        assert_eq!(coord.get_csn(TxnId(2)), Some(Csn::new(2)));
        assert_eq!(coord.last_committed_csn(), Csn::new(3));
    }

    #[test]
    fn test_commit_coordinator_snapshot() {
        let coord = CommitCoordinator::new(ShardId(0));
        coord.assign_csn(TxnId(1), StructuredLsn::ZERO);
        coord.assign_csn(TxnId(2), StructuredLsn::ZERO);

        let snap = coord.take_snapshot();
        assert!(snap.is_visible(ShardId(0), Csn::new(1)));
        assert!(snap.is_visible(ShardId(0), Csn::new(2)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(3)));
    }

    #[test]
    fn test_commit_coordinator_recovery() {
        let records = vec![
            CommitRecord::new(TxnId(1), Csn::new(1), StructuredLsn::new(0, 100)),
            CommitRecord::new(TxnId(2), Csn::new(2), StructuredLsn::new(0, 200)),
            CommitRecord::new(TxnId(3), Csn::new(3), StructuredLsn::new(0, 300)),
        ];
        let recovery = recover_csn_state(ShardId(0), &records);
        assert_eq!(recovery.last_committed_csn.get(&ShardId(0)), Some(&Csn::new(3)));

        let coord = CommitCoordinator::from_recovered(ShardId(0), &recovery);
        assert_eq!(coord.last_committed_csn(), Csn::new(3));
        assert_eq!(coord.get_csn(TxnId(2)), Some(Csn::new(2)));

        // New commits resume from CSN 4
        let csn4 = coord.assign_csn(TxnId(4), StructuredLsn::new(1, 0));
        assert_eq!(csn4, Csn::new(4));
    }

    #[test]
    fn test_commit_coordinator_gc() {
        let coord = CommitCoordinator::new(ShardId(0));
        for i in 1..=10u64 {
            coord.assign_csn(TxnId(i), StructuredLsn::ZERO);
        }

        let removed = coord.gc_before(Csn::new(6));
        assert_eq!(removed, 5);
        assert!(coord.get_csn(TxnId(1)).is_none());
        assert!(coord.get_csn(TxnId(6)).is_some());
    }

    // -- Crash Scenarios --

    #[test]
    fn test_crash_before_csn_assignment() {
        // Simulate: WAL flushed but CSN not yet assigned (crash between step 1 and 2)
        // After recovery, the WAL commit record exists but CSN is reconstructed
        let records = vec![
            CommitRecord::new(TxnId(1), Csn::new(1), StructuredLsn::new(0, 100)),
            CommitRecord::new(TxnId(2), Csn::new(2), StructuredLsn::new(0, 200)),
            // TxnId(3) WAL was flushed but CSN was not assigned → not in recovery
        ];
        let recovery = recover_csn_state(ShardId(0), &records);
        let coord = CommitCoordinator::from_recovered(ShardId(0), &recovery);

        // TxnId(3) is NOT committed (CSN never assigned)
        assert!(coord.get_csn(TxnId(3)).is_none());
        // TxnId(1) and TxnId(2) are committed
        assert_eq!(coord.get_csn(TxnId(1)), Some(Csn::new(1)));
        assert_eq!(coord.get_csn(TxnId(2)), Some(Csn::new(2)));

        // Snapshot sees only committed txns
        let snap = coord.take_snapshot();
        assert!(snap.is_visible(ShardId(0), Csn::new(2)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(3)));
    }

    #[test]
    fn test_crash_after_csn_assignment() {
        // Simulate: CSN assigned, commit record in WAL → recovery sees it
        let records = vec![
            CommitRecord::new(TxnId(1), Csn::new(1), StructuredLsn::new(0, 100)),
            CommitRecord::new(TxnId(2), Csn::new(2), StructuredLsn::new(0, 200)),
            CommitRecord::new(TxnId(3), Csn::new(3), StructuredLsn::new(0, 300)),
        ];
        let recovery = recover_csn_state(ShardId(0), &records);
        let coord = CommitCoordinator::from_recovered(ShardId(0), &recovery);

        // All three are committed and visible
        let snap = coord.take_snapshot();
        assert!(snap.is_visible(ShardId(0), Csn::new(1)));
        assert!(snap.is_visible(ShardId(0), Csn::new(2)));
        assert!(snap.is_visible(ShardId(0), Csn::new(3)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(4)));
    }

    #[test]
    fn test_long_txn_snapshot_isolation() {
        let coord = CommitCoordinator::new(ShardId(0));

        // Txn 1 commits
        coord.assign_csn(TxnId(1), StructuredLsn::ZERO);
        // Long-running reader takes snapshot at CSN 1
        let reader_snap = coord.take_snapshot();

        // Txn 2, 3, 4 commit after the snapshot
        coord.assign_csn(TxnId(2), StructuredLsn::ZERO);
        coord.assign_csn(TxnId(3), StructuredLsn::ZERO);
        coord.assign_csn(TxnId(4), StructuredLsn::ZERO);

        // Reader's snapshot should NOT see txn 2, 3, 4
        assert!(reader_snap.is_visible(ShardId(0), Csn::new(1)));
        assert!(!reader_snap.is_visible(ShardId(0), Csn::new(2)));
        assert!(!reader_snap.is_visible(ShardId(0), Csn::new(3)));
        assert!(!reader_snap.is_visible(ShardId(0), Csn::new(4)));
    }

    // -- Cross-Shard Snapshot --

    #[test]
    fn test_cross_shard_snapshot() {
        let xcoord = CrossShardSnapshotCoordinator::new();
        xcoord.update_shard(ShardId(0), Csn::new(100));
        xcoord.update_shard(ShardId(1), Csn::new(50));
        xcoord.update_shard(ShardId(2), Csn::new(200));

        let snap = xcoord.take_snapshot();
        assert_eq!(snap.shard_count(), 3);

        // Each shard has its own visibility boundary
        assert!(snap.is_visible(ShardId(0), Csn::new(100)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(101)));
        assert!(snap.is_visible(ShardId(1), Csn::new(50)));
        assert!(!snap.is_visible(ShardId(1), Csn::new(51)));
    }

    #[test]
    fn test_cross_shard_vector_consistency() {
        // Simulate: two shards, coordinator takes snapshot
        let coord0 = CommitCoordinator::new(ShardId(0));
        let coord1 = CommitCoordinator::new(ShardId(1));

        // Both shards commit independently
        coord0.assign_csn(TxnId(1), StructuredLsn::ZERO);
        coord0.assign_csn(TxnId(2), StructuredLsn::ZERO);
        coord1.assign_csn(TxnId(3), StructuredLsn::ZERO);

        // Build vector snapshot
        let xcoord = CrossShardSnapshotCoordinator::new();
        xcoord.update_shard(ShardId(0), coord0.last_committed_csn());
        xcoord.update_shard(ShardId(1), coord1.last_committed_csn());
        let snap = xcoord.take_snapshot();

        // Shard 0: CSN 1,2 visible; CSN 3 not relevant (different shard)
        assert!(snap.is_visible(ShardId(0), Csn::new(1)));
        assert!(snap.is_visible(ShardId(0), Csn::new(2)));
        assert!(!snap.is_visible(ShardId(0), Csn::new(3)));

        // Shard 1: CSN 1 visible (only 1 commit on shard 1)
        assert!(snap.is_visible(ShardId(1), Csn::new(1)));
        assert!(!snap.is_visible(ShardId(1), Csn::new(2)));
    }

    // -- Performance Guardrail --

    #[test]
    fn test_csn_assignment_overhead() {
        // Verify CSN assignment is fast (just an atomic increment)
        let coord = CommitCoordinator::new(ShardId(0));
        let start = std::time::Instant::now();
        for i in 1..=100_000u64 {
            coord.assign_csn(TxnId(i), StructuredLsn::ZERO);
        }
        let elapsed = start.elapsed();
        // Should complete in well under 1 second (typically <50ms)
        assert!(
            elapsed.as_millis() < 1000,
            "CSN assignment too slow: {}ms for 100K commits",
            elapsed.as_millis()
        );
    }
}
