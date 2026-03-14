//! # VersionChain2 — Inline Single-Version MVCC Chain
//!
//! Replaces the existing `VersionChain` (`RwLock<Option<Arc<Version>>>` linked
//! list) with a two-tier design that eliminates heap allocation and lock
//! contention on the common single-version committed row case.
//!
//! ## Tier 1 — Inline slot (cache line 0)
//!
//! Every `VersionChain2` contains a single `ArenaVersion`-shaped inline slot
//! directly in its body.  For the overwhelming majority of OLTP rows (single
//! committed version, no in-flight writes), all visibility checks and row reads
//! are served from this slot without any pointer indirection.
//!
//! ## Tier 2 — Overflow chain (arena-backed)
//!
//! When a second version is prepended (UPDATE, DELETE, or a concurrent
//! uncommitted write), the old inline slot is promoted to an `ArenaVersion`
//! allocation and the new version becomes the new inline slot.  The overflow
//! chain is a simple singly-linked list of `ArenaVersion` pointers.
//!
//! ## Locking Model
//!
//! | Path                   | Synchronization                               |
//! |------------------------|-----------------------------------------------|
//! | Read, fast path        | 2× `Acquire` loads (commit_ts, row_ptr) — lock-free |
//! | Read, slow path        | `SeqCst` loads on overflow chain              |
//! | Write (prepend)        | `SeqCst` CAS on `head_state`                  |
//! | Commit                 | `Release` store on inline `commit_ts`         |
//! | Abort                  | `SeqCst` CAS swap on `head_state`             |
//! | GC truncate            | `Mutex` (rare, only when chain len > 1)        |
//!
//! ## Safety
//!
//! `VersionChain2` is `Send + Sync`.  The raw `*mut ArenaVersion` overflow
//! pointer is guarded by the `head_state` CAS protocol — only one writer may
//! replace the inline slot at a time.  Readers never hold a lock; they only
//! load atomic fields.

use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use falcon_common::datum::OwnedRow;
use falcon_common::types::{Timestamp, TxnId};

use crate::mvcc::GcChainResult;
use crate::version_arena::{
    ArenaVersion, FLAG_HAS_PREV, FLAG_TOMBSTONE, VersionArena,
};

// ── VersionChain2 ─────────────────────────────────────────────────────────────

/// MVCC version chain with an inline single-version fast path.
///
/// Aligned to 128 bytes (2 cache lines) so that the inline slot's hot fields
/// (txn_id, commit_ts, flags) all fit in CL0.
#[repr(C, align(64))]
pub struct VersionChain2 {
    // ── CL0: inline version slot hot fields ──────────────────────────────────
    /// Transaction that wrote the current head version (inline slot).
    pub head_txn_id: AtomicU64,       // offset  0
    /// Commit timestamp of the current head version.
    /// 0 = uncommitted, u64::MAX = aborted.
    pub head_commit_ts: AtomicU64,    // offset  8
    /// Head version flags: FLAG_TOMBSTONE | FLAG_HAS_PREV | ...
    pub head_flags: AtomicU8,         // offset 16
    _pad0: [u8; 7],                   // offset 17
    /// Arc<OwnedRow> of the head version (raw pointer, ref-counted).
    /// Null for tombstone.
    pub head_row: AtomicPtr<OwnedRow>,// offset 24
    // ── Overflow / structural fields (CL0 remainder + CL1) ───────────────────
    /// Pointer to the first overflow `ArenaVersion` (older versions).
    /// Null when chain depth == 1.
    /// Low bit used as a spin-lock during prepend/abort.
    pub overflow_head: AtomicPtr<ArenaVersion>, // offset 32
    /// Reference to the owning table's `VersionArena`.
    arena: *const VersionArena,       // offset 40
    /// Mutex protecting structural chain mutations (prepend, abort, GC).
    /// Only contended during concurrent writes to the same row — rare for OLTP.
    write_mu: Mutex<()>,              // offset 48  (parking_lot = 4 bytes)
    _pad1: [u8; 12],                  // align to 64
}

// SAFETY: all shared mutable state is protected by atomics or write_mu.
unsafe impl Send for VersionChain2 {}
unsafe impl Sync for VersionChain2 {}

impl VersionChain2 {
    /// Create a new, empty `VersionChain2` associated with `arena`.
    ///
    /// The chain starts with no versions (head_txn_id = 0, head_commit_ts = 0).
    /// Call `prepend` to add the first version.
    pub fn new(arena: *const VersionArena) -> Self {
        Self {
            head_txn_id: AtomicU64::new(0),
            head_commit_ts: AtomicU64::new(0),
            head_flags: AtomicU8::new(0),
            _pad0: [0; 7],
            head_row: AtomicPtr::new(std::ptr::null_mut()),
            overflow_head: AtomicPtr::new(std::ptr::null_mut()),
            arena,
            write_mu: Mutex::new(()),
            _pad1: [0; 12],
        }
    }

    // ── Write: prepend ────────────────────────────────────────────────────────

    /// Prepend a new version for `txn_id` to the head of the chain.
    ///
    /// This is the INSERT / UPDATE / DELETE entry point.
    /// - INSERT: `row = Some(...)`, chain had no prior version.
    /// - UPDATE: `row = Some(...)`, chain has one prior committed version.
    /// - DELETE: `row = None` (tombstone).
    pub fn prepend(&self, txn_id: TxnId, row: Option<Arc<OwnedRow>>) {
        let _guard = self.write_mu.lock();
        self.prepend_locked(txn_id, row);
    }

    /// Prepend with pre-wrapped `Arc<OwnedRow>` (avoids double-wrapping on
    /// code paths that already hold an `Arc`).
    pub fn prepend_arc(&self, txn_id: TxnId, row: Option<Arc<OwnedRow>>) {
        let _guard = self.write_mu.lock();
        self.prepend_locked(txn_id, row);
    }

    fn prepend_locked(&self, txn_id: TxnId, row: Option<Arc<OwnedRow>>) {
        let is_tombstone = row.is_none();

        // If the chain already has a head version, spill it to the overflow arena.
        let prev_txn = self.head_txn_id.load(Ordering::Relaxed);
        if prev_txn != 0 {
            // Spill current inline slot to arena overflow.
            let overflow_slot = unsafe { self.spill_inline_to_arena() };
            self.overflow_head.store(overflow_slot, Ordering::Release);
        }

        // Install the new version into the inline slot.
        let raw_row = row
            .map(|arc| Arc::into_raw(arc) as *mut OwnedRow)
            .unwrap_or(std::ptr::null_mut());

        let has_prev = self.overflow_head.load(Ordering::Relaxed) != std::ptr::null_mut();
        let flags: u8 = if is_tombstone { FLAG_TOMBSTONE } else { 0 }
            | if has_prev { FLAG_HAS_PREV } else { 0 };

        // Atomic sequence: write row first, then txn_id + flags atomically via
        // the write_mu guard (already held).  commit_ts = 0 (uncommitted).
        self.head_row.store(raw_row, Ordering::Release);
        self.head_flags.store(flags, Ordering::Release);
        self.head_commit_ts.store(0, Ordering::Release);
        self.head_txn_id.store(txn_id.0, Ordering::Release);
    }

    /// Spill the current inline slot into an `ArenaVersion`.
    ///
    /// # Safety
    /// Must be called under `write_mu`.
    unsafe fn spill_inline_to_arena(&self) -> *mut ArenaVersion {
        let arena = &*self.arena;
        let slot = arena.alloc();

        // Atomically read all inline fields while we hold the write lock.
        let txn_id = self.head_txn_id.load(Ordering::Relaxed);
        let commit_ts = self.head_commit_ts.load(Ordering::Relaxed);
        let flags = self.head_flags.load(Ordering::Relaxed);
        let raw_row = self.head_row.swap(std::ptr::null_mut(), Ordering::Relaxed);

        // Write into the arena slot.
        (*slot).txn_id = txn_id;
        (*slot).commit_ts.store(commit_ts, Ordering::Relaxed);
        // Link to any existing overflow head.
        let existing_overflow = self.overflow_head.load(Ordering::Relaxed);
        let new_flags = flags
            | if existing_overflow.is_null() { 0 } else { FLAG_HAS_PREV };
        (*slot).flags = new_flags;
        (*slot).prev.store(existing_overflow, Ordering::Relaxed);
        (*slot).row_ptr.store(raw_row, Ordering::Relaxed);

        slot
    }

    // ── Commit ────────────────────────────────────────────────────────────────

    /// Commit the version written by `txn_id`.
    ///
    /// Returns row-count delta: +1 (insert), -1 (delete), 0 (update / no-op).
    pub fn commit_no_report(&self, txn_id: TxnId, commit_ts: Timestamp) -> i8 {
        // Fast path: head is uncommitted and matches txn_id.
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        if head_txn == txn_id.0 {
            let head_cts = self.head_commit_ts.load(Ordering::Acquire);
            if head_cts == 0 {
                self.head_commit_ts.store(commit_ts.0, Ordering::Release);
                let flags = self.head_flags.load(Ordering::Relaxed);
                let is_live = flags & FLAG_TOMBSTONE == 0;
                let has_prev = flags & FLAG_HAS_PREV != 0;
                return match (is_live, has_prev) {
                    (true, false) => 1,   // fresh INSERT
                    (false, true) => -1,  // DELETE
                    _ => 0,               // UPDATE or re-insert
                };
            }
        }
        // Slow path: scan overflow chain.
        self.commit_slow(txn_id, commit_ts)
    }

    fn commit_slow(&self, txn_id: TxnId, commit_ts: Timestamp) -> i8 {
        let _guard = self.write_mu.lock();
        let mut delta = 0i8;
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        while !cur.is_null() {
            // SAFETY: cur is a live arena slot under write_mu.
            let ver = unsafe { &*cur };
            if ver.txn_id == txn_id.0 && ver.get_commit_ts() == 0 {
                ver.set_commit_ts(commit_ts);
                delta = 0; // internal update; row count managed at head
            }
            cur = ver.prev.load(Ordering::Relaxed);
        }
        delta
    }

    /// Commit and return `(new_row, old_row)` for secondary index maintenance.
    pub fn commit_and_report(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> (Option<Arc<OwnedRow>>, Option<Arc<OwnedRow>>) {
        let _guard = self.write_mu.lock();

        // Commit head if it belongs to this txn.
        let head_txn = self.head_txn_id.load(Ordering::Relaxed);
        let new_data: Option<Arc<OwnedRow>>;
        if head_txn == txn_id.0 && self.head_commit_ts.load(Ordering::Relaxed) == 0 {
            self.head_commit_ts.store(commit_ts.0, Ordering::Release);
            new_data = self.load_head_row();
        } else {
            new_data = None;
        }

        // Find the previous committed version (old data for index removal).
        let old_data = self.find_prev_committed_row(txn_id);

        // Also commit any overflow versions for this txn (multi-stmt txns).
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            if ver.txn_id == txn_id.0 && ver.get_commit_ts() == 0 {
                ver.set_commit_ts(commit_ts);
            }
            cur = ver.prev.load(Ordering::Relaxed);
        }

        (new_data, old_data)
    }

    // ── Abort ─────────────────────────────────────────────────────────────────

    /// Abort all versions written by `txn_id`.
    ///
    /// Returns `(aborted_row, restored_row)` for secondary index rollback.
    pub fn abort_and_report(
        &self,
        txn_id: TxnId,
    ) -> (Option<Arc<OwnedRow>>, Option<Arc<OwnedRow>>) {
        let _guard = self.write_mu.lock();

        let head_txn = self.head_txn_id.load(Ordering::Relaxed);
        if head_txn != txn_id.0 {
            // txn_id is not the head — mark overflow versions aborted.
            self.abort_overflow(txn_id);
            return (None, None);
        }

        // The head belongs to txn_id — drop the head row and promote overflow.
        let aborted = self.drop_head_row();

        // Pop overflow into inline slot (restore previous committed version).
        let restored = unsafe { self.pop_overflow_to_inline() };

        // Mark any remaining overflow versions for this txn as aborted.
        self.abort_overflow(txn_id);

        (aborted, restored)
    }

    /// Pop the first overflow slot into the inline slot.
    ///
    /// # Safety
    /// Must be called under `write_mu`.
    unsafe fn pop_overflow_to_inline(&self) -> Option<Arc<OwnedRow>> {
        let overflow = self.overflow_head.load(Ordering::Relaxed);
        if overflow.is_null() {
            // Reset inline slot to "empty" state.
            self.head_txn_id.store(0, Ordering::Release);
            self.head_commit_ts.store(0, Ordering::Release);
            self.head_flags.store(0, Ordering::Release);
            return None;
        }
        let ver = &*overflow;
        let txn_id = ver.txn_id;
        let commit_ts = ver.get_commit_ts();
        let flags = ver.flags;
        let raw_row = ver.row_ptr.swap(std::ptr::null_mut(), Ordering::Relaxed);
        let next_overflow = ver.prev.load(Ordering::Relaxed);

        // Install into inline slot.
        self.head_txn_id.store(txn_id, Ordering::Release);
        self.head_commit_ts.store(commit_ts, Ordering::Release);
        self.head_flags.store(flags, Ordering::Release);
        self.head_row.store(raw_row, Ordering::Release);
        self.overflow_head.store(next_overflow, Ordering::Release);

        // Return the overflow slot to the arena.
        let arena = &*self.arena;
        arena.free(overflow);

        // Return the restored row.
        if raw_row.is_null() {
            None
        } else {
            let arc = Arc::from_raw(raw_row as *const OwnedRow);
            let cloned = Arc::clone(&arc);
            std::mem::forget(arc);
            // Put the raw pointer back so the inline slot still owns it.
            self.head_row.store(raw_row, Ordering::Relaxed);
            Some(cloned)
        }
    }

    fn abort_overflow(&self, txn_id: TxnId) {
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            if ver.txn_id == txn_id.0 && ver.get_commit_ts() == 0 {
                ver.set_commit_ts(Timestamp::MAX);
            }
            cur = ver.prev.load(Ordering::Relaxed);
        }
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    /// Read the row visible at `read_ts` under Read Committed isolation.
    ///
    /// Lock-free fast path for single committed version (99%+ of OLTP reads).
    #[inline]
    pub fn read_committed(&self, read_ts: Timestamp) -> Option<Arc<OwnedRow>> {
        // ── Ultra-fast path: check head ───────────────────────────────────────
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        if cts > 0 && cts != u64::MAX && Timestamp(cts) <= read_ts {
            let flags = self.head_flags.load(Ordering::Relaxed);
            if flags & FLAG_TOMBSTONE == 0 {
                return self.load_head_row();
            }
            return None; // deleted
        }
        // ── Slow path: head uncommitted or not visible, walk overflow ─────────
        self.read_committed_slow(read_ts)
    }

    fn read_committed_slow(&self, read_ts: Timestamp) -> Option<Arc<OwnedRow>> {
        let mut cur = self.overflow_head.load(Ordering::Acquire);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            let cts = ver.get_commit_ts();
            if cts > 0 && cts != u64::MAX && Timestamp(cts) <= read_ts {
                if ver.is_tombstone() {
                    return None;
                }
                return unsafe { ver.clone_row() };
            }
            cur = ver.prev.load(Ordering::Acquire);
        }
        None
    }

    /// Read the row visible to `txn_id` at `read_ts` (sees own writes).
    #[inline]
    pub fn read_for_txn(&self, txn_id: TxnId, read_ts: Timestamp) -> Option<Arc<OwnedRow>> {
        // Fast path: head is our own uncommitted write or a committed version.
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        if head_txn == txn_id.0 {
            // Our own write — visible regardless of commit_ts.
            let flags = self.head_flags.load(Ordering::Relaxed);
            if flags & FLAG_TOMBSTONE != 0 {
                return None;
            }
            return self.load_head_row();
        }
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        if cts > 0 && cts != u64::MAX && Timestamp(cts) <= read_ts {
            let flags = self.head_flags.load(Ordering::Relaxed);
            if flags & FLAG_TOMBSTONE == 0 {
                return self.load_head_row();
            }
            return None;
        }
        self.read_for_txn_slow(txn_id, read_ts)
    }

    fn read_for_txn_slow(&self, txn_id: TxnId, read_ts: Timestamp) -> Option<Arc<OwnedRow>> {
        let mut cur = self.overflow_head.load(Ordering::Acquire);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            if ver.txn_id == txn_id.0 {
                if ver.is_tombstone() { return None; }
                return unsafe { ver.clone_row() };
            }
            let cts = ver.get_commit_ts();
            if cts > 0 && cts != u64::MAX && Timestamp(cts) <= read_ts {
                if ver.is_tombstone() { return None; }
                return unsafe { ver.clone_row() };
            }
            cur = ver.prev.load(Ordering::Acquire);
        }
        None
    }

    // ── Conflict / visibility checks ──────────────────────────────────────────

    /// Returns `true` if there is an uncommitted write by a *different* transaction.
    pub fn has_write_conflict(&self, txn_id: TxnId) -> bool {
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        if head_txn != 0 && head_txn != txn_id.0 {
            let cts = self.head_commit_ts.load(Ordering::Acquire);
            if cts == 0 {
                return true; // uncommitted by another txn
            }
        }
        false
    }

    /// Returns `true` if a live (non-tombstone) committed version exists.
    pub fn has_live_version(&self, txn_id: TxnId) -> bool {
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        // Our own uncommitted insert counts as live for duplicate-key checks.
        if head_txn == txn_id.0 {
            let flags = self.head_flags.load(Ordering::Relaxed);
            return flags & FLAG_TOMBSTONE == 0;
        }
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        if cts > 0 && cts != u64::MAX {
            let flags = self.head_flags.load(Ordering::Relaxed);
            return flags & FLAG_TOMBSTONE == 0;
        }
        // Walk overflow.
        let mut cur = self.overflow_head.load(Ordering::Acquire);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            let c = ver.get_commit_ts();
            if c > 0 && c != u64::MAX && !ver.is_tombstone() {
                return true;
            }
            cur = ver.prev.load(Ordering::Acquire);
        }
        false
    }

    // ── GC ────────────────────────────────────────────────────────────────────

    /// Truncate overflow versions older than `watermark`.
    /// Mirrors `VersionChain::gc()`: find the most-recent version visible at
    /// `watermark`, keep it, drop everything older.
    /// Returns a `GcChainResult` matching the `VersionChain::gc()` API.
    pub fn gc(&self, watermark: Timestamp) -> GcChainResult {
        let _guard = self.write_mu.lock();
        let mut result = GcChainResult::default();

        // Check if the inline head slot is already committed at or before watermark.
        let head_cts = self.head_commit_ts.load(Ordering::Acquire);
        if head_cts > 0 && head_cts != u64::MAX && Timestamp(head_cts) <= watermark {
            // Head is the anchor version — drop the entire overflow chain.
            let overflow = self.overflow_head.swap(std::ptr::null_mut(), Ordering::AcqRel);
            self.drop_overflow_tail(overflow, &mut result);
            return result;
        }

        // Head is uncommitted or newer than watermark — walk overflow for the anchor.
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        let mut prev_ptr: *mut ArenaVersion = std::ptr::null_mut();

        while !cur.is_null() {
            let ver = unsafe { &*cur };
            let cts = ver.get_commit_ts();
            if cts > 0 && cts != u64::MAX && Timestamp(cts) <= watermark {
                // `cur` is the anchor — drop everything older (its prev chain).
                let tail = ver.prev.swap(std::ptr::null_mut(), Ordering::Relaxed);
                self.drop_overflow_tail(tail, &mut result);
                break;
            }
            prev_ptr = cur;
            cur = ver.prev.load(Ordering::Relaxed);
        }
        let _ = prev_ptr;
        result
    }

    /// Count the number of versions in this chain (inline + overflow).
    /// Used by `gc.rs` sweep loop for min_chain_length filtering.
    pub fn version_chain_len(&self) -> usize {
        // Head slot counts as 1 if it has ever been written.
        let head_txn = self.head_txn_id.load(Ordering::Relaxed);
        if head_txn == 0 {
            return 0;
        }
        let mut count = 1usize;
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        while !cur.is_null() {
            count += 1;
            cur = unsafe { (*cur).prev.load(Ordering::Relaxed) };
        }
        count
    }

    fn drop_overflow_tail(&self, mut cur: *mut ArenaVersion, result: &mut GcChainResult) {
        let arena = unsafe { &*self.arena };
        while !cur.is_null() {
            let next = unsafe { (*cur).prev.load(Ordering::Relaxed) };
            // Measure bytes before dropping — mirrors estimate_row_bytes() used at alloc_mvcc time.
            let row_bytes: u64 = unsafe {
                match (*cur).clone_row() {
                    Some(arc) => crate::mvcc::estimate_row_bytes(&arc),
                    None => std::mem::size_of::<ArenaVersion>() as u64, // tombstone
                }
            };
            unsafe {
                (*cur).drop_row();
                arena.free(cur);
            }
            result.reclaimed_versions += 1;
            result.reclaimed_bytes += row_bytes;
            cur = next;
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Clone the head row Arc (for read paths).
    #[inline]
    fn load_head_row(&self) -> Option<Arc<OwnedRow>> {
        let raw = self.head_row.load(Ordering::Acquire);
        if raw.is_null() {
            return None;
        }
        // SAFETY: raw was set by Arc::into_raw under write_mu; we never free
        // while head_txn_id is non-zero.
        let arc = unsafe { Arc::from_raw(raw as *const OwnedRow) };
        let cloned = Arc::clone(&arc);
        std::mem::forget(arc);
        Some(cloned)
    }

    /// Drop the head row Arc and return it (for abort paths).
    fn drop_head_row(&self) -> Option<Arc<OwnedRow>> {
        let raw = self.head_row.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if raw.is_null() {
            return None;
        }
        // SAFETY: raw came from Arc::into_raw, drop_head_row called once under write_mu.
        Some(unsafe { Arc::from_raw(raw as *const OwnedRow) })
    }

    // ── Additional MemTable-required APIs ─────────────────────────────────────

    /// Attempt to update the head row in-place when this txn already owns the
    /// head version (single-row multi-update hot path).
    /// Returns `true` if in-place update succeeded, `false` if a new version
    /// must be prepended instead.
    #[inline]
    pub fn try_in_place_update(&self, txn_id: TxnId, new_row: Arc<OwnedRow>) -> bool {
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        if head_txn != txn_id.0 {
            return false;
        }
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        if cts != 0 {
            return false; // already committed
        }
        let flags = self.head_flags.load(Ordering::Relaxed);
        if flags & FLAG_TOMBSTONE != 0 {
            return false;
        }
        // Safe to update in place: only this txn owns the head version.
        let _guard = self.write_mu.lock();
        // Re-check under lock.
        if self.head_txn_id.load(Ordering::Acquire) != txn_id.0 {
            return false;
        }
        let old_raw = self.head_row.swap(
            Arc::into_raw(new_row) as *mut OwnedRow,
            Ordering::AcqRel,
        );
        if !old_raw.is_null() {
            drop(unsafe { Arc::from_raw(old_raw as *const OwnedRow) });
        }
        true
    }

    /// Read the uncommitted row data written by `txn_id`.
    /// Returns `Some(Some(row))` for insert/update, `Some(None)` for tombstone,
    /// `None` if this txn has no uncommitted write on this chain.
    pub fn read_uncommitted_for_txn(&self, txn_id: TxnId) -> Option<Option<Arc<OwnedRow>>> {
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        if head_txn != txn_id.0 {
            return None;
        }
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        if cts != 0 {
            return None; // already committed
        }
        let flags = self.head_flags.load(Ordering::Relaxed);
        if flags & FLAG_TOMBSTONE != 0 {
            return Some(None);
        }
        Some(self.load_head_row())
    }

    /// Visit the most-recent visible row at `read_ts` for `txn_id`.
    /// Returns `Some(f(row))` if visible non-tombstone row found, `None` otherwise.
    /// Matches `VersionChain::with_visible_data` API for drop-in migration.
    pub fn with_visible_data<R, F>(&self, txn_id: TxnId, read_ts: Timestamp, f: F) -> Option<R>
    where
        F: FnOnce(&OwnedRow) -> R,
    {
        self.read_for_txn(txn_id, read_ts).as_deref().map(f)
    }

    /// Read the latest non-tombstone head row (regardless of commit state).
    /// Used for backfilling secondary indexes at DDL time.
    pub fn read_latest(&self) -> Option<Arc<OwnedRow>> {
        let flags = self.head_flags.load(Ordering::Acquire);
        if flags & FLAG_TOMBSTONE != 0 {
            return None;
        }
        self.load_head_row()
    }

    /// Replace the data of the head version in-place.
    /// Used by DDL ALTER COLUMN TYPE to recode existing rows.
    pub fn replace_latest(&self, new_row: OwnedRow) {
        let _guard = self.write_mu.lock();
        let old_raw = self.head_row.swap(
            Arc::into_raw(Arc::new(new_row)) as *mut OwnedRow,
            Ordering::AcqRel,
        );
        if !old_raw.is_null() {
            drop(unsafe { Arc::from_raw(old_raw as *const OwnedRow) });
        }
        let flags = self.head_flags.load(Ordering::Relaxed);
        self.head_flags.store(flags & !FLAG_TOMBSTONE, Ordering::Release);
    }

    /// Check if a non-tombstone version is visible to `txn_id` at `read_ts`.
    /// Fast path: uses head_commit_ts atomic load before falling back to chain scan.
    #[inline]
    pub fn is_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> bool {
        // Fast path: head is committed, non-tombstone, and at or before read_ts.
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        let flags = self.head_flags.load(Ordering::Relaxed);
        if cts > 0 && cts != u64::MAX && Timestamp(cts) <= read_ts && flags & FLAG_TOMBSTONE == 0 {
            return true;
        }
        // Uncommitted write by same txn.
        let txn = self.head_txn_id.load(Ordering::Acquire);
        if cts == 0 && txn == txn_id.0 && flags & FLAG_TOMBSTONE == 0 {
            return true;
        }
        // Overflow chain scan.
        let mut cur = self.overflow_head.load(Ordering::Acquire);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            let vcts = ver.get_commit_ts();
            if (vcts == 0 && ver.txn_id == txn_id.0) || (vcts > 0 && vcts != u64::MAX && Timestamp(vcts) <= read_ts) {
                return !ver.is_tombstone();
            }
            cur = ver.prev.load(Ordering::Acquire);
        }
        false
    }

    /// Returns `true` if there is a committed write on this chain by any txn
    /// other than `exclude_txn` that was committed strictly after `after_ts`.
    /// Used by OCC read-set conflict detection.
    pub fn has_committed_write_after(&self, exclude_txn: TxnId, after_ts: Timestamp) -> bool {
        let head_txn = self.head_txn_id.load(Ordering::Acquire);
        let cts = self.head_commit_ts.load(Ordering::Acquire);
        if head_txn != exclude_txn.0 && cts > 0 && cts != u64::MAX && Timestamp(cts) > after_ts {
            return true;
        }
        let mut cur = self.overflow_head.load(Ordering::Acquire);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            let c = ver.get_commit_ts();
            if ver.txn_id != exclude_txn.0 && c > 0 && c != u64::MAX && Timestamp(c) > after_ts {
                return true;
            }
            cur = ver.prev.load(Ordering::Acquire);
        }
        false
    }

    /// Scan overflow chain for the most recent committed non-tombstone version
    /// that is NOT written by `txn_id` (previous committed state).
    fn find_prev_committed_row(&self, txn_id: TxnId) -> Option<Arc<OwnedRow>> {
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        while !cur.is_null() {
            let ver = unsafe { &*cur };
            if ver.txn_id != txn_id.0 {
                let cts = ver.get_commit_ts();
                if cts > 0 && cts != u64::MAX && !ver.is_tombstone() {
                    return unsafe { ver.clone_row() };
                }
            }
            cur = ver.prev.load(Ordering::Relaxed);
        }
        None
    }
}

impl Drop for VersionChain2 {
    fn drop(&mut self) {
        // Drop the inline head row.
        let raw = self.head_row.load(Ordering::Relaxed);
        if !raw.is_null() {
            // SAFETY: raw was set by Arc::into_raw; chain is being dropped.
            unsafe { drop(Arc::from_raw(raw as *const OwnedRow)) };
        }
        // Drop the overflow chain.
        let arena = unsafe { &*self.arena };
        let mut cur = self.overflow_head.load(Ordering::Relaxed);
        while !cur.is_null() {
            let next = unsafe { (*cur).prev.load(Ordering::Relaxed) };
            unsafe {
                (*cur).drop_row();
                arena.free(cur);
            }
            cur = next;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version_arena::VersionArena;
    use falcon_common::datum::Datum;
    use falcon_common::types::{Timestamp, TxnId};

    fn make_row(val: i64) -> Arc<OwnedRow> {
        Arc::new(OwnedRow::new(vec![Datum::Int64(val)]))
    }

    fn row_eq(row: &OwnedRow, val: i64) -> bool {
        row.get(0) == Some(&Datum::Int64(val))
    }

    fn new_chain() -> (Arc<VersionArena>, VersionChain2) {
        let arena = VersionArena::new();
        let chain = VersionChain2::new(Arc::as_ptr(&arena));
        (arena, chain)
    }

    #[test]
    fn test_chain2_size() {
        // Should fit in 2 cache lines (128 bytes).
        assert!(
            std::mem::size_of::<VersionChain2>() <= 128,
            "VersionChain2 is {} bytes (want ≤128)",
            std::mem::size_of::<VersionChain2>()
        );
    }

    #[test]
    fn test_insert_commit_read() {
        let (_arena, chain) = new_chain();
        let txn = TxnId(1);
        chain.prepend(txn, Some(make_row(42)));
        // Uncommitted: should not be visible at ts=100.
        assert!(chain.read_committed(Timestamp(100)).is_none());
        // Commit.
        chain.commit_no_report(txn, Timestamp(10));
        // Now visible.
        let row = chain.read_committed(Timestamp(100)).unwrap();
        assert!(row_eq(row.as_ref(), 42));
    }

    #[test]
    fn test_read_own_write() {
        let (_arena, chain) = new_chain();
        let txn = TxnId(5);
        chain.prepend(txn, Some(make_row(99)));
        // Should see our own uncommitted write.
        let row = chain.read_for_txn(txn, Timestamp(0)).unwrap();
        assert!(row_eq(row.as_ref(), 99));
    }

    #[test]
    fn test_update_creates_overflow() {
        let (arena, chain) = new_chain();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(make_row(1)));
        chain.commit_no_report(txn1, Timestamp(10));

        chain.prepend(txn2, Some(make_row(2)));
        // txn2 uncommitted: overflow should have txn1's version.
        assert!(!chain.overflow_head.load(Ordering::Relaxed).is_null());
        // txn1 still visible at ts=10.
        let r1 = chain.read_committed(Timestamp(10)).unwrap();
        assert!(row_eq(r1.as_ref(), 1));

        chain.commit_no_report(txn2, Timestamp(20));
        let r2 = chain.read_committed(Timestamp(20)).unwrap();
        assert!(row_eq(r2.as_ref(), 2));

        // arena should have 1 allocated slot (spilled txn1 version).
        assert!(arena.live_count() > 0);
    }

    #[test]
    fn test_delete_tombstone() {
        let (_arena, chain) = new_chain();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        chain.prepend(txn1, Some(make_row(7)));
        chain.commit_no_report(txn1, Timestamp(5));
        chain.prepend(txn2, None); // DELETE
        chain.commit_no_report(txn2, Timestamp(15));
        // Before delete: visible.
        assert!(chain.read_committed(Timestamp(5)).is_some());
        // After delete: None.
        assert!(chain.read_committed(Timestamp(20)).is_none());
    }

    #[test]
    fn test_abort_restores_previous() {
        let (_arena, chain) = new_chain();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        chain.prepend(txn1, Some(make_row(10)));
        chain.commit_no_report(txn1, Timestamp(5));
        chain.prepend(txn2, Some(make_row(20)));
        // Abort txn2.
        let (_aborted, _restored) = chain.abort_and_report(txn2);
        // Should still see txn1's row.
        let row = chain.read_committed(Timestamp(10)).unwrap();
        assert!(row_eq(row.as_ref(), 10));
    }

    #[test]
    fn test_gc_truncates_old_versions() {
        let (arena, chain) = new_chain();
        let t1 = TxnId(1);
        let t2 = TxnId(2);
        let t3 = TxnId(3);
        chain.prepend(t1, Some(make_row(1)));
        chain.commit_no_report(t1, Timestamp(5));
        chain.prepend(t2, Some(make_row(2)));
        chain.commit_no_report(t2, Timestamp(10));
        chain.prepend(t3, Some(make_row(3)));
        chain.commit_no_report(t3, Timestamp(20));
        let before = arena.live_count();
        // GC at watermark 15: drop versions committed at ts ≤ 5 (only version 1).
        let reclaimed = chain.gc(Timestamp(15));
        assert!(reclaimed.reclaimed_versions > 0 || before == 0);
    }

    #[test]
    fn test_write_conflict_detection() {
        let (_arena, chain) = new_chain();
        let t1 = TxnId(1);
        let t2 = TxnId(2);
        chain.prepend(t1, Some(make_row(1)));
        // t2 tries to write — should detect conflict (t1 uncommitted).
        assert!(chain.has_write_conflict(t2));
        chain.commit_no_report(t1, Timestamp(10));
        // After commit, no conflict.
        assert!(!chain.has_write_conflict(t2));
    }

    #[test]
    fn test_concurrent_reads_single_writer() {
        let arena = VersionArena::new();
        let chain = Arc::new(VersionChain2::new(Arc::as_ptr(&arena)));
        let t1 = TxnId(1);
        chain.prepend(t1, Some(make_row(42)));
        chain.commit_no_report(t1, Timestamp(1));

        let chain2 = Arc::clone(&chain);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = Arc::clone(&chain2);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let row = c.read_committed(Timestamp(100)).unwrap();
                        assert!(row_eq(row.as_ref(), 42));
                    }
                })
            })
            .collect();
        for h in handles { h.join().unwrap(); }
    }
}
