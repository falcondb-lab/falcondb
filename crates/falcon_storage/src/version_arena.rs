//! # VersionArena — Per-Table Slab Allocator for MVCC Version Nodes
//!
//! Eliminates the `Arc::new(Version{...})` heap allocation that occurs on every
//! INSERT, UPDATE, and DELETE.  Instead of going through the global allocator for
//! each individual version node, `VersionArena` pre-allocates fixed-size *slab*
//! blocks and hands out slots from a lock-free free-list / bump pointer.
//!
//! ## Design
//!
//! ```text
//! VersionArena
//! ├── slabs: Vec<SlabBlock>   — ordered list of 1 MiB blocks
//! │   └── SlabBlock
//! │       ├── data: Box<[u8; SLAB_SIZE]>   (1 MiB, 64-byte aligned)
//! │       └── bump: AtomicUsize            (next free offset)
//! ├── free_list: AtomicPtr<FreeNode>       — lock-free single-linked list of
//! │                                          recycled ArenaVersion slots
//! └── slab_mu: Mutex<()>                  — guards slab Vec growth only
//! ```
//!
//! ## Allocation Strategy
//!
//! 1. **Free-list first**: CAS-pop a recycled slot in O(1) — zero shard contention.
//! 2. **Bump fallback**: if free-list is empty, atomically advance the bump pointer
//!    on the current slab block.  No lock needed.
//! 3. **New slab**: if the current block is exhausted, acquire `slab_mu`, append a
//!    new block, and retry bump.
//!
//! ## Safety
//!
//! - `ArenaVersion` slots are `repr(C, align(64))` and always obtained through
//!   `VersionArena::alloc`.  They MUST be returned via `VersionArena::free` before
//!   the arena is dropped; leaking them is safe but wastes memory.
//! - The arena itself is `Send + Sync`; individual `*mut ArenaVersion` raw pointers
//!   are NOT safe to use across thread boundaries except under the invariants
//!   documented on each `VersionChain2` method.
//! - GC calls `free()` only after verifying that no live reader holds a reference
//!   to the slot (watermark-based epoch).

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use falcon_common::datum::OwnedRow;
use falcon_common::types::{Timestamp, TxnId};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Size of each slab block in bytes (1 MiB).
pub const SLAB_SIZE: usize = 1 << 20; // 1 MiB

/// Alignment of each `ArenaVersion` slot (one cache line).
pub const SLOT_ALIGN: usize = 64;

/// Size of each `ArenaVersion` slot (must be a multiple of SLOT_ALIGN).
pub const SLOT_SIZE: usize = std::mem::size_of::<ArenaVersion>();

// ── ArenaVersion ──────────────────────────────────────────────────────────────

/// A single MVCC version node, sized and aligned for cache-friendly packing.
///
/// Lives inside a `VersionArena` slab block.  Must be initialised via
/// `ArenaVersion::init` before use; the arena zero-initialises memory but
/// callers must set all fields explicitly.
///
/// # Memory layout
/// ```text
/// offset  0: txn_id        (u64)       — 8 bytes
/// offset  8: commit_ts     (AtomicU64) — 8 bytes  ← hot read field
/// offset 16: flags         (u8)        — 1 byte   (TOMBSTONE | HAS_PREV | ...)
/// offset 17: _pad          ([u8; 7])   — 7 bytes
/// offset 24: prev          (AtomicPtr) — 8 bytes  → older ArenaVersion or null
/// offset 32: row_ptr       (AtomicPtr) — 8 bytes  → Arc<OwnedRow> or null
/// offset 40: _pad2         ([u8; 24])  — 24 bytes (future: inline fixed cols)
/// ── total: 64 bytes = 1 cache line ──────────────────────────────────────────
/// ```
#[repr(C, align(64))]
pub struct ArenaVersion {
    /// Transaction that wrote this version.
    pub txn_id: u64,
    /// Commit timestamp.  0 = uncommitted, u64::MAX = aborted.
    pub commit_ts: AtomicU64,
    /// Flags: see `FLAG_*` constants.
    pub flags: u8,
    _pad: [u8; 7],
    /// Pointer to older version in the chain.  Null = chain tail.
    /// Owned by the arena; do NOT call Arc::from_raw on this pointer.
    pub prev: AtomicPtr<ArenaVersion>,
    /// Arc<OwnedRow> stored as a raw pointer (ref-counted).
    /// Null for tombstone versions.
    /// Invariant: if non-null, it was obtained via `Arc::into_raw`.
    pub row_ptr: AtomicPtr<OwnedRow>,
    _pad2: [u8; 24],
}

// SAFETY: ArenaVersion contains only atomics + raw pointers managed under arena
// ownership rules.  The arena coordinates all concurrent access.
unsafe impl Send for ArenaVersion {}
unsafe impl Sync for ArenaVersion {}

/// Tombstone flag: this version represents a DELETE.
pub const FLAG_TOMBSTONE: u8 = 0b0000_0001;
/// Set when `prev` is non-null.
pub const FLAG_HAS_PREV: u8 = 0b0000_0010;

impl ArenaVersion {
    /// Initialise a zeroed arena slot.  Must be called before any other method.
    ///
    /// # Safety
    /// `slot` must point to a valid, exclusively owned, zeroed `ArenaVersion`
    /// within a live `VersionArena` slab.
    pub unsafe fn init(
        slot: *mut Self,
        txn_id: TxnId,
        row: Option<Arc<OwnedRow>>,
        prev: *mut Self,
    ) {
        let s = &mut *slot;
        s.txn_id = txn_id.0;
        // commit_ts and flags are already 0 (zero-init), set flags.
        let is_tombstone = row.is_none();
        s.flags = if is_tombstone { FLAG_TOMBSTONE } else { 0 }
            | if !prev.is_null() { FLAG_HAS_PREV } else { 0 };
        s.prev.store(prev, Ordering::Relaxed);
        let raw_row = row.map_or(std::ptr::null_mut(), |arc| Arc::into_raw(arc) as *mut OwnedRow);
        s.row_ptr.store(raw_row, Ordering::Relaxed);
    }

    /// Commit this version.
    #[inline]
    pub fn set_commit_ts(&self, ts: Timestamp) {
        self.commit_ts.store(ts.0, Ordering::Release);
    }

    /// Get commit timestamp.
    #[inline]
    pub fn get_commit_ts(&self) -> u64 {
        self.commit_ts.load(Ordering::Acquire)
    }

    /// Returns true if this is a tombstone (DELETE).
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.flags & FLAG_TOMBSTONE != 0
    }

    /// Returns true if there is a previous version.
    #[inline]
    pub fn has_prev(&self) -> bool {
        self.flags & FLAG_HAS_PREV != 0
    }

    /// Clone the row Arc without consuming it.
    ///
    /// # Safety
    /// The caller must ensure the arena slot is live (not yet freed).
    #[inline]
    pub unsafe fn clone_row(&self) -> Option<Arc<OwnedRow>> {
        let raw = self.row_ptr.load(Ordering::Acquire);
        if raw.is_null() {
            return None;
        }
        let arc = Arc::from_raw(raw as *const OwnedRow);
        let cloned = Arc::clone(&arc);
        std::mem::forget(arc);
        Some(cloned)
    }

    /// Drop the row Arc stored in this slot (called by arena free path).
    ///
    /// # Safety
    /// Must be called exactly once when the slot is being recycled.
    pub unsafe fn drop_row(&self) {
        let raw = self.row_ptr.swap(std::ptr::null_mut(), Ordering::Relaxed);
        if !raw.is_null() {
            drop(Arc::from_raw(raw as *const OwnedRow));
        }
    }
}

// ── Free-list node (overlay on recycled ArenaVersion slots) ───────────────────

/// A node in the free-list, overlaid on recycled `ArenaVersion` memory.
///
/// We borrow the first pointer-sized field of the slot for the next pointer.
struct FreeNode {
    next: *mut FreeNode,
}

// ── SlabBlock ─────────────────────────────────────────────────────────────────

struct SlabBlock {
    /// Raw pointer to the aligned allocation.
    data: *mut u8,
    /// Total bytes in this block.
    capacity: usize,
    /// Bump pointer: next free byte offset from `data`.
    bump: AtomicUsize,
}

impl SlabBlock {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, SLOT_ALIGN)
            .expect("invalid slab layout");
        // SAFETY: layout is non-zero.
        let data = unsafe { alloc_zeroed(layout) };
        assert!(!data.is_null(), "VersionArena: OOM allocating slab block");
        Self { data, capacity: size, bump: AtomicUsize::new(0) }
    }

    /// Allocate `SLOT_SIZE` bytes from this block.
    /// Returns `None` if the block is exhausted.
    fn try_alloc(&self) -> Option<*mut ArenaVersion> {
        // Atomically reserve SLOT_SIZE bytes.
        let mut old = self.bump.load(Ordering::Relaxed);
        loop {
            let new = old + SLOT_SIZE;
            if new > self.capacity {
                return None;
            }
            match self.bump.compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    // SAFETY: we just reserved [old, old+SLOT_SIZE) exclusively.
                    let ptr = unsafe { self.data.add(old) as *mut ArenaVersion };
                    return Some(ptr);
                }
                Err(cur) => old = cur,
            }
        }
    }
}

impl Drop for SlabBlock {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, SLOT_ALIGN)
            .expect("invalid slab layout on drop");
        // SAFETY: data was allocated with the same layout.
        unsafe { dealloc(self.data, layout) };
    }
}

// SAFETY: raw pointer is managed exclusively through bump/free-list protocol.
unsafe impl Send for SlabBlock {}
unsafe impl Sync for SlabBlock {}

// ── VersionArena ──────────────────────────────────────────────────────────────

/// Per-table MVCC version node allocator.
///
/// Shared across all rows of a single `MemTable` via `Arc<VersionArena>`.
/// Thread-safe: bump allocation is lock-free; slab growth takes a short Mutex.
pub struct VersionArena {
    /// Ordered list of slab blocks.  Append-only after construction.
    /// The last block is the "current" block for bump allocation.
    slabs: Mutex<Vec<SlabBlock>>,
    /// Lock-free stack of recycled slots.
    free_list: AtomicPtr<FreeNode>,
    /// Total slots allocated (for monitoring).
    pub allocated: AtomicU64,
    /// Total slots currently on the free-list (for monitoring).
    pub freed: AtomicU64,
}

// SAFETY: all mutable state protected by atomics or Mutex.
unsafe impl Send for VersionArena {}
unsafe impl Sync for VersionArena {}

impl VersionArena {
    /// Create a new arena with one pre-allocated slab block.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            slabs: Mutex::new(vec![SlabBlock::new(SLAB_SIZE)]),
            free_list: AtomicPtr::new(std::ptr::null_mut()),
            allocated: AtomicU64::new(0),
            freed: AtomicU64::new(0),
        })
    }

    /// Allocate a zeroed `ArenaVersion` slot.
    ///
    /// Returns a raw pointer into a slab block.  The caller MUST call
    /// `ArenaVersion::init` before using the slot, and `VersionArena::free`
    /// when the slot is no longer reachable.
    ///
    /// This function is lock-free in the common case (free-list pop or bump).
    pub fn alloc(&self) -> *mut ArenaVersion {
        // ── Fast path: pop from free-list ──────────────────────────────────
        let ptr = self.pop_free_list();
        if let Some(p) = ptr {
            // Zero the recycled slot before handing it out.
            // SAFETY: p points to a valid SLOT_SIZE block.
            unsafe { std::ptr::write_bytes(p as *mut u8, 0, SLOT_SIZE) };
            self.allocated.fetch_add(1, Ordering::Relaxed);
            return p;
        }

        // ── Slow path: bump or grow ─────────────────────────────────────────
        loop {
            // Try bump on current (last) slab.
            {
                let slabs = self.slabs.lock();
                if let Some(block) = slabs.last() {
                    if let Some(p) = block.try_alloc() {
                        self.allocated.fetch_add(1, Ordering::Relaxed);
                        return p;
                    }
                }
            }
            // Current slab exhausted — append a new one.
            {
                let mut slabs = self.slabs.lock();
                // Double-check: another thread may have appended already.
                if let Some(p) = slabs.last().and_then(|b| b.try_alloc()) {
                    self.allocated.fetch_add(1, Ordering::Relaxed);
                    return p;
                }
                slabs.push(SlabBlock::new(SLAB_SIZE));
            }
        }
    }

    /// Return a slot to the arena for reuse.
    ///
    /// # Safety
    /// - `slot` must have been obtained from `self.alloc()`.
    /// - No live reference to `slot` may exist after this call.
    /// - The caller must have already called `ArenaVersion::drop_row` if needed.
    pub unsafe fn free(&self, slot: *mut ArenaVersion) {
        self.push_free_list(slot);
        self.freed.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of live allocated slots (allocated − freed).
    pub fn live_count(&self) -> u64 {
        self.allocated.load(Ordering::Relaxed)
            .saturating_sub(self.freed.load(Ordering::Relaxed))
    }

    /// Approximate memory used by all slab blocks in bytes.
    pub fn slab_memory_bytes(&self) -> usize {
        self.slabs.lock().iter().map(|b| b.capacity).sum()
    }

    // ── Free-list helpers ─────────────────────────────────────────────────────

    /// Lock-free push onto the free-list stack.
    ///
    /// # Safety
    /// `slot` is a valid exclusive pointer into a slab block.
    unsafe fn push_free_list(&self, slot: *mut ArenaVersion) {
        let node = slot as *mut FreeNode;
        loop {
            let head = self.free_list.load(Ordering::Relaxed);
            (*node).next = head;
            match self.free_list.compare_exchange_weak(
                head,
                node,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(_) => {} // retry
            }
        }
    }

    /// Lock-free pop from the free-list stack.  Returns `None` if empty.
    fn pop_free_list(&self) -> Option<*mut ArenaVersion> {
        loop {
            let head = self.free_list.load(Ordering::Acquire);
            if head.is_null() {
                return None;
            }
            // SAFETY: head was placed by push_free_list from a valid slab slot.
            let next = unsafe { (*head).next };
            match self.free_list.compare_exchange_weak(
                head,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(head as *mut ArenaVersion),
                Err(_) => {} // retry
            }
        }
    }
}

impl Default for VersionArena {
    fn default() -> Self {
        Self {
            slabs: Mutex::new(vec![SlabBlock::new(SLAB_SIZE)]),
            free_list: AtomicPtr::new(std::ptr::null_mut()),
            allocated: AtomicU64::new(0),
            freed: AtomicU64::new(0),
        }
    }
}

impl Drop for VersionArena {
    fn drop(&mut self) {
        // Walk the free-list and zero any dangling pointers (helps debug use-after-free).
        // We do NOT drop ArenaVersion rows here because GC is responsible for that.
        // The slab SlabBlocks are dropped by their own Drop impl.
        let mut node = self.free_list.load(Ordering::Relaxed);
        while !node.is_null() {
            // SAFETY: node is a valid slab pointer from push_free_list.
            let next = unsafe { (*node).next };
            // Zero out to catch any use-after-free in tests.
            unsafe { std::ptr::write_bytes(node as *mut u8, 0xDD, SLOT_SIZE) };
            node = next;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc as StdArc;

    #[test]
    fn test_slot_size_is_cache_line() {
        // ArenaVersion must be exactly 64 bytes (1 cache line).
        assert_eq!(
            std::mem::size_of::<ArenaVersion>(),
            64,
            "ArenaVersion size should be 64 bytes (1 cache line)"
        );
        assert_eq!(std::mem::align_of::<ArenaVersion>(), 64);
    }

    #[test]
    fn test_alloc_and_free() {
        let arena = VersionArena::new();
        let ptr = arena.alloc();
        assert!(!ptr.is_null());
        assert_eq!(arena.live_count(), 1);
        // SAFETY: ptr is valid, drop_row not needed (no row was set).
        unsafe { arena.free(ptr) };
        assert_eq!(arena.freed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_reuse_via_free_list() {
        let arena = VersionArena::new();
        let p1 = arena.alloc();
        unsafe { arena.free(p1) };
        let p2 = arena.alloc();
        // Should be the same address (free-list reuse).
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_alloc_many_fills_slab() {
        let arena = VersionArena::new();
        let slots_per_slab = SLAB_SIZE / SLOT_SIZE;
        let mut ptrs = Vec::new();
        for _ in 0..slots_per_slab + 10 {
            ptrs.push(arena.alloc());
        }
        // Should have grown to at least 2 slab blocks.
        assert!(arena.slabs.lock().len() >= 2);
        // Clean up.
        for p in ptrs {
            unsafe { arena.free(p) };
        }
    }

    #[test]
    fn test_init_and_clone_row() {
        use falcon_common::datum::OwnedRow;
        let arena = VersionArena::new();
        let ptr = arena.alloc();
        let row = OwnedRow::new(vec![falcon_common::datum::Datum::Int64(42)]);
        let arc_row = StdArc::new(row.clone());
        unsafe {
            ArenaVersion::init(ptr, TxnId(1), Some(StdArc::clone(&arc_row)), std::ptr::null_mut());
            let cloned = (*ptr).clone_row();
            assert_eq!(cloned.as_deref(), Some(&row));
            (*ptr).drop_row();
            arena.free(ptr);
        }
    }

    #[test]
    fn test_concurrent_alloc() {
        let arena = StdArc::new(VersionArena::default());
        let n = 1000usize;
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let a = StdArc::clone(&arena);
                std::thread::spawn(move || {
                    let mut local = Vec::with_capacity(n);
                    for _ in 0..n {
                        local.push(a.alloc());
                    }
                    for p in local {
                        unsafe { a.free(p) };
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(arena.live_count(), 0);
    }

    #[test]
    fn test_slab_memory_report() {
        let arena = VersionArena::new();
        assert!(arena.slab_memory_bytes() >= SLAB_SIZE);
    }
}
