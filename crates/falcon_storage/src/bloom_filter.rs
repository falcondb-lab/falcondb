//! # Bloom Filter (P-bloom)
//!
//! A space-efficient probabilistic data structure for membership testing.
//! Used as a pre-filter before secondary index lookups and point reads to
//! eliminate definitely-absent keys without touching the B-Tree / DashMap.
//!
//! ## Design
//! - Double-hashing scheme with k independent hash functions derived from
//!   two 64-bit FNV-1a hashes.  No external dependencies required.
//! - Fixed-size bit array (`BloomFilter`) for per-MemTable use.
//! - Scalable variant (`ScalableBloomFilter`) that grows via filter chaining
//!   as more elements are inserted, maintaining a stable false-positive rate.
//!
//! ## Parameters
//! - Default false-positive rate: 1% (k=7, m/n ≈ 9.6 bits/element).
//! - Minimum capacity: 64 elements.
//!
//! ## Usage
//! ```rust,ignore
//! let mut bf = BloomFilter::with_capacity_and_fpr(100_000, 0.01);
//! bf.insert(b"some_key");
//! assert!(bf.contains(b"some_key"));       // true positive
//! assert!(!bf.contains(b"absent_key"));    // true negative (with high prob)
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Default target false-positive rate: 1%.
pub const DEFAULT_FPR: f64 = 0.01;

/// Minimum number of bits per element (for FPR ≤ 10%).
const MIN_BITS_PER_ELEM: f64 = 6.0;

/// Maximum number of hash functions (caps memory overhead).
const MAX_HASH_FUNCS: usize = 20;

// ── Bit Array ─────────────────────────────────────────────────────────────────

/// Lock-free bit array backed by `AtomicU64` words.
///
/// Set operations use `fetch_or` (relaxed) — safe for concurrent inserts.
/// Read operations use `load` (relaxed) — no ordering needed for a filter.
pub struct BitArray {
    words: Vec<AtomicU64>,
    num_bits: usize,
}

impl BitArray {
    pub fn new(num_bits: usize) -> Self {
        let words = (num_bits + 63) / 64;
        Self {
            words: (0..words).map(|_| AtomicU64::new(0)).collect(),
            num_bits,
        }
    }

    #[inline]
    pub fn set(&self, bit: usize) {
        let word = bit / 64;
        let mask = 1u64 << (bit % 64);
        self.words[word].fetch_or(mask, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self, bit: usize) -> bool {
        let word = bit / 64;
        let mask = 1u64 << (bit % 64);
        self.words[word].load(Ordering::Relaxed) & mask != 0
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Merge another bit array into this one (for combining shard filters).
    pub fn merge(&self, other: &BitArray) {
        assert_eq!(self.words.len(), other.words.len(), "BitArray size mismatch");
        for (a, b) in self.words.iter().zip(other.words.iter()) {
            a.fetch_or(b.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    /// Serialize to a compact byte vector (little-endian u64 words).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.words.len() * 8);
        for w in &self.words {
            out.extend_from_slice(&w.load(Ordering::Relaxed).to_le_bytes());
        }
        out
    }

    /// Deserialize from bytes (inverse of `to_bytes`).
    pub fn from_bytes(bytes: &[u8], num_bits: usize) -> Self {
        let words_needed = (num_bits + 63) / 64;
        let mut words = Vec::with_capacity(words_needed);
        for i in 0..words_needed {
            let offset = i * 8;
            let val = if offset + 8 <= bytes.len() {
                u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            words.push(AtomicU64::new(val));
        }
        Self { words, num_bits }
    }
}

impl std::fmt::Debug for BitArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitArray(bits={}, words={})", self.num_bits, self.words.len())
    }
}

// ── Double-hash generation ─────────────────────────────────────────────────────

/// Generate `k` bit positions using enhanced double hashing.
///
/// Uses two FNV-1a 64-bit hash seeds to derive k independent hashes:
///   h_i(x) = (h1(x) + i * h2(x)) % m
///
/// This avoids the O(k) hashing cost while maintaining independence.
#[inline]
fn hash_positions(key: &[u8], k: usize, m: usize) -> impl Iterator<Item = usize> {
    let (h1, h2) = fnv1a_dual(key);
    (0..k).map(move |i| {
        h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize % m
    })
}

/// Compute two independent FNV-1a 64-bit hashes by seeding differently.
#[inline]
fn fnv1a_dual(key: &[u8]) -> (u64, u64) {
    const FNV_PRIME: u64 = 0x00000100000001B3;
    const SEED1: u64 = 0xcbf29ce484222325;
    const SEED2: u64 = 0x517cc1b727220a95;

    let mut h1 = SEED1;
    let mut h2 = SEED2;
    for &b in key {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(FNV_PRIME);
        h2 ^= u64::from(b);
        h2 = h2.wrapping_mul(FNV_PRIME);
        h2 = h2.rotate_left(17);
    }
    (h1, h2 | 1) // h2 must be odd for double-hashing correctness
}

// ── BloomFilter ───────────────────────────────────────────────────────────────

/// Fixed-capacity Bloom filter.
///
/// Thread-safe for concurrent inserts and lookups (lock-free via `AtomicU64`).
/// Not safe for concurrent serialization during modification.
pub struct BloomFilter {
    bits: BitArray,
    k: usize,
    /// Approximate number of elements inserted (non-atomic counter, best-effort).
    count: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BloomFilter(bits={}, k={}, count={})",
            self.bits.num_bits(),
            self.k,
            self.count.load(Ordering::Relaxed)
        )
    }
}

impl BloomFilter {
    /// Create a filter sized for `capacity` elements at `fpr` false-positive rate.
    pub fn with_capacity_and_fpr(capacity: usize, fpr: f64) -> Self {
        let (m, k) = optimal_params(capacity.max(64), fpr);
        Self {
            bits: BitArray::new(m),
            k,
            count: AtomicU64::new(0),
        }
    }

    /// Create a filter with explicit bit count and hash function count.
    pub fn with_params(num_bits: usize, k: usize) -> Self {
        Self {
            bits: BitArray::new(num_bits.max(64)),
            k: k.clamp(1, MAX_HASH_FUNCS),
            count: AtomicU64::new(0),
        }
    }

    /// Insert a key into the filter.
    pub fn insert(&self, key: &[u8]) {
        let m = self.bits.num_bits();
        for pos in hash_positions(key, self.k, m) {
            self.bits.set(pos);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns `false` if the key is **definitely** not in the set.
    /// Returns `true` if the key **may** be in the set (possible false positive).
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        let m = self.bits.num_bits();
        hash_positions(key, self.k, m).all(|pos| self.bits.get(pos))
    }

    /// Number of bits in the filter.
    pub fn num_bits(&self) -> usize {
        self.bits.num_bits()
    }

    /// Number of hash functions.
    pub fn num_hashes(&self) -> usize {
        self.k
    }

    /// Approximate element count (best-effort, non-atomic).
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Estimated current false-positive rate based on fill level.
    pub fn estimated_fpr(&self) -> f64 {
        let n = self.count.load(Ordering::Relaxed) as f64;
        let m = self.bits.num_bits() as f64;
        let k = self.k as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Serialize to bytes (for persistence or transfer).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.bits.num_bits() as u64).to_le_bytes());
        out.extend_from_slice(&(self.k as u64).to_le_bytes());
        out.extend_from_slice(&self.bits.to_bytes());
        out
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 16 { return None; }
        let num_bits = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let k = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
        let bits = BitArray::from_bytes(&data[16..], num_bits);
        Some(Self {
            bits,
            k,
            count: AtomicU64::new(0),
        })
    }

    /// Merge another filter into this one (union).
    /// Both filters must have identical parameters.
    pub fn merge(&self, other: &BloomFilter) {
        assert_eq!(self.bits.num_bits(), other.bits.num_bits(), "filter size mismatch");
        assert_eq!(self.k, other.k, "filter hash count mismatch");
        self.bits.merge(&other.bits);
        self.count.fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

// ── Scalable Bloom Filter ─────────────────────────────────────────────────────

/// Scalable Bloom filter — grows automatically as elements are inserted.
///
/// Uses a chain of `BloomFilter` slices, each with a tightened FPR budget
/// (multiplied by `TIGHTENING_RATIO` per slice).  New elements are always
/// inserted into the latest slice; lookups check all slices.
///
/// This guarantees the overall FPR never exceeds the initial target,
/// regardless of how many elements are inserted.
pub struct ScalableBloomFilter {
    filters: Vec<BloomFilter>,
    /// Per-filter capacity before growing.
    initial_capacity: usize,
    /// Initial false-positive rate target.
    target_fpr: f64,
    /// FPR tightening ratio per slice (typically 0.5).
    tightening_ratio: f64,
}

impl std::fmt::Debug for ScalableBloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScalableBloomFilter(slices={}, initial_cap={}, fpr={:.4})",
            self.filters.len(),
            self.initial_capacity,
            self.target_fpr
        )
    }
}

const TIGHTENING_RATIO: f64 = 0.5;

impl ScalableBloomFilter {
    /// Create a scalable filter starting at `initial_capacity` elements.
    pub fn new(initial_capacity: usize, target_fpr: f64) -> Self {
        let first = BloomFilter::with_capacity_and_fpr(initial_capacity, target_fpr * TIGHTENING_RATIO);
        Self {
            filters: vec![first],
            initial_capacity: initial_capacity.max(64),
            target_fpr,
            tightening_ratio: TIGHTENING_RATIO,
        }
    }

    /// Insert a key, growing the filter chain if the last slice is saturated.
    pub fn insert(&mut self, key: &[u8]) {
        let last = self.filters.last().unwrap();
        // Grow when current slice is over 80% of its capacity.
        let ratio = last.count() as f64 / self.initial_capacity as f64;
        if ratio > 0.8 {
            let new_fpr = self.target_fpr
                * self.tightening_ratio.powi(self.filters.len() as i32 + 1);
            let new_cap = self.initial_capacity * (1 << self.filters.len());
            self.filters.push(BloomFilter::with_capacity_and_fpr(new_cap, new_fpr));
        }
        self.filters.last().unwrap().insert(key);
    }

    /// Check membership — false means definitely absent; true means possibly present.
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.filters.iter().any(|f| f.contains(key))
    }

    /// Total number of bits across all slices.
    pub fn total_bits(&self) -> usize {
        self.filters.iter().map(|f| f.num_bits()).sum()
    }

    /// Total number of elements inserted across all slices.
    pub fn total_count(&self) -> u64 {
        self.filters.iter().map(|f| f.count()).sum()
    }

    /// Number of filter slices.
    pub fn num_slices(&self) -> usize {
        self.filters.len()
    }
}

// ── Parameter computation ─────────────────────────────────────────────────────

/// Compute optimal (m, k) for a Bloom filter with `n` elements and `fpr`.
///
/// m = -n * ln(fpr) / (ln(2)^2)
/// k = (m / n) * ln(2)
pub fn optimal_params(n: usize, fpr: f64) -> (usize, usize) {
    let fpr = fpr.clamp(1e-10, 0.5);
    let ln2 = std::f64::consts::LN_2;
    let m = (-(n as f64) * fpr.ln() / (ln2 * ln2)).ceil() as usize;
    let m = m.max(64); // minimum 64 bits
    let bpe = (m as f64) / (n as f64);
    let k = (bpe * ln2).round() as usize;
    let k = k.clamp(1, MAX_HASH_FUNCS);
    (m, k)
}

// ── MemTable integration helper ───────────────────────────────────────────────

/// Per-table Bloom filter that wraps an encoded primary key.
///
/// Stored inside `MemTable` (or `TableHandle`) and updated on every insert.
/// Point lookups check the filter first; a definitive `false` avoids a
/// DashMap shard lock acquisition entirely.
pub struct TableBloomFilter {
    inner: BloomFilter,
    /// Expected table size at filter creation (for sizing decisions).
    pub initial_capacity: usize,
}

impl std::fmt::Debug for TableBloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableBloomFilter({:?})", self.inner)
    }
}

impl TableBloomFilter {
    /// Create sized for an expected `row_count` rows.
    pub fn new(row_count: usize) -> Self {
        let capacity = row_count.max(1024);
        Self {
            inner: BloomFilter::with_capacity_and_fpr(capacity, DEFAULT_FPR),
            initial_capacity: capacity,
        }
    }

    /// Insert a primary key (already encoded as bytes).
    #[inline]
    pub fn insert_pk(&self, pk: &[u8]) {
        self.inner.insert(pk);
    }

    /// Returns `false` if the pk is **definitely** absent (safe to skip lookup).
    /// Returns `true` if the pk **may** be present (must do real lookup).
    #[inline]
    pub fn may_contain_pk(&self, pk: &[u8]) -> bool {
        self.inner.contains(pk)
    }

    /// Estimated false-positive rate.
    pub fn estimated_fpr(&self) -> f64 {
        self.inner.estimated_fpr()
    }

    /// Number of bits.
    pub fn num_bits(&self) -> usize {
        self.inner.num_bits()
    }

    /// Number of elements inserted.
    pub fn count(&self) -> u64 {
        self.inner.count()
    }

    /// Merge another filter (for combining shard-local filters into a global one).
    pub fn merge(&self, other: &TableBloomFilter) {
        self.inner.merge(&other.inner);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_contains() {
        let bf = BloomFilter::with_capacity_and_fpr(1000, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");
        assert!(bf.contains(b"hello"));
        assert!(bf.contains(b"world"));
        assert!(!bf.contains(b"absent"));
    }

    #[test]
    fn test_fpr_within_bounds() {
        let n = 10_000usize;
        let target_fpr = 0.01;
        let bf = BloomFilter::with_capacity_and_fpr(n, target_fpr);

        for i in 0..n {
            bf.insert(format!("key-{i}").as_bytes());
        }

        // All inserted keys must be found.
        for i in 0..n {
            assert!(bf.contains(format!("key-{i}").as_bytes()), "false negative at {i}");
        }

        // Sample false-positive rate over unseen keys.
        let mut fp = 0usize;
        let test_count = 10_000usize;
        for i in n..n + test_count {
            if bf.contains(format!("absent-{i}").as_bytes()) {
                fp += 1;
            }
        }
        let observed_fpr = fp as f64 / test_count as f64;
        // Allow 3x slack over target (statistical variation).
        assert!(
            observed_fpr < target_fpr * 3.0,
            "FPR too high: {} vs target {}",
            observed_fpr,
            target_fpr
        );
    }

    #[test]
    fn test_optimal_params_sanity() {
        let (m, k) = optimal_params(1000, 0.01);
        assert!(m > 1000, "need at least 1 bit per element");
        assert!(k >= 3 && k <= 10, "k should be in reasonable range: {k}");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let bf = BloomFilter::with_capacity_and_fpr(100, 0.01);
        bf.insert(b"test_key");
        let bytes = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&bytes).expect("deserialize");
        assert!(bf2.contains(b"test_key"));
    }

    #[test]
    fn test_scalable_filter() {
        let mut sbf = ScalableBloomFilter::new(100, 0.01);
        for i in 0..500usize {
            sbf.insert(format!("k{i}").as_bytes());
        }
        for i in 0..500usize {
            assert!(sbf.contains(format!("k{i}").as_bytes()));
        }
        assert!(sbf.num_slices() > 1, "should have grown");
    }

    #[test]
    fn test_table_bloom_filter() {
        let tbf = TableBloomFilter::new(1000);
        tbf.insert_pk(b"\x01\x00\x00\x01");
        assert!(tbf.may_contain_pk(b"\x01\x00\x00\x01"));
        assert!(!tbf.may_contain_pk(b"\xff\xff\xff\xff"));
    }
}
