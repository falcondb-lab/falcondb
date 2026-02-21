//! Bloom filter for SST negative-lookup elimination.
//!
//! Each SST file carries a bloom filter of all keys it contains.
//! On a point lookup, the bloom filter is checked first — if the key is
//! definitely not in the SST, the file is skipped entirely.

use std::hash::{Hash, Hasher};

/// A simple bloom filter using double hashing (Kirsch-Mitzenmacker).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a bloom filter sized for `expected_keys` with the given
    /// false-positive rate (e.g. 0.01 = 1%).
    pub fn new(expected_keys: usize, fp_rate: f64) -> Self {
        let expected_keys = expected_keys.max(1);
        let fp_rate = fp_rate.clamp(1e-10, 1.0);

        // Optimal number of bits: m = -n * ln(p) / (ln2)^2
        let num_bits = (-(expected_keys as f64) * fp_rate.ln() / (2.0_f64.ln().powi(2)))
            .ceil() as usize;
        let num_bits = num_bits.max(64);

        // Optimal number of hashes: k = (m/n) * ln2
        let num_hashes = ((num_bits as f64 / expected_keys as f64) * 2.0_f64.ln())
            .ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 30);

        let words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; words],
            num_bits,
            num_hashes,
        }
    }

    /// Insert a key into the bloom filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = self.bit_index(h1, h2, i);
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    /// Check if a key might be in the set. Returns `false` if definitely not present.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = self.bit_index(h1, h2, i);
            let word = idx / 64;
            let bit = idx % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the bloom filter to bytes for SST footer storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12 + self.bits.len() * 8);
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        buf.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        buf
    }

    /// Deserialize a bloom filter from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let word_count = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;
        if data.len() < 12 + word_count * 8 {
            return None;
        }
        let mut bits = Vec::with_capacity(word_count);
        for i in 0..word_count {
            let offset = 12 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            bits.push(word);
        }
        Some(Self { bits, num_bits, num_hashes })
    }

    /// Number of bits in the filter.
    pub fn size_bits(&self) -> usize {
        self.num_bits
    }

    /// Approximate memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8 + std::mem::size_of::<Self>()
    }

    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        let mut hasher = FnvHasher::new();
        key.hash(&mut hasher);
        let h1 = hasher.finish();

        let mut hasher2 = FnvHasher::with_seed(0x517cc1b727220a95);
        key.hash(&mut hasher2);
        let h2 = hasher2.finish();

        (h1, h2)
    }

    fn bit_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (combined % self.num_bits as u64) as usize
    }
}

/// Simple FNV-1a hasher for bloom filter hashing.
struct FnvHasher {
    state: u64,
}

impl FnvHasher {
    fn new() -> Self {
        Self { state: 0xcbf29ce484222325 }
    }

    fn with_seed(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= byte as u64;
            self.state = self.state.wrapping_mul(0x100000001b3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        // Very unlikely to be a false positive for a random key
        // (but not guaranteed — bloom filters have FP)
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        let n = 10_000;
        let mut bf = BloomFilter::new(n, 0.01);
        for i in 0..n {
            bf.insert(&i.to_le_bytes());
        }

        // All inserted keys must be found
        for i in 0..n {
            assert!(bf.may_contain(&i.to_le_bytes()));
        }

        // Check FP rate on non-inserted keys
        let test_count = 10_000;
        let mut false_positives = 0;
        for i in n..(n + test_count) {
            if bf.may_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }
        let fp_rate = false_positives as f64 / test_count as f64;
        // Should be roughly ≤ 2% (we target 1%, allow some variance)
        assert!(fp_rate < 0.03, "FP rate too high: {:.4}", fp_rate);
    }

    #[test]
    fn test_bloom_serialize_roundtrip() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"key1");
        bf.insert(b"key2");

        let bytes = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(bf2.may_contain(b"key1"));
        assert!(bf2.may_contain(b"key2"));
        assert_eq!(bf.num_bits, bf2.num_bits);
        assert_eq!(bf.num_hashes, bf2.num_hashes);
    }

    #[test]
    fn test_bloom_empty() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.may_contain(b"anything"));
    }
}
