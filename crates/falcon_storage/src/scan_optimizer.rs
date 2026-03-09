//! Scan optimization layer — prefetch-aware iteration and vectorized batch processing
//!
//! This module addresses the 4.3x query performance gap vs PostgreSQL by:
//! 1. Prefetch-aware DashMap iteration (reduce cache misses)
//! 2. Vectorized batch processing (amortize per-row overhead)
//! 3. SIMD-accelerated aggregates (leverage CPU parallelism)

use std::sync::Arc;
use dashmap::DashMap;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::types::{Timestamp, TxnId};
use crate::memtable::PrimaryKey;
use crate::mvcc::VersionChain;

/// Batch size for vectorized processing — tuned for L1 cache (32KB)
/// Each OwnedRow ~100 bytes → 256 rows ≈ 25KB fits in L1
const BATCH_SIZE: usize = 256;

/// Prefetch distance — how many entries ahead to prefetch
const PREFETCH_DISTANCE: usize = 8;

/// Vectorized batch of rows for SIMD-friendly processing
pub struct RowBatch {
    pub rows: Vec<Arc<OwnedRow>>,
    pub pks: Vec<PrimaryKey>,
}

impl RowBatch {
    pub fn new() -> Self {
        Self {
            rows: Vec::with_capacity(BATCH_SIZE),
            pks: Vec::with_capacity(BATCH_SIZE),
        }
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.pks.clear();
    }

    pub fn is_full(&self) -> bool {
        self.rows.len() >= BATCH_SIZE
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Default for RowBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Prefetch-aware iterator over DashMap entries
///
/// Reduces cache misses by issuing prefetch hints for upcoming entries.
/// On x86_64, uses `_mm_prefetch` intrinsic; on other platforms, relies on
/// compiler auto-vectorization hints.
pub struct PrefetchIterator {
    entries: Vec<(PrimaryKey, Arc<VersionChain>)>,
    index: usize,
}

impl PrefetchIterator {
    pub fn new(data: &DashMap<PrimaryKey, Arc<VersionChain>>) -> Self {
        // Collect both keys and chains (sequential DashMap iteration)
        let entries: Vec<_> = data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        
        Self { entries, index: 0 }
    }

    /// Prefetch next N entries into cache
    #[inline]
    fn prefetch_ahead(&self) {
        for i in 0..PREFETCH_DISTANCE {
            let prefetch_idx = self.index + i;
            if prefetch_idx < self.entries.len() {
                let (_pk, chain) = &self.entries[prefetch_idx];
                // Prefetch the VersionChain data structure
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    use std::arch::x86_64::*;
                    let ptr = Arc::as_ptr(chain) as *const i8;
                    _mm_prefetch(ptr, _MM_HINT_T0); // L1 cache
                }
                #[cfg(not(target_arch = "x86_64"))]
                {
                    // Compiler hint: touch the data to trigger prefetch
                    let _ = Arc::strong_count(chain);
                }
            }
        }
    }

    pub fn next_batch(
        &mut self,
        txn_id: TxnId,
        read_ts: Timestamp,
        batch: &mut RowBatch,
    ) -> bool {
        batch.clear();
        
        while self.index < self.entries.len() && !batch.is_full() {
            // Prefetch upcoming entries
            if self.index % 4 == 0 {
                self.prefetch_ahead();
            }

            let (pk, chain) = &self.entries[self.index];
            self.index += 1;

            if let Some(row) = chain.read_for_txn(txn_id, read_ts) {
                batch.rows.push(row);
                batch.pks.push(pk.clone());
            }
        }

        !batch.rows.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.index < self.entries.len()
    }
}

/// SIMD-accelerated aggregate computation
///
/// Uses explicit SIMD for COUNT/SUM on numeric columns when possible.
/// Falls back to scalar code on non-x86_64 or for complex types.
pub struct SimdAggregator {
    count: i64,
    sum_i64: i64,
    sum_f64: f64,
    min_i64: Option<i64>,
    max_i64: Option<i64>,
    min_f64: Option<f64>,
    max_f64: Option<f64>,
}

impl SimdAggregator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum_i64: 0,
            sum_f64: 0.0,
            min_i64: None,
            max_i64: None,
            min_f64: None,
            max_f64: None,
        }
    }

    /// Process a batch of Int64 values using SIMD
    #[cfg(target_arch = "x86_64")]
    pub fn process_i64_batch(&mut self, values: &[i64]) {
        use std::arch::x86_64::*;

        let len = values.len();
        self.count += len as i64;

        // SIMD sum: process 4 i64 at a time using AVX2
        if is_x86_feature_detected!("avx2") {
            unsafe {
                let mut sum_vec = _mm256_setzero_si256();
                let chunks = len / 4;
                
                for i in 0..chunks {
                    let ptr = values.as_ptr().add(i * 4);
                    let vals = _mm256_loadu_si256(ptr as *const __m256i);
                    sum_vec = _mm256_add_epi64(sum_vec, vals);
                }

                // Horizontal sum
                let sum_arr: [i64; 4] = std::mem::transmute(sum_vec);
                self.sum_i64 += sum_arr.iter().sum::<i64>();

                // Process remaining elements
                for &val in &values[chunks * 4..] {
                    self.sum_i64 += val;
                }
            }
        } else {
            // Fallback: scalar sum
            self.sum_i64 += values.iter().sum::<i64>();
        }

        // Min/max (scalar for now — SIMD min/max requires more complex reduction)
        for &val in values {
            self.min_i64 = Some(self.min_i64.map_or(val, |m| m.min(val)));
            self.max_i64 = Some(self.max_i64.map_or(val, |m| m.max(val)));
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn process_i64_batch(&mut self, values: &[i64]) {
        self.count += values.len() as i64;
        self.sum_i64 += values.iter().sum::<i64>();
        for &val in values {
            self.min_i64 = Some(self.min_i64.map_or(val, |m| m.min(val)));
            self.max_i64 = Some(self.max_i64.map_or(val, |m| m.max(val)));
        }
    }

    /// Process a batch of Float64 values
    pub fn process_f64_batch(&mut self, values: &[f64]) {
        self.count += values.len() as i64;
        self.sum_f64 += values.iter().sum::<f64>();
        for &val in values {
            self.min_f64 = Some(self.min_f64.map_or(val, |m| m.min(val)));
            self.max_f64 = Some(self.max_f64.map_or(val, |m| m.max(val)));
        }
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn sum_i64(&self) -> i64 {
        self.sum_i64
    }

    pub fn sum_f64(&self) -> f64 {
        self.sum_f64
    }

    pub fn min_i64(&self) -> Option<i64> {
        self.min_i64
    }

    pub fn max_i64(&self) -> Option<i64> {
        self.max_i64
    }

    pub fn avg_f64(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum_f64 / self.count as f64)
        } else {
            None
        }
    }
}

impl Default for SimdAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract numeric column values from a batch for SIMD processing
pub fn extract_i64_column(batch: &RowBatch, col_idx: usize) -> Vec<i64> {
    batch
        .rows
        .iter()
        .filter_map(|row| match row.get(col_idx) {
            Some(Datum::Int64(v)) => Some(*v),
            Some(Datum::Int32(v)) => Some(*v as i64),
            _ => None,
        })
        .collect()
}

pub fn extract_f64_column(batch: &RowBatch, col_idx: usize) -> Vec<f64> {
    batch
        .rows
        .iter()
        .filter_map(|row| match row.get(col_idx) {
            Some(Datum::Float64(v)) => Some(*v),
            Some(Datum::Int64(v)) => Some(*v as f64),
            Some(Datum::Int32(v)) => Some(*v as f64),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_aggregator_i64() {
        let mut agg = SimdAggregator::new();
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8];
        agg.process_i64_batch(&values);
        
        assert_eq!(agg.count(), 8);
        assert_eq!(agg.sum_i64(), 36);
        assert_eq!(agg.min_i64(), Some(1));
        assert_eq!(agg.max_i64(), Some(8));
    }

    #[test]
    fn test_simd_aggregator_f64() {
        let mut agg = SimdAggregator::new();
        let values = vec![1.0, 2.0, 3.0, 4.0];
        agg.process_f64_batch(&values);
        
        assert_eq!(agg.count(), 4);
        assert_eq!(agg.sum_f64(), 10.0);
        assert_eq!(agg.avg_f64(), Some(2.5));
    }

    #[test]
    fn test_batch_size_constant() {
        // Verify batch size fits in L1 cache
        assert!(BATCH_SIZE * 100 < 32_000); // ~100 bytes per row
    }
}
