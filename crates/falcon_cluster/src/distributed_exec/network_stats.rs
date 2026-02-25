//! P2-3: Network byte tracking and pushdown cost model for distributed queries.
//!
//! Tracks bytes transferred between coordinator and shards during scatter/gather,
//! enabling:
//! - **Observability**: bytes_in/out per shard, per query
//! - **Pushdown validation**: compare bytes with/without pushdown
//! - **Tail-latency correlation**: network volume vs latency
//!
//! # Design
//!
//! Row byte estimation uses the same `estimate_datum_bytes` function as the
//! memory tracker, ensuring consistency. Network stats are collected per-query
//! and aggregated into cumulative counters.

use std::sync::atomic::{AtomicU64, Ordering};

use falcon_common::datum::{Datum, OwnedRow};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Per-Query Network Stats
// ═══════════════════════════════════════════════════════════════════════════

/// Network transfer statistics for a single distributed query execution.
#[derive(Debug, Clone, Default)]
pub struct QueryNetworkStats {
    /// Total bytes received from all shards (shard → coordinator).
    pub bytes_in: u64,
    /// Total rows received from all shards before gather-phase filtering.
    pub rows_in: u64,
    /// Total bytes after gather-phase processing (coordinator output).
    pub bytes_out: u64,
    /// Total rows after gather-phase processing.
    pub rows_out: u64,
    /// Per-shard breakdown: (shard_id, bytes, rows, latency_us).
    pub per_shard: Vec<ShardTransferStats>,
    /// Whether a LIMIT was pushed down to shards.
    pub limit_pushed_down: bool,
    /// Whether a filter was pushed down to shards.
    pub filter_pushed_down: bool,
    /// Whether partial aggregation was used.
    pub partial_agg_used: bool,
    /// Byte reduction ratio (1.0 = no reduction, 0.5 = 50% reduction).
    pub reduction_ratio: f64,
}

impl QueryNetworkStats {
    /// Compute the byte reduction ratio.
    pub fn compute_reduction(&mut self) {
        if self.bytes_in > 0 {
            self.reduction_ratio = self.bytes_out as f64 / self.bytes_in as f64;
        } else {
            self.reduction_ratio = 1.0;
        }
    }
}

/// Per-shard transfer statistics.
#[derive(Debug, Clone, Default)]
pub struct ShardTransferStats {
    pub shard_id: u64,
    /// Bytes sent from this shard to coordinator.
    pub bytes: u64,
    /// Rows sent from this shard.
    pub rows: usize,
    /// Shard-local execution latency (microseconds).
    pub latency_us: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Row Byte Estimation
// ═══════════════════════════════════════════════════════════════════════════

/// Estimate the network transfer size of a row in bytes.
/// Uses a conservative estimate: header + per-datum size.
pub fn estimate_row_bytes(row: &OwnedRow) -> u64 {
    let header = 24u64; // Vec overhead + alignment
    let data: u64 = row.values.iter().map(estimate_datum_wire_bytes).sum();
    header + data
}

/// Estimate the wire-transfer size of a single datum.
/// More conservative than in-memory estimate (includes type tag + length prefix).
fn estimate_datum_wire_bytes(d: &Datum) -> u64 {
    match d {
        Datum::Null => 1,
        Datum::Boolean(_) => 2,
        Datum::Int32(_) | Datum::Date(_) => 5,
        Datum::Int64(_) | Datum::Float64(_)
        | Datum::Time(_) | Datum::Timestamp(_) => 9,
        Datum::Text(s) => 5 + s.len() as u64,
        Datum::Bytea(b) => 5 + b.len() as u64,
        Datum::Interval(_, _, _) | Datum::Uuid(_) | Datum::Decimal(_, _) => 17,
        Datum::Jsonb(v) => 5 + v.to_string().len() as u64,
        Datum::Array(a) => {
            5 + a.iter().map(estimate_datum_wire_bytes).sum::<u64>()
        }
    }
}

/// Estimate total bytes for a set of rows.
pub fn estimate_rows_bytes(rows: &[OwnedRow]) -> u64 {
    rows.iter().map(estimate_row_bytes).sum()
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Cumulative Network Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Cumulative network transfer metrics across all distributed queries.
pub struct CumulativeNetworkMetrics {
    /// Total bytes received from shards (lifetime).
    pub total_bytes_in: AtomicU64,
    /// Total bytes output after gather (lifetime).
    pub total_bytes_out: AtomicU64,
    /// Total rows received from shards.
    pub total_rows_in: AtomicU64,
    /// Total rows output after gather.
    pub total_rows_out: AtomicU64,
    /// Queries that used LIMIT pushdown.
    pub queries_with_limit_pushdown: AtomicU64,
    /// Queries that used partial aggregation.
    pub queries_with_partial_agg: AtomicU64,
    /// Queries that used filter pushdown.
    pub queries_with_filter_pushdown: AtomicU64,
    /// Total distributed queries.
    pub total_queries: AtomicU64,
    /// Total bytes saved by pushdown (estimated).
    pub bytes_saved_by_pushdown: AtomicU64,
}

impl CumulativeNetworkMetrics {
    pub const fn new() -> Self {
        Self {
            total_bytes_in: AtomicU64::new(0),
            total_bytes_out: AtomicU64::new(0),
            total_rows_in: AtomicU64::new(0),
            total_rows_out: AtomicU64::new(0),
            queries_with_limit_pushdown: AtomicU64::new(0),
            queries_with_partial_agg: AtomicU64::new(0),
            queries_with_filter_pushdown: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            bytes_saved_by_pushdown: AtomicU64::new(0),
        }
    }

    /// Record a query's network stats.
    pub fn record(&self, stats: &QueryNetworkStats) {
        self.total_bytes_in.fetch_add(stats.bytes_in, Ordering::Relaxed);
        self.total_bytes_out.fetch_add(stats.bytes_out, Ordering::Relaxed);
        self.total_rows_in.fetch_add(stats.rows_in, Ordering::Relaxed);
        self.total_rows_out.fetch_add(stats.rows_out, Ordering::Relaxed);
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        if stats.limit_pushed_down {
            self.queries_with_limit_pushdown.fetch_add(1, Ordering::Relaxed);
        }
        if stats.partial_agg_used {
            self.queries_with_partial_agg.fetch_add(1, Ordering::Relaxed);
        }
        if stats.filter_pushed_down {
            self.queries_with_filter_pushdown.fetch_add(1, Ordering::Relaxed);
        }
        if stats.bytes_in > stats.bytes_out {
            self.bytes_saved_by_pushdown
                .fetch_add(stats.bytes_in - stats.bytes_out, Ordering::Relaxed);
        }
    }

    /// Snapshot for reporting.
    pub fn snapshot(&self) -> NetworkMetricsSnapshot {
        let bytes_in = self.total_bytes_in.load(Ordering::Relaxed);
        let bytes_out = self.total_bytes_out.load(Ordering::Relaxed);
        let total_q = self.total_queries.load(Ordering::Relaxed);
        NetworkMetricsSnapshot {
            total_bytes_in: bytes_in,
            total_bytes_out: bytes_out,
            total_rows_in: self.total_rows_in.load(Ordering::Relaxed),
            total_rows_out: self.total_rows_out.load(Ordering::Relaxed),
            queries_with_limit_pushdown: self.queries_with_limit_pushdown.load(Ordering::Relaxed),
            queries_with_partial_agg: self.queries_with_partial_agg.load(Ordering::Relaxed),
            queries_with_filter_pushdown: self.queries_with_filter_pushdown.load(Ordering::Relaxed),
            total_queries: total_q,
            bytes_saved_by_pushdown: self.bytes_saved_by_pushdown.load(Ordering::Relaxed),
            overall_reduction_ratio: if bytes_in > 0 { bytes_out as f64 / bytes_in as f64 } else { 1.0 },
            avg_bytes_per_query: if total_q > 0 { bytes_in / total_q } else { 0 },
        }
    }
}

impl Default for CumulativeNetworkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot for reporting.
#[derive(Debug, Clone, Default)]
pub struct NetworkMetricsSnapshot {
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub total_rows_in: u64,
    pub total_rows_out: u64,
    pub queries_with_limit_pushdown: u64,
    pub queries_with_partial_agg: u64,
    pub queries_with_filter_pushdown: u64,
    pub total_queries: u64,
    pub bytes_saved_by_pushdown: u64,
    pub overall_reduction_ratio: f64,
    pub avg_bytes_per_query: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Pushdown Cost Estimator
// ═══════════════════════════════════════════════════════════════════════════

/// Estimate whether pushdown is beneficial for a query.
#[derive(Debug, Clone)]
pub struct PushdownEstimate {
    /// Estimated bytes without pushdown (full row transfer).
    pub without_pushdown_bytes: u64,
    /// Estimated bytes with pushdown (after filter/limit/projection).
    pub with_pushdown_bytes: u64,
    /// Estimated reduction ratio.
    pub estimated_reduction: f64,
    /// Whether pushdown is recommended.
    pub recommend_pushdown: bool,
}

/// Estimate pushdown benefit given row count, avg row size, and selectivity.
pub fn estimate_pushdown_benefit(
    total_rows: u64,
    avg_row_bytes: u64,
    selectivity: f64,    // fraction of rows that pass filter (0.0 - 1.0)
    limit: Option<u64>,  // if LIMIT is present
    shard_count: u64,
) -> PushdownEstimate {
    let without = total_rows * avg_row_bytes;
    let after_filter = (total_rows as f64 * selectivity) as u64;
    let after_limit = match limit {
        Some(lim) => after_filter.min(lim * shard_count), // each shard returns at most LIMIT
        None => after_filter,
    };
    let with = after_limit * avg_row_bytes;
    let reduction = if without > 0 { with as f64 / without as f64 } else { 1.0 };

    PushdownEstimate {
        without_pushdown_bytes: without,
        with_pushdown_bytes: with,
        estimated_reduction: reduction,
        recommend_pushdown: reduction < 0.8, // recommend if > 20% reduction
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_datum_wire_bytes() {
        assert_eq!(estimate_datum_wire_bytes(&Datum::Null), 1);
        assert_eq!(estimate_datum_wire_bytes(&Datum::Int32(42)), 5);
        assert_eq!(estimate_datum_wire_bytes(&Datum::Int64(42)), 9);
        assert_eq!(estimate_datum_wire_bytes(&Datum::Boolean(true)), 2);
        assert_eq!(estimate_datum_wire_bytes(&Datum::Text("hello".into())), 10);
        assert_eq!(estimate_datum_wire_bytes(&Datum::Float64(3.14)), 9);
    }

    #[test]
    fn test_estimate_row_bytes() {
        let row = OwnedRow {
            values: vec![Datum::Int32(1), Datum::Text("hello".into())],
        };
        let bytes = estimate_row_bytes(&row);
        // header(24) + int32(5) + text(10) = 39
        assert_eq!(bytes, 39);
    }

    #[test]
    fn test_estimate_rows_bytes() {
        let rows = vec![
            OwnedRow { values: vec![Datum::Int32(1)] },
            OwnedRow { values: vec![Datum::Int32(2)] },
        ];
        let bytes = estimate_rows_bytes(&rows);
        // 2 * (24 + 5) = 58
        assert_eq!(bytes, 58);
    }

    #[test]
    fn test_query_network_stats_reduction() {
        let mut stats = QueryNetworkStats {
            bytes_in: 1000,
            bytes_out: 500,
            rows_in: 100,
            rows_out: 50,
            ..Default::default()
        };
        stats.compute_reduction();
        assert!((stats.reduction_ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_cumulative_metrics_record() {
        let metrics = CumulativeNetworkMetrics::new();
        let stats = QueryNetworkStats {
            bytes_in: 1000,
            bytes_out: 300,
            rows_in: 100,
            rows_out: 30,
            limit_pushed_down: true,
            partial_agg_used: false,
            filter_pushed_down: true,
            ..Default::default()
        };
        metrics.record(&stats);
        let snap = metrics.snapshot();
        assert_eq!(snap.total_bytes_in, 1000);
        assert_eq!(snap.total_bytes_out, 300);
        assert_eq!(snap.total_queries, 1);
        assert_eq!(snap.queries_with_limit_pushdown, 1);
        assert_eq!(snap.queries_with_filter_pushdown, 1);
        assert_eq!(snap.queries_with_partial_agg, 0);
        assert_eq!(snap.bytes_saved_by_pushdown, 700);
    }

    #[test]
    fn test_cumulative_metrics_multiple_queries() {
        let metrics = CumulativeNetworkMetrics::new();
        for i in 0..5 {
            let stats = QueryNetworkStats {
                bytes_in: 1000,
                bytes_out: 500 - i * 100,
                rows_in: 100,
                rows_out: 50,
                partial_agg_used: i % 2 == 0,
                ..Default::default()
            };
            metrics.record(&stats);
        }
        let snap = metrics.snapshot();
        assert_eq!(snap.total_queries, 5);
        assert_eq!(snap.total_bytes_in, 5000);
        assert_eq!(snap.queries_with_partial_agg, 3); // i=0,2,4
        assert_eq!(snap.avg_bytes_per_query, 1000);
    }

    #[test]
    fn test_pushdown_estimate_with_limit() {
        let est = estimate_pushdown_benefit(
            100_000,  // 100k rows
            100,      // 100 bytes/row
            1.0,      // no filter selectivity
            Some(10), // LIMIT 10
            4,        // 4 shards
        );
        // Without: 100k * 100 = 10MB
        assert_eq!(est.without_pushdown_bytes, 10_000_000);
        // With: min(100000, 10*4) * 100 = 40 * 100 = 4000
        assert_eq!(est.with_pushdown_bytes, 4000);
        assert!(est.recommend_pushdown);
        assert!(est.estimated_reduction < 0.01);
    }

    #[test]
    fn test_pushdown_estimate_with_filter() {
        let est = estimate_pushdown_benefit(
            10_000, // 10k rows
            100,    // 100 bytes/row
            0.1,    // 10% selectivity
            None,   // no limit
            4,      // 4 shards
        );
        // Without: 10k * 100 = 1MB
        assert_eq!(est.without_pushdown_bytes, 1_000_000);
        // With: 1000 * 100 = 100k
        assert_eq!(est.with_pushdown_bytes, 100_000);
        assert!(est.recommend_pushdown);
    }

    #[test]
    fn test_pushdown_estimate_no_benefit() {
        let est = estimate_pushdown_benefit(
            100,  // 100 rows
            100,  // 100 bytes/row
            1.0,  // no filter
            None, // no limit
            4,
        );
        // Without = with = 100 * 100 = 10k
        assert_eq!(est.without_pushdown_bytes, 10_000);
        assert_eq!(est.with_pushdown_bytes, 10_000);
        assert!(!est.recommend_pushdown);
    }

    #[test]
    fn test_shard_transfer_stats() {
        let s = ShardTransferStats {
            shard_id: 1,
            bytes: 5000,
            rows: 50,
            latency_us: 1200,
        };
        assert_eq!(s.shard_id, 1);
        assert_eq!(s.bytes, 5000);
    }

    #[test]
    fn test_network_metrics_snapshot_ratio() {
        let metrics = CumulativeNetworkMetrics::new();
        let stats1 = QueryNetworkStats {
            bytes_in: 2000,
            bytes_out: 1000,
            ..Default::default()
        };
        let stats2 = QueryNetworkStats {
            bytes_in: 3000,
            bytes_out: 500,
            ..Default::default()
        };
        metrics.record(&stats1);
        metrics.record(&stats2);
        let snap = metrics.snapshot();
        // overall: 1500 out / 5000 in = 0.3
        assert!((snap.overall_reduction_ratio - 0.3).abs() < 0.01);
        assert_eq!(snap.bytes_saved_by_pushdown, 3500);
    }
}
