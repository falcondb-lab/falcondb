// crates/falcon_bench/src/metrics.rs
//
// BenchMetrics: 完整性能指标结构体
// HdrHistogram: 无锁延迟直方图（1µs 粒度，0..10ms，溢出桶）

use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Deserialize, Serialize};

// ─── 指标结构体 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BenchMetrics {
    pub bench_id:      String,
    pub workers:       usize,
    pub dataset:       String,
    pub duration_secs: f64,
    pub total_ops:     u64,

    // ── 吞吐 ──────────────────────────────────
    pub tps:           f64,
    pub qps:           f64,

    // ── 延迟分布（µs） ────────────────────────
    pub latency_avg_us:   f64,
    pub latency_p50_us:   f64,
    pub latency_p95_us:   f64,
    pub latency_p99_us:   f64,
    pub latency_p999_us:  f64,
    pub latency_max_us:   f64,

    // ── CPU（平台相关，不支持时为 None）──────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_cycles_per_op:   Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions_per_op: Option<f64>,

    // ── 内存分配（需 tracking_allocator feature）─
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alloc_count_per_op: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alloc_bytes_per_op: Option<f64>,

    // ── 数据移动 ──────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_copied_per_op: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_bytes_per_txn:   Option<f64>,

    // ── CPU micro-arch（Linux perf_event，可选）─
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_cache_miss_per_op:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llc_cache_miss_per_op: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_miss_per_op:    Option<f64>,
    /// 估算：CAS retry 次数 × ~40 cycles/CAS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atomic_stall_cycles:   Option<f64>,

    // ── 事务质量 ──────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_rate: Option<f64>,

    // ── 扩展性曲线 ────────────────────────────
    #[serde(default)]
    pub scalability_curve: Vec<ScalabilityPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalabilityPoint {
    pub workers: usize,
    pub tps:     f64,
    pub p99_us:  f64,
}

impl BenchMetrics {
    /// 输出单行摘要，用于终端 / CI 日志
    pub fn summary_line(&self) -> String {
        format!(
            "[{}] workers={} tps={:.0} p50={:.0}µs p95={:.0}µs p99={:.0}µs p999={:.0}µs abort={:.2}%",
            self.bench_id,
            self.workers,
            self.tps,
            self.latency_p50_us,
            self.latency_p95_us,
            self.latency_p99_us,
            self.latency_p999_us,
            self.abort_rate.unwrap_or(0.0) * 100.0,
        )
    }

    /// 输出 JSON（单行，用于 CI 采集）
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

// ─── HDR Histogram ──────────────────────────────────────────────────────────

/// 无锁延迟直方图。
///
/// 粒度：1µs；范围：0..=10_000µs (10ms)。
/// 超出 10ms 的样本计入 `overflow`，不丢弃。
///
/// 多线程场景下每个 worker 持有独立副本，最终由 `merge` 聚合，
/// 避免高并发下 AtomicU64 cache line 竞争。
pub struct HdrHistogram {
    buckets:  Box<[AtomicU64; 10_001]>,
    overflow: AtomicU64,
    total:    AtomicU64,
    sum_us:   AtomicU64,
}

impl HdrHistogram {
    pub fn new() -> Self {
        // SAFETY: AtomicU64 与 u64 layout 相同，零初始化是合法初始值。
        let buckets: Box<[AtomicU64; 10_001]> = unsafe {
            let layout = std::alloc::Layout::new::<[AtomicU64; 10_001]>();
            let ptr = std::alloc::alloc_zeroed(layout) as *mut [AtomicU64; 10_001];
            Box::from_raw(ptr)
        };
        Self {
            buckets,
            overflow: AtomicU64::new(0),
            total:    AtomicU64::new(0),
            sum_us:   AtomicU64::new(0),
        }
    }

    /// 记录一次延迟样本（µs）。设计为热路径：3x fetch_add + 1x array index。
    #[inline(always)]
    pub fn record_us(&self, us: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us.min(u64::MAX - 1), Ordering::Relaxed);
        if us <= 10_000 {
            self.buckets[us as usize].fetch_add(1, Ordering::Relaxed);
        } else {
            self.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 计算分位数（0.0..=100.0）。返回 µs。
    pub fn percentile(&self, pct: f64) -> f64 {
        let total = self.total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let target = ((total as f64) * pct / 100.0).ceil() as u64;
        let mut cum = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            cum += b.load(Ordering::Relaxed);
            if cum >= target {
                return i as f64;
            }
        }
        // 溢出区间：返回 10ms + overflow bucket 中位估算
        let overflow = self.overflow.load(Ordering::Relaxed);
        10_000.0 + (overflow as f64 / 2.0)
    }

    pub fn avg_us(&self) -> f64 {
        let total = self.total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.sum_us.load(Ordering::Relaxed) as f64 / total as f64
    }

    pub fn count(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// 从直方图提取延迟六元组 (avg, p50, p95, p99, p999, max)
    pub fn extract(&self) -> LatencySnapshot {
        LatencySnapshot {
            avg_us:   self.avg_us(),
            p50_us:   self.percentile(50.0),
            p95_us:   self.percentile(95.0),
            p99_us:   self.percentile(99.0),
            p999_us:  self.percentile(99.9),
            max_us:   self.percentile(100.0),
        }
    }

    /// 合并另一个直方图的数据到 self（用于聚合多个 worker 的局部直方图）
    pub fn merge(&self, other: &HdrHistogram) {
        for (a, b) in self.buckets.iter().zip(other.buckets.iter()) {
            let v = b.load(Ordering::Relaxed);
            if v > 0 {
                a.fetch_add(v, Ordering::Relaxed);
            }
        }
        self.overflow.fetch_add(other.overflow.load(Ordering::Relaxed), Ordering::Relaxed);
        self.total.fetch_add(other.total.load(Ordering::Relaxed), Ordering::Relaxed);
        self.sum_us.fetch_add(other.sum_us.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

impl Default for HdrHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LatencySnapshot {
    pub avg_us:  f64,
    pub p50_us:  f64,
    pub p95_us:  f64,
    pub p99_us:  f64,
    pub p999_us: f64,
    pub max_us:  f64,
}

// ─── 全局 bench 结果集（用于序列化输出和回归对比）──────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BenchResults {
    pub run_id:     String,
    pub git_commit: String,
    pub timestamp:  String,
    pub machine:    String,
    pub results:    std::collections::HashMap<String, BenchMetrics>,
}

impl BenchResults {
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    pub fn load_from_file(path: &str) -> std::io::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        serde_json::from_str(&s).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hdr_percentile_empty() {
        let h = HdrHistogram::new();
        assert_eq!(h.percentile(99.0), 0.0);
        assert_eq!(h.avg_us(), 0.0);
    }

    #[test]
    fn hdr_single_sample() {
        let h = HdrHistogram::new();
        h.record_us(500);
        assert_eq!(h.percentile(50.0), 500.0);
        assert_eq!(h.percentile(99.0), 500.0);
        assert_eq!(h.avg_us(), 500.0);
    }

    #[test]
    fn hdr_uniform_distribution() {
        let h = HdrHistogram::new();
        for i in 1..=100 {
            h.record_us(i);
        }
        assert_eq!(h.percentile(50.0), 50.0);
        assert_eq!(h.percentile(95.0), 95.0);
        assert_eq!(h.percentile(99.0), 99.0);
        assert_eq!(h.count(), 100);
    }

    #[test]
    fn hdr_overflow_bucket() {
        let h = HdrHistogram::new();
        h.record_us(50_000); // 50ms > 10ms
        assert!(h.percentile(100.0) > 10_000.0);
        assert_eq!(h.count(), 1);
    }

    #[test]
    fn hdr_merge() {
        let a = HdrHistogram::new();
        let b = HdrHistogram::new();
        for i in 0..50u64  { a.record_us(i * 10); }
        for i in 50..100u64 { b.record_us(i * 10); }
        a.merge(&b);
        assert_eq!(a.count(), 100);
    }

    #[test]
    fn bench_metrics_json_roundtrip() {
        let m = BenchMetrics {
            bench_id: "e2e/point_select_pk".into(),
            workers: 32,
            tps: 485_000.0,
            latency_p99_us: 143.0,
            ..Default::default()
        };
        let json = m.to_json();
        let m2: BenchMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(m2.bench_id, "e2e/point_select_pk");
        assert!((m2.tps - 485_000.0).abs() < 1.0);
    }
}
