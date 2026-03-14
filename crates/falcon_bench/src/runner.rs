// crates/falcon_bench/src/runner.rs
//
// E2E 并发压测框架。
//
// 设计目标：
//   - 每个 worker 独立线程，独立 session 上下文，无跨线程共享的 Session
//   - 预热阶段（warmup_txns）不计入延迟/TPS
//   - 测量阶段用无锁 HdrHistogram（每 worker 局部，最终 merge）
//   - 支持时长模式（duration_secs）和事务数模式（measure_txns）
//   - 不走网络，直接调用 in-process handler

use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::metrics::{BenchMetrics, HdrHistogram};

// ─── BenchRunner ────────────────────────────────────────────────────────────

pub struct BenchRunner {
    pub bench_id:      String,
    pub workers:       usize,
    /// 每 worker 预热事务数（不计入结果）
    pub warmup_txns:   u64,
    /// 每 worker 最大测量事务数（0 = 仅由 duration_secs 控制）
    pub measure_txns:  u64,
    /// 测量时长（0 = 仅由 measure_txns 控制；两者均非 0 取先到者）
    pub duration_secs: u64,
    pub dataset:       String,
}

impl Default for BenchRunner {
    fn default() -> Self {
        Self {
            bench_id:      "unnamed".into(),
            workers:       1,
            warmup_txns:   1_000,
            measure_txns:  0,
            duration_secs: 10,
            dataset:       "medium".into(),
        }
    }
}

impl BenchRunner {
    /// 运行并发 workload。
    ///
    /// `factory(worker_id)` 为每个 worker 创建独立上下文 `Ctx`（例如 PgSession + Handler）。
    /// `workload(&mut ctx, seq_num)` 执行单个事务，返回 `Ok(())` 或 `Err(abort)`。
    ///
    /// 内部流程：
    ///   1. 启动 `workers` 个线程，各自调用 `factory` 构造独立上下文
    ///   2. 全部完成预热后，通过 `Barrier` 同步启动测量
    ///   3. 每次 Ok → 记录延迟；每次 Err → 计入 abort
    ///   4. 达到时长或事务数上限后停止
    ///   5. 合并所有 worker 的局部 HdrHistogram，输出 BenchMetrics
    pub fn run<Ctx, F, G, E>(&self, factory: F, workload: G) -> BenchMetrics
    where
        Ctx: Send + 'static,
        F: Fn(usize) -> Ctx + Send + Sync + 'static,
        G: Fn(&mut Ctx, u64) -> Result<(), E> + Send + Sync + Clone + 'static,
        E: std::fmt::Debug,
    {
        let stop    = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(self.workers + 1));
        let factory = Arc::new(factory);
        let workload = Arc::new(workload);

        // 每 worker 共享 AtomicU64 计数 + 独立局部 HdrHistogram（Arc<HdrHistogram>）
        let global_ops    = Arc::new(AtomicU64::new(0));
        let global_aborts = Arc::new(AtomicU64::new(0));

        // 每 worker 持有自己的局部 histogram，最终 merge 到 merged_hist
        let worker_hists: Vec<Arc<HdrHistogram>> = (0..self.workers)
            .map(|_| Arc::new(HdrHistogram::new()))
            .collect();

        let mut handles = Vec::with_capacity(self.workers);

        for worker_id in 0..self.workers {
            let stop_w     = Arc::clone(&stop);
            let barrier_w  = Arc::clone(&barrier);
            let factory_w  = Arc::clone(&factory);
            let workload_w = Arc::clone(&workload);
            let ops_w      = Arc::clone(&global_ops);
            let aborts_w   = Arc::clone(&global_aborts);
            let hist_w     = Arc::clone(&worker_hists[worker_id]);
            let warmup     = self.warmup_txns;
            let measure    = self.measure_txns;

            handles.push(std::thread::spawn(move || {
                let mut ctx = factory_w(worker_id);

                // 预热
                for seq in 0..warmup {
                    let _ = workload_w(&mut ctx, seq);
                }

                barrier_w.wait(); // 等所有 worker 完成预热

                let mut seq = warmup;
                let mut local_ops:    u64 = 0;
                let mut local_aborts: u64 = 0;

                loop {
                    if stop_w.load(Ordering::Relaxed) {
                        break;
                    }
                    if measure > 0 && local_ops >= measure {
                        break;
                    }

                    let t0 = Instant::now();
                    match workload_w(&mut ctx, seq) {
                        Ok(()) => {
                            let us = t0.elapsed().as_micros() as u64;
                            hist_w.record_us(us);
                            local_ops += 1;
                        }
                        Err(_) => {
                            local_aborts += 1;
                        }
                    }
                    seq += 1;
                }

                ops_w.fetch_add(local_ops, Ordering::Relaxed);
                aborts_w.fetch_add(local_aborts, Ordering::Relaxed);
            }));
        }

        // 等所有 worker 预热完成，开始计时
        barrier.wait();
        let wall_start = Instant::now();

        if self.duration_secs > 0 {
            std::thread::sleep(Duration::from_secs(self.duration_secs));
            stop.store(true, Ordering::Relaxed);
        }

        for h in handles {
            let _ = h.join();
        }
        let elapsed = wall_start.elapsed().as_secs_f64();

        // 合并所有 worker 的局部 histogram
        let merged = HdrHistogram::new();
        for wh in &worker_hists {
            merged.merge(wh);
        }

        let ops    = global_ops.load(Ordering::Relaxed);
        let aborts = global_aborts.load(Ordering::Relaxed);
        let snap   = merged.extract();

        BenchMetrics {
            bench_id:        self.bench_id.clone(),
            workers:         self.workers,
            dataset:         self.dataset.clone(),
            duration_secs:   elapsed,
            total_ops:       ops,
            tps:             ops as f64 / elapsed,
            qps:             ops as f64 / elapsed,
            latency_avg_us:  snap.avg_us,
            latency_p50_us:  snap.p50_us,
            latency_p95_us:  snap.p95_us,
            latency_p99_us:  snap.p99_us,
            latency_p999_us: snap.p999_us,
            latency_max_us:  snap.max_us,
            abort_rate:      Some(aborts as f64 / (ops + aborts).max(1) as f64),
            ..Default::default()
        }
    }

    /// 扩展性曲线测量：在 thread_counts 列表上依次运行，收集 (workers, TPS, P99) 曲线。
    pub fn scalability_curve<Ctx, F, G, E>(
        &self,
        thread_counts: &[usize],
        factory: F,
        workload: G,
    ) -> Vec<crate::metrics::ScalabilityPoint>
    where
        Ctx: Send + 'static,
        F: Fn(usize) -> Ctx + Send + Sync + Clone + 'static,
        G: Fn(&mut Ctx, u64) -> Result<(), E> + Send + Sync + Clone + 'static,
        E: std::fmt::Debug,
    {
        thread_counts.iter().map(|&w| {
            let runner = BenchRunner {
                workers: w,
                bench_id: format!("{}_{}t", self.bench_id, w),
                ..BenchRunner {
                    bench_id:      self.bench_id.clone(),
                    workers:       w,
                    warmup_txns:   self.warmup_txns,
                    measure_txns:  self.measure_txns,
                    duration_secs: self.duration_secs,
                    dataset:       self.dataset.clone(),
                }
            };
            let m = runner.run(factory.clone(), workload.clone());
            crate::metrics::ScalabilityPoint {
                workers: w,
                tps:     m.tps,
                p99_us:  m.latency_p99_us,
            }
        }).collect()
    }
}

// ─── RDTSC cycles measurement（x86_64 only）────────────────────────────────

/// 采集单次操作的 CPU cycles（用于 Criterion custom measurement）
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub fn rdtsc() -> u64 {
    // 非 x86_64：回退到 Instant（精度较低）
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_basic_tps() {
        let runner = BenchRunner {
            bench_id:      "test/noop".into(),
            workers:       2,
            warmup_txns:   10,
            measure_txns:  0,
            duration_secs: 1,
            dataset:       "tiny".into(),
        };

        let metrics = runner.run(
            |_worker_id| (),
            |_ctx, _seq| -> Result<(), ()> { Ok(()) },
        );

        assert!(metrics.tps > 0.0, "TPS should be positive");
        assert!(metrics.total_ops > 0, "Should have completed some ops");
        assert!(metrics.latency_p99_us >= 0.0);
        println!("{}", metrics.summary_line());
    }

    #[test]
    fn runner_abort_rate() {
        let runner = BenchRunner {
            bench_id:      "test/abort50pct".into(),
            workers:       1,
            warmup_txns:   0,
            measure_txns:  0,
            duration_secs: 1,
            dataset:       "tiny".into(),
        };

        let counter = Arc::new(AtomicU64::new(0));
        let c2 = Arc::clone(&counter);

        let metrics = runner.run(
            move |_| Arc::clone(&c2),
            |ctr, _seq| -> Result<(), &'static str> {
                let v = ctr.fetch_add(1, Ordering::Relaxed);
                if v % 2 == 0 { Ok(()) } else { Err("abort") }
            },
        );

        // ~50% abort rate
        let rate = metrics.abort_rate.unwrap_or(0.0);
        assert!(rate > 0.3 && rate < 0.7, "Expected ~50% abort rate, got {rate:.2}");
    }
}
