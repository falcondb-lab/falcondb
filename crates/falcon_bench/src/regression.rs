// crates/falcon_bench/src/regression.rs
//
// 回归阈值检查：对比当前 bench 结果与 baseline，输出 FAIL/WARN/IMPROVE/NEW 报告。
// PR 门禁规则：任何 Fail 项 → 阻止合并；Warn 项 → 标记不阻止。

use crate::metrics::{BenchMetrics, BenchResults};
use serde::{Deserialize, Serialize};

// ─── 阈值配置 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionConfig {
    /// TPS 下降超过此比例 → FAIL，阻止合并
    pub tps_fail_pct:    f64,   // default: 5.0
    /// P99 延迟上升超过此比例 → FAIL
    pub p99_fail_pct:    f64,   // default: 10.0
    /// WAL flush µs 上升超过此比例 → FAIL（针对 wal/* bench）
    pub wal_fail_pct:    f64,   // default: 15.0
    /// alloc/op 增加超过此比例 → WARN（不阻止合并）
    pub alloc_warn_pct:  f64,   // default: 20.0
    /// 低于此变化视为噪声，不触发任何报告
    pub noise_floor_pct: f64,   // default: 2.0
    /// TPS 提升超过此比例 → IMPROVE（正面改进提示）
    pub improve_pct:     f64,   // default: 5.0
}

impl Default for RegressionConfig {
    fn default() -> Self {
        Self {
            tps_fail_pct:    5.0,
            p99_fail_pct:    10.0,
            wal_fail_pct:    15.0,
            alloc_warn_pct:  20.0,
            noise_floor_pct: 2.0,
            improve_pct:     5.0,
        }
    }
}

// ─── 报告类型 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "level", rename_all = "UPPERCASE")]
pub enum RegressionReport {
    Fail {
        bench_id:      String,
        metric:        String,
        delta_pct:     f64,
        threshold_pct: f64,
        baseline:      f64,
        current:       f64,
    },
    Warn {
        bench_id:      String,
        metric:        String,
        delta_pct:     f64,
        threshold_pct: f64,
    },
    Improve {
        bench_id:  String,
        metric:    String,
        delta_pct: f64,
        baseline:  f64,
        current:   f64,
    },
    New {
        bench_id: String,
    },
}

impl RegressionReport {
    pub fn is_fail(&self) -> bool {
        matches!(self, Self::Fail { .. })
    }

    pub fn bench_id(&self) -> &str {
        match self {
            Self::Fail    { bench_id, .. } => bench_id,
            Self::Warn    { bench_id, .. } => bench_id,
            Self::Improve { bench_id, .. } => bench_id,
            Self::New     { bench_id }     => bench_id,
        }
    }

    pub fn one_line(&self) -> String {
        match self {
            Self::Fail { bench_id, metric, delta_pct, threshold_pct, baseline, current } =>
                format!("❌ FAIL [{bench_id}] {metric}: {delta_pct:+.1}% (threshold: {threshold_pct:.0}%, baseline: {baseline:.0}, current: {current:.0})"),
            Self::Warn { bench_id, metric, delta_pct, threshold_pct } =>
                format!("⚠️  WARN [{bench_id}] {metric}: {delta_pct:+.1}% (threshold: {threshold_pct:.0}%)"),
            Self::Improve { bench_id, metric, delta_pct, baseline, current } =>
                format!("🎉 IMPROVE [{bench_id}] {metric}: {delta_pct:+.1}% (baseline: {baseline:.0} → current: {current:.0})"),
            Self::New { bench_id } =>
                format!("🆕 NEW [{bench_id}] No baseline; current result recorded as new baseline"),
        }
    }
}

// ─── 回归检查器 ──────────────────────────────────────────────────────────────

pub struct RegressionChecker {
    pub baseline: BenchResults,
    pub config:   RegressionConfig,
}

impl RegressionChecker {
    pub fn new(baseline: BenchResults) -> Self {
        Self { baseline, config: RegressionConfig::default() }
    }

    pub fn with_config(baseline: BenchResults, config: RegressionConfig) -> Self {
        Self { baseline, config }
    }

    /// 对比当前结果与 baseline，返回所有报告项。
    pub fn check(&self, current: &BenchResults) -> Vec<RegressionReport> {
        let mut reports = Vec::new();
        for (id, cur) in &current.results {
            let Some(base) = self.baseline.results.get(id) else {
                reports.push(RegressionReport::New { bench_id: id.clone() });
                continue;
            };
            self.check_tps(id, base, cur, &mut reports);
            self.check_p99(id, base, cur, &mut reports);
            self.check_alloc(id, base, cur, &mut reports);
            if id.starts_with("wal/") {
                self.check_wal_latency(id, base, cur, &mut reports);
            }
        }
        reports
    }

    /// `true` = 所有检查通过（无 FAIL），PR 可以合并
    pub fn is_pass(reports: &[RegressionReport]) -> bool {
        !reports.iter().any(|r| r.is_fail())
    }

    /// 生成 Markdown PR 评论（含摘要表 + 详细失败列表）
    pub fn to_markdown(reports: &[RegressionReport], current: &BenchResults) -> String {
        let pass = Self::is_pass(reports);
        let mut md = String::new();

        md.push_str("## 🏁 FalconDB Performance Gate\n\n");

        if pass {
            md.push_str("✅ **All performance gates passed.**\n\n");
        } else {
            md.push_str("❌ **Performance regression detected — merge blocked.**\n\n");
        }

        // Summary table
        md.push_str("| Bench | TPS | P99 µs | TPS vs Base | P99 vs Base | Status |\n");
        md.push_str("|---|---:|---:|---:|---:|:---:|\n");

        let mut bench_ids: Vec<&String> = current.results.keys().collect();
        bench_ids.sort();

        for id in &bench_ids {
            let m = &current.results[*id];
            let tps_str = format!("{:.0}", m.tps);
            let p99_str = format!("{:.0}", m.latency_p99_us);

            let (tps_delta, p99_delta) = if let Some(base) = current.results.get(*id) {
                // note: for delta display we'd need baseline, skip here
                let _ = base;
                ("—".to_owned(), "—".to_owned())
            } else {
                ("—".to_owned(), "—".to_owned())
            };

            let status = if reports.iter().any(|r| r.is_fail() && r.bench_id() == *id) {
                "❌"
            } else if reports.iter().any(|r| matches!(r, RegressionReport::Improve { bench_id, .. } if bench_id == *id)) {
                "🎉"
            } else if reports.iter().any(|r| matches!(r, RegressionReport::Warn { bench_id, .. } if bench_id == *id)) {
                "⚠️"
            } else {
                "✅"
            };

            md.push_str(&format!("| `{id}` | {tps_str} | {p99_str} | {tps_delta} | {p99_delta} | {status} |\n"));
        }

        // Detail section for failures/warnings
        let fails_warns: Vec<&RegressionReport> = reports.iter()
            .filter(|r| r.is_fail() || matches!(r, RegressionReport::Warn { .. }))
            .collect();

        if !fails_warns.is_empty() {
            md.push_str("\n### Details\n\n");
            for r in fails_warns {
                md.push_str(&format!("- {}\n", r.one_line()));
            }
        }

        // Improvements
        let improves: Vec<&RegressionReport> = reports.iter()
            .filter(|r| matches!(r, RegressionReport::Improve { .. }))
            .collect();
        if !improves.is_empty() {
            md.push_str("\n### Improvements 🎉\n\n");
            for r in improves {
                md.push_str(&format!("- {}\n", r.one_line()));
            }
        }

        md
    }

    // ── 内部检查方法 ──────────────────────────────────────────────────────────

    fn check_tps(&self, id: &str, base: &BenchMetrics, cur: &BenchMetrics, out: &mut Vec<RegressionReport>) {
        if base.tps == 0.0 { return; }
        let delta_pct = (cur.tps - base.tps) / base.tps * 100.0;
        if delta_pct.abs() < self.config.noise_floor_pct { return; }

        if delta_pct < -self.config.tps_fail_pct {
            out.push(RegressionReport::Fail {
                bench_id: id.to_owned(), metric: "TPS".into(),
                delta_pct, threshold_pct: self.config.tps_fail_pct,
                baseline: base.tps, current: cur.tps,
            });
        } else if delta_pct > self.config.improve_pct {
            out.push(RegressionReport::Improve {
                bench_id: id.to_owned(), metric: "TPS".into(),
                delta_pct, baseline: base.tps, current: cur.tps,
            });
        }
    }

    fn check_p99(&self, id: &str, base: &BenchMetrics, cur: &BenchMetrics, out: &mut Vec<RegressionReport>) {
        if base.latency_p99_us == 0.0 { return; }
        let delta_pct = (cur.latency_p99_us - base.latency_p99_us) / base.latency_p99_us * 100.0;
        if delta_pct.abs() < self.config.noise_floor_pct { return; }

        if delta_pct > self.config.p99_fail_pct {
            out.push(RegressionReport::Fail {
                bench_id: id.to_owned(), metric: "P99_us".into(),
                delta_pct, threshold_pct: self.config.p99_fail_pct,
                baseline: base.latency_p99_us, current: cur.latency_p99_us,
            });
        } else if delta_pct < -self.config.improve_pct {
            out.push(RegressionReport::Improve {
                bench_id: id.to_owned(), metric: "P99_us".into(),
                delta_pct, baseline: base.latency_p99_us, current: cur.latency_p99_us,
            });
        }
    }

    fn check_alloc(&self, id: &str, base: &BenchMetrics, cur: &BenchMetrics, out: &mut Vec<RegressionReport>) {
        let (Some(b), Some(c)) = (base.alloc_count_per_op, cur.alloc_count_per_op) else { return };
        if b == 0.0 { return; }
        let delta_pct = (c - b) / b * 100.0;
        if delta_pct < self.config.noise_floor_pct { return; }
        if delta_pct > self.config.alloc_warn_pct {
            out.push(RegressionReport::Warn {
                bench_id: id.to_owned(), metric: "alloc/op".into(),
                delta_pct, threshold_pct: self.config.alloc_warn_pct,
            });
        }
    }

    fn check_wal_latency(&self, id: &str, base: &BenchMetrics, cur: &BenchMetrics, out: &mut Vec<RegressionReport>) {
        // WAL bench 用 P99 作为 flush 延迟代理指标
        if base.latency_p99_us == 0.0 { return; }
        let delta_pct = (cur.latency_p99_us - base.latency_p99_us) / base.latency_p99_us * 100.0;
        if delta_pct > self.config.wal_fail_pct {
            out.push(RegressionReport::Fail {
                bench_id: id.to_owned(), metric: "WAL_P99_us".into(),
                delta_pct, threshold_pct: self.config.wal_fail_pct,
                baseline: base.latency_p99_us, current: cur.latency_p99_us,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::BenchMetrics;
    use std::collections::HashMap;

    fn make_results(tps: f64, p99: f64) -> BenchResults {
        let mut results = HashMap::new();
        results.insert("e2e/point_select".into(), BenchMetrics {
            bench_id: "e2e/point_select".into(),
            tps, latency_p99_us: p99,
            ..Default::default()
        });
        BenchResults { run_id: "test".into(), results, ..Default::default() }
    }

    #[test]
    fn regression_tps_fail() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let current  = make_results(90_000.0, 100.0); // -10% TPS → FAIL
        let reports  = checker.check(&current);
        assert!(!RegressionChecker::is_pass(&reports));
        assert!(reports.iter().any(|r| r.is_fail()));
    }

    #[test]
    fn regression_tps_ok() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let current  = make_results(97_000.0, 100.0); // -3% TPS → within threshold
        let reports  = checker.check(&current);
        assert!(RegressionChecker::is_pass(&reports));
    }

    #[test]
    fn regression_p99_fail() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let current  = make_results(100_000.0, 120.0); // +20% P99 → FAIL
        let reports  = checker.check(&current);
        assert!(!RegressionChecker::is_pass(&reports));
    }

    #[test]
    fn regression_noise_floor() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let current  = make_results(99_500.0, 101.0); // <2% both → noise
        let reports  = checker.check(&current);
        assert!(RegressionChecker::is_pass(&reports));
        assert!(reports.is_empty());
    }

    #[test]
    fn regression_new_bench() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let mut results = HashMap::new();
        results.insert("e2e/new_bench".into(), BenchMetrics {
            bench_id: "e2e/new_bench".into(), tps: 50_000.0,
            ..Default::default()
        });
        let current = BenchResults { results, ..Default::default() };
        let reports = checker.check(&current);
        assert!(RegressionChecker::is_pass(&reports));
        assert!(reports.iter().any(|r| matches!(r, RegressionReport::New { .. })));
    }

    #[test]
    fn regression_improve() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let current  = make_results(120_000.0, 80.0); // +20% TPS, -20% P99 → IMPROVE
        let reports  = checker.check(&current);
        assert!(RegressionChecker::is_pass(&reports));
        assert!(reports.iter().any(|r| matches!(r, RegressionReport::Improve { .. })));
    }

    #[test]
    fn markdown_output_contains_table() {
        let baseline = make_results(100_000.0, 100.0);
        let checker  = RegressionChecker::new(baseline);
        let current  = make_results(90_000.0, 100.0);
        let reports  = checker.check(&current);
        let md = RegressionChecker::to_markdown(&reports, &current);
        assert!(md.contains("FalconDB Performance Gate"));
        assert!(md.contains("❌"));
        assert!(md.contains("e2e/point_select"));
    }
}
