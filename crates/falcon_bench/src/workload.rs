// crates/falcon_bench/src/workload.rs
//
// Workload 枚举：描述 bench 场景的操作混合比例和并发参数。
// 每个 Workload 携带足够信息，让 bench 框架自动生成 SQL 和请求序列。

use crate::dataset::PkSampler;

/// OLTP workload 类型
#[derive(Debug, Clone)]
pub enum WorkloadKind {
    /// 100% point SELECT by PK
    PointSelect,
    /// 100% INSERT（自增 PK）
    InsertOnly,
    /// 100% UPDATE by PK（uniform 分布）
    UpdateOnly,
    /// 100% DELETE by PK
    DeleteOnly,
    /// INSERT ON CONFLICT DO UPDATE（upsert）
    UpsertHeavy,
    /// BEGIN; SELECT FOR UPDATE; UPDATE; COMMIT（读改写）
    ReadModifyWrite,
    /// 单事务 INSERT N 行
    BatchInsert { batch_size: usize },
    /// 混合：pct_select% SELECT + pct_update% UPDATE + remainder INSERT
    Mixed { pct_select: u8, pct_update: u8 },
    /// Zipfian 热点：所有操作集中在少量 key
    HotspotKey { pct_read: u8 },
}

/// 单个 bench 任务的完整规格
#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub bench_id:      String,
    pub kind:          WorkloadKind,
    pub workers:       usize,
    pub warmup_txns:   u64,
    pub duration_secs: u64,
    pub dataset_name:  &'static str,
}

impl WorkloadSpec {
    /// 从 bench_id 字符串推导标准规格（用于 CI 批量运行）
    pub fn standard_suite() -> Vec<WorkloadSpec> {
        vec![
            // ── E2E 核心场景 ──────────────────────────────────────────────
            WorkloadSpec {
                bench_id:      "e2e/point_select_pk_1t".into(),
                kind:          WorkloadKind::PointSelect,
                workers:       1,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/point_select_pk_32t".into(),
                kind:          WorkloadKind::PointSelect,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/insert_only_32t".into(),
                kind:          WorkloadKind::InsertOnly,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/update_only_32t".into(),
                kind:          WorkloadKind::UpdateOnly,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/delete_only_32t".into(),
                kind:          WorkloadKind::DeleteOnly,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/upsert_heavy_32t".into(),
                kind:          WorkloadKind::UpsertHeavy,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/rmw_32t".into(),
                kind:          WorkloadKind::ReadModifyWrite,
                workers:       32,
                warmup_txns:   500,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/batch_insert_100_16t".into(),
                kind:          WorkloadKind::BatchInsert { batch_size: 100 },
                workers:       16,
                warmup_txns:   200,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/mixed_oltp_32t".into(),
                kind:          WorkloadKind::Mixed { pct_select: 50, pct_update: 30 },
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/mixed_oltp_64t".into(),
                kind:          WorkloadKind::Mixed { pct_select: 50, pct_update: 30 },
                workers:       64,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/hotspot_zipf99_32t".into(),
                kind:          WorkloadKind::HotspotKey { pct_read: 30 },
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "hotspot",
            },
            WorkloadSpec {
                bench_id:      "e2e/uniform_32t".into(),
                kind:          WorkloadKind::PointSelect,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "medium",
            },
            WorkloadSpec {
                bench_id:      "e2e/skewed_zipf70_32t".into(),
                kind:          WorkloadKind::PointSelect,
                workers:       32,
                warmup_txns:   1_000,
                duration_secs: 10,
                dataset_name:  "skewed",
            },
        ]
    }

    /// CI smoke suite — 只跑最关键的几个 bench，约 90 秒完成
    pub fn ci_smoke_suite() -> Vec<WorkloadSpec> {
        Self::standard_suite()
            .into_iter()
            .filter(|s| matches!(
                s.bench_id.as_str(),
                "e2e/point_select_pk_32t"
                | "e2e/insert_only_32t"
                | "e2e/mixed_oltp_32t"
            ))
            .map(|mut s| { s.duration_secs = 5; s })
            .collect()
    }
}

/// SQL 生成器：从 WorkloadKind + PkSampler 生成 SQL 语句
pub struct SqlGenerator {
    pub kind:        WorkloadKind,
    pub pk_sampler:  PkSampler,
    next_insert_pk:  i64,
}

impl SqlGenerator {
    pub fn new(kind: WorkloadKind, pk_sampler: PkSampler) -> Self {
        Self { kind, pk_sampler, next_insert_pk: 10_000_000 }
    }

    /// 生成下一条 SQL（单语句模式；ReadModifyWrite 等多步事务需特殊处理）
    pub fn next_sql(&mut self) -> String {
        let pk = self.pk_sampler.next_pk();
        match &self.kind {
            WorkloadKind::PointSelect =>
                format!("SELECT id, balance, name FROM bench_t WHERE id = {pk}"),

            WorkloadKind::InsertOnly => {
                let new_pk = self.next_insert_pk;
                self.next_insert_pk += 1;
                format!("INSERT INTO bench_t(id, balance, name) VALUES({new_pk}, {}, 'user_{new_pk:010}')",
                        new_pk % 1_000_000)
            }

            WorkloadKind::UpdateOnly =>
                format!("UPDATE bench_t SET balance = balance + 1 WHERE id = {pk}"),

            WorkloadKind::DeleteOnly =>
                format!("DELETE FROM bench_t WHERE id = {pk}"),

            WorkloadKind::UpsertHeavy => {
                let new_pk = self.next_insert_pk;
                self.next_insert_pk += 1;
                format!("INSERT INTO bench_t(id, balance, name) VALUES({new_pk}, 0, 'upsert_{new_pk}') \
                         ON CONFLICT(id) DO UPDATE SET balance = bench_t.balance + 1")
            }

            WorkloadKind::ReadModifyWrite =>
                // 多步事务：调用方需拆分
                format!("UPDATE bench_t SET balance = balance + 1 WHERE id = {pk}"),

            WorkloadKind::BatchInsert { batch_size } => {
                let mut sql = String::from("INSERT INTO bench_t(id, balance, name) VALUES");
                for i in 0..*batch_size {
                    let bpk = self.next_insert_pk + i as i64;
                    if i > 0 { sql.push(','); }
                    sql.push_str(&format!("({bpk}, {}, 'batch_{bpk}')", bpk % 1_000_000));
                }
                self.next_insert_pk += *batch_size as i64;
                sql
            }

            WorkloadKind::Mixed { pct_select, pct_update } => {
                let r = (pk.unsigned_abs() % 100) as u8;
                if r < *pct_select {
                    format!("SELECT id, balance FROM bench_t WHERE id = {pk}")
                } else if r < pct_select + pct_update {
                    format!("UPDATE bench_t SET balance = balance + 1 WHERE id = {pk}")
                } else {
                    let new_pk = self.next_insert_pk;
                    self.next_insert_pk += 1;
                    format!("INSERT INTO bench_t(id, balance, name) VALUES({new_pk}, 0, 'ins_{new_pk}')")
                }
            }

            WorkloadKind::HotspotKey { pct_read } => {
                let r = (pk.unsigned_abs() % 100) as u8;
                if r < *pct_read {
                    format!("SELECT id, balance FROM bench_t WHERE id = {pk}")
                } else {
                    format!("UPDATE bench_t SET balance = balance + 1 WHERE id = {pk}")
                }
            }
        }
    }
}
