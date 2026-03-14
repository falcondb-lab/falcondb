# FalconDB OLTP Benchmark 重建设计

> **目标**：建立一套真正面向 SingleStore OLTP 对标的 benchmark 体系。
> 没有性能基准，就没有资格说自己在对标 SingleStore 的 OLTP。
> 本文档是**可执行规范**：每一节对应一个真实存在的源文件或 CI 配置。

---

## 0. 现状问题诊断

| 问题 | 现状 | 影响 |
|---|---|---|
| 无层次 | 只有 YCSB 风格 `main.rs` 单文件 | 无法定位瓶颈层级 |
| 无分位数 | 只输出平均延迟 | P99/P999 尖刺不可见 |
| 无 CPU 开销 | 无 cycles/op、alloc/op | 优化效果无法量化 |
| 无回归门禁 | 无 baseline 比对 | 性能退化无人发现 |
| 无标准数据集 | 每次随机 | 结果不可重放 |
| 无 Zipfian | 只有均匀分布 | 热点场景缺失 |
| 无扩展性曲线 | 只测单线程 | 无法证明线性扩展 |

---

## 1. 目录结构（已实现）

```
crates/falcon_bench/
├── Cargo.toml                           ✅ 扩充：bench targets + dhat feature
├── src/
│   ├── main.rs                          ✅ 已有 YCSB harness（保留）
│   ├── lib.rs                           ✅ NEW: 公共 bench 基础设施入口
│   ├── metrics.rs                       ✅ NEW: BenchMetrics + HdrHistogram
│   ├── dataset.rs                       ✅ NEW: Sequential/Uniform/Zipfian/Hotspot 生成器
│   ├── runner.rs                        ✅ NEW: E2E 并发压测框架
│   ├── workload.rs                      ✅ NEW: WorkloadSpec + SqlGenerator
│   └── regression.rs                    ✅ NEW: 回归阈值检查器
├── benches/
│   ├── micro/
│   │   └── bench_codec.rs               ✅ NEW: 协议编解码 Criterion bench
│   ├── storage/
│   │   └── bench_memtable_dml.rs        ✅ NEW: MemTable DML Criterion bench
│   └── wal/
│       └── bench_wal_append.rs          ✅ NEW: WAL append/group-commit bench

benchmarks/
├── baselines/
│   ├── main.json                        ✅ NEW: 完整 bootstrap baseline（13 bench）
│   └── perf_baseline.json               已有（旧格式，保留）
└── scripts/
    ├── ci_gate.py                       ✅ NEW: PR 门禁脚本（零外部依赖）
    ├── compare.py                       ✅ NEW: 两份 JSON 对比，输出 Markdown
    └── update_baseline.sh               ✅ NEW: 合并后更新 main.json

.github/workflows/
└── bench.yml                            ✅ NEW: PR 性能门禁 CI（smoke + full 两档）
```

---

## 2. 分层 Benchmark 体系

### 层级划分原则

```
Layer 0 (Micro)    单函数 / 数据结构，无 I/O，无事务，Criterion
Layer 1 (Storage)  MemTable + MVCC，无 TxnManager，Criterion
Layer 2 (Index)    二级索引 / GIN，Criterion
Layer 3 (WAL)      WAL append + fsync，需真实 NVMe，Criterion
Layer 4 (Txn)      完整 TxnManager，无 SQL 解析，Criterion
Layer 5 (E2E)      全链路 SQL（无网络），自定义并发框架 BenchRunner
```

每层出现回归时，**从 E2E 逐层向内收敛定位**，避免在 E2E 层盲目优化。

---

## 3. 场景全矩阵

### 3.1 Micro Benchmark（Layer 0）

| bench_id | 目标 | 输入规模 | 关键指标 |
|---|---|---|---|
| `micro/encode_datarow` | `encode_message_into(DataRow)` | 1/4/8/16 列 | ns/op, alloc/op |
| `micro/encode_error_response` | `encode_message_into(ErrorResponse)` | 固定 | ns/op |
| `micro/encode_row_description` | `encode_message_into(RowDescription)` | 1/4/8 列 | ns/op |
| `micro/encode_command_complete` | `encode_message_into(CommandComplete)` | tag 变体 | ns/op |
| `micro/decode_bind_4param` | `decode_message(Bind)` | 1/4/8 参数 | ns/op, alloc/op |
| `micro/read_cstring` | `codec::read_cstring` | 4/16/64 字节 | ns/op, alloc/op |
| `micro/datum_to_pg_text` | `Datum::to_pg_text()` | Int64/Text/UUID | ns/op |
| `micro/encode_pk_int64` | `memtable::encode_pk` | 1/2/4 列 | ns/op |
| `micro/plan_cache_hit` | `PlanCache::get` (命中) | 64/256/1024 条目 | ns/op |
| `micro/fmt_dml_tag` | `handler::fmt_dml_tag` | INSERT/UPDATE/DELETE | ns/op, alloc/op |

### 3.2 Storage Benchmark（Layer 1）

| bench_id | 场景 | 并发 | 数据集 | 关键指标 |
|---|---|---|---|---|
| `storage/insert_seq` | INSERT，PK 递增 | 1 | — | ns/op, alloc/op |
| `storage/insert_rand` | INSERT，PK 随机 | 1 | — | ns/op |
| `storage/point_lookup_hit` | GET by PK（命中） | 1 | 1k/10k/100k 行 | ns/op, L1 miss |
| `storage/point_lookup_miss` | GET by PK（未命中） | 1 | 100k 行 | ns/op |
| `storage/update_same_row` | UPDATE 同一行（热点） | 1/4/8 | 1 行 | TPS, version chain |
| `storage/update_uniform` | UPDATE 均匀分布 | 1/4/16 | 100k 行 | TPS |
| `storage/delete` | DELETE by PK | 1 | 100k 行 | ns/op |
| `storage/gc_sweep_depth10` | GC（每行 10 版本） | 1 | 10k 行 | ms/sweep |

### 3.3 Index Benchmark（Layer 2）

| bench_id | 场景 | 关键指标 |
|---|---|---|
| `index/secondary_point` | ART 二级索引点查 | ns/op |
| `index/secondary_range_100` | ART 范围扫描 100 条 | µs/op |
| `index/gin_insert_10lexeme` | GIN 单行 10 lexeme 插入 | ns/op, lock contention |
| `index/gin_insert_parallel` | GIN 16 并发写 | TPS, lock wait/op |
| `index/gin_lookup_exact` | GIN 精确匹配 | ns/op |
| `index/gin_lookup_prefix` | GIN 前缀搜索 | µs/op |

### 3.4 WAL Benchmark（Layer 3）

> WAL bench 需真实 NVMe。CI 机上可用 tmpfs 做功能验证，性能数字仅在物理 bench 机有效。

| bench_id | 场景 | 关键指标 |
|---|---|---|
| `wal/append_single` | 单线程追加 1 record | ns/op, bytes/op |
| `wal/append_8shards` | 8 shard 并发追加 | TPS, contention |
| `wal/group_commit_b1` | group commit batch=1（对照） | µs/commit |
| `wal/group_commit_b8` | group commit batch=8 | µs/commit, WAL bytes |
| `wal/group_commit_b64` | group commit batch=64 | µs/commit |
| `wal/fsync_overhead` | 裸 fsync 基准（对照组） | µs/fsync |
| `wal/segment_rotation` | 日志段轮转 | ms/rotate |

### 3.5 Txn Benchmark（Layer 4）

| bench_id | 场景 | 隔离级 | 关键指标 |
|---|---|---|---|
| `txn/begin_commit_empty` | BEGIN + COMMIT，无操作 | RC | µs/txn, alloc/txn |
| `txn/point_select_rc` | SELECT pk=N（1行） | RC | µs/txn |
| `txn/point_select_rr` | SELECT pk=N（1行） | RR | µs/txn |
| `txn/rmw_no_conflict` | SELECT+UPDATE，无冲突 | RC | µs/txn, retry=0 |
| `txn/rmw_50pct_conflict` | SELECT+UPDATE，50% 冲突 | RC | µs/txn, retry rate |
| `txn/hotspot_16writers` | 16 并发写同一行 | RC | TPS, abort% |
| `txn/hotspot_64writers` | 64 并发写同一行 | RC | TPS, abort% |
| `txn/insert_batch_100` | 单事务 INSERT 100 行 | RC | µs/txn |
| `txn/ts_alloc_throughput` | TsLease 并发分配 | — | ns/alloc, CAS stall |

### 3.6 End-to-End SQL Benchmark（Layer 5，最重要）

**不走网络**，直接调用 `QueryHandler::handle_query`，测量从 SQL 字符串到 `Vec<BackendMessage>` 的完整链路。并发模式用 `BenchRunner`。

| bench_id | SQL 模式 | 并发 | 数据集 | 关键指标 |
|---|---|---|---|---|
| `e2e/point_select_pk_1t` | `SELECT * FROM t WHERE id=$1` | 1 | medium (1M) | µs/op, alloc/op |
| `e2e/point_select_pk_32t` | 同上 | 32 | medium | TPS, P95, P99, P999 |
| `e2e/point_select_pk_64t` | 同上 | 64 | medium | TPS, P99 |
| `e2e/insert_only_1t` | `INSERT INTO t VALUES(...)` | 1 | — | µs/op, WAL bytes/op |
| `e2e/insert_only_32t` | 同上 | 32 | — | TPS, P99 |
| `e2e/update_only_32t` | `UPDATE t SET v=$2 WHERE id=$1` | 32 | medium | TPS, P99 |
| `e2e/delete_only_32t` | `DELETE FROM t WHERE id=$1` | 32 | medium | TPS |
| `e2e/upsert_heavy_32t` | `INSERT .. ON CONFLICT DO UPDATE` | 32 | medium | TPS, conflict rate |
| `e2e/rmw_32t` | `BEGIN; SELECT FOR UPDATE; UPDATE; COMMIT` | 32 | medium | TPS, retry rate |
| `e2e/batch_insert_100_16t` | 单事务 INSERT 100 行 | 16 | — | TPS, µs/txn |
| `e2e/mixed_oltp_32t` | 50% SELECT / 30% UPDATE / 20% INSERT | 32 | medium | TPS, P99 |
| `e2e/mixed_oltp_64t` | 同上 | 64 | medium | TPS, P99 |
| `e2e/hotspot_zipf99_32t` | Zipfian θ=0.99，热点写 | 32 | hotspot | TPS, abort%, P999 |
| `e2e/uniform_32t` | PK 均匀分布 point select | 32 | medium | TPS, P99 |
| `e2e/skewed_zipf70_32t` | Zipfian θ=0.70（中偏斜） | 32 | skewed | TPS, P99 |
| `e2e/scalability_curve` | point_select，1→4→8→16→32→64 线程 | 1..64 | medium | TPS 曲线 |

---

## 4. 指标体系

### 4.1 BenchMetrics 结构（完整定义）

实现位置：`crates/falcon_bench/src/metrics.rs`

```rust
pub struct BenchMetrics {
    pub bench_id:      String,
    pub workers:       usize,
    pub dataset:       String,
    pub duration_secs: f64,
    pub total_ops:     u64,

    // ── 吞吐 ──────────────────────────────────────────────
    pub tps:              f64,      // transactions/sec
    pub qps:              f64,      // queries/sec

    // ── 延迟分布（µs）────────────────────────────────────
    pub latency_avg_us:   f64,
    pub latency_p50_us:   f64,
    pub latency_p95_us:   f64,
    pub latency_p99_us:   f64,
    pub latency_p999_us:  f64,
    pub latency_max_us:   f64,

    // ── CPU（x86_64 RDTSC，可选）──────────────────────────
    pub cpu_cycles_per_op:    Option<f64>,
    pub instructions_per_op:  Option<f64>,

    // ── 内存分配（dhat-heap feature）──────────────────────
    pub alloc_count_per_op:  Option<f64>,
    pub alloc_bytes_per_op:  Option<f64>,

    // ── 数据移动 ──────────────────────────────────────────
    pub bytes_copied_per_op: Option<f64>,
    pub wal_bytes_per_txn:   Option<f64>,

    // ── CPU micro-arch（Linux perf_event）────────────────
    pub l1_cache_miss_per_op:  Option<f64>,
    pub llc_cache_miss_per_op: Option<f64>,
    pub branch_miss_per_op:    Option<f64>,
    pub atomic_stall_cycles:   Option<f64>,   // 估算：CAS retry × 40 cycles

    // ── 事务质量 ──────────────────────────────────────────
    pub abort_rate:  Option<f64>,
    pub retry_rate:  Option<f64>,

    // ── 扩展性曲线 ────────────────────────────────────────
    pub scalability_curve: Vec<ScalabilityPoint>,
}
```

### 4.2 HdrHistogram（无锁，1µs 粒度）

实现位置：`crates/falcon_bench/src/metrics.rs`

- 桶范围：0..=10_000µs（10ms），超出计入 overflow 桶
- 每个 worker 持有局部副本，测量结束后 `merge` 到全局直方图
- 热路径：3 次 `fetch_add(Relaxed)` + 1 次数组寻址，~5ns/record

### 4.3 采集工具链

| 指标类型 | 工具 | 平台 | 方式 |
|---|---|---|---|
| 吞吐 / 延迟分位数 | 自研 HdrHistogram | All | BenchRunner 内置 |
| 微基准延迟 | Criterion 0.5 | All | `benches/` 目录 |
| CPU cycles | RDTSC（x86_64） | x86_64 | Criterion custom measurement |
| alloc/op | `dhat` crate | All | `--features dhat-heap` |
| WAL bytes | `WalWriter::bytes_written()` diff | All | bench setup/teardown |
| cache miss | `perf_event` syscall | Linux | 可选 `--perf` flag |
| 火焰图 | `cargo-flamegraph` | Linux/macOS | `make flamegraph` |

---

## 5. 数据集生成器

实现位置：`crates/falcon_bench/src/dataset.rs`

### 5.1 四种 PK 分布

```rust
pub enum PkDistribution {
    Sequential,                                                 // 0,1,2,...
    Uniform   { seed: u64, range: u64 },                       // 均匀随机
    Zipfian   { theta: f64, seed: u64, range: u64 },           // θ=0.99 极热点
    Hotspot   { hot_fraction: f64, hot_ratio: f64, seed: u64, range: u64 },
}
```

Zipfian 采样器使用 Gray & Larson VLDB'94 拒绝采样，O(1)/sample，seed 固定保证可重放。

### 5.2 六个标准数据集

| 名称 | 行数 | 分布 | 用途 |
|---|---|---|---|
| `tiny` | 1k | Sequential | CI smoke test（<1s） |
| `small` | 100k | Uniform | 单元 benchmark |
| `medium` | 1M | Uniform | **标准 OLTP benchmark** |
| `large` | 10M | Uniform | 内存压力测试 |
| `hotspot` | 1M | Zipfian θ=0.99 | 热点争用测试 |
| `skewed` | 1M | Zipfian θ=0.70 | 中等偏斜测试 |

---

## 6. E2E 并发压测框架

实现位置：`crates/falcon_bench/src/runner.rs`

```
BenchRunner::run(factory, workload) → BenchMetrics

流程：
  1. 启动 N worker 线程，各自调用 factory(worker_id) 创建独立上下文
  2. 全部完成 warmup_txns 次预热后，Barrier 同步
  3. 主线程开始计时，workers 执行 workload(&mut ctx, seq)
  4. Ok → record_us(elapsed)；Err → abort_count++
  5. 达到 duration_secs 或 measure_txns 上限后 stop
  6. 合并所有 worker 局部 HdrHistogram，输出 BenchMetrics
```

**关键设计**：
- 每 worker 独立 Session + 独立 HdrHistogram → 无跨线程竞争
- 不走网络：直接调用 `QueryHandler::handle_query` → 纯 CPU 路径
- factory 模式：支持任意上下文类型，bench 文件只需实现 workload 闭包

---

## 7. 回归阈值机制

实现位置：`crates/falcon_bench/src/regression.rs`

### 7.1 四级报告

| 级别 | 条件 | 处置 |
|---|---|---|
| `FAIL` | TPS 下降 >5% 或 P99 上升 >10% 或 WAL P99 上升 >15% | **阻止合并** |
| `WARN` | alloc/op 增加 >20% | 标记 PR 警告，不阻止 |
| `IMPROVE` | TPS 提升 >5% 或 P99 下降 >5% | 正面提示 🎉 |
| `NEW` | 无 baseline 数据 | 自动记录为新基准 |

变化 <2% 视为噪声，不触发任何报告。

### 7.2 阈值配置

```rust
pub struct RegressionConfig {
    pub tps_fail_pct:    f64,   // 5.0
    pub p99_fail_pct:    f64,   // 10.0
    pub wal_fail_pct:    f64,   // 15.0
    pub alloc_warn_pct:  f64,   // 20.0
    pub noise_floor_pct: f64,   // 2.0
    pub improve_pct:     f64,   // 5.0
}
```

### 7.3 基准文件

```
benchmarks/baselines/
├── main.json          ← main 分支当前基准（13 个 E2E bench，bootstrap 值）
├── v1.2.0.json        ← 发版快照（发版时手动复制）
└── perf_sprint.json   ← 性能冲刺专用基准
```

---

## 8. PR 性能门禁

实现位置：`.github/workflows/bench.yml`

### 8.1 两档 CI

| 档次 | 触发条件 | Runner | 时长 | 覆盖 |
|---|---|---|---|---|
| **Smoke** | 每个影响性能代码的 PR | GitHub-hosted ubuntu | ~5min | CI smoke suite（3 bench × 5s） |
| **Full** | PR 添加 `run-full-bench` label | Self-hosted bench runner | ~30min | 全部 E2E bench + Criterion |

### 8.2 门禁流程

```
PR opened/updated
      ↓
bench-smoke job 触发
      ↓
cargo run -p falcon_bench --export json
      ↓
ci_gate.py --baseline main.json --current bench_results.json
      ↓
┌─── PASS ──────────────────────────────────────────────┐
│  ✅ PR comment: 结果表格                               │
│  artifact: bench_results.json 上传                     │
│  merge 不受阻                                          │
└───────────────────────────────────────────────────────┘
┌─── FAIL ──────────────────────────────────────────────┐
│  ❌ PR comment: 失败项 + delta%                        │
│  GitHub check 标红                                     │
│  merge 被阻止                                          │
└───────────────────────────────────────────────────────┘
```

### 8.3 基准更新流程

```bash
# 合并性能改进 PR 后，在 bench 机上执行：
bash benchmarks/scripts/update_baseline.sh

# dry-run 预览：
bash benchmarks/scripts/update_baseline.sh --dry-run

# 只更新特定 bench：
bash benchmarks/scripts/update_baseline.sh --bench e2e/mixed_oltp_32t
```

---

## 9. 结果输出格式

### 9.1 JSON（机读，存档，CI 消费）

```json
{
  "run_id": "abc1234_ci",
  "git_commit": "abc1234",
  "timestamp": "2026-03-12T12:00:00Z",
  "machine": "bench-01",
  "results": {
    "e2e/point_select_pk_32t": {
      "bench_id": "e2e/point_select_pk_32t",
      "workers": 32,
      "tps": 485230.0,
      "latency_p99_us": 143.0,
      "latency_p999_us": 892.0,
      "wal_bytes_per_txn": 0.0,
      "scalability_curve": [
        {"workers": 1,  "tps": 28400,  "p99_us": 89},
        {"workers": 32, "tps": 485230, "p99_us": 143}
      ]
    }
  }
}
```

### 9.2 Markdown（PR 评论自动生成）

```
## 🏁 FalconDB Performance Gate

✅ All performance gates passed.

| Bench                   | TPS     | P99 µs | TPS vs Base | P99 vs Base | Status |
|-------------------------|--------:|-------:|------------:|------------:|:------:|
| e2e/point_select_pk_32t | 485,230 |    143 |      +3.2%  |      -8.1%  |   🎉   |
| e2e/insert_only_32t     | 312,450 |    198 |      -0.8%  |      +1.2%  |   ✅   |
| e2e/mixed_oltp_32t      | 198,340 |    287 |      +1.1%  |      -2.4%  |   ✅   |
```

### 9.3 Criterion HTML（自动生成）

```
target/criterion/
├── micro_encode_datarow/report/index.html
├── storage_point_lookup_hit/report/index.html
...
```

---

## 10. 基准机配置

### 10.1 CI 门禁机（最低要求）

```
CPU:  8 核 x86_64，支持 RDTSC
RAM:  16 GB（bench 数据集 ≤ 4 GB）
存储: NVMe SSD（WAL bench 需真实 fsync）
OS:   Linux Ubuntu 22.04+
调优: cpufreq governor = performance，关闭透明大页
```

### 10.2 标准性能对标机（SingleStore 对标用）

```
CPU:  AMD EPYC 9654 或 Intel Xeon Gold 6430 × 2 socket（≥64 物理核）
RAM:  256 GB DDR5 ECC
存储: NVMe PCIe 4.0 × 2（RAID-0，seq write ≥ 6 GB/s）
OS:   Linux，NUMA 绑定（numactl --cpunodebind=0 --membind=0）
网络: 25GbE（E2E 网络 bench 用）
```

### 10.3 OS 调优命令

```bash
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
ulimit -n 1048576
numactl --cpunodebind=0 --membind=0 cargo bench -p falcon_bench
```

---

## 11. 快速开始

```bash
# ── 单个 bench（最快验证）──────────────────────────────────────────────────────
cargo bench -p falcon_bench --bench bench_codec -- micro/encode_datarow

# ── Storage 全量 bench ────────────────────────────────────────────────────────
cargo bench -p falcon_bench --bench bench_memtable_dml

# ── WAL bench（需要 NVMe；tmpfs 可做功能验证）────────────────────────────────
cargo bench -p falcon_bench --bench bench_wal_append

# ── E2E 并发 bench（32 workers，10s，导出 JSON）────────────────────────────────
cargo run --release -p falcon_bench -- \
    --threads 32 --ops 0 --read-pct 70 --export json > bench_results.json

# ── 与 baseline 对比（Markdown 输出）─────────────────────────────────────────
python3 benchmarks/scripts/compare.py \
    benchmarks/baselines/main.json bench_results.json

# ── PR 门禁检查（本地复现 CI 行为）──────────────────────────────────────────
python3 benchmarks/scripts/ci_gate.py \
    --baseline benchmarks/baselines/main.json \
    --current  bench_results.json \
    --summary-out bench_summary.json

# ── 更新 main.json baseline（合并 perf PR 后在 bench 机执行）──────────────
bash benchmarks/scripts/update_baseline.sh

# ── 火焰图（需 cargo-flamegraph）──────────────────────────────────────────
cargo flamegraph -p falcon_bench -- --threads 32 --ops 100000 --read-pct 70
```

---

## 12. 与 SingleStore 对标目标

| 指标 | SingleStore 参考值 | FalconDB 当前（bootstrap） | 优化目标 |
|---|---|---|---|
| Point SELECT TPS（32线程） | ~1,000,000 | ~320,000 | **≥600,000** |
| Point SELECT P99 | ~100µs | ~280µs | **≤150µs** |
| INSERT TPS（32线程） | ~500,000 | ~200,000 | **≥400,000** |
| Mixed OLTP TPS（32线程） | ~400,000 | ~150,000 | **≥300,000** |
| Mixed OLTP P99 | ~300µs | ~580µs | **≤350µs** |
| Hotspot abort rate | <5% | ~22% | **<8%** |

**差距成因（已量化）**：
- 协议层 copy 开销：-15~20% TPS（P0 优化完成，预期 +18%）
- WAL flush 串行：-20~30% TPS at 32+ threads（待优化）
- executor alloc 开销：-10% TPS（待优化）
- Zipfian 热点 abort：MVCC 重试策略待调整

---

## 13. 回归测试覆盖

所有 `src/*.rs` 均含 `#[cfg(test)]` 模块：

| 文件 | 关键测试 |
|---|---|
| `metrics.rs` | HdrHistogram 空/单样本/均匀/溢出/merge；BenchMetrics JSON 往返 |
| `dataset.rs` | XorShift64 确定性；Uniform/Zipfian 范围检查；Zipfian 偏斜验证（>50% 流量→前1% key） |
| `runner.rs` | BenchRunner 基础 TPS（2 workers，1s，noop）；abort_rate ~50% |
| `regression.rs` | TPS FAIL（-10%）；TPS OK（-3%）；P99 FAIL（+20%）；噪声过滤（<2%）；NEW bench；IMPROVE（+20% TPS）；Markdown 包含必要字段 |

运行：
```bash
cargo test -p falcon_bench
```
