# FalconDB 对标 SingleStore OLTP 第一阶段整改验收方案

> **文档性质**：技术 PM 验收规范，法律效力等同内部里程碑合同。  
> **原则**：整改结束后，FalconDB 必须拿得出**性能证据**，而不是只有**架构叙事**。  
> **版本**：Phase 1 · 2026-03 · 12 周整改期

---

## 一、执行摘要（一页）

### 背景

FalconDB 对标 SingleStore Rowstore OLTP 内核。当前单节点在标准 pgbench workload C 下
仅能达到约 **46K TPS（16 核）**，距 SingleStore 参考值 **~400K TPS** 有 8× 差距。
差距成因已完整审计（见 `docs/OLTP_PERF_PLAN.md`），集中在 5 层：

1. WAL group commit 无批量 → 单线程写上限约 47K TPS
2. commit 路径 PrimaryKey 3× Vec<u8> clone → 每事务额外 3 次堆分配
3. pk_order RwLock<BTreeSet> 串行写锁 → 多核 stall
4. txn_local DashMap 在 multi-op 事务路径必经 → hash + shard 锁
5. 协议层 to_lowercase() / format!() alloc 散落热路径 → 每 SQL 多次堆分配

### Phase 1 任务

12 周内完成 P0（5 项）+ P1（6 项）全部整改，
以 **可运行 benchmark 数字** 作为唯一验收证据，
不接受"架构已改"但无数字的交付。

### Phase 1 胜利条件摘要

| 维度 | 目标 |
|---|---|
| 单机 OLTP TPS（16c） | ≥70,000 |
| 单机 OLTP TPS（64c） | ≥100,000 |
| 点查 P99 延迟 | ≤0.5ms |
| commit P99 延迟 | ≤0.8ms |
| batch INSERT 吞吐 | ≥600K rows/s |
| 多核扩展比（1c→64c） | ≥4.0× |
| WAL group commit batch | ≥8 records/flush |
| 测试通过率 | 100%（4200+ tests） |

---

## 二、胜利条件（Victory Conditions）

### 2.1 单机 OLTP 性能提升目标

测量工具：`pgbench`（PostgreSQL pgbench，连接到 FalconDB）  
工作负载：**workload C**（SELECT only，point lookup by PK，100K 行预热）  
持续时间：60 秒（正式）/ 10 秒（快速验证）  
网络：Unix socket 或 localhost TCP，排除网络抖动

| KPI | 当前基线 | Phase 1 目标 | 测量命令 |
|---|---|---|---|
| TPS @1c  | ~8,000   | ≥18,000 | `pgbench -c 1  -j 1  -T 60 -M simple` |
| TPS @4c  | ~22,000  | ≥45,000 | `pgbench -c 4  -j 4  -T 60 -M simple` |
| TPS @8c  | ~35,000  | ≥60,000 | `pgbench -c 8  -j 8  -T 60 -M simple` |
| TPS @16c | ~46,000  | ≥70,000 | `pgbench -c 16 -j 16 -T 60 -M simple` |
| TPS @32c | ~51,000  | ≥85,000 | `pgbench -c 32 -j 32 -T 60 -M simple` |
| TPS @64c | ~52,000  | ≥100,000| `pgbench -c 64 -j 64 -T 60 -M simple` |
| 线性扩展比 1c→64c | ~6.5× | ≥8.0×  | 以上数据计算 |

### 2.2 点查延迟下降目标

工作负载：`SELECT * FROM bench_t WHERE id = $1`（prepared statement，Extended Query）  
数据集：100 万行，Uniform 分布，预热 10 秒后采集 60 秒  
并发：1c（单线程延迟代表无争用基线）和 32c（代表争用场景）

| KPI | 当前基线 | Phase 1 目标 | 说明 |
|---|---|---|---|
| P50 @1c  | ~0.12ms | ≤0.05ms | 单核热路径极致 |
| P99 @1c  | ~1.2ms  | ≤0.5ms  | 主要受 WAL + alloc 影响 |
| P999 @1c | ~5ms    | ≤2ms    | GC pause / lock tail |
| P50 @32c | ~0.18ms | ≤0.08ms | 多核竞争 |
| P99 @32c | ~3.5ms  | ≤1.0ms  | DashMap 锁争用消除后 |
| P999 @32c| ~18ms   | ≤5ms    | 热点 key 场景 |

测量方法：`cargo run -p falcon_bench -- --threads 32 --export json`，读取 `latency_p99_us` 字段

### 2.3 commit P99 下降目标

工作负载：单行 INSERT + COMMIT（write-heavy，最能体现 WAL + commit 路径开销）  
测量：`e2e/insert_only_32t` bench（32 并发，10 秒）

| KPI | 当前基线 | Phase 1 目标 | 主因 |
|---|---|---|---|
| commit avg latency | ~500µs | ≤200µs | WAL group commit |
| commit P95 | ~900µs | ≤350µs | PK alloc 消除 |
| commit P99 | ~2.0ms | ≤800µs | txn_local thread-local |
| commit P999 | ~12ms  | ≤3ms   | pk_order 锁消除 |

### 2.4 batch INSERT 提升目标

工作负载：单事务 INSERT 100 行，16 并发  
测量：`e2e/batch_insert_100_16t` bench

| KPI | 当前基线 | Phase 1 目标 |
|---|---|---|
| 行吞吐（rows/s） | ~340K | ≥600K |
| µs/txn（100行） | ~4,400µs | ≤2,000µs |
| WAL bytes/txn | ~12,800 | ≤10,000（pre-serialization 压缩） |

### 2.5 多核扩展性目标

扩展性曲线必须满足以下约束（用 `e2e/scalability_curve` bench 采集）：

| 线程数 | TPS 最低要求 | 扩展比（vs 1c） | 说明 |
|---|---|---|---|
| 1  | ≥18,000  | 1.0× | 基准 |
| 4  | ≥65,000  | ≥3.6× | 近线性 |
| 8  | ≥120,000 | ≥6.7× | |
| 16 | ≥200,000 | ≥11×  | |
| 32 | ≥320,000 | ≥17×  | |
| 64 | ≥480,000 | ≥26×  | 允许超线性（NUMA 感知） |

> **注**：以上为 E2E point_select bench 数字，不含网络开销。pgbench 数字因协议开销偏低 ~30%。

### 2.6 WAL 吞吐提升目标

测量：`wal/group_commit_b8` 和 `wal/group_commit_b64` Criterion bench  
测量机器：NVMe SSD（seq write ≥ 3 GB/s），非 tmpfs

| KPI | 当前基线 | Phase 1 目标 |
|---|---|---|
| group commit batch size @32c | ~1–2 | ≥8 |
| WAL append 吞吐（单线程） | ~200K records/s | ≥400K records/s |
| WAL flush 延迟 P99 | ~3ms | ≤1ms |
| WAL flush 延迟 P999 | ~15ms | ≤4ms |
| fsync 次数/秒 @32c | ~50K | ≤8K（批量合并） |

---

## 三、P0 必须完成项

以下 5 项 P0 整改**必须全部完成**，缺任何一项视为 Phase 1 未通过。

### 3.1 P0 功能必须完成项

| ID | 整改项 | 验收标准 | 代码位置 |
|---|---|---|---|
| P0-1 | WAL group commit follower spin 优化 | `SHOW falcon.wal_stats` 显示 avg_batch_size ≥ 8 @32c | `wal.rs: durable_wal_commit` |
| P0-2 | pk_order 默认 large_mem_mode + lazy rebuild | `EXPLAIN` 点查路径不含 `pk_order_lookup`；`CREATE TABLE` 默认无 pk_order 开销 | `memtable.rs: commit_keys` |
| P0-3 | PrimaryKey → Arc<[u8]> 全路径替换 | heaptrack / dhat 显示 commit 路径 PK alloc 次数 ≤ 1（Arc clone） | `engine.rs` `memtable.rs` |
| P0-4 | txn_local 改 thread_local! 全覆盖（含 multi-op） | flamegraph 中 `DashMap::insert/remove` 在 commit 路径消失 | `engine.rs: TXN_STATE` |
| P0-5 | VersionChain 改 lock-free（AtomicPtr + epoch） | `loom` 模型检查通过；concurrent read benchmark 无 Mutex 竞争 | `mvcc.rs` |

### 3.2 P0 benchmark 必须通过项

以下 benchmark 数字**必须全部达标**，作为 Phase 1 通过的量化证据：

| bench_id | 最低 TPS | 最高 P99 | 测量方式 |
|---|---|---|---|
| `e2e/point_select_pk_32t` | ≥320,000 | ≤1,000µs | `cargo bench` / `BenchRunner` |
| `e2e/insert_only_32t` | ≥200,000 | ≤1,500µs | 同上 |
| `e2e/mixed_oltp_32t` | ≥150,000 | ≤2,000µs | 同上 |
| `e2e/mixed_oltp_64t` | ≥220,000 | ≤2,500µs | 同上 |
| `wal/group_commit_b8` | — | ≤500µs/commit | `cargo bench --bench bench_wal_append` |
| `storage/point_lookup_hit` (100k) | — | ≤2µs/op | `cargo bench --bench bench_memtable_dml` |
| pgbench workload C @16c | ≥70,000 TPS | — | `pgbench -c 16 -j 16 -T 60` |
| pgbench workload C @64c | ≥100,000 TPS | — | `pgbench -c 64 -j 64 -T 60` |

> **注**：E2E bench 不含网络，pgbench 含协议。两套数字都必须采集，不可互相替代。

### 3.3 P0 profiling 结果必须达标项

以下 profiling 结论**必须有截图/数据文件作为证据**，存入 `docs/phase1_evidence/`：

| 证据项 | 工具 | 达标标准 | 证据文件名 |
|---|---|---|---|
| commit 热路径 PK alloc 次数 | heaptrack 或 dhat | ≤ 1 次/commit（Arc clone） | `heaptrack_commit_path.png` |
| DashMap 在 commit flamegraph 占比 | cargo-flamegraph | < 2% CPU 占比 | `flamegraph_commit_32t.svg` |
| WAL flush 占 commit 总时间比 | perf stat | < 40%（group commit 有效批量化） | `perf_stat_insert_32t.txt` |
| VersionChain Mutex 在读路径消失 | flamegraph | `Mutex::lock` 不出现在 `read_committed` 调用栈 | `flamegraph_read_32t.svg` |
| 全局 alloc/commit 次数 | dhat-heap feature | ≤ 12 次/committed txn（单行 INSERT） | `dhat_insert_1t.json` |

---

## 四、不达标时的回退策略

### 4.1 单项 P0 不达标

| 不达标项 | 回退策略 | 最长等待 |
|---|---|---|
| P0-1 WAL group commit | 回退到当前 condvar 实现，接受 TPS 上限 ~50K，记录为 Phase 2 遗留 | 2 周 |
| P0-2 pk_order | 保留 `large_mem_mode` flag，不强制默认，提供 `ALTER SYSTEM SET large_mem_mode = on` | 立即可回退 |
| P0-3 Arc<[u8]> | 回退到 Vec<u8>，只做 P1-5 协议层 alloc 优化保住部分收益 | 1 周 |
| P0-4 thread_local | 回退到 DashMap，接受 multi-op 路径额外 hash 开销 | 立即可回退 |
| P0-5 lock-free VersionChain | **最高风险项**：若 loom 检查失败或数据竞争，立即回退到 Mutex 实现，标记为 Phase 3 | 1 天 |

### 4.2 整体 TPS 目标不达标

若 12 周末 pgbench @16c < 70K TPS：

1. **分析差距来源**：运行 `cargo-flamegraph`，找出 CPU 占比 >5% 的函数
2. **延期接受条件**：达到 55K TPS 且 P99 < 1ms，可申请 2 周延期
3. **降级交付**：若最终仍未达标，必须提供**书面原因分析**（瓶颈位置、剩余工作量）
   而不是沉默或更改目标

### 4.3 稳定性回退

若 `cargo test --workspace` 失败率 > 0%：

1. 立即回退最近一个 commit
2. 不允许以"暂时跳过失败测试"的方式交付
3. 新增测试不得少于 20 个（覆盖 P0 改动的核心路径）

---

## 五、完整交付件清单

### 5.1 架构文档

| 文档 | 位置 | 状态 |
|---|---|---|
| OLTP 性能整改总方案 | `docs/OLTP_PERF_PLAN.md` | ✅ 已完成 |
| 网络与协议热路径优化方案 | `docs/network_protocol_optimization.md` | ✅ 已完成 |
| Benchmark 重建设计 | `docs/benchmark_design.md` | ✅ 已完成 |
| WAL 设计文档 | `docs/design/wal.md` | ✅ 已完成 |
| MVCC 设计文档 | `docs/design/mvcc.md` | ✅ 已完成 |
| ADR-003 WAL bincode 序列化 | `docs/adr/ADR-003-wal-bincode-serialization.md` | ✅ 已完成 |
| ADR-007 PG wire 协议兼容 | `docs/adr/ADR-007-pg-wire-protocol-compatibility.md` | ✅ 已完成 |
| **Phase 1 验收方案（本文档）** | `docs/phase1_acceptance.md` | ✅ 本文档 |

### 5.2 代码 PR 列表

Phase 1 结束时以下 PR 必须已合并到 main：

| PR 编号 | 标题 | 关联 P0/P1 | 必须包含测试 |
|---|---|---|---|
| PR-N01 | `perf(wal): group commit follower spin + futex` | P0-1 | WAL bench 数字对比 |
| PR-N02 | `perf(memtable): disable pk_order by default, lazy rebuild` | P0-2 | scan_by_pk 回归测试 |
| PR-N03 | `perf(storage): PrimaryKey → Arc<[u8]> full replacement` | P0-3 | commit 路径 alloc 数断言 |
| PR-N04 | `perf(engine): txn_local thread_local full coverage` | P0-4 | multi-insert TPS 对比 |
| PR-N05 | `perf(mvcc): lock-free VersionChain with epoch reclamation` | P0-5 | loom 检查 CI job |
| PR-N06 | `perf(protocol): zero-alloc query dispatch full coverage` | P1-5 | alloc/query 断言 |
| PR-N07 | `perf(memtable): secondary index sharded RwLock 16 buckets` | P1-1 | concurrent write bench |
| PR-N08 | `perf(wal): pre-serialization extend to batch INSERT` | P1-2 | WAL bytes/txn 回归 |
| PR-N09 | `perf(common): OwnedRow SmallVec inline for ≤8 cols` | P1-3 | clone 开销 benchmark |
| PR-N10 | `perf(engine): read-only txn skip read_set recording` | P1-4 | read-only TPS 对比 |
| PR-N11 | `perf(engine): merge unique check into insert-time` | P1-6 | UNIQUE constraint 测试 |
| PR-N12 | `bench: falcon_bench complete infrastructure` | Bench | 所有 bench 通过 |

已完成协议层优化（N7/N8 系列，`fmt_dml_tag`、`read_cstring` split_to、fast path DML tag）：
- `crates/falcon_protocol_pg/src/codec.rs` N7
- `crates/falcon_protocol_pg/src/handler.rs` N8
- `crates/falcon_protocol_pg/src/handler_fast_path.rs` N8

### 5.3 Benchmark 报告

Phase 1 结束时必须提交以下 benchmark 报告，存入 `docs/phase1_evidence/`：

| 报告文件 | 内容 | 生成命令 |
|---|---|---|
| `bench_e2e_final.json` | 全部 13 个 E2E bench 结果（32t/64t） | `cargo run -p falcon_bench -- --threads 32 --export json` |
| `bench_criterion_micro.html` | Criterion micro bench HTML 报告 | `cargo bench -p falcon_bench --bench bench_codec` |
| `bench_criterion_storage.html` | Criterion storage bench HTML 报告 | `cargo bench --bench bench_memtable_dml` |
| `bench_criterion_wal.html` | Criterion WAL bench HTML 报告 | `cargo bench --bench bench_wal_append` |
| `pgbench_workload_c.txt` | pgbench workload C 原始输出（16c/32c/64c） | `pgbench -c 16 -j 16 -T 60 -r` |
| `bench_scalability_curve.json` | 1→4→8→16→32→64 线程扩展性曲线 | BenchRunner scalability_curve |
| `bench_regression_report.md` | 与 bootstrap baseline 的 delta 表 | `python3 benchmarks/scripts/compare.py` |

### 5.4 Flamegraph

Phase 1 结束时必须提交以下 flamegraph，存入 `docs/phase1_evidence/flamegraphs/`：

| 文件名 | 场景 | 生成命令 |
|---|---|---|
| `flamegraph_point_select_32t.svg` | 32c point select 热路径 | `cargo flamegraph -p falcon_bench -- --threads 32 --read-pct 100` |
| `flamegraph_insert_32t.svg` | 32c INSERT 热路径 | `cargo flamegraph -p falcon_bench -- --threads 32 --read-pct 0` |
| `flamegraph_mixed_32t.svg` | 32c mixed OLTP | `cargo flamegraph -p falcon_bench -- --threads 32 --read-pct 70` |
| `flamegraph_wal_commit.svg` | WAL commit 路径（聚焦 wal.rs） | `cargo flamegraph --bench bench_wal_append` |
| `flamegraph_mvcc_read.svg` | VersionChain read_committed 路径 | `cargo flamegraph --bench bench_mvcc` |

必须满足：
- 没有 `DashMap::insert` 出现在 insert/commit 热路径的 top 10 函数
- 没有 `Mutex::lock` 出现在 `read_committed` 调用栈
- `wal::durable_wal_commit` 在 insert 路径占比 ≤ 40%

### 5.5 perf / cargo instruments / heaptrack 采样结果

| 文件名 | 工具 | 场景 | 关键指标 |
|---|---|---|---|
| `perf_stat_insert_32t.txt` | `perf stat` | 32c INSERT 30s | IPC, cache-misses, branch-misses |
| `perf_stat_select_32t.txt` | `perf stat` | 32c SELECT 30s | 同上 |
| `heaptrack_insert_1t.txt` | heaptrack | 1c INSERT 100K txn | alloc/txn, peak RSS |
| `heaptrack_select_1t.txt` | heaptrack | 1c SELECT 100K txn | alloc/txn |
| `dhat_insert_1t.json` | dhat (feature flag) | 1c INSERT 10K txn | total allocations, bytes |
| `valgrind_callgrind.out` | callgrind (可选) | 1c mixed 1K txn | instruction count/txn |

生成命令示例：
```bash
# perf stat（Linux）
perf stat -e cycles,instructions,cache-misses,branch-misses \
  cargo run --release -p falcon_bench -- --threads 32 --read-pct 0 --ops 100000

# heaptrack（Linux）
heaptrack cargo run --release -p falcon_bench -- --threads 1 --ops 100000 --read-pct 0
heaptrack_print heaptrack.falcon_bench.*.gz -s > heaptrack_insert_1t.txt

# dhat
cargo run --release -p falcon_bench --features dhat-heap -- --threads 1 --ops 10000
```

### 5.6 Crash Recovery 报告

| 测试场景 | 测试方法 | 通过标准 |
|---|---|---|
| WAL 中途断电模拟（truncate） | `dd if=/dev/zero of=wal/segment_0 bs=1 count=1024 seek=8192` 后重启 | 重启后数据一致，未提交事务回滚 |
| Kill -9 在 commit 之前 | `kill -9 $(pgrep falcon_server)` at random timing | 重启后点查结果与预期一致 |
| Kill -9 在 WAL flush 之后 commit ack 之前 | 插桩 `std::process::abort()` | 重启后该事务可见（durability） |
| 磁盘满时 WAL 写入 | `fallocate -l 100M /tmp/limit && mount --bind` | 返回明确错误，服务不 panic |
| 连续 10 次 crash-restart 循环 | shell 脚本自动化 | 每次重启后 `SELECT COUNT(*)` 结果单调不降 |

文件：`docs/phase1_evidence/crash_recovery_report.md`

### 5.7 稳定性报告

持续压测 **72 小时**（3 天），记录以下指标：

| 指标 | 标准 |
|---|---|
| 服务 panic 次数 | 0 |
| 数据不一致次数 | 0 |
| OOM kill 次数 | 0 |
| P999 延迟最大值 | ≤ 10ms |
| TPS 波动（标准差 / 均值） | ≤ 5% |
| 内存增长（RSS 72h 末 vs 初） | ≤ 10%（无内存泄漏） |
| `cargo test --workspace` 通过率 | 100% |

测试命令：
```bash
# 72h 压测
cargo run --release -p falcon_bench -- \
  --threads 32 --read-pct 70 --ops 0 \
  --export json > stability_run_$(date +%Y%m%d).json &

# 每小时采样 RSS
while true; do
  ps -o rss= -p $(pgrep falcon_server) >> memory_samples.txt
  sleep 3600
done
```

文件：`docs/phase1_evidence/stability_report.md`

### 5.8 回归测试报告

| 测试类别 | 数量目标 | 工具 |
|---|---|---|
| 单元测试（`#[test]`） | ≥ 4,200（不少于基线） | `cargo test --workspace` |
| P0 新增测试 | ≥ 20 个（覆盖 commit 路径改动） | 同上 |
| bench 基础设施测试 | ≥ 30 个（metrics/dataset/regression/runner） | `cargo test -p falcon_bench` |
| 集成测试（psql 协议兼容） | ≥ 50 个 SQL 场景 | `cargo test -p falcon_protocol_pg` |
| loom 并发模型检查 | VersionChain 关键路径 | `cargo test --features loom` |

文件：`docs/phase1_evidence/regression_report.md`

---

## 六、KPI 表（一页）

### Phase 1 核心 KPI

| # | KPI | 当前基线 | Phase 1 目标 | 权重 | 测量工具 |
|---|---|---|---|---|---|
| K01 | pgbench workload C @16c TPS | 46,000 | **≥70,000** | ★★★★★ | pgbench |
| K02 | pgbench workload C @64c TPS | 52,000 | **≥100,000** | ★★★★★ | pgbench |
| K03 | E2E point_select P99 @32c | ~3,500µs | **≤1,000µs** | ★★★★★ | BenchRunner |
| K04 | E2E insert_only P99 @32c | ~4,200µs | **≤1,500µs** | ★★★★☆ | BenchRunner |
| K05 | E2E mixed_oltp TPS @32c | ~150,000 | **≥200,000** | ★★★★★ | BenchRunner |
| K06 | batch INSERT rows/s | ~340,000 | **≥600,000** | ★★★★☆ | BenchRunner |
| K07 | commit 路径 alloc/txn | ~15次 | **≤8次** | ★★★★☆ | dhat |
| K08 | WAL group commit batch @32c | ~1–2 | **≥8** | ★★★★☆ | SHOW stats |
| K09 | WAL flush P99 | ~3ms | **≤1ms** | ★★★★☆ | Criterion |
| K10 | 多核扩展比 1c→64c | ~6.5× | **≥8×** | ★★★☆☆ | BenchRunner curve |
| K11 | 全量测试通过率 | 100% | **100%** | ★★★★★ | cargo test |
| K12 | 72h 稳定压测 panic=0 | — | **panic=0** | ★★★★★ | 自动化脚本 |
| K13 | flamegraph DashMap 占比 @commit | ~15% | **<2%** | ★★★☆☆ | flamegraph |
| K14 | VersionChain Mutex 占比 @read | ~8% | **0%（lock-free）** | ★★★☆☆ | flamegraph |

### 胜利判定规则

- **全部通过**：K01–K14 全部达标 → Phase 1 完全通过 ✅
- **有条件通过**：K01/K02/K03/K05/K11/K12（★★★★★ 项）全部达标，其余 ≥70% 达标 → 条件通过，剩余项进入 Phase 2
- **不通过**：任意 ★★★★★ 项未达标 → Phase 1 不通过，不得宣称"对标 SingleStore"

---

## 七、风险表（一页）

| # | 风险 | 概率 | 影响 | 缓解措施 | 触发时应对 |
|---|---|---|---|---|---|
| R01 | **P0-5 VersionChain lock-free 引入数据竞争** | 高 | 严重 | loom 模型检查必须通过；先在 shadow branch 运行 72h | 立即回退到 Mutex；标记 Phase 3 |
| R02 | **WAL group commit 改动破坏 DCG 保证** | 中 | 严重 | 改动仅涉及 follower spin 策略，不改 FUA；crash recovery 测试必须通过 | 回退 spin 逻辑，保留原 condvar |
| R03 | **P0-3 Arc<[u8]> 接口改动范围过大，引入 borrow 错误** | 中 | 中 | 分批替换：先 storage crate 内部，再 engine，再 protocol | 分阶段回退，优先保证 storage 内部一致 |
| R04 | **thread_local txn_local 在 async 上下文 unsafe** | 高 | 高 | 所有 commit 路径在 `spawn_blocking` 内执行（同步），不跨 await；增加 `debug_assert!(!in_async_context())` | 回退到 DashMap；记录异步边界设计债 |
| R05 | **pgbench 数字受 NVMe 型号影响大，无法复现** | 中 | 中 | 固定 bench 机器规格（见 benchmark_design.md §10）；同时提供"内存模拟 WAL"和"真实 NVMe"两套数字 | 说明硬件差异，提供相对改进率而非绝对值 |
| R06 | **OwnedRow SmallVec 改动破坏 JSONB/Array 大行** | 低 | 中 | 仅对 ≤8 列且无 BLOB/JSONB 的行使用 inline；大行保持 Vec<Datum> | 编译时 `const_assert!(size_of::<SmallRow>() <= 256)` |
| R07 | **72h 稳定压测发现内存泄漏** | 中 | 高 | epoch-based GC 必须配合 GC thread 定期推进；VersionChain 新增 retired list size 监控 | 暂停新 feature，专项定位 epoch 推进逻辑 |
| R08 | **P1 工作量超出 12 周** | 中 | 低 | P1 可降级到"有条件通过"；P0 不可降级 | 将 P1 未完成项移入 Phase 2，书面记录 |
| R09 | **benchmark 数字被质疑（无独立复现）** | 低 | 高 | 所有 bench 脚本和数据集完全开源（benchmarks/ 目录）；README 提供一键复现命令 | 邀请外部评审者在独立机器复现 |

---

## 八、Phase 2 规划（一页）

### Phase 2 目标（12 周后，第 13–24 周）

在 Phase 1 基础上，目标：**E2E point_select @32c ≥ 600K TPS，P99 ≤ 200µs**，
全面对标 SingleStore Rowstore 公开 benchmark 数字。

### Phase 2 技术项

| ID | 整改项 | 预期收益 | 依赖 Phase 1 |
|---|---|---|---|
| P2-1 | **auto_analyze 改后台定时器** | 消除 commit 路径 thread::spawn | 无 |
| P2-2 | **CDC compile-feature 隔离** | 消除非 CDC 构建的原子加载开销 | 无 |
| P2-3 | **txn_read_sets per-thread arena** | 消除只读事务 DashMap remove | P0-4 |
| P2-4 | **批量提交 WAL 合并（multi-stmt txn）** | 多 DML 单次 FUA | P0-1 |
| P2-5 | **GinIndex → ART / concurrent skip-list** | FTS 写路径并发度 × 8+ | 无 |
| P2-6 | **协议层 zero-copy Bind 参数**：param_values 改 Bytes 引用，零 copy 解码 | 每 prepared query 节省 N 次 Vec 分配 | P1-5 |
| P2-7 | **NUMA-aware 内存分配**：DashMap shard 绑定 NUMA node | 双 socket 机器扩展性 × 1.5+ | 无 |
| P2-8 | **Connection pooling 内置**（PgBouncer 语义）：减少连接建立开销 | 高并发场景 TPS + 20% | 无 |
| P2-9 | **读路径 prefetch 优化**：DashMap 点查前 `_mm_prefetch` 提示 L1 cache | 点查延迟 -15% | P0-5 |

### Phase 2 KPI 目标

| KPI | Phase 1 目标 | Phase 2 目标 | 与 SingleStore 差距 |
|---|---|---|---|
| Point SELECT TPS @32c | ≥320,000 | **≥600,000** | SingleStore ~1,000,000 |
| Point SELECT P99 @32c | ≤1,000µs | **≤200µs** | SingleStore ~100µs |
| Mixed OLTP TPS @32c | ≥200,000 | **≥400,000** | SingleStore ~400,000 ✅ |
| INSERT TPS @32c | ≥200,000 | **≥500,000** | SingleStore ~500,000 ✅ |
| P99 commit latency | ≤800µs | **≤150µs** | SingleStore ~100µs |

### Phase 2 里程碑

```
Week 13–16  P2-1/P2-2/P2-4（低风险，立即做）
            协议层 zero-copy Bind 参数（P2-6）
            NUMA-aware DashMap（P2-7）

Week 17–20  GinIndex ART 替换（P2-5）
            per-thread arena read-sets（P2-3）
            prefetch 优化（P2-9）

Week 21–24  Connection pool 内置（P2-8）
            全套 SingleStore 对比 benchmark（公开发布）
            第三方独立复现报告
```

### Phase 3 展望（24 周后）

- **列存 + HTAP**：引入 columnstore 层，mixed OLAP/OLTP workload
- **分布式扩展**：Raft 层优化，跨节点 OLTP TPS 扩展
- **公开 benchmark 论文**：提交 VLDB/SIGMOD industrial track

---

## 九、验收执行流程

```
Week 12 Friday
      │
      ▼
1. 运行全量 cargo test --workspace（必须 100% 通过）
      │
      ▼
2. 在 bench 机上执行 bash benchmarks/scripts/update_baseline.sh --full
      │
      ▼
3. 运行 python3 benchmarks/scripts/ci_gate.py 对比 bootstrap baseline
      │
      ▼
4. 手动运行 pgbench -c 16/32/64 各 3 次，取中位数
      │
      ▼
5. 生成 flamegraph（5 个场景）+ heaptrack（2 个场景）
      │
      ▼
6. 运行 72h 稳定压测
      │
      ▼
7. 运行 crash recovery 测试（5 个场景）
      │
      ▼
8. 汇总所有证据文件到 docs/phase1_evidence/
      │
      ▼
9. 提交 phase1_acceptance_signoff.md（包含实际数字 vs 目标对比）
      │
      ▼
10. 技术 PM 签字：Phase 1 通过 / 有条件通过 / 不通过
```

---

*文档维护责任人：技术 PM*  
*下次更新：Week 6 中期检查（更新 P0 实际完成状态）*  
*最终更新：Week 12 验收完成后填写实际数字*
