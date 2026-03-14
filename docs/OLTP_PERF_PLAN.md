# FalconDB 面向极致 OLTP 性能整改总方案

> 版本：第一阶段 · 2026-03  
> 定位：对标 SingleStore Rowstore 风格 OLTP 内核，不扩展 HTAP / OLAP / 列存能力  
> 禁止范围：列存新增、OLAP optimizer、过度抽象、"架构好看"优先于热路径极短

---

## 一、结论（先给结论）

FalconDB 当前单节点短事务性能瓶颈集中在 **5 个层面**：

1. **WAL 写入**：每笔提交一次 FUA（`FILE_FLAG_WRITE_THROUGH`），单线程下理论上限约 47K TPS，多核无法突破
2. **commit 热路径分配**：`PrimaryKey`（`Vec<u8>`）在 insert → write_set → commit → pk_order 之间被 clone ≥3 次
3. **`pk_order: RwLock<BTreeSet>`**：每次单键提交走一次 `write()` 锁，高并发下成为 stall 点
4. **DashMap 哈希开销**：`txn_local`（`DashMap<TxnId, TxnLocalState>`）是写路径必经点，每次插入/取出含哈希 + shard 锁
5. **协议层解析**：`to_lowercase()` / `String` 分配、`to_ascii_uppercase` 逐帧 alloc 散落在 hot path

理论峰值：消除上述瓶颈后，单机 16 核可达 **70K–90K TPS**（pgbench workload C，NVMe SLC warm）。

---

## 二、Top 15 问题清单

| # | 问题 | 模块 | 严重度 | 当前状态 |
|---|------|------|--------|---------|
| 1 | 每笔提交一次 FUA，无批量 group commit | `wal.rs` `engine.rs` | P0 | 部分 group commit，但 follower 侧仍有 mutex queue |
| 2 | `pk_order: RwLock<BTreeSet>` 在单键提交热路径上 | `memtable.rs` `engine.rs:2282` | P0 | `large_mem_mode` 可绕过，但默认未开 |
| 3 | `PrimaryKey`（`Vec<u8>`）commit 路径 3× clone | `engine.rs` `memtable.rs` | P0 | 无共享所有权 |
| 4 | `txn_local: DashMap<TxnId, TxnLocalState>` 每次提交至少 1 次 hash+锁 | `engine.rs` | P0 | thread-local cache 仅覆盖单insert场景 |
| 5 | `VersionChain` 使用 `Mutex<Vec<MvccVersion>>`，读取需全链扫描 | `mvcc.rs` | P0 | 无 lock-free 实现 |
| 6 | `secondary_indexes: RwLock<Vec<SecondaryIndex>>` 每个 SecondaryIndex 有独立 `RwLock<BTreeMap>` | `memtable.rs` | P1 | 无分桶，全局 write 锁 |
| 7 | `GinIndex::postings: RwLock<BTreeMap>` — 全局写锁 | `memtable.rs` | P1 | 无 skip-list / ART |
| 8 | `validate_unique_constraints_for_commit` 在提交前走二次 BTreeMap read | `memtable.rs` | P1 | 可与 insert-time check 合并 |
| 9 | WAL bincode 序列化：`serialize_into` 每次分配 header buffer | `wal.rs` | P1 | WAL pre-serialization 仅覆盖 autocommit single-insert |
| 10 | `OwnedRow = Vec<Datum>` — Datum 含 `String`/`Vec`，点查返回需 deep clone | `mvcc.rs` `executor_query.rs` | P1 | 无 inline / arena 行格式 |
| 11 | `txn_read_sets: DashMap<TxnId, Vec<TxnReadOp>>` — OCC read-set 记录每次点查 | `engine.rs` | P1 | read-only 事务仍需 take_read_set |
| 12 | `handler_session.rs` 协议层 `to_lowercase()` alloc on every query | `falcon_protocol_pg` | P1 | 部分 fast-path 已绕过 |
| 13 | `take_local_state` 调 `txn_local.remove()` 时 DashMap shard 独占锁 | `engine.rs` | P2 | 频率高、成本被低估 |
| 14 | `auto_analyze` 在 commit 热路径上 spawn thread | `engine.rs:2789` | P2 | 每 500 次 commit 一次，但 spawn 本身有 OS 开销 |
| 15 | `cdc_manager.emit_commit()` 在 commit 热路径上 — 即使 CDC 未启用也有函数调用 | `engine.rs:2771` | P2 | `is_enabled()` 已有，但仍有一次原子加载 + 跳转 |

---

## 三、P0 / P1 / P2 整改表

### P0 — 必须做，直接决定 TPS 上限

| ID | 整改项 | 代码位置 | 影响面 | 风险 | 预期收益 |
|----|--------|---------|--------|------|---------|
| P0-1 | **group commit 强化**：follower 侧改 futex/condvar spin，消除 mutex queue blocking | `wal.rs: durable_wal_commit` | WAL 写路径 | 中（需保证 LSN 顺序） | +30–50% TPS @32c+ |
| P0-2 | **`pk_order` 默认禁用或改 skip-list**：`large_mem_mode = true` 默认；范围扫描时 lazy rebuild | `memtable.rs: commit_keys` `engine.rs:2282` | ORDER BY PK 查询 | 低（scan 路径加 rebuild 逻辑） | 消除 commit 路径 write lock |
| P0-3 | **`PrimaryKey` 引用计数化**：insert 时 `Arc<[u8]>`，write_set 存 `Arc`，commit 时直接 clone Arc（8 bytes） | `engine.rs` `memtable.rs` `engine_dml.rs` | 全写路径 | 中（接口改动广） | 减少 ~3× `Vec<u8>` 分配/clone |
| P0-4 | **`txn_local` 改 per-thread `UnsafeCell` 或 `thread_local!` 全覆盖**：目前仅 single-insert 走 TXN_LOCAL_CACHE，multi-op 仍 DashMap | `engine.rs` `engine_dml.rs` | 所有多语句事务 | 中（需 flush 时机对齐） | 消除 multi-insert 路径 DashMap 访问 |
| P0-5 | **`VersionChain` 改 lock-free**：用 `AtomicPtr` + hazard pointer 或 `crossbeam::epoch` 替代 `Mutex<Vec>` | `mvcc.rs` | 所有读写 | 高（MVCC 核心） | 读路径零锁 |

### P1 — 高收益，2–4 周内完成

| ID | 整改项 | 代码位置 | 影响面 | 风险 | 预期收益 |
|----|--------|---------|--------|------|---------|
| P1-1 | **`SecondaryIndex.tree` 改分桶 RwLock** 或 `dashmap`：16 桶，按 key 首字节 hash 路由 | `memtable.rs: SecondaryIndex` | 有二级索引的表 | 低 | 减少 write lock 争用 8–16× |
| P1-2 | **WAL pre-serialization 扩展**：覆盖 multi-insert batch（目前仅 single autocommit） | `wal.rs` `engine_dml.rs` | batch INSERT | 低 | 减少 bincode alloc |
| P1-3 | **`OwnedRow` 行格式 inline 化**：小行（≤8 列，无 JSONB/Array）用 `SmallVec<[Datum;8]>` 消除 Vec heap alloc | `falcon_common: datum.rs` | 全存储路径 | 中 | 减少点查 clone 开销 |
| P1-4 | **read-only 事务跳过 read_set 记录**：`BEGIN READ ONLY` 或 autocommit SELECT 不向 `txn_read_sets` 写入 | `engine.rs: take_read_set` | 只读路径 | 低 | 消除只读事务 DashMap 写入 |
| P1-5 | **协议层 zero-alloc query dispatch**：`contains_ci_bytes` 替代 `to_lowercase()` 在所有系统命令检测处 | `handler_session.rs` `handler.rs` | 所有入站 SQL | 低 | 每 SQL 节省 1 次 String alloc |
| P1-6 | **`validate_unique_constraints_for_commit` 与 insert-time check 合并**：insert 时已持有 DashMap shard 锁，可原子完成两阶段检查 | `memtable.rs: insert` `commit_keys` | 有 UNIQUE 约束的表 | 中 | 去掉 commit 前第二次 BTreeMap read |

### P2 — 长尾优化，4–8 周

| ID | 整改项 | 代码位置 | 影响面 | 风险 | 预期收益 |
|----|--------|---------|--------|------|---------|
| P2-1 | **`auto_analyze` 改定时器触发**：从 commit 热路径移出，改为后台 10s 定时 | `engine.rs:2789` | 所有表 | 低 | 消除 commit 路径 thread spawn |
| P2-2 | **CDC `emit_commit` 改 compile-feature 分支**：`#[cfg(feature="cdc")]` 隔离，非 CDC 构建零开销 | `engine.rs:2771` | 非 CDC 构建 | 低 | 消除原子加载 + 间接调用 |
| P2-3 | **`txn_read_sets` DashMap 替换为 per-thread arena**：与 P0-4 协同，read-set 生命周期与事务绑定 | `engine.rs` | OCC 验证路径 | 中 | 减少 DashMap remove 争用 |
| P2-4 | **批量提交合并（multi-statement txn）**：多个 DML 在同一 txn 内，单次 `durable_wal_commit` | `engine.rs: commit_txn_local` | 显式事务 | 低 | 减少 WAL FUA 次数 |
| P2-5 | **`GinIndex` 改 ART / concurrent BTreeMap**：`BTreeMap` 全局写锁换 `ART`（`art-tree` crate）或分桶 | `memtable.rs: GinIndex` | FTS 查询 | 中 | FTS 写路径并发度提升 |

---

## 四、取舍建议：单机优先 vs 分布式一致性

**结论：本轮优先单机热路径，分布式一致性不降级。**

具体原则：

- **WAL group commit（P0-1）**：只优化 follower 侧 spin，FUA 仍保持 — DCG（Deterministic Commit Guarantee）不变
- **`VersionChain` lock-free（P0-5）**：epoch-based reclamation，不影响 MVCC snapshot isolation 语义
- **`pk_order` 禁用（P0-2）**：仅影响 `ORDER BY PK` 的 scan_top_k_by_pk 优化，走 full DashMap scan 降级，一致性不受影响
- **禁止** 为了 TPS 数字而放松 `CommitAck = primary_durable` 默认值
- **禁止** 异步提交（commit-then-ack before WAL flush）

---

## 五、12 周整改路线图

```
Week 1–2   [P0-2] pk_order 默认 large_mem_mode，lazy rebuild
           [P0-3] PrimaryKey → Arc<[u8]> 接口替换（storage crate 内部）
           [P1-4] read-only 事务跳过 read_set 记录
           [P1-5] 协议层 zero-alloc query dispatch 全覆盖
           [P2-1] auto_analyze 移出 commit 热路径

Week 3–4   [P0-1] group commit follower spin 优化（futex/condvar 对比测试）
           [P1-2] WAL pre-serialization 扩展到 batch INSERT
           [P2-2] CDC compile-feature 隔离

Week 5–6   [P0-4] txn_local thread-local 全覆盖（multi-op 路径）
           [P1-6] unique check 两阶段合并为 insert 时单次
           [P1-1] SecondaryIndex 分桶 RwLock

Week 7–8   [P0-3] 收尾 + 全路径 PK alloc profiling
           [P1-3] OwnedRow SmallVec inline 化（先做列数 ≤8 的常见 schema）
           [P2-4] 批量提交 WAL 合并

Week 9–10  [P0-5] VersionChain lock-free（epoch-based，hazard pointer）
           [P2-3] txn_read_sets per-thread arena

Week 11–12 [P2-5] GinIndex ART 替换
           全链路 flamegraph + perf stat 验收
           pgbench / YCSB / sysbench 三套基准全跑
```

---

## 六、模块级整改说明

### 6.1 `crates/falcon_storage/src/wal.rs` — WAL Group Commit

**现状**：`durable_wal_commit` 中，leader 线程 flush，followers `try_lock` 失败后 spin 等待 `flush_cvar`。  
**问题**：`flush_cvar.wait()` 底层是 pthread condvar，唤醒延迟 5–20µs，在高并发下变成 batch 吞吐的瓶颈。  
**改法**：followers 先 spin 200–500 次（`std::hint::spin_loop()`），再降级 condvar wait。同时把 `flush_cvar` 换成 `atomic_wait`（Linux `futex`）减少用户态→内核态切换。

### 6.2 `crates/falcon_storage/src/memtable.rs` — pk_order + SecondaryIndex

**`pk_order`**：  
- 改为 `large_mem_mode = true` 作为默认，`pk_order` 仅在 `SHOW CREATE TABLE ... ORDER BY pk` 或 `CREATE TABLE ... WITH (pk_index=true)` 时启用  
- 范围扫描 fallback：`scan_range_by_pk` 先检查 `pk_order.read()` 是否 dirty（新增 `AtomicBool pk_order_dirty`），dirty 时 full DashMap collect + sort

**`SecondaryIndex.tree`**：  
```rust
// 改前
pub tree: RwLock<BTreeMap<Vec<u8>, Vec<PrimaryKey>>>,

// 改后（16 桶）
pub buckets: [RwLock<BTreeMap<Vec<u8>, Vec<PrimaryKey>>>; 16],
```
按 `key_bytes[0] & 0x0F` 路由到对应桶，写锁粒度降低 16×。

### 6.3 `crates/falcon_storage/src/engine.rs` — txn_local / commit 路径

**现状**：`TXN_LOCAL_CACHE` 只覆盖 single-insert autocommit，multi-insert 仍通过 `txn_local: DashMap` 传递。  
**改法**：  
```rust
thread_local! {
    static TXN_STATE: RefCell<HashMap<TxnId, TxnLocalState>> = RefCell::new(HashMap::new());
}
```
将 `txn_local` 的 `entry(txn_id).or_default()` 全部替换为 `TXN_STATE.with(|m| m.borrow_mut().entry(txn_id).or_default())`。  
flush 时机：`commit_txn_local` 开头 `flush_local_cache()` 不变，清理路径改为 thread-local remove。

### 6.4 `crates/falcon_storage/src/mvcc.rs` — VersionChain lock-free

**现状**：`VersionChain { inner: Mutex<Vec<MvccVersion>> }`  
**目标**：
```rust
pub struct VersionChain {
    head: AtomicPtr<MvccNode>,   // most-recent version
    len:  AtomicU32,
}
struct MvccNode {
    version: MvccVersion,
    next: *mut MvccNode,         // older version
}
```
用 `crossbeam::epoch` 管理节点生命周期，prepend 为 CAS 操作（无锁），GC 时 epoch-defer free。  
风险：需要严格验证 ABA 问题，上线前跑完 `loom` 模型检查。

### 6.5 `crates/falcon_protocol_pg/src/handler_session.rs` — 协议层 alloc

**改法**（已有基础，补全剩余漏点）：
```rust
// 改前
let sql_lower = sql.to_lowercase();
if sql_lower == "select version()" { ... }

// 改后
fn eq_ignore_ascii(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).all(|(x,y)| x.to_ascii_lowercase() == y)
}
if eq_ignore_ascii(sql.trim(), "select version()") { ... }
```

---

## 七、代码改造优先顺序

```
1. memtable.rs  — pk_order large_mem_mode 默认开启         (低风险, 立即)
2. engine.rs    — auto_analyze 移出 commit 路径             (低风险, 立即)
3. handler*.rs  — zero-alloc query dispatch 补全            (低风险, 立即)
4. engine.rs    — CDC emit_commit compile-feature 隔离      (低风险, 1天)
5. wal.rs       — group commit spin 优化                    (中风险, 1周)
6. engine_dml.rs — txn_local thread-local 全覆盖             (中风险, 2周)
7. memtable.rs  — SecondaryIndex 分桶 RwLock                (低风险, 2周)
8. engine.rs    — PrimaryKey → Arc<[u8]>                    (中风险, 3周)
9. wal.rs       — pre-serialization batch 扩展              (低风险, 3周)
10. mvcc.rs     — VersionChain lock-free                    (高风险, 6周)
```

---

## 八、最终验收 KPI 表

| 指标 | 当前基线 | 目标（12周后） | 测量方法 |
|------|---------|--------------|---------|
| pgbench workload C 16c TPS | ~46K | **≥70K** | `pgbench -c 16 -j 16 -T 30 -M simple` |
| pgbench workload C 64c TPS | ~52K | **≥100K** | `pgbench -c 64 -j 64 -T 30 -M simple` |
| 单键点查 P50 延迟 | ~0.35ms | **≤0.15ms** | `pgbench -c 1 -j 1 -T 10 custom_pointlookup.sql` |
| 单键点查 P99 延迟 | ~1.2ms | **≤0.5ms** | 同上，99th percentile |
| 单键点查 P999 延迟 | ~5ms | **≤2ms** | 同上，999th percentile |
| batch INSERT 10K行/批 吞吐 | ~340K rows/s | **≥600K rows/s** | bulk_insert benchmark |
| commit 路径 PrimaryKey alloc 次数 | 3× | **≤1×（Arc clone）** | `perf mem` / heaptrack |
| commit 路径 DashMap 访问次数（单键 autocommit） | 2–3次 | **0次（thread-local）** | 代码审查 + flamegraph |
| WAL group commit batch size @32c | ~1–2 | **≥8** | `SHOW falcon.txn_stats` |
| 全部现有测试通过率 | 4,200+ / 4,200+ | **100%** | `cargo test --workspace` |
| cargo clippy warnings | 0 | **0** | CI |

---

## 九、明确禁止事项（整改边界）

- **不新增列存**：不修改 `columnstore.rs`、`executor_columnar.rs`
- **不新增 OLAP optimizer**：`falcon_planner` 不引入代价模型中的列统计路径
- **不牺牲 DCG**：WAL FUA 默认策略不变，group commit 优化不改变 durability 保证
- **不引入新 trait 层**：hot path 上不增加 `dyn` dispatch（`StorageTable` 已有的维持现状，不新增）
- **不引入 `async` 嵌套**：commit 路径保持同步（`spawn_blocking` 仍在 tokio worker 上 inline 执行）
- **不为"漂亮代码"重构**：只动 hot path，其他路径原则上不改

---

*文档维护：每轮整改完成后更新基线 TPS 数据，更新 P0/P1/P2 完成状态。*
