# FalconDB — 工业级 OLTP 极致性能整改方案 V2

> 版本：第二阶段完整方案 · 2026-03  
> 定位：对标 SingleStore Rowstore 风格 OLTP 内核  
> 目标：单机 16 核 **≥100K TPS** (pgbench workload C，NVMe SLC warm)  
> 禁止范围：Columnstore、HTAP、OLAP、复杂分布式分析、过度抽象

---

## 一、OLTP 热路径分解（完整调用链）

```
Client SQL  
  → falcon_protocol_pg: parse_sql() → handle_system_query() → QueryHandler::execute()  
  → falcon_planner: Planner::plan()  
  → StorageEngine::insert() [engine_dml.rs:53]  
      → TABLE_CACHE TLS 命中 → insert_rowstore() [engine_dml.rs:122]  
          → MemTable::insert() [memtable.rs:737]  
              → encode_pk() → Vec<u8> 堆分配  
              → DashMap::entry(pk) → shard lock  
              → VersionChain::prepend_arc() → Arc::new(Version{...}) 堆分配  
          → TXN_LOCAL_CACHE TLS write  
  → StorageEngine::commit_txn_local() [engine.rs:2758]  
      → take_local_state() → TXN_LOCAL_CACHE drain  
      → validate_unique_constraints_for_commit()  
      → durable_wal_commit() [engine.rs:1771]  
          → WalWriter::append_preserialized() / append_multi()  
          → GroupCommitSyncer::notify_and_wait() — spin-500 → condvar  
      → VersionChain::commit_no_report() — fast path: single AtomicStore  
      → table.committed_row_count.fetch_add()  
      → JobScheduler auto_analyze (条件触发)  
```

**当前 P99 瓶颈分布（基于代码审计估算）：**

| 阶段 | 占比 | 主要原因 |
|------|------|---------|
| WAL fsync 等待 | ~35% | NVMe FUA ~20µs，group commit 摊销不足 |
| DashMap 锁争用 | ~20% | txn_local + engine_tables + pk_hash |
| Arc::new(Version) 堆分配 | ~15% | 每次写入一次 global allocator |
| PrimaryKey Vec<u8> clone | ~10% | insert→write_set→commit_keys 链上 3次 |
| VersionChain::commit_and_report RwLock | ~8% | 有二级索引时 head.read() + data.read() |
| 协议层 parse_sql + bind | ~7% | 简单 INSERT/SELECT 仍全走 planner |
| 其余（GC、analyze、CDC） | ~5% | 条件触发，峰值时叠加 |

---

## 二、Top 20 性能瓶颈清单

### P0 — 阻断 TPS 上限（必须做）

| ID | 瓶颈 | 模块/位置 | 根因 | 当前状态 |
|----|------|----------|------|---------|
| **N-01** | `VersionChain`（mvcc.rs）每次 INSERT/UPDATE 分配 `Arc::new(Version{...})` | `mvcc.rs:260` `memtable.rs:770` | `Arc::new` → global allocator，8~32 次/µs 时触发 jemalloc 线程缓存 miss | `VersionChain2` + `VersionArena` 已实现但 MemTable 尚未迁移 |
| **N-02** | `PrimaryKey = Vec<u8>` 在 insert→write_set→commit_keys 路径上 clone ≥2次 | `memtable.rs:191` `engine_dml.rs:159` `memtable.rs:1242` | `Vec<u8>` 每次 clone = `malloc + memcpy`；16 核下 ~3M alloc/s | `Arc<[u8]>` 改造未完成；`pk.clone()` 仍在 commit_keys 内 |
| **N-03** | `commit_no_report` 在 `no_idx` fast path 仍走 `head.read()` RwLock | `mvcc.rs:342` | `parking_lot::RwLock::read()` 即使无争用也有 ~8ns CAS；100K TPS 下 = 800µs/s CPU | `VersionChain2.commit_no_report` 完全 lock-free（已实现，待迁移） |
| **N-04** | `txn_local: DashMap<TxnId, TxnLocalState>` 在多语句事务 flush 路径上仍有 shard 锁 | `engine.rs:483` `flush_local_cache` | cross-thread flush 时 `entry().or_default()` 独占 shard；高并发下 8~32 个线程竞争同一 shard | TLS TXN_LOCAL_CACHE 已覆盖 single-thread 路径，cross-thread flush 仍有 DashMap |
| **N-05** | `group_commit` 的 `notify_and_wait` 使用 `std::sync::Mutex` + `Condvar` | `group_commit.rs:246` | `std::sync::Mutex` 在 Linux 用 `futex`，每次 lock/unlock ~20ns；spin-500 后进 condvar 时 `pthread_cond_signal` = syscall | 已有 spin-500 fast path；condvar 仍用 `std::sync` 而非 `parking_lot` |
| **N-06** | GC sweep 对每条 `VersionChain` 调用 `head.write()` 全链扫描 | `gc.rs` `mvcc.rs:488` | GC 线程持 write lock 期间阻塞所有读者；1M 行表 GC sweep ~50ms，P99 spike | `VersionChain2.gc()` 已实现 arena free-list，待迁移 |

### P1 — 高收益（2~4 周）

| ID | 瓶颈 | 模块/位置 | 根因 | 当前状态 |
|----|------|----------|------|---------|
| **N-07** | `MemTable::commit_keys` 每个 key 调用 `commit_and_report`，链扫描 + 2× `data.read()` | `mvcc.rs:384` `memtable.rs:1238` | 有索引时每 commit 2次 `RwLock::read()`（new_data + old_data）；单行更新重复扫描 | 已有 `try_in_place_update` 但仅在 UPDATE 路径使用 |
| **N-08** | `encode_pk()` 在 `memtable.rs:199` 每次 INSERT 分配新 `Vec<u8>` | `memtable.rs:198` | `Vec::with_capacity(64)` = 1次 malloc；每行 INSERT 一次 | `encode_pk_stack()` 已实现但 `batch_insert` 仍用 `encode_pk` |
| **N-09** | `index_update_on_commit` 调用 `idx.encode_key_buf()` 两次（remove + insert） | `memtable.rs:1709,1715` | 每次 commit 对每个索引做 2× key encode，结果未复用 | 无缓存，重复计算 |
| **N-10** | `record_write` 中 `!state.write_set.iter().any(...)` 线性去重扫描 | `engine.rs:2071` | O(n) 扫描 write_set，multi-stmt txn 写 N 行时 = O(N²) | `record_write_no_dedup` 已有但部分路径仍用有去重版本 |
| **N-11** | `Version.data: RwLock<VersionData>` — 读取 row 需要额外 RwLock | `mvcc.rs:72` | 即使只读 `data` 中的 `Arc<OwnedRow>`，也需要获取 `RwLock::read()`；`VersionChain2` 直接存 `AtomicPtr<OwnedRow>` | 未迁移到 `VersionChain2` |
| **N-12** | `SecondaryIndex.tree: ArtTree` 写操作串行（ArtTree 内部单锁） | `memtable.rs:496` `art.rs` | ArtTree 写路径单锁，不支持并发写；多核 commit 时索引写成为串行瓶颈 | 已有 `has_secondary_idx` fast-exit；ArtTree 本身不支持细粒度并发 |
| **N-13** | `VersionChain::prepend_arc` 在写前清除 `fast_row`（2次原子操作） | `mvcc.rs:255-258` | `fast_commit_ts.store(0)` + `clear_fast_row()`（内含 `Arc::drop`）在每次写入时执行 | 无法消除，但 `VersionChain2` 的 inline-slot 设计避免了 fast_row 的独立 Arc 管理 |
| **N-14** | `wal.append()` 的 `APPEND_BUF` TLS + `bincode::serialize_into` 每次分配内部 buffer | `wal.rs:932-966` | `bincode::serialize_into` 写入 `Vec<u8>` 时，Vec 可能 realloc；大行时 TLS APPEND_BUF 不够 | `serialize_record` pre-serialization 已覆盖 insert；`append_multi` 也已有 SERIAL_BUF TLS |

### P2 — 长尾优化（4~8 周）

| ID | 瓶颈 | 模块/位置 | 根因 | 当前状态 |
|----|------|----------|------|---------|
| **N-15** | `parse_sql()` 对所有查询走完整 parser → planner | `handler_session.rs` `falcon_planner` | 简单 `INSERT INTO t VALUES(...)` 仍走 parse_sql→bind→plan；pgbench workload C 下 parser 占 ~5% | 无 fast-path parser bypass |
| **N-16** | `handler_session.rs` 的系统查询路径 `to_ascii_lowercase()` 每次分配 String | `handler_session.rs:36` | 即使 first-byte 过滤后，仍需 `to_ascii_lowercase()` = heap alloc；驱动初始化时每秒数百次 | 已有 first-byte fast-exit；lowercase alloc 仍存在 |
| **N-17** | `engine_tables: DashMap<TableId, TableHandle>` 每次 INSERT 第二次走 DashMap（无 TABLE_CACHE 时） | `engine_dml.rs:76` | batch_insert 没有 TABLE_CACHE TLS；每行 batch 走一次 DashMap 查询 | TABLE_CACHE 仅覆盖 `insert()`；`batch_insert` 未用 |
| **N-18** | GC 的 `version_chain_len()` 在每次 sweep 对每条链调用 `head.read()` | `mvcc.rs:588` | GC skip logic 需要链长度，但获取链长需要遍历全链；1M 行时 sweep = O(n) read locks | `gc_candidates` 原子计数已有，但 GC 未用它做 skip |
| **N-19** | `committed_row_count` 的 `StripedI64::fetch_add` 在无索引路径上仍在 commit 热路径上 | `engine.rs:2826` | 16 stripes 已减少 false sharing；但仍是 commit 路径上额外的原子操作 | 可改为 lazy batch update |
| **N-20** | `TxnWriteOp { table_id: TableId, pk: PrimaryKey }` 的 `pk: Vec<u8>` 在 write_set 中 clone | `engine.rs:72` `engine_dml.rs:172` | write_set 持有 pk clone；commit 时 `commit_keys` 再用这些 pk 作为 key lookup；可改为 `Arc<[u8]>` 共享 | 已有 `pk_ret = pk.clone()` 优化，但 write_set 中仍是 owned Vec |

---

## 三、P0/P1/P2 整改表（含代码位置、风险、收益）

### N-01: MemTable 迁移到 VersionChain2 + VersionArena

**目标**：消除每次 INSERT/UPDATE 的 `Arc::new(Version{...})` 全局堆分配。

**代码位置**：
- `memtable.rs:770,818` — `Arc::new(VersionChain::new())`
- `memtable.rs:349` — `pub data: DashMap<PrimaryKey, Arc<VersionChain>>`
- `version_chain2.rs` — 已实现，需补全 `is_visible`/`version_chain_len` 两个 GC 接口
- `gc.rs:240,253-256` — 调用 `version_chain_len`、`gc().reclaimed_versions` 等，需适配 `VersionChain2.gc()` 返回 `u64`

**整改步骤**：
1. `gc.rs`：将 `chain.gc(watermark).reclaimed_versions` 改为 `chain.gc(watermark)` (u64)，消除 `GcChainResult` 依赖
2. `gc_budget.rs`：同上
3. `mvcc.rs` 中 `is_visible` → `VersionChain2` 已有 `read_committed`，补 `is_visible` alias
4. `memtable.rs` struct：增加 `arena: Arc<VersionArena>` 字段，两个构造函数初始化
5. `memtable.rs` data 字段：改 `DashMap<PrimaryKey, Arc<VersionChain2>>`
6. `memtable.rs` import：`use crate::version_chain2::VersionChain2; use crate::version_arena::VersionArena;`

**预期收益**：INSERT hot path 消除 `malloc` 调用，+15~20% TPS @16c+  
**风险**：中-高（MVCC 核心，需通过全部 SI litmus tests）  
**验收**：`cargo test --test si_litmus_tests` 全绿；flamegraph `Arc::new` 消失

---

### N-02: PrimaryKey 改 `Arc<[u8]>`

**目标**：insert→write_set→commit_keys 全链路 PK 只做 `Arc::clone`（8 bytes 原子 incr）。

**代码位置**：
- `memtable.rs:191` — `pub type PrimaryKey = Vec<u8>` → 改 `Arc<[u8]>`
- `memtable.rs:198` — `encode_pk` 返回 `Arc<[u8]>`（`Arc::from(buf.as_slice())`）
- `engine_dml.rs:159` — `pk_ret = pk.clone()` → 已是 `Arc::clone` 语义
- `engine.rs:2072` — `TxnWriteOp.pk` 改 `Arc<[u8]>`
- `memtable.rs:1242` — `pk_inserts.push(pk.clone())` → Arc::clone，无 memcpy

**影响面**：`PrimaryKey` 全仓库 grep，约 180 处引用；DashMap key 从 `Vec<u8>` 改为 `Arc<[u8]>`（需实现 `Hash + Eq + Borrow<[u8]>`）

**实现要点**：
```rust
// memtable.rs
pub type PrimaryKey = Arc<[u8]>;

// encode_pk 结尾
Arc::from(buf.as_slice())

// DashMap<Arc<[u8]>, _> 需要:
impl Borrow<[u8]> for Arc<[u8]> // 标准库已有
```

**预期收益**：commit 路径 -2次 malloc/memcpy，+8~12% TPS  
**风险**：高（接口改动广，DashMap key 类型变化）  
**验收**：`cargo check` 零错误；pgbench workload C TPS 提升 ≥8%

---

### N-03/N-11: MemTable 迁移到 VersionChain2（依赖 N-01）

见 N-01，合并执行。`VersionChain2.commit_no_report` 的 commit 快路径仅 2次 AtomicLoad + 1次 AtomicStore，无 RwLock。

---

### N-04: cross-thread flush 路径消除 DashMap

**目标**：`flush_local_cache` 的跨线程场景用 per-txn `Arc<Mutex<TxnLocalState>>` 替代 DashMap entry，或改为 channel-based handoff。

**代码位置**：`engine.rs:2139-2165` `flush_local_cache`

**方案**：引入 `txn_shared: DashMap<TxnId, Arc<Mutex<TxnLocalState>>>`，仅在跨线程场景（`begin_txn` 时分配 Arc，`flush_local_cache` 写入 Arc 内部），commit 时直接 `Arc::try_unwrap` 取所有权。

**预期收益**：多语句跨线程事务 DashMap 争用 -50%  
**风险**：低（仅影响跨线程路径）

---

### N-05: group_commit 改 parking_lot Condvar

**目标**：condvar slow path 从 `std::sync::Mutex` 改 `parking_lot::Mutex`，减少 futex overhead。

**代码位置**：`group_commit.rs:246-299`

**整改**：
```rust
// 改前
use std::sync::{Mutex, Condvar};
// 改后  
use parking_lot::{Mutex, Condvar};
// wait_timeout 签名略有不同，需调整
```

**预期收益**：condvar slow path ~20ns → ~10ns；P99 抖动 -10~15%  
**风险**：极低

---

### N-06: GC 迁移到 VersionChain2

依赖 N-01。`VersionChain2.gc()` 使用 arena free-list，不持 write lock 遍历；GC 期间读者完全无阻塞。

---

### N-07: commit_and_report 索引 key encode 缓存

**目标**：`index_update_on_commit` 中每个索引只 encode 一次，remove + insert 复用。

**代码位置**：`memtable.rs:1698-1736`

```rust
// 改前：remove 用 encode_key_buf，insert 再 encode_key_buf
// 改后：
let key_bytes = idx.encode_key_buf(old_row); // 一次
idx.remove(&key_bytes, pk);
// 新行 key
let new_key_bytes = idx.encode_key_buf(new_row);
idx.insert(new_key_bytes.into_vec(), pk.clone());
// 不变，但避免两次 encode：用 SmallVec cache
```

更彻底方案：`index_update_on_commit` 接收已 encode 的 `(old_key_bytes, new_key_bytes)` 预计算。

**预期收益**：有索引表 commit 路径 key encode 次数 -50%  
**风险**：极低

---

### N-08: batch_insert 改用 encode_pk_stack

**代码位置**：`engine_dml.rs:290` `batch_insert` columnstore 路径仍用 `encode_pk`

```rust
// 改前
pks.push(crate::memtable::encode_pk(row, cs.schema.pk_indices()));
// 改后
let pk_stack = crate::memtable::encode_pk_stack(row, cs.schema.pk_indices());
pks.push(pk_stack.to_vec()); // 仅在需要 owned Vec 时转换
```

**预期收益**：batch INSERT 消除 N 次 `Vec::with_capacity(64)` alloc  
**风险**：极低

---

### N-10: record_write 线性去重消除

**代码位置**：`engine.rs:2071`

**整改**：autocommit single-insert 路径（最常见）改用 `record_write_no_dedup`；去重责任前移到调用方 contract。

```rust
// engine_dml.rs insert_rowstore 结尾已在 TXN_LOCAL_CACHE 直接 push，
// 不调用 record_write — 已绕过去重。
// 检查所有 record_write 调用方，确认是否可安全改为 no_dedup。
```

**预期收益**：多行事务 write_set 构建从 O(N²) 降为 O(N)  
**风险**：低（需保证无重复 PK 的 caller contract）

---

### N-12: SecondaryIndex 写并发优化

**目标**：ArtTree 单锁串行改为分桶 DashMap（trade ordered scan for concurrent write）或引入细粒度 RwLock。

**代码位置**：`memtable.rs:496` `art.rs`

**方案 A**（保序，低风险）：为 ArtTree 增加分段锁，按 key 前缀路由到 16 个独立 ArtTree 分桶，各分桶独立 RwLock。  
**方案 B**（高并发，较高风险）：引入 `crossbeam-skiplist::SkipMap` 替换 ArtTree，支持无锁并发读写但需放弃 ART 的空间优势。

推荐方案 A，改动量小，收益稳定。

---

### N-15: 简单 INSERT/SELECT fast-path parser bypass

**目标**：对符合 `INSERT INTO <table> VALUES(...)` / `SELECT * FROM <table> WHERE pk = ?` 模式的查询，跳过完整 parse_sql → plan 流程，直接构造 PhysicalPlan。

**代码位置**：`handler_session.rs` / `handler.rs` QueryHandler::execute

**实现**：
```rust
fn try_fast_path_insert(sql: &str) -> Option<FastInsertPlan> { ... }
fn try_fast_path_point_get(sql: &str) -> Option<FastGetPlan> { ... }
```
字节级扫描，无 regex，无 alloc。

**预期收益**：pgbench workload C parser 开销从 ~5% → ~0.5%  
**风险**：中（需正确覆盖 VALUES 多行、参数化查询 `$1` 等）

---

### N-17: batch_insert TABLE_CACHE TLS

**代码位置**：`engine_dml.rs:210-214` — `batch_insert` 每次走 `engine_tables.get()`

```rust
// batch_insert 开头增加 TABLE_CACHE TLS 命中检查（与 insert() 同模式）
let cached = TABLE_CACHE.with(|c| {
    let slot = c.borrow();
    match slot.as_ref() {
        Some((e, ep, id, t)) if *e == eid && *ep == epoch && *id == table_id => Some(Arc::clone(t)),
        _ => None,
    }
});
```

**预期收益**：batch INSERT DashMap 查询次数 -1/batch  
**风险**：极低

---

### N-18: GC skip 用 gc_candidates 原子计数

**代码位置**：`gc.rs` sweep 循环；`memtable.rs:366` `gc_candidates: AtomicU64`

**整改**：sweep 前检查 `memtable.gc_candidates.load() == 0`，则直接跳过该表，避免 O(n) shard-lock 遍历。

```rust
if table.gc_candidates.load(Ordering::Relaxed) == 0 {
    continue; // INSERT-only table, no multi-version chains to GC
}
```

**预期收益**：INSERT-only 工作负载 GC 从 O(n) → O(1) skip  
**风险**：极低

---

## 四、Rowstore 内存布局优化

### VersionChain2 对齐策略
```
VersionChain2 [64B, align=64]:
  CL0: head_txn_id(8) + head_commit_ts(8) + head_flags(1) + pad(7) + head_row*(8) + overflow_head*(8) + arena*(8) + write_mu(4) + pad(12) = 64B
```
- 热读路径（`read_committed`）只触碰 CL0 的前 24 bytes
- `write_mu` 和 `overflow_head` 在同一 CL，写路径 cache miss 最小

### OwnedRow 已优化
`SmallVec<[Datum; 8]>` ≤8 列不触碰堆（已实现）。  
进一步：对 `Datum::Int64` / `Datum::Int32` / `Datum::Bool` 的 row，考虑 compact binary format（`[u64; N]`），避免 enum tag 浪费。

### DashMap shard 数量
当前 `new()` 默认 256 shards，`new_large_mem()` 支持 4096 shards。  
建议：OLTP 场景 default 改 1024（`1 << 10`），减少同一 shard 内多 key 碰撞；`new_large_mem` 维持 4096+。

### MemTable 字段排列（cache-line 意识）
当前 `MemTable` 结构体字段混排。建议将最热字段前置：
```rust
pub struct MemTable {
    // CL 0-1: 读热路径（point-get）
    pub data: DashMap<...>,        // 指针，8B
    pub has_secondary_idx: AtomicBool, // 1B
    // CL 2+: 写热路径
    pub secondary_indexes: RwLock<...>,
    pub committed_row_count: StripedI64,
    // 冷字段
    pub schema: TableSchema,
    pub pk_order: RwLock<...>,
    ...
}
```

---

## 五、WAL 与 group commit 优化

### 当前架构（已实现）
- **分片 WAL**：`WAL_SHARD_COUNT` 个 shard，每线程 thread-local shard affinity，减少 shard lock 争用
- **TLS APPEND_BUF**：`bincode::serialize_into` 写入 TLS Vec，避免 per-record alloc
- **Pre-serialization**：insert/batch_insert 在 DML 时 pre-serialize WAL record，commit 时 `append_preserialized` 零 serialize 开销
- **Group commit**：spin-500 + condvar；syncer 线程可 core-pin

### 剩余优化

#### WAL-1: condvar 改 parking_lot（N-05）
见上文，`group_commit.rs` 将 `std::sync::Mutex/Condvar` 全改 `parking_lot`。

#### WAL-2: WAL shard flush 并行化
当前 `flush_all_shards` 串行 fsync 各 shard file。改为：
- 单个 WAL file（不分文件），多 shard 写入同一 fd，单次 `fdatasync`
- 或：多 file 时用 `io_uring` batch fsync（Linux only，feature-gate）

#### WAL-3: 大事务 WAL record 零拷贝
`WalRecord::BatchInsert { rows: Vec<OwnedRow> }` 在 serialize 时需 clone rows。  
改为：WAL 序列化直接引用 `Arc<OwnedRow>`，利用 `serde` custom serialize 避免 deep clone。

#### WAL-4: WAL 文件预分配
`fallocate(FALLOC_FL_KEEP_SIZE)` 预分配 64MB WAL segment，避免 ext4/xfs metadata 更新成为 fsync 瓶颈。

---

## 六、执行器 fast path 特化

### 已实现
- Single-key rowstore fast path（`engine.rs:2778`）：单 key，无索引，直接 `commit_no_report`，跳过 `pre_validate_write_set`
- TABLE_CACHE TLS：`insert()` 跳过 `engine_tables` DashMap 查询

### 待实现

#### EXEC-1: prepared statement cache
pgbench 使用 extended query protocol（Prepare + Execute）。当前每次 Bind 阶段走完整 plan。  
改为：`PreparedStatement` 缓存 PhysicalPlan，Bind 时只替换参数。

**代码位置**：`handler_session.rs` `parse_prepare_statement` / `parse_execute_statement`

#### EXEC-2: point-get 短路
```
SELECT col FROM t WHERE pk = $1
```
识别为 point-get，直接调 `StorageEngine::get(table_id, pk, txn_id, read_ts)`，跳过 planner。

#### EXEC-3: multi-row INSERT VALUES 向量化
```
INSERT INTO t VALUES(r1),(r2),...,(rN)
```
解析时构建 `Vec<OwnedRow>`，直接调 `batch_insert`，单次 WAL record + 单次 DashMap batch。

---

## 七、内存分配器与对象生命周期优化

### ALLOC-1: jemalloc 替换 system allocator
```toml
# Cargo.toml (top-level)
[dependencies]
jemallocator = "0.5"

# main.rs
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
```
预期：高并发 malloc/free 路径 +10~20% 吞吐。

### ALLOC-2: VersionArena 替换 Version Arc::new（N-01 的核心）
`VersionArena`：1MB slab blocks，lock-free free-list 回收，bump-pointer 分配。  
每次 INSERT/UPDATE 分配一个 `ArenaVersion` slot = CAS pop（~5ns vs malloc ~30ns）。

### ALLOC-3: PrimaryKey 改 Arc<[u8]>（N-02）
8 字节原子 incr 替代 malloc + memcpy。

### ALLOC-4: write_set Vec 预分配
`TxnLocalState::write_set: Vec<TxnWriteOp>` 默认空 Vec，第一次 push 时分配。  
改为 `SmallVec<[TxnWriteOp; 4]>` 覆盖 ≤4 ops 单语句事务无堆分配。

```rust
// engine.rs:73
pub write_set: SmallVec<[TxnWriteOp; 4]>,
```

### ALLOC-5: read_set Vec 预分配
同上，`read_set: SmallVec<[TxnReadOp; 8]>` 覆盖 ≤8 reads 的 autocommit 查询。

---

## 八、多核扩展与争用热点治理

### CONTENTION-1: txn_local DashMap shard 争用
**热点**：`take_local_state` 调用 `self.txn_local.remove(&txn_id)`，高并发时多核竞争同一 shard。  
**治理**：
- TXN_LOCAL_CACHE TLS 已大幅减少命中 DashMap 的频率
- 进一步：`txn_local` shard 数从默认（通常 16）改为 256，用 `DashMap::with_shard_amount(256)`

### CONTENTION-2: engine_tables DashMap 读争用
**热点**：`engine_tables.get(&table_id)` 在 batch_insert 路径未缓存时每行一次。  
**治理**：N-17（TABLE_CACHE TLS 覆盖 batch_insert）

### CONTENTION-3: secondary_indexes RwLock 写争用
**热点**：`index_update_on_commit` 在每次 commit 持 read lock；DDL 时写 lock 阻塞所有读者。  
**治理**：N-12（ArtTree 分桶）；DDL 频率低，可接受短暂写 lock

### CONTENTION-4: catalog RwLock DDL 争用
**热点**：`catalog: RwLock<Catalog>` 在每次 table name lookup 时持 read lock。  
**治理**：将热查询（table_id → schema）缓存到 thread-local，DDL 时 epoch invalidation（类似 TABLE_CACHE 机制）。

### CONTENTION-5: committed_row_count StripedI64 fetch_add
已有 16 stripes，基本消除 false sharing。  
进一步：commit fast path（无索引单键）将 `fetch_add` lazy batch，每 64 commits 做一次。

### CONTENTION-6: WAL shard lock
`shard.lock()` 在 `wal.rs:953`。  
**治理**：降低 serialize 时间（pre-serialization 已实现）；增大 shard count（`WAL_SHARD_COUNT = 32` → 64）。

---

## 九、网络协议层零拷贝优化

### NET-1: 响应序列化零拷贝
当前 `BackendMessage::DataRow` 构建 `Vec<Option<Vec<u8>>>` per-row。  
改为：直接 write 到 `tokio::io::BufWriter`，避免中间 Vec。

### NET-2: 批量 DataRow flush
当前每行调用 `send_row()`。改为：积攒 N 行后一次 `write_all`，减少 syscall。

### NET-3: prepared statement result schema cache
`RowDescription` message 在 Describe 阶段发送后，Execute 阶段不再重复发送（Postgres wire protocol 已支持）。  
确保 `PgSession` 缓存已发送的 `RowDescription`，避免重复序列化。

### NET-4: zero-copy point-get 响应
Point-get 返回单行：`Arc<OwnedRow>` 直接 serialize 到 wire，不 clone OwnedRow 内容。  
`ver.data.read().clone_hot()` → 改为返回 `Arc<OwnedRow>` 引用，序列化时 borrow `&OwnedRow`。

---

## 十、基准测试与性能门禁体系

### BENCH-1: pgbench 标准基准
```bash
# 建库
pgbench -i -s 100 -h 127.0.0.1 -p 5433 -U falcon falcondb
# Workload C (read-only point-get)
pgbench -c 16 -j 8 -T 60 -S falcondb
# Workload A (50% UPDATE)
pgbench -c 16 -j 8 -T 60 falcondb
# Workload B (小事务 INSERT)
pgbench -c 16 -j 8 -T 60 -f insert_workload.sql falcondb
```

**门禁标准**：

| 阶段 | Workload C TPS | Workload A TPS | P99 latency |
|------|---------------|----------------|-------------|
| 当前基线 | ~45K | ~28K | <5ms |
| Phase 2 目标 | ≥80K | ≥50K | <2ms |
| Phase 3 目标 | ≥100K | ≥65K | <1ms |

### BENCH-2: YCSB 基准
```bash
# Workload A: 50% reads + 50% updates
ycsb run falcondb -P workloads/workloada -p operationcount=1000000 -p threadcount=16
# Workload F: 50% reads + 50% RMW
ycsb run falcondb -P workloads/workloadf -p operationcount=1000000 -p threadcount=16
```

### BENCH-3: 自定义微基准（Rust criterion）
```rust
// benches/insert_commit.rs
criterion_group!(benches, bench_single_insert_commit, bench_batch_insert_commit,
                 bench_point_get, bench_update_same_row_10x);
```

每个 P0/P1 整改项对应一个 micro-bench，PR 合并前必须运行，收益需 ≥5%。

### BENCH-4: 火焰图持续集成
```bash
cargo flamegraph --bench insert_commit -- --bench
# 门禁：Arc::new 不出现在 commit 热路径前 5 函数
# 门禁：bincode::serialize_into 不出现在 commit 热路径前 10 函数
```

### BENCH-5: P99/P999 延迟门禁
使用 `hdrhistogram` 记录 commit latency 分布：
- P99 ≤ 2ms（Phase 2）
- P999 ≤ 5ms（Phase 2）
- P99 ≤ 1ms（Phase 3）
- P999 ≤ 3ms（Phase 3）

### BENCH-6: 内存分配压力测试
```bash
# valgrind massif 或 heaptrack
heaptrack ./target/release/falcondb-bench
# 门禁：commit 热路径 malloc 调用次数 ≤ 1/txn（仅 Arc<VersionChain2> 的 slab slot）
```

---

## 十一、12 周落地计划

```
Week 1-2:  [N-05] group_commit → parking_lot Condvar（1天）
           [N-17] batch_insert TABLE_CACHE TLS（0.5天）
           [N-18] GC skip gc_candidates == 0（0.5天）
           [N-08] batch_insert encode_pk_stack（0.5天）
           [N-10] record_write 去重消除（1天）
           [N-07] index_update_on_commit key encode 缓存（1天）
           [ALLOC-4/5] write_set/read_set SmallVec（1天）
           建立 criterion micro-bench 基线

Week 3-4:  [N-01] VersionChain2 迁移 Part1：gc.rs / gc_budget.rs API 适配
           [N-01] VersionChain2 迁移 Part2：MemTable data 字段 + 构造函数 + VersionArena
           SI litmus tests 全绿验收
           flamegraph 验证 Arc::new(Version) 消失

Week 5-6:  [N-02] PrimaryKey → Arc<[u8]>（影响面最广，需最多测试）
           全量回归测试 + pgbench 基线对比
           P99 门禁验收（≤2ms）

Week 7-8:  [N-12] SecondaryIndex ArtTree 分桶（方案A，16桶）
           [N-04] cross-thread flush Arc<Mutex> 方案
           [ALLOC-1] jemalloc 全局替换
           pgbench workload A/C 基准对比

Week 9-10: [N-15] 简单 INSERT/SELECT fast-path parser bypass
           [EXEC-1] prepared statement PhysicalPlan cache
           [EXEC-3] multi-row INSERT VALUES → batch_insert
           协议层 NET-1/NET-2 零拷贝

Week 11-12:[WAL-4] WAL segment 预分配 fallocate
           [WAL-2] WAL flush 并行化（io_uring feature-gate）
           [CONTENTION-4] catalog TLS epoch cache
           全链路 flamegraph + perf stat 验收
           pgbench / YCSB / criterion 三套基准完整跑
           P99/P999 门禁验收
```

---

## 十二、每阶段验收标准

### Phase 1（Week 1-2）— 低风险热点清除
- [ ] `cargo check -p falcon_storage` 零错误
- [ ] `cargo test -p falcon_storage` 零失败
- [ ] criterion bench：`bench_batch_insert` TPS 提升 ≥10%
- [ ] `group_commit` condvar slow path latency P99 降低 ≥15%（heaptrack 验证）

### Phase 2（Week 3-6）— VersionChain2 迁移 + PK 优化
- [ ] `cargo test --test si_litmus_tests` 全绿（21 个 SI test）
- [ ] `cargo test --test recovery_tests` 全绿
- [ ] pgbench Workload C 16c TPS ≥ 80K（从基线 ~45K）
- [ ] flamegraph：`Arc::new` 不在 commit 热路径 top-10
- [ ] P99 commit latency ≤ 2ms（hdrhistogram）

### Phase 3（Week 7-10）— 索引 + 协议层
- [ ] 有二级索引表 UPDATE TPS 提升 ≥ 20%
- [ ] pgbench Workload A 16c TPS ≥ 50K
- [ ] prepared statement 路径 parser 开销 < 1%（flamegraph）
- [ ] P99 ≤ 1.5ms

### Phase 4（Week 11-12）— WAL + 全链路验收
- [ ] WAL segment fsync 时间 P99 ≤ 200µs（`perf stat fdatasync`）
- [ ] pgbench Workload C 16c TPS ≥ 100K
- [ ] pgbench Workload A 16c TPS ≥ 65K
- [ ] P99 ≤ 1ms，P999 ≤ 3ms
- [ ] YCSB Workload A throughput ≥ 70K ops/s @16c

---

## 附录：关键文件索引

| 文件 | 核心结构 | 主要热点 |
|------|---------|---------|
| `mvcc.rs` | `VersionChain`, `Version` | `prepend_arc`, `commit_no_report`, `commit_and_report` |
| `version_chain2.rs` | `VersionChain2` | lock-free commit, inline slot |
| `version_arena.rs` | `VersionArena` | slab alloc, free-list |
| `memtable.rs` | `MemTable`, `SecondaryIndex`, `GinIndex` | `insert`, `commit_keys`, `index_update_on_commit` |
| `engine.rs` | `StorageEngine`, `TxnLocalState` | `commit_txn_local`, `take_local_state`, `TXN_LOCAL_CACHE` |
| `engine_dml.rs` | — | `insert_rowstore`, `batch_insert` |
| `wal.rs` | `WalWriter` | `append`, `append_multi`, `append_preserialized` |
| `group_commit.rs` | `GroupCommitSyncer` | `notify_and_wait`, `syncer_loop` |
| `gc.rs` | `GcConfig`, `GcStats` | `run_gc_with_config` sweep loop |
| `handler_session.rs` | `QueryHandler` | `handle_system_query`, system query dispatch |

---

*本文档基于 2026-03 代码审计生成，与 `OLTP_PERF_PLAN.md` V1 对应整改项已标注当前状态。整改请严格按 P0→P1→P2 顺序执行，每项完成后运行对应验收命令后再进入下一项。*
