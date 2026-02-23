# USTM — User-Space Tiered Memory Engine

> FalconDB 下一代存储访问层设计（替代 mmap）

---

## 1. 问题定义：mmap 为何不够好

| 缺陷 | 根因 | 影响 |
|------|------|------|
| Page Fault 延迟不可预测 | 缺页由内核异步触发，数据库无法控制 | P99 毛刺 |
| 无法精确控制驻留策略 | LRU 由 OS 决定，不懂数据库语义 | 热数据被误换出 |
| TLB Shootdown | 多核下解除映射需跨核中断 | CPU 开销 |
| 无法与 async I/O 集成 | mmap 是同步阻塞模型 | 无法利用 io_uring |
| 粒度固定 4KB | OS Page Size 不匹配数据库页大小 | 小 I/O 浪费，大 I/O 低效 |
| 写路径不安全 | msync 语义模糊，难以保证持久化顺序 | WAL 正确性风险 |

**核心矛盾**：mmap 把「哪些数据在内存、哪些在磁盘」的决策权交给了 OS，
但数据库拥有远比 OS 更丰富的信息（查询计划、事务优先级、访问频率）。

---

## 2. 设计目标

1. **确定性延迟**：任何数据访问的延迟上界可预测
2. **零拷贝读取**：热数据路径无 memcpy，性能不低于 mmap
3. **数据库感知的驻留策略**：利用查询语义控制冷热
4. **异步 I/O 原生**：基于 io_uring / IOCP，不依赖内核缺页机制
5. **分层存储**：DRAM → NVMe → 对象存储，显式管理
6. **写路径确定性**：严格控制持久化顺序，WAL-first 保证

---

## 3. 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                     Query Executor / Txn Layer                  │
│          (通过 PageHandle 访问数据，感知不到存储位置)              │
├─────────────────────────────────────────────────────────────────┤
│                  USTM — User-Space Tiered Memory                │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │   Hot Zone    │  │  Warm Zone   │  │    Cold Zone       │    │
│  │   (DRAM)      │  │  (DRAM/NVMe) │  │  (NVMe/Object)    │    │
│  │  Arena Alloc  │  │  Page Cache  │  │  Async Fetch       │    │
│  │  Zero-copy    │  │  Eviction    │  │  Prefetch Queue    │    │
│  └──────┬───────┘  └──────┬───────┘  └────────┬───────────┘    │
│         │                 │                    │                │
│         └────────┬────────┘────────────────────┘                │
│                  │                                              │
│         ┌───────┴────────┐                                      │
│         │  I/O Scheduler │  ← io_uring (Linux) / IOCP (Win)    │
│         │  + Prefetcher  │  ← 查询计划驱动的预取                 │
│         └───────┬────────┘                                      │
│                 │                                               │
├─────────────────┼───────────────────────────────────────────────┤
│                 ▼                                               │
│         ┌──────────────┐                                        │
│         │  Direct I/O  │  ← 绕过 OS Page Cache，完全自管理      │
│         │  (O_DIRECT)  │                                        │
│         └──────────────┘                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. 核心组件设计

### 4.1 PageHandle — 统一访问接口

上层代码（执行器、事务管理器）通过 `PageHandle` 读写数据，
完全不感知数据当前在 DRAM、NVMe 还是远程存储。

```rust
/// 页面句柄 — 上层代码唯一的数据访问接口
pub struct PageHandle {
    page_id: PageId,
    /// 当前驻留层级
    tier: AtomicU8,         // 0=Hot(DRAM), 1=Warm(PageCache), 2=Cold(Disk)
    /// 指向实际数据的指针（Hot/Warm 时有效）
    data: AtomicPtr<u8>,
    /// 引用计数（Pin 语义，被引用时不可换出）
    pin_count: AtomicU32,
    /// 访问统计
    access_count: AtomicU64,
    last_access_ns: AtomicU64,
    /// 所属事务的优先级（用于驱逐决策）
    priority: AccessPriority,
}

/// 访问优先级 — 数据库语义驱动
pub enum AccessPriority {
    /// 索引内部节点：几乎每次查询都命中，永不换出
    IndexInternal,
    /// 索引叶节点：高频访问
    IndexLeaf,
    /// 热点行（最近 N 秒内被事务访问）
    HotRow,
    /// 温数据（最近被扫描但非点查）
    WarmScan,
    /// 冷数据（历史/归档）
    Cold,
    /// 预取页（可能用到，可立即丢弃）
    Prefetched,
}
```

**关键特性**：
- **Pin 语义**：正在被事务使用的页面 `pin_count > 0`，绝不会被驱逐
- **优先级驱逐**：不是简单的 LRU，而是按 `AccessPriority` + 访问频率综合决策
- **零拷贝**：Hot Zone 的数据通过 `data` 指针直接访问，无 memcpy

### 4.2 Three-Zone Memory Manager — 三区内存管理

```
┌─────────────────────────────────────────────────┐
│ Hot Zone (DRAM Arena)                           │
│ ────────────────────                            │
│ • 固定大小（如总内存的 60%）                      │
│ • Arena 分配器，无碎片                            │
│ • MemTable + 索引内部节点 + 活跃事务写集合        │
│ • 永不落盘（WAL 保证持久化）                      │
│ • 访问延迟：< 100ns                              │
├─────────────────────────────────────────────────┤
│ Warm Zone (DRAM Page Cache)                     │
│ ────────────────────                            │
│ • 动态大小（总内存的 20-30%）                     │
│ • 缓存最近从磁盘读取的 SST 页                    │
│ • 数据库感知的驱逐算法（见 4.3）                  │
│ • 访问延迟：< 200ns（DRAM 命中）                  │
├─────────────────────────────────────────────────┤
│ Cold Zone (NVMe / Object Storage)               │
│ ────────────────────                            │
│ • 容量 = 磁盘容量，无上限                        │
│ • 通过 io_uring 异步批量读取                     │
│ • Prefetcher 根据查询计划提前加载                 │
│ • 访问延迟：50-500µs（NVMe）                     │
└─────────────────────────────────────────────────┘
```

### 4.3 LIRS-2 驱逐算法 — 替代 LRU

传统 LRU 的问题：一次全表扫描会把所有热数据挤出缓存。

USTM 采用改进的 **LIRS-2**（Low Inter-reference Recency Set）算法：

```rust
pub struct Lirs2Evictor {
    /// LIR（Low Inter-Reference）集合：高频访问页，受保护
    lir_set: LinkedHashMap<PageId, PageMeta>,
    /// HIR（High Inter-Reference）集合：低频或一次性访问页
    hir_resident: LinkedHashMap<PageId, PageMeta>,
    hir_nonresident: LinkedHashMap<PageId, ()>,
    /// 数据库语义加权
    priority_boost: HashMap<AccessPriority, f64>,
}
```

**核心规则**：
1. **首次访问**的页面进入 HIR（低优先级），不影响热数据
2. **二次命中**的页面提升到 LIR（受保护），不会被轻易驱逐
3. **全表扫描**的页面始终留在 HIR，扫描结束后被立即回收
4. **索引内部节点**直接标记为 LIR，永驻 Warm Zone

**效果**：一次全表扫描不会污染缓存，解决了 LRU 的最大痛点。

### 4.4 Prefetcher — 查询计划驱动的预取

mmap 依赖 OS 的 readahead，完全不懂 SQL 语义。
USTM 的 Prefetcher 直接从查询计划获取信息：

```rust
pub struct QueryAwarePrefetcher {
    /// 异步 I/O 提交队列
    io_ring: IoUring,
    /// 预取请求队列（按优先级排序）
    queue: BinaryHeap<PrefetchRequest>,
    /// 每秒最大预取 I/O 次数（防止抢占正常 I/O）
    rate_limiter: TokenBucket,
}

pub struct PrefetchRequest {
    page_id: PageId,
    /// 预计何时需要（从查询计划推算）
    deadline_ns: u64,
    /// 请求来源
    source: PrefetchSource,
}

pub enum PrefetchSource {
    /// 索引范围扫描：预取下一批叶节点
    IndexRangeScan { next_leaf_pages: Vec<PageId> },
    /// 顺序扫描：预取后续数据块
    SeqScan { stride: usize },
    /// 嵌套循环 JOIN：预取内表探测页
    NestedLoopProbe { probe_pages: Vec<PageId> },
    /// Compaction：低优先级批量预取
    Compaction { batch: Vec<PageId> },
}
```

**关键**：
- 执行器在执行索引扫描时，提前告诉 Prefetcher 接下来要访问哪些页
- Prefetcher 通过 io_uring 异步提交，数据到达 Warm Zone 时查询刚好用到
- 延迟从「同步缺页中断 500µs」变成「异步预取 0µs」（命中时）

### 4.5 I/O Scheduler — 统一调度

所有磁盘 I/O 经过统一调度器，避免 Compaction 抢占查询 I/O：

```rust
pub struct IoScheduler {
    /// 高优先级：用户查询的数据读取
    query_queue: VecDeque<IoRequest>,
    /// 中优先级：预取请求
    prefetch_queue: VecDeque<IoRequest>,
    /// 低优先级：Compaction、GC、检查点
    background_queue: VecDeque<IoRequest>,
    /// io_uring 实例
    ring: IoUring,
    /// 每个优先级的 I/O 带宽配额
    quotas: [TokenBucket; 3],
}
```

**调度规则**：
- 查询 I/O 永远优先
- Compaction I/O 不超过总带宽的 30%
- 预取 I/O 不超过总带宽的 20%
- 剩余带宽按需分配

---

## 5. 写路径：严格的持久化顺序

mmap 的 msync 无法保证写入顺序，USTM 完全绕开 mmap 的写路径：

```
写入请求
   │
   ├──→ ① WAL（Direct I/O + fsync）  ← 必须先持久化
   │
   ├──→ ② Hot Zone MemTable（DRAM）   ← 内存写入，立即可读
   │
   └──→ ③ 后台 Flush → SST 文件       ← 异步，不影响延迟
         │
         └──→ Cold Zone（磁盘）
```

**保证**：
- WAL 写入使用 `O_DIRECT + fsync`，绝对持久化
- MemTable 写入是纯内存操作，无磁盘 I/O
- SST Flush 是后台异步任务，不在事务关键路径上

---

## 6. 与 mmap 的对比

| 维度 | mmap | USTM |
|------|------|------|
| **延迟可预测性** | ✗ 缺页中断不可控 | ✓ 预取 + Pin 保证上界 |
| **缓存驱逐策略** | OS LRU（不懂数据库） | LIRS-2 + 语义优先级 |
| **全表扫描抗性** | ✗ 扫描污染整个缓存 | ✓ 扫描页隔离在 HIR |
| **异步 I/O** | ✗ 同步缺页 | ✓ io_uring 原生异步 |
| **写路径安全** | ✗ msync 顺序模糊 | ✓ WAL-first + Direct I/O |
| **多核扩展性** | ✗ TLB Shootdown | ✓ 无 TLB 依赖 |
| **内存控制精度** | ✗ OS Page (4KB) 粒度 | ✓ 数据库页粒度（可变） |
| **实现复杂度** | 低 | 高（需要自管理所有组件） |
| **跨平台** | 好 | 需适配（io_uring=Linux, IOCP=Windows） |

---

## 7. 实现状态（已完成）

> 本节记录实际代码实现情况，对应 `crates/falcon_storage/src/ustm/`。

### 7.1 已实现模块

| 模块 | 文件 | 状态 | 测试数 |
|------|------|------|--------|
| 核心类型 | `page.rs` | ✅ 完成 | 7 |
| LIRS-2 驱逐 | `lirs2.rs` | ✅ 完成 | 9 |
| I/O 调度器 | `io_scheduler.rs` | ✅ 完成（同步 I/O 后端） | 4 |
| 查询预取器 | `prefetcher.rs` | ✅ 完成 | 7 |
| 三区管理器 | `zones.rs` | ✅ 完成 | 8 |
| 引擎协调器 | `engine.rs` | ✅ 完成 | 10 |
| 模块声明 | `mod.rs` | ✅ 完成 | — |

**合计：45 个单元测试，全部通过。**

### 7.2 StorageEngine 集成

| 集成点 | 文件 | 状态 |
|--------|------|------|
| `ustm` 字段 + 4 个构造函数 | `engine.rs` | ✅ |
| `set_ustm_config()` / `ustm_stats()` / `shutdown()` | `engine.rs` | ✅ |
| INSERT / UPDATE / DELETE / GET / SCAN（Rowstore） | `engine_dml.rs` | ✅ |
| INSERT / UPDATE / DELETE / GET / SCAN（LSM） | `engine_dml.rs` | ✅ |
| CREATE TABLE / DROP TABLE | `engine_ddl.rs` | ✅ |
| `[ustm]` 配置节 | `falcon_common/config.rs` | ✅ |
| 服务器启动集成 | `falcon_server/main.rs` | ✅ |
| `examples/primary.toml` 示例配置 | — | ✅ |

### 7.3 DML 路径集成细节

```
INSERT (Rowstore) → ustm.alloc_hot(PageId, HotRow)
INSERT (LSM)      → ustm.insert_warm(PageId, HotRow)   ← 磁盘表走 Warm Zone
UPDATE (Rowstore) → ustm.alloc_hot(PageId, HotRow)     ← 刷新 Hot Zone
UPDATE (LSM)      → ustm.insert_warm(PageId, HotRow)   ← 刷新 Warm Zone
DELETE (Rowstore) → ustm.alloc_hot(PageId, HotRow)     ← 写入 tombstone
DELETE (LSM)      → ustm.unregister_page(PageId)       ← 主动驱逐
GET    (Rowstore) → ustm.insert_warm(PageId, HotRow)   ← LIRS-2 访问跟踪
GET    (LSM)      → ustm.insert_warm(PageId, HotRow)   ← 读缓存跟踪
SCAN   (Rowstore) → ustm.prefetch_hint(SeqScan)        ← 顺序扫描预取
SCAN   (LSM)      → ustm.prefetch_hint(SeqScan, sst_path) ← 指向 SST 目录
```

### 7.4 DDL 路径集成细节

```
CREATE TABLE → ustm.alloc_hot(meta_page, IndexInternal)  ← 永不驱逐
DROP TABLE   → ustm.unregister_page(meta_page)           ← 清理元数据页
```

### 7.5 待完成（后续迭代）

| 项目 | 说明 | 优先级 |
|------|------|--------|
| io_uring 后端 | 当前使用同步 I/O，Linux 生产环境需替换 | P1 |
| IOCP 后端 | Windows 异步 I/O 适配 | P2 |
| LIRS-2 侵入式链表 | 当前 `VecDeque::retain()` 是 O(n)，大规模需优化 | P1 |
| USTM 统计 SHOW 命令 | `SHOW falcon.ustm_stats` 暴露给 SQL 层 | P2 |
| 基准测试 | YCSB 对比 mmap 性能数据 | P2 |

---

## 8. 实现路线图（原始计划 vs 实际）

| 阶段 | 交付物 | 计划工作量 | 实际状态 |
|------|--------|-----------|---------|
| **P0** | PageHandle + Hot/Warm/Cold 三区管理 | 2-3 周 | ✅ 完成 |
| **P1** | Direct I/O 读写 + 替换 mmap 读取 SST | 2 周 | ✅ 同步 I/O 完成（io_uring 待接入） |
| **P2** | LIRS-2 驱逐算法 | 1-2 周 | ✅ 完成 |
| **P3** | io_uring 集成 (Linux) / IOCP (Windows) | 2-3 周 | ⏳ 待实现 |
| **P4** | QueryAwarePrefetcher | 2 周 | ✅ 完成 |
| **P5** | I/O Scheduler（优先级调度） | 1 周 | ✅ 完成 |
| **P6** | StorageEngine 全路径集成 | — | ✅ 完成（Rowstore + LSM） |
| **P7** | 基准测试 + 调优 | 2 周 | ⏳ 待实现 |

---

## 9. 适用场景

USTM 特别适合：
- **OLTP 数据库**：确定性延迟是核心需求
- **数据集 > 内存**：需要智能的冷热分层（LSM + Warm Zone）
- **混合负载**：OLTP 查询与后台 Compaction 并存
- **高并发**：多核扩展性要求高（无 TLB Shootdown）

不适合：
- 数据集 < 内存（纯 MemTable 更简单高效）
- 嵌入式数据库（LMDB 的 mmap 模型更轻量）
