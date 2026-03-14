# FalconDB — PG 兼容分布式内存 OLTP 数据库

> [English](ARCHITECTURE.md) | 简体中文

## 0. 核心特性：确定性提交保证（DCG）

> **只要 FalconDB 返回"已提交"，该事务就能在任何单节点崩溃、故障切换和恢复中存活，零例外。**

**确定性提交保证（DCG）** 由提交点模型（`falcon_common::consistency` §1）强制执行：在活跃提交策略下，服务器在 WAL 持久化之前绝不向客户端确认提交。四个提交阶段——**WAL 已记录 → WAL 已持久化 → 已可见 → 已确认**——单向前进，任何回退都会被拒绝并返回 `InvariantViolation`。

- 证据：[docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- 时序图：[docs/commit_sequence.md](docs/commit_sequence.md)
- 非目标：[docs/non_goals.md](docs/non_goals.md)

---

## 1. 架构概览

### 1.1 数据流（正常路径：SELECT）

```
psql ──TCP──▶ [协议层（PG Wire）]
                  │
                  ▼
            [SQL 前端：Parse → Bind → Analyze]
                  │
                  ▼
            [Planner / Router]
                  │
          ┌───────┴────────┐
          │ 单分片         │ 多分片
          ▼                ▼
    [本地执行器]       [分布式协调器]
          │                │
          ▼                ▼
    [事务管理器 ─── MVCC ─── 存储引擎]
          │
          ▼
    [内存表 + 索引]
          │
          ▼ (异步)
    [WAL / Checkpoint]
```

### 1.2 数据流（写路径）

```
Client INSERT ──▶ PG Wire ──▶ SQL 前端 ──▶ Planner/Router
    ──▶ 事务开始 ──▶ 分片定位（按 PK 哈希）
    ──▶ LocalTxn（快速路径，单分片，无 2PC）
        │  或 GlobalTxn（慢速路径，多分片，XA-2PC）
    ──▶ 提交 ──▶ 写入 MemTable + WAL fsync
    ──▶ WAL 异步传输到副本
    ──▶ 回复客户端
```

### 1.3 节点角色（M1：主节点 / 副本节点）

M1 采用每分片主副本拓扑：
- **主节点**：接受读写；WAL 持久化后确认提交。
- **副本节点**：接收 WAL chunk；提升前只读。
- **提升（Promote）**：基于隔离（Fencing）的故障切换（fence → catch-up → swap → unfence）。

未来（M2+）：拆分为独立的计算 / 存储 / 元数据角色。

---

## 2. 模块清单

### 2.1 `protocol_pg` — PostgreSQL 网络协议

| 项目 | 说明 |
|------|------|
| **职责** | 接受 TCP 连接，处理 PG 启动/认证握手，解析 Simple Query / Extended Query 消息，序列化 RowDescription/DataRow/CommandComplete/Error 响应 |
| **输入** | 原始 TCP 字节流 |
| **输出** | 解析后的 `Statement` + 会话上下文；序列化的 PG 响应帧 |
| **核心类型** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **依赖** | `common`（类型、错误） |
| **可替换性** | 整个 crate 可替换为 MySQL 协议或 HTTP/gRPC 网关 |
| **开源复用** | `pgwire` crate（MIT）用于消息编解码；自定义会话管理 |

### 2.2 `sql_frontend` — 解析器 / 分析器 / 绑定器

| 项目 | 说明 |
|------|------|
| **职责** | SQL 文本 → AST → 带目录引用的已解析逻辑计划 |
| **输入** | SQL 字符串 + 目录快照 |
| **输出** | `BoundStatement`（类型安全的已解析 AST） |
| **核心类型** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **依赖** | `common`（类型、目录 trait） |
| **可替换性** | 解析器后端（sqlparser-rs vs 自定义 PEG）、方言配置 |
| **开源复用** | `sqlparser-rs`（Apache-2.0）用于解析；自定义绑定器/分析器 |

### 2.3 `planner_router` — 查询规划器、基于代价的优化器与分布式路由器

| 项目 | 说明 |
|------|------|
| **职责** | 逻辑计划 → 优化后物理计划；基于代价的索引/扫描选择；JOIN 重排与策略；确定分片目标；下推过滤器 |
| **输入** | `BoundStatement` + 分片映射 + `TableStatsMap` + `IndexedColumns` |
| **输出** | `PhysicalPlan`（带分片注解的算子树） |
| **CBO 优化器** | `plan_optimized()` 流水线：BoundStatement → LogicalPlan → 优化规则 → 基于代价的物理降低。使用 `estimate_selectivity()`（NDV、MCV、直方图）、`seq_scan_cost()` / `index_scan_cost()` / `prefer_index_scan()` 选择 SeqScan 或 IndexScan。基于行数的 JOIN 策略（NL/Hash/MergeSort）和重排。不支持的语句降级到 `plan_with_indexes()`。 |
| **自动分析集成** | `StorageEngine::track_rows_modified()` 在修改超过阈值（max(行数/5, 500)）后触发内联 `analyze_table()`；统计信息通过 `build_table_stats()` 流入 CBO。 |
| **核心类型** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown`, `TableStatsMap`, `TableStatsInfo`, `ColumnStatsInfo`, `IndexedColumns` |
| **依赖** | `sql_frontend`, `cluster_meta`, `common` |
| **可替换性** | 优化规则、代价模型、路由策略 |

### 2.4 `executor` — 执行引擎

| 项目 | 说明 |
|------|------|
| **职责** | 执行物理计划算子，产生结果行；强制执行每事务 READ ONLY 和超时保护；带结构化中止原因的查询调控器；大表扫描的融合流式聚合；列存 GROUP BY AGG 向量化下推 |
| **输入** | `PhysicalPlan` + 事务句柄 |
| **输出** | `RowStream`（`Row` 的迭代器） |
| **核心类型** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `Insert`, `Update`, `Delete`, `QueryGovernor`, `GovernorAbortReason` |
| **快速路径** | `exec_fused_aggregate`（单次 WHERE+GROUP BY+聚合，零行克隆）；`try_streaming_aggs`（简单列引用聚合）；`try_pk_ordered_limit`（有界堆 top-K）；`exec_columnar_group_agg`（列存向量化哈希聚合，带 zone-map 过滤下推） |
| **依赖** | `storage`（通过 trait）, `txn`, `common` |
| **可替换性** | 逐行 ↔ 向量化；推送 ↔ 拉取模型 |

### 2.5 `txn` — 事务管理器

| 项目 | 说明 |
|------|------|
| **职责** | 开始/提交/中止事务；MVCC 时间戳分配；OCC 验证（SI）；快速路径（LocalTxn）和慢速路径（GlobalTxn）提交；延迟直方图；事务历史环形缓冲区；每事务 READ ONLY 模式、超时和执行摘要 |
| **输入** | 来自执行器的事务操作 |
| **输出** | 事务句柄、提交/中止决策、`TxnStatsSnapshot`、`TxnRecord` 历史 |
| **核心类型** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel`, `TxnType`, `TxnPath`, `TxnContext`, `TxnRecord`, `TxnOutcome`, `LatencyStats`, `TxnExecSummary` |
| **依赖** | `storage`（版本可见性、OCC 读集）, `common` |
| **可替换性** | OCC vs 2PL vs 混合；时间戳预言机（本地 vs 分布式） |

### 2.6 `storage` — 多引擎存储层

| 项目 | 说明 |
|------|------|
| **职责** | 多引擎 MVCC 版本存储；维护索引（哈希 PK + BTree 二级 + 唯一 + 复合/覆盖/前缀）；WAL 写入 + 恢复；MVCC 垃圾回收；复制统计；零拷贝行迭代；流式聚合计算；自动分析 + CBO 统计馈送；CDC 前后镜像 |
| **表分发** | 单个 `engine_tables: DashMap<TableId, TableHandle>` 存储所有表。`TableHandle` 枚举多态分发：Rowstore 有显式 DML/WAL/CDC 快速路径；磁盘引擎（LSM、RocksDB、redb）通过 `as_storage_table() → &dyn StorageTable` 走统一 DML + USTM 预取路径。 |
| **存储引擎** | **Rowstore**（内存，默认）· **LSM**（磁盘，默认特性）· **RocksDB**（`--features rocksdb`，C++ 库）· **redb**（`--features redb`，纯 Rust）· DiskRowstore（存根）· Columnstore（存储 + 向量化 AGG 下推） |
| **StorageTable trait** | 核心 DML 契约（`insert`/`update`/`delete`/`get`/`scan`/`commit_key`/`abort_key`），带默认批量和可见性方法。USTM 集成通过 `prefetch_hint`/`prefetch_evict`/`scan_prefetch_hint`（默认空操作；LsmTable 覆盖）。 |
| **核心类型** | `StorageEngine`, `TableHandle`, `StorageTable`（trait）, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `WalRecord`, `GcConfig`, `GcStats`, `GcRunner`, `LsmEngine`, `RocksDbTable`, `RedbTable` |
| **MVCC** | `VersionData` 枚举：`Hot(Arc<OwnedRow>)`（内存中）；`Cold(ColdHandle)`（压缩在 ColdStore，按需解析）；`Tombstone`。`with_visible_data`（零拷贝闭包行访问）；`for_each_visible`（无 Vec 分配的流式 DashMap 迭代）；`scan_top_k_by_pk`（O(N log K) top-K 有界堆）；`compute_simple_aggs`（单次 COUNT/SUM/MIN/MAX） |
| **自动分析** | `rows_modified: DashMap<TableId, AtomicU64>` 跟踪每表 DML 计数；提交后调用 `track_rows_modified()`；在阈值 max(行数/5, 500) 处触发内联 `analyze_table()`；统计信息存储在 `table_stats` 并被 CBO 消费 |
| **CDC** | 前后镜像：当 `cdc_manager.is_enabled()` 时，`update()` 和 `delete()` 在变更前通过 `table.get()` 读取旧行；`emit_update(old_row, new_row)` 和 `emit_delete(old_row)` 提供完整变更捕获 |
| **WAL 优化** | 8 分片插入槽（`WalShard`）+ 线程本地亲和性；批量 WAL 写入（`txn_wal_buf` + `append_multi`）；缓存刷新 fd；`WRITE_THROUGH` + Leader-Follower 组提交（500 次自旋快速路径）；双缓冲 `flush_split`；组合每事务 DashMap（`TxnLocalState`）；WAL 恢复计数器推进（`recovered_max_ts`/`recovered_max_txn_id`） |
| **依赖** | `common`, `falcon_segment_codec` |
| **可替换性** | 通过 `ENGINE=` 子句按表选择引擎；索引实现（BTree、SkipList、ART） |

### 2.7 `raft` — 共识（通过 `replication.role = "raft_member"` 走生产路径）

> Raft 共识现在在生产路径上。在 `falcon.toml` 中设置 `replication.role = "raft_member"` 并配置 `[raft]` 部分，使用 Raft 支持的复制替代 WAL 传输。单节点模式（`SingleNodeConsensus`）仍作为空操作默认可用。

| 项目 | 说明 |
|------|------|
| **职责** | WAL 复制的 Raft 共识；Leader 选举；故障切换监视器 |
| **生产状态** | `role = raft_member` 时**激活**。`role = standalone` 时为单节点空操作存根。 |
| **核心类型** | `RaftGroup`, `RaftConsensus`, `GrpcNetworkFactory`, `RaftTransportService`, `LocalRaftHandle` |
| **集成** | `falcon_cluster::RaftShardCoordinator` 桥接 Raft 提交 → `StorageEngine`（通过 `ApplyFn`） |
| **配置** | `falcon.toml` 中的 `[raft]` 部分；`node_id`, `node_ids`, `raft_listen_addr`, `peers` |
| **依赖** | `common`, `openraft`, `tonic` |

### 2.8 `cluster` — 集群、复制与分布式执行

| 项目 | 说明 |
|------|------|
| **职责** | 分片映射；基于 WAL 的主副本复制（`WalChunk`, `ReplicationTransport`）；带隔离的提升/故障切换；分散/聚集分布式执行；DDL 协调；准入控制（读/写/DDL 许可）；节点运营模式（Normal/ReadOnly/Drain） |
| **输入** | DDL 请求、DML 路由、WAL chunk、提升命令 |
| **输出** | 复制状态、分片路由查询结果、故障切换指标 |
| **核心类型** | `ShardMap`, `ShardReplicaGroup`, `WalChunk`, `ReplicationTransport`, `InProcessTransport`, `ChannelTransport`, `ReplicationMetrics`, `DistributedQueryEngine`, `FailurePolicy`, `AdmissionControl`, `NodeModeController` |
| **依赖** | `storage`, `txn`, `common` |
| **可替换性** | 传输层（进程内 → M2 gRPC）；元数据存储（嵌入式 vs etcd） |

### 2.9 `observability` — 指标 / 追踪 / 日志

| 项目 | 说明 |
|------|------|
| **职责** | 发射指标（Prometheus）、结构化追踪（OpenTelemetry）、结构化日志 |
| **输入** | 各层的埋点 |
| **输出** | Prometheus 端点、OTLP 导出、日志文件/标准输出 |
| **核心类型** | 指标宏、`TraceContext`、日志宏 |
| **依赖** | `common` |
| **开源复用** | `metrics` + `metrics-exporter-prometheus`，`tracing` + `tracing-opentelemetry`，`tracing-subscriber` |

### 2.10 `common` — 共享基础

| 项目 | 说明 |
|------|------|
| **职责** | 共享类型、错误层次结构、配置、编解码、PG 类型系统、Datum 表示（含 `Decimal`）、RBAC（角色、权限、审计） |
| **核心类型** | `Datum`, `DataType`, `Row`, `OwnedRow`, `TableId`, `ShardId`, `NodeId`, `FalconError`, `Config`, `RoleCatalog`, `PrivilegeManager`, `Role`, `Privilege` |
| **依赖** | 无（叶子 crate） |

---

## 3. 存储引擎详解

### 3.1 引擎对比

| 引擎 | 存储方式 | 持久化 | 数据上限 | 适用场景 |
|------|---------|--------|---------|---------|
| **Rowstore**（默认） | 内存（MVCC 版本链） | 仅 WAL（启用 WAL 时崩溃安全） | 受可用内存限制 | 低延迟 OLTP、热数据 |
| **LSM** | 磁盘（LSM-Tree + WAL） | 全磁盘持久化 | 受磁盘空间限制 | 大数据集、冷数据、持久化优先场景 |
| **RocksDB** | 磁盘（RocksDB 库） | 全磁盘持久化 | 受磁盘空间限制 | 生产磁盘 OLTP，经过验证的 LSM 压实 |
| **redb** | 磁盘（纯 Rust 嵌入式 KV） | 全磁盘持久化 | 受磁盘空间限制 | 轻量嵌入式，零 C 依赖 |

### 3.2 版本体系（Edition）

通过 Cargo feature 标志控制构建的产品版本：

| 版本 | Feature | 存储引擎 | 定位 |
|------|---------|---------|------|
| **Community** | `--no-default-features` | Rowstore（内存） | 开源社区版 |
| **Standard** | `--features standard` | RocksDB（磁盘优先） | 兼容 PG、可无缝迁移的分布式 OLTP |
| **Enterprise** | `--features enterprise` | RocksDB + Columnstore | Standard + PITR + TDE + CDC + 多租户 |
| **Analytics** | `--features analytics` | Columnstore + RocksDB | OLAP 工作负载，列存优先 |

启动时通过 `tracing::info!(edition = ..., "FalconDB edition: {}")` 输出当前版本，并在 `SELECT version()` 中显示。

### 3.3 `TableHandle` 分发设计

```rust
pub enum TableHandle {
    Rowstore(Arc<MemTable>),      // 快速路径：显式 DML/WAL/CDC 分支
    Lsm(Arc<LsmTable>),           // → dyn StorageTable
    RocksDb(Arc<RocksDbTable>),   // → dyn StorageTable（feature-gated）
    Redb(Arc<RedbTable>),         // → dyn StorageTable（feature-gated）
    Columnstore(Arc<ColumnStoreTable>),
    DiskRowstore(Arc<DiskRowstoreTable>),
}

impl TableHandle {
    fn as_storage_table(&self) -> Option<&dyn StorageTable>;
    fn as_rowstore(&self) -> Option<&Arc<MemTable>>;
}
```

新增引擎步骤：实现 `StorageTable` trait → 添加 `TableHandle` 变体 → 接入 DDL。

---

## 4. 事务与一致性设计

### 4.1 快速路径 + 慢速路径 + OCC

- **LocalTxn（快速路径）**：单分片事务在快照隔离下用 OCC 验证提交，无 2PC 开销。`TxnContext.txn_path = Fast`。
- **GlobalTxn（慢速路径）**：跨分片事务使用 XA-2PC（prepare → commit）。`TxnContext.txn_path = Slow`。
- **硬不变量强制**：`TxnContext.validate_commit_invariants()` 返回 `InvariantViolation` 错误（非仅 debug_assert）。
- **统一提交入口**：`StorageEngine::commit_txn(txn_id, commit_ts, txn_type)` 是唯一公开 API。
- **隔离级别**：Read Committed（默认）或快照隔离（可配置）。SI 下提交时 OCC 读集验证。
- **唯一约束强制**：提交时重新验证，违规时原子回滚。

### 4.2 MVCC 垃圾回收

- **安全点**：`gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL 感知**：绝不回收未提交（commit_ts==0）或已中止（commit_ts==MAX）版本
- **复制安全**：尊重副本已应用时间戳
- **无锁每键**：通过 DashMap 迭代的每链裁剪，无全局锁，无 stop-the-world
- **后台运行器**：`GcRunner` 线程，可配置间隔和批次大小
- **可观测性**：`SHOW falcon.gc_stats` 暴露安全点、已回收版本/字节、链统计

### 4.3 复制——WAL 传输（gRPC）

```
主节点 ──WAL chunk──▶ 副本节点
       （gRPC 流，tonic，SubscribeWal RPC）
```

| RPC | 类型 | 用途 |
|-----|------|------|
| `SubscribeWal` | 服务端流式 | 副本从 LSN 订阅；主节点推送 `WalChunkMessage` |
| `AckWal` | 单次 | 副本确认已应用的 LSN 以跟踪延迟 |
| `GetCheckpoint` | 服务端流式 | 新副本请求完整快照用于自举 |

- **Scheme A（默认）**：主节点 WAL 持久化后提交。副本异步应用。
- **Scheme B（同步）**：`SyncReplicationWaiter` 阻塞提交，直到 N 个副本报告 `applied_lsn ≥ commit_lsn`。
- **自举**：新副本调用 `GetCheckpoint` RPC → 接收完整 `CheckpointData` 快照 → 切换到 WAL 流。
- **提升/故障切换**：5 步隔离协议（fence → catch-up → swap → unfence → 更新分片映射）。

### 4.4 WAL 写入优化

```
线程 A（Leader）: append → 内锁下交换双缓冲 → 锁外 FUA 写入（~21µs）
线程 B-N（Follower）: append → 等待 Leader 刷新完成
```

- **批量 WAL 写入**：`InsertRow` 记录延迟到提交时通过 `txn_wal_buf` DashMap + `append_multi`
- **缓存刷新 fd**：`flush_fd_cache` 避免每次刷新调用 `DuplicateHandle`
- **双缓冲 `flush_split`**：内锁下交换（即时），锁外 FUA 写入
- **单键提交快速路径**：自动提交单行 INSERT 跳过行克隆和唯一约束重验证
- **固定数组 `append_multi`**：常见情况（1 个延迟 + 提交记录）使用栈上数组而非 Vec

### 4.5 WAL 恢复流程

1. 加载 `checkpoint.bin` → 恢复目录 + 已提交行
2. 从 checkpoint segment 开始读取 WAL
3. 跳过记录直到 `WalRecord::Checkpoint` 标记
4. 重放标记后的 Insert/Update/Delete/Commit/Abort 记录
5. 中止所有未提交事务（崩溃语义）
6. 重建二级索引
7. 通过 `TxnManager::advance_counters_past()` 推进事务计数器

### 4.6 查询取消实现

```
客户端 ──CancelRequest(pid, secret)──▶ 服务端（新 TCP 连接）
服务端: cancel_registry[pid].cancelled.store(true)
查询循环: 每 50ms 通过 tokio::select! 轮询 cancelled 标志
```

- 使用 `AtomicBool` 轮询（非 `tokio::CancellationToken`），因查询执行器运行在 `spawn_blocking`（同步上下文）中
- 取消延迟 ≤ 50ms

---

## 5. 核心 Trait 草图

```rust
// StorageTable — 磁盘引擎的核心 DML 契约
pub trait StorageTable: Send + Sync {
    fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError>;
    fn update(&self, pk: &PrimaryKey, new_row: &OwnedRow, txn_id: TxnId) -> Result<(), StorageError>;
    fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError>;
    fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Result<Option<OwnedRow>, StorageError>;
    fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)>;
    fn commit_key(&self, pk: &PrimaryKey, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError>;
    fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId);
    // USTM 预取（默认空操作；LsmTable 覆盖）
    fn prefetch_hint(&self, _table_id: TableId, _pk: &PrimaryKey, _ustm: &UstmEngine) {}
}

// TxnManager — 事务生命周期
#[async_trait]
pub trait TxnManager: Send + Sync + 'static {
    async fn begin(&self, isolation: IsolationLevel) -> Result<TxnHandle>;
    async fn commit(&self, txn: TxnHandle) -> Result<()>;
    async fn abort(&self, txn: TxnHandle) -> Result<()>;
}

// Consensus — 可插拔共识
#[async_trait]
pub trait Consensus: Send + Sync + 'static {
    async fn propose(&self, shard: ShardId, entry: LogEntry) -> Result<()>;
    async fn add_member(&self, shard: ShardId, node: NodeId) -> Result<()>;
    async fn remove_member(&self, shard: ShardId, node: NodeId) -> Result<()>;
}
```

---

## 6. 目录结构

```
falcondb/
├── Cargo.toml                   # workspace 根
├── README.md                    # 用户指南（构建、集群、配置、提升、基准、指标）
├── README_CN.md                 # 中文用户指南
├── ARCHITECTURE.md              # 英文架构文档
├── ARCHITECTURE_zh.md           # 本文件
├── crates/
│   ├── falcon_common/           # 共享类型、错误、配置、datum、schema、RLS、RBAC
│   ├── falcon_storage/          # 表、LSM/RocksDB/redb 引擎、索引、WAL、GC、TDE、USTM、PITR、CDC
│   ├── falcon_txn/              # 事务管理、MVCC、OCC、统计
│   ├── falcon_sql_frontend/     # 解析器、绑定器、分析器
│   ├── falcon_planner/          # 逻辑 + 物理规划、路由提示
│   ├── falcon_executor/         # 算子执行、表达式求值、调控器
│   ├── falcon_protocol_pg/      # PG 网络协议、SHOW 命令处理
│   ├── falcon_protocol_native/  # 原生二进制协议编解码、压缩、类型映射
│   ├── falcon_native_server/    # 原生协议服务器、会话、执行器桥接
│   ├── falcon_raft/             # Raft 共识（通过 raft_member 角色走生产路径）
│   ├── falcon_proto/            # Protobuf 定义 + tonic 代码生成
│   ├── falcon_cluster/          # 复制、故障切换、分散/聚集、轮次、迁移、监督器
│   ├── falcon_observability/    # 指标、追踪、日志设置
│   ├── falcon_segment_codec/    # Zstd/LZ4 段压缩、字典、解压缩池
│   ├── falcon_enterprise/       # 控制平面 HA、企业安全、运维自动化
│   ├── falcon_server/           # 主二进制 + 集成测试
│   ├── falcon_cli/              # 交互式 CLI、集群管理、导入/导出
│   └── falcon_bench/            # YCSB 基准测试框架
├── clients/
│   ├── falcondb-jdbc/           # Java JDBC 驱动（Maven 项目，HA 故障切换）
│   ├── falcondb-go/             # Go 客户端（原生协议、HA 种子列表、连接池）
│   ├── falcondb-python/         # Python 客户端（DB-API 2.0、HA 故障切换、连接池）
│   └── falcondb-node/           # Node.js 客户端（异步、TLS、HA 故障切换、TypeScript 类型）
├── docs/                        # 路线图、RPO/RTO、协议兼容性、原生协议规范
├── examples/                    # primary.toml, replica.toml
├── benchmarks/                  # 基准测试结果与配置
├── pocs/                        # 一键可运行的验证 PoC
└── scripts/                     # 演示、故障切换、基准测试、CI 门控、Windows 配置
```

---

## 7. 错误与异步策略

### 7.1 错误策略

- 库 crate：`thiserror` 枚举（`StorageError`, `TxnError`, `ProtocolError` 等）
- 所有错误通过 `From` impl 汇总到 `common` 中的 `FalconError`
- 二进制（`main.rs`）：`anyhow` 用于顶层组装
- PG 错误映射：`FalconError → PgErrorResponse`（SQLSTATE 码）

### 7.2 异步策略

- 运行时：`tokio` 多线程
- 所有 I/O（网络、WAL fsync）异步
- CPU 密集工作（解析、规划）：同步，仅在可测量慢时包装在 `spawn_blocking` 中
- 复制流：由 `tokio` 任务管理的长生命周期 gRPC 流

---

## 8. 性能目标与测试

### 8.1 性能目标

| 指标 | 目标（单节点，内存，无复制） |
|------|--------------------------|
| 主键点读 QPS | ≥ 200K |
| 点读 P50 / P99 | < 0.2ms / < 1ms |
| 点写 QPS | ≥ 100K |
| 点写 P50 / P99 | < 0.5ms / < 2ms |
| 混合（50/50 读写）QPS | ≥ 120K |
| 每 100 万行内存（8 列，均值 100B） | < 500MB |

| 指标 | 目标（3 节点，WAL 复制） |
|------|----------------------|
| 写 QPS（单分片） | ≥ 50K |
| 写 P99 | < 5ms |
| 线性扩展系数（3→9 节点，3x 分片） | ≥ 2.5x |

### 8.2 测试覆盖（18 个 crate，4,200+ 测试）

| Crate | 测试数 | 覆盖范围 |
|-------|-------|---------|
| `falcon_cluster` | 1,050+ | 复制、故障切换、分散/聚集、GC、准入控制、集群运维、熔断器、2PC、令牌桶、轮次、Leader 租约、迁移、节流、监督器、控制平面、企业安全、企业运维、确定性加固 |
| `falcon_storage` | 820+ | MVCC、WAL、GC、索引、OCC、SI 石蕊测试、DDL 并发、恢复、checkpoint、二级索引、写集、WAL 观察者、USTM、LSM、RocksDB、redb |
| `falcon_server`（集成） | 420+ | 完整 SQL 端到端、配置迁移、服务管理、CLI 子命令 |
| `falcon_common` | 250+ | 配置验证、节点角色、datum 类型（含 Decimal）、schema、错误层次、RBAC、RLS、安全性、一致性 |
| `falcon_protocol_pg` | 240+ | SHOW 命令、错误路径、事务生命周期、语句超时、PgServer 配置、空闲超时、information_schema、视图、计划缓存 |
| `falcon_executor` | 200+ | 算子执行、表达式求值、窗口函数、子查询、调控器 v2、优先级调度器、RBAC 强制、溢出指标 |
| `falcon_sql_frontend` | 150+ | 解析、绑定、分析、视图展开、ALTER TABLE 重命名、谓词规范化、参数推断 |
| `falcon_txn` | 100+ | 事务生命周期、OCC、统计、历史、死锁检测、READ ONLY 模式、超时、执行摘要 |
| `falcon_planner` | 90+ | 计划生成、路由提示、分布式包装、视图/DDL 计划、代价模型 |
| **合计** | **4,200+**（通过） | 417 个 `.rs` 文件，18 个 crate，`cargo clippy --workspace`: 0 warnings |

### 8.3 测试分层

| 层级 | 范围 | 工具 |
|------|------|------|
| **单元** | 解析器、执行器算子、MVCC 可见性、GC 链、OCC 验证 | `#[test]` |
| **集成** | 完整 SQL 流水线：DDL、DML、JOIN、子查询、聚合、窗口函数 | `falcon_server/tests/integration_test.rs` |
| **集群** | 复制、故障切换、提升、分散/聚集、GC+复制综合 | `falcon_cluster/src/tests.rs` |
| **处理器** | SHOW 命令（gc_stats、replication_stats、txn_stats、txn_history） | `falcon_protocol_pg/src/handler.rs` |
| **基准** | YCSB 工作负载、快/慢速路径、横向扩展、故障切换影响 | `falcon_bench` |
| **数据完整性** | 崩溃恢复、WAL 重放、GC+提升综合正确性 | 自动化测试 |

---

## 9. 里程碑

| 阶段 | 范围 | 状态 |
|------|------|------|
| **M1** | 稳定 OLTP、快/慢速路径事务、WAL 复制、故障切换、GC、基准测试 | **完成** |
| **M2** | gRPC WAL 流式传输（tonic）、多节点部署、WAL 观察者、副本应用 | **完成** |
| **M3** | 生产加固：只读副本、优雅关闭、健康检查、查询超时、连接限制 | **完成** |
| **M4** | 查询优化（哈希 JOIN、CBO、表统计）、PG 协议加固（预处理语句）、EXPLAIN ANALYZE、慢查询日志、快照 checkpoint | **完成** |
| **M5** | 目录查询（information_schema）、视图、ALTER TABLE RENAME、查询计划缓存 | **完成** |
| **M6** | JSONB 类型、关联子查询、FK 级联操作、ALTER COLUMN TYPE、递归 CTE | **完成** |
| **M7** | 序列函数（CREATE/DROP SEQUENCE、nextval/currval/setval、SERIAL）、窗口函数 | **完成** |
| **M8** | DATE 类型（字面量、算术、CURRENT_DATE、EXTRACT、DATE_TRUNC 等）、COPY 命令（FROM STDIN、TO STDOUT、CSV/文本格式） | **完成** |

---

## 10. 开源依赖矩阵

| 模块 | 依赖 | 许可证 | 用途 |
|------|------|--------|------|
| **PG Wire** | `pgwire` 0.25+ | MIT | 完整 PG 消息编解码、认证、TLS |
| **SQL 解析器** | `sqlparser-rs` 0.50+ | Apache-2.0 | 经过验证的 PG 方言，活跃社区 |
| **Raft** | `openraft` 0.10+ | MIT | 纯逻辑 Raft，无运行时耦合，异步原生 |
| **异步运行时** | `tokio` 1.x | MIT | 事实标准；sync 边界通过 `spawn_blocking` |
| **RPC** | `tonic`（gRPC） | MIT | 成熟，代码生成，流式，HTTP/2 |
| **序列化** | `prost` + `bincode` | Apache-2.0 / MIT | Protobuf 用于 RPC；bincode 用于内部快速序列化 |
| **指标** | `metrics` + `metrics-exporter-prometheus` | MIT | 轻量，Prometheus 原生 |
| **追踪** | `tracing` + `tracing-subscriber` + `tracing-opentelemetry` | MIT | 标准 Rust 追踪生态 |
| **并发数据结构** | `crossbeam`（epoch GC、队列）+ `dashmap` | MIT | 经过生产验证的并发数据结构 |
| **内存分配** | `tikv-jemallocator` | MIT | Jemalloc，可预测性能，更少碎片 |
| **配置** | `serde` + `toml` | MIT | 标准配置解析 |
| **错误** | `thiserror`（库）+ `anyhow`（二进制） | MIT | 库中类型化错误，二进制中人体工程学 |
| **CLI** | `clap` | MIT | 服务器二进制参数解析 |

**Fork 策略**：不 Fork 任何依赖。所有依赖均使用已发布的 crate。每个边界的抽象 trait 确保未来可替换而无需 Fork。
