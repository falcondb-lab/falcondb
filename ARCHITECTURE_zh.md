# FalconDB — 兼容 PG 协议的分布式内存 OLTP 数据库

> **[English](ARCHITECTURE.md)** | 简体中文 | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. 核心特性：确定性提交保证 (DCG)

> **如果 FalconDB 返回 "committed"，该事务将在任何单节点崩溃、任何故障转移以及任何恢复过程中幸存——零例外。**

**确定性提交保证**由提交点模型（`falcon_common::consistency` 中的 §1）强制执行：在活动提交策略下，WAL 持久化之前绝不会发送客户端 ACK。四个提交阶段——**WAL 已记录 → WAL 已持久化 → 可见 → 已确认**——严格单向推进；任何回退尝试都会被拒绝为 `InvariantViolation`。

- 证据：[docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- 时序图：[docs/commit_sequence.md](docs/commit_sequence.md)
- 非目标：[docs/non_goals.md](docs/non_goals.md)

## 1. 架构概览

### 1.1 数据流（正常路径：SELECT）

```
psql ──TCP──▶ [协议层 (PG Wire)]
                  │
                  ▼
            [SQL 前端: Parse → Bind → Analyze]
                  │
                  ▼
            [计划器 / 路由器]
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
    [WAL / 检查点]
```

### 1.2 数据流（写路径）

```
客户端 INSERT ──▶ PG Wire ──▶ SQL 前端 ──▶ 计划器/路由器
    ──▶ 开始事务 ──▶ 分片定位（基于 PK 哈希）
    ──▶ LocalTxn（快速路径，单分片，无 2PC）
        │  或 GlobalTxn（慢速路径，多分片，XA-2PC）
    ──▶ 提交 ──▶ 应用到 MemTable + WAL fsync
    ──▶ WAL 发送到副本（异步，方案 A）
    ──▶ 确认客户端
```

### 1.3 节点角色（M1：主节点 / 副本）

M1 对每个分片采用主-副本拓扑：
- **主节点**：接受读写；WAL 持久化后提交确认。
- **副本**：接收 WAL 块；在提升之前为只读。
- **提升**：基于 fencing 的故障转移（fence → 追赶 → 切换 → unfence）。

未来（M2+）：拆分为专用的计算 / 存储 / 元数据角色。

---

## 2. 模块清单

### 2.1 `protocol_pg` — PostgreSQL Wire 协议

| 项目 | 详情 |
|------|------|
| **职责** | 接受 TCP 连接，处理 PG 启动/认证握手，解析 Simple Query / Extended Query 消息，序列化 RowDescription/DataRow/CommandComplete/Error 响应 |
| **输入** | 原始 TCP 字节流 |
| **输出** | 解析后的 `Statement` + 会话上下文；序列化的 PG 响应帧 |
| **核心类型** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **依赖** | `common`（类型、错误） |
| **可替换** | 整个 crate 可以替换为 MySQL 协议或 HTTP/gRPC 网关 |
| **开源复用** | `pgwire` crate（MIT）用于消息编解码；自定义会话管理 |

### 2.2 `sql_frontend` — 解析器 / 分析器 / 绑定器

| 项目 | 详情 |
|------|------|
| **职责** | SQL 文本 → AST → 带目录引用的已解析逻辑计划 |
| **输入** | SQL 字符串 + 目录快照 |
| **输出** | `BoundStatement`（有类型、已解析的 AST） |
| **核心类型** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **依赖** | `common`（类型、目录 trait） |
| **可替换** | 解析器后端（sqlparser-rs vs 自定义 PEG），方言配置 |
| **开源复用** | `sqlparser-rs`（Apache-2.0）用于解析；自定义绑定器/分析器 |

### 2.3 `planner_router` — 查询计划器 & 分布式路由器

| 项目 | 详情 |
|------|------|
| **职责** | 逻辑计划 → 物理计划；确定分片目标；下推过滤器 |
| **输入** | `BoundStatement` + 分片映射 |
| **输出** | `PhysicalPlan`（带分片注解的算子树） |
| **核心类型** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |
| **依赖** | `sql_frontend`, `cluster_meta`, `common` |
| **可替换** | 优化规则、成本模型、路由策略 |

### 2.4 `executor` — 执行引擎

| 项目 | 详情 |
|------|------|
| **职责** | 执行物理计划算子，产生结果行；强制每事务 READ ONLY 和超时保护；带结构化中止原因的查询治理器；大表扫描的融合流式聚合 |
| **输入** | `PhysicalPlan` + 事务句柄 |
| **输出** | `RowStream`（`Row` 的迭代器） |
| **核心类型** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `Insert`, `Update`, `Delete`, `QueryGovernor`, `GovernorAbortReason`, `ProjAccum`, `exec_fused_aggregate` |
| **快速路径** | `exec_fused_aggregate` — WHERE+GROUP BY+聚合单遍 MVCC 链（零行拷贝）；`try_streaming_aggs` — 通过 `compute_simple_aggs` 的简单列引用聚合；`try_pk_ordered_limit` — 通过 `scan_top_k_by_pk` 的有界堆 top-K |
| **依赖** | `storage`（通过 trait）, `txn`, `common` |
| **可替换** | 逐行 ↔ 向量化；推 ↔ 拉模型 |

### 2.5 `txn` — 事务管理器

| 项目 | 详情 |
|------|------|
| **职责** | 开始/提交/中止事务；MVCC 时间戳分配；OCC 验证（SI）；快速路径（LocalTxn）和慢速路径（GlobalTxn）提交；延迟直方图；事务历史环形缓冲区；每事务 READ ONLY 模式、超时和执行摘要 |
| **输入** | 来自执行器的事务操作 |
| **输出** | 事务句柄、提交/中止决策、`TxnStatsSnapshot`、`TxnRecord` 历史 |
| **核心类型** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel`, `TxnType`, `TxnPath`, `TxnContext`, `TxnRecord`, `TxnOutcome`, `LatencyStats`, `TxnExecSummary` |
| **依赖** | `storage`（版本可见性、OCC 读集）, `common` |
| **可替换** | OCC vs 2PL vs 混合；时间戳 oracle（本地 vs 分布式） |

### 2.6 `storage` — 内存存储引擎

| 项目 | 详情 |
|------|------|
| **职责** | 在内存中存储元组及 MVCC 版本；维护索引（哈希 PK + BTree 二级索引 + 唯一索引 + 复合/覆盖/前缀索引）；WAL 写入 + 恢复；MVCC 垃圾回收；复制统计；LSM 磁盘引擎；零拷贝行迭代；流式聚合计算 |
| **输入** | 带事务上下文的 Get/Put/Delete/Scan |
| **输出** | 元组（给定事务的可见版本） |
| **核心类型** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `WalRecord`, `GcConfig`, `GcStats`, `GcRunner`, `ReplicationStats`, `LsmEngine` |
| **MVCC 快速路径** | `with_visible_data` — 基于闭包的零拷贝行访问；`for_each_visible` — 无 Vec 分配的 DashMap 流式迭代；`scan_top_k_by_pk` — O(N log K) 的有界 `BinaryHeap` top-K；`compute_simple_aggs` — 单遍 COUNT/SUM/MIN/MAX；所有可见性检查中 head 版本的 Arc-clone 消除 |
| **依赖** | `common` |
| **可替换** | 整个引擎（RocksDB 适配器、FoundationDB 适配器）；索引实现（BTree、SkipList、ART） |

### 2.7 `raft` — 共识（桩——不在生产路径上）

> **⚠️ 此 crate 是单节点桩。它不用于任何生产数据路径。**
> FalconDB 的复制是基于 gRPC 的 WAL 传输（参见 §5.5）。Raft 只是未来探索的占位符。

| 项目 | 详情 |
|------|------|
| **职责** | 共识 trait 定义 + 单节点无操作桩 |
| **生产状态** | **未使用。**所有 RPC 返回 `Unreachable("single-node mode")` |
| **核心类型** | `RaftNode`（桩）, `LogEntry`, `StateMachine` trait |
| **依赖** | `common` |
| **未来** | 可能在 ≥3 节点部署中实现自动 leader 选举；未排期 |

### 2.8 `cluster` — 集群、复制与分布式执行

| 项目 | 详情 |
|------|------|
| **职责** | 分片映射；基于 WAL 的主-副本复制（`WalChunk`, `ReplicationTransport`）；带 fencing 的提升/故障转移；scatter/gather 分布式执行；DDL 协调；准入控制（读/写/DDL 许可）；节点运行模式（Normal/ReadOnly/Drain） |
| **输入** | DDL 请求、DML 路由、WAL 块、提升命令 |
| **输出** | 已复制状态、分片路由查询结果、故障转移指标 |
| **核心类型** | `ShardMap`, `ShardReplicaGroup`, `WalChunk`, `ReplicationTransport`, `InProcessTransport`, `ChannelTransport`, `ReplicationMetrics`, `DistributedQueryEngine`, `FailurePolicy`, `GatherLimits`, `AdmissionControl`, `NodeModeController`, `NodeOperationalMode` |
| **依赖** | `storage`, `txn`, `common` |
| **可替换** | 传输层（进程内 → M2 中的 gRPC）；元数据存储（嵌入式 vs etcd） |

### 2.9 `observability` — 指标 / 追踪 / 日志

| 项目 | 详情 |
|------|------|
| **职责** | 发出指标（Prometheus）、结构化追踪（OpenTelemetry）、结构化日志 |
| **输入** | 所有层的埋点 |
| **输出** | Prometheus 端点、OTLP 导出、日志文件/stdout |
| **核心类型** | Metric 宏、`TraceContext`、日志宏 |
| **依赖** | `common` |
| **开源复用** | `metrics` + `metrics-exporter-prometheus`, `tracing` + `tracing-opentelemetry`, `tracing-subscriber` |

### 2.10 `common` — 共享基础设施

| 项目 | 详情 |
|------|------|
| **职责** | 共享类型、错误层级、配置、编解码、PG 类型系统、datum 表示（含 `Decimal`）、RBAC（角色、权限、审计） |
| **核心类型** | `Datum`, `DataType`, `Row`, `OwnedRow`, `TableId`, `ShardId`, `NodeId`, `FalconError`, `Config`, `RoleCatalog`, `PrivilegeManager`, `Role`, `Privilege` |
| **依赖** | 无（叶子 crate） |

---

## 3. MVP 范围

### 3.1 已支持

| 类别 | 范围 |
|------|------|
| **协议** | PG wire Simple Query + Extended Query（Parse/Bind/Describe/Execute/Sync/Close/Flush）；trust 认证；文本格式；系统目录 shim（version(), SET, SHOW, \dt） |
| **SQL DDL** | `CREATE TABLE [IF NOT EXISTS]`，`SERIAL`/`BIGSERIAL` 自增列，`DROP TABLE [IF EXISTS]`，`TRUNCATE TABLE`，`ALTER TABLE ADD/DROP/RENAME COLUMN`，`ALTER TABLE RENAME TO`，`ALTER COLUMN TYPE`，`CREATE INDEX`，`CREATE VIEW [OR REPLACE]`，`DROP VIEW [IF EXISTS]` |
| **SQL DML** | `INSERT`（全部/部分命名列），`INSERT ... SELECT`，`INSERT ... ON CONFLICT`（upsert），`UPDATE`，`DELETE`（均支持 `RETURNING`），`SELECT`，`WHERE`，`ORDER BY`，`LIMIT`，`OFFSET`，`DISTINCT`，`GROUP BY`，`HAVING`，聚合函数（COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND, BOOL_OR, ARRAY_AGG），`LIKE`，`ILIKE`，`BETWEEN`，`IN`，`IS NULL`/`IS NOT NULL`，`CAST`，`JOIN`（INNER, LEFT, RIGHT, CROSS, FULL OUTER），子查询，`CASE WHEN`/`COALESCE`/`NULLIF`，CTE，递归 CTE，关联子查询，`UNION`/`INTERSECT`/`EXCEPT`，`EXPLAIN`，窗口函数，260+ 标量函数 |
| **类型** | `INT`, `SMALLINT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`/`REAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE`；`GENERATE_SERIES` 表值函数 |
| **事务** | `BEGIN`, `COMMIT`, `ROLLBACK`；Read Committed 隔离级别 |
| **存储** | 内存行存储；PK 哈希索引；BTreeMap 二级索引；`CREATE INDEX` 含回填 |
| **持久化** | WAL（分段 64MB，组提交，清除）；崩溃恢复 |
| **分布式** | PK 哈希分片；基于 gRPC WAL 传输的复制；单分片快速路径 + 跨分片 2PC 慢速路径 |
| **可观测性** | Prometheus 指标端点；结构化日志；基本 trace ID |
| **管理** | 健康检查端点；节点列表；分片映射查询 |

### 3.2 不支持（M1）

- 逻辑复制、CDC
- 在线 schema 变更
- 多租户
- 自动再平衡（仅手动）
- 自定义类型（JSONB 以外）

---

## 4. 开源复用矩阵

| 模块 | 候选 | 许可证 | 原因 | 风险 | 适配策略 |
|------|------|--------|------|------|----------|
| **PG Wire** | `pgwire` 0.25+ | MIT | 完整 PG 消息编解码，认证，TLS 就绪 | API 变动 | 封装在 `PgCodec` trait 中；如替换仅需改适配器 |
| **SQL 解析器** | `sqlparser-rs` 0.50+ | Apache-2.0 | 久经考验的 PG 方言，活跃社区 | 缺少部分 PG 语法 | 通过 `Parser::parse_sql(PostgreSqlDialect)` 直接使用；按需扩展 AST |
| **Raft** | `openraft` 0.10+ | MIT | 纯逻辑 Raft，无运行时耦合，async 原生 | 复杂泛型 | **仅桩——不在生产路径上。** 单节点无操作。为未来 ≥3 节点自动 leader 选举预留 |
| **异步运行时** | `tokio` 1.x | MIT | 事实标准；后续可通过 tokio-uring 支持 io_uring | 锁定 | 所有异步代码使用 `tokio`；同步边界通过 `spawn_blocking` |
| **RPC** | `tonic` (gRPC) | MIT | 成熟，代码生成，流式，HTTP/2 | 内部消息开销 | 定义 `Transport` trait；MVP 使用 tonic；后续可换自定义 TCP |
| **序列化** | `prost` + `bincode` | Apache-2.0 / MIT | Protobuf 用于 RPC；bincode 用于内部快速序列化 | — | `Codec` trait 抽象序列化 |
| **指标** | `metrics` + `metrics-exporter-prometheus` | MIT | 轻量，Prometheus 原生 | — | 直接使用 `metrics` 宏；exporter 可替换 |
| **追踪** | `tracing` + `tracing-subscriber` + `tracing-opentelemetry` | MIT | 标准 Rust 追踪生态 | — | 各处使用 `tracing` 宏；启动时配置 subscriber |
| **并发结构** | `crossbeam`（epoch GC, 队列）+ `dashmap` | MIT / MIT | 生产验证的并发数据结构 | — | 封装在存储内部类型中 |
| **内存分配器** | `tikv-jemallocator` | MIT | Jemalloc 带来可预测性能、更少碎片 | 平台 | Feature-gated；回退到系统分配器 |
| **BTree 索引** | `crossbeam-skiplist` 或自定义 | MIT | 并发有序索引 | 范围扫描性能有限 | 在 `Index` trait 后面；后续可换 ART/Bw-Tree |
| **WAL** | 自定义（薄层） | — | 简单追加日志；无需复杂库 | 必须正确处理 fsync | `WalWriter` trait；可换 `wal` crate 或 io_uring 后端 |
| **配置** | `serde` + `toml` | MIT | 标准配置解析 | — | — |
| **错误** | `thiserror`（lib）+ `anyhow`（bin） | MIT | 库中类型化错误，二进制中人性化 | — | `FalconError` 枚举使用 `thiserror` |
| **CLI** | `clap` | MIT | 服务器二进制参数解析 | — | — |

**Fork 政策**：不 fork。所有依赖使用已发布的 crate。每个边界的抽象 trait 确保未来可以替换而无需 fork。

---

## 5. 事务与一致性设计

### 5.1 设计替代方案考量（历史，未实现）

> 以下两条路线在设计阶段进行了评估，但**均未实现**。
> 保留在此仅作架构背景参考。

- **路线 A（每分片 Raft + 2PC）**：每个分片作为 Raft 组 → 线性化写入，跨分片 2PC。拒绝：2PC 延迟开销，复杂的协调器故障处理。
- **路线 B（每分片 Raft + 事务协调器）**：每分片 Raft，协调器带时间戳和并行 prepare。拒绝：工程成本显著，对当前部署模型来说相对于 WAL 传输只有边际收益。

### 5.2 已实现：快速路径 + 慢速路径 + OCC

- **LocalTxn（快速路径）**：单分片事务在快照隔离下通过 OCC 验证提交。无 2PC 开销。`TxnContext.txn_path = Fast`。
- **GlobalTxn（慢速路径）**：跨分片事务使用 XA-2PC（prepare → commit）。`TxnContext.txn_path = Slow`。
- **硬不变量强制**：`TxnContext.validate_commit_invariants()` 返回 `InvariantViolation` 错误（非仅 debug_assert）。LocalTxn 必须有 1 个分片 + 快速路径；GlobalTxn 不得使用快速路径。
- **统一提交入口**：`StorageEngine::commit_txn(txn_id, commit_ts, txn_type)` 是唯一公共 API。原始 `commit_txn_local`/`commit_txn_global` 为 `pub(crate)`。
- **隔离级别**：Read Committed（默认）或 Snapshot Isolation（可配置）。SI 下提交时的 OCC 读集验证。
- **唯一约束强制**：提交时重新验证，违规时原子回滚。
- **自动分类**：计划器路由提示决定 Local vs Global；`observe_involved_shards()` 在需要时将 Local 升级为 Global。
- **跨分片读取**：在分片 leader 上 scatter-gather，可配置故障策略（Strict 或 BestEffort）和内存限制。

### 5.4 MVCC 垃圾回收

- **安全点**：`gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL 感知**：永不回收未提交（commit_ts==0）或已中止（commit_ts==MAX）的版本
- **复制安全**：尊重副本已应用的时间戳
- **每键无锁**：通过 DashMap 迭代进行每链修剪，无全局锁，无 stop-the-world
- **后台运行器**：`GcRunner` 线程，可配置间隔和批大小
- **可观测性**：`SHOW falcon.gc_stats` 公开安全点、已回收版本/字节、链统计

### 5.5 复制 — 基于 gRPC 的 WAL 传输

FalconDB 使用**WAL 传输复制**作为唯一的生产复制机制。

#### WAL 传输（生产路径）

```
主节点 ──WAL 块──▶ 副本
         (gRPC 流, tonic, SubscribeWal RPC)
```

- **传输**：`ReplicationTransport` trait → `InProcessTransport`（测试）→ `GrpcTransport`（生产，tonic gRPC）
- **线格式**：`WalChunk` 帧含 `start_lsn`、`end_lsn`、CRC32 校验和；JSON 序列化的 `WalRecord` 负载
- **提交确认（方案 A）**：主节点在 WAL 持久化（fsync）时提交。副本异步应用。
- **法定人数确认（方案 B）**：`SyncReplicationWaiter` 阻塞提交直到 N 个副本报告 `applied_lsn ≥ commit_lsn`。
- **引导**：新副本调用 `GetCheckpoint` RPC → 接收完整 `CheckpointData` 快照 → 切换到 WAL 流。
- **提升/故障转移**：5 步 fencing 协议（fence → 追赶 → 切换 → unfence → 更新分片映射）。
- **状态**：**已完全实现并测试**（9/9 `m2_multinode_e2e` 测试通过）。

> **⚠️ 硬边界**：`falcon_raft` crate 是单节点桩。它**不**用于任何生产数据路径。WAL 传输副本**不是** Raft followers。它们通过 `apply_wal_record_to_engine()` 直接将 WAL 记录应用到 `StorageEngine`。基于 Raft 的复制未排期且没有目标里程碑。

#### 取消请求实现

PG 取消请求使用 `AtomicBool` 轮询（非 `tokio::CancellationToken`）完全实现：

```
客户端 ──CancelRequest(pid, secret)──▶ 服务器（新 TCP 连接）
服务器: cancel_registry[pid].cancelled.store(true)
查询循环: 通过 tokio::select! 每 50ms 轮询 cancelled
```

- **为何使用 `AtomicBool` 而非 `CancellationToken`**：查询执行器运行在 `spawn_blocking`（同步上下文）中；`CancellationToken` 需要异步上下文。异步包装器中的 50ms 轮询循环实现等效行为。
- **延迟**：取消在客户端请求到达后 50ms 内生效。
- **扩展查询协议**：在每个 `Execute` 消息开始时和 `spawn_blocking` 返回后检查取消标志。

---

## 6. 仓库结构与核心 Trait

```
falcon/
├── Cargo.toml                  (工作区根)
├── README.md                   (用户指南：构建、集群、配置、提升、基准测试、指标)
├── ARCHITECTURE.md             (本文件)
├── .gitattributes              (LF 强制，.ps1/.bat 使用 CRLF)
├── .github/workflows/ci.yml    (CI: check, test, clippy, fmt, failover-gate, windows)
├── crates/
│   ├── falcon_common/           (共享类型、错误、配置、datum、schema、RLS、RBAC)
│   ├── falcon_storage/          (表、LSM 引擎、索引、WAL、GC、TDE、分区、PITR、CDC)
│   ├── falcon_txn/              (事务管理、MVCC、OCC、统计)
│   ├── falcon_sql_frontend/     (解析器、绑定器、分析器)
│   ├── falcon_planner/          (逻辑 + 物理计划、路由提示)
│   ├── falcon_executor/         (算子执行、表达式求值、治理器)
│   ├── falcon_protocol_pg/      (PG wire 协议、SHOW 命令处理)
│   ├── falcon_protocol_native/  (原生二进制协议编解码、压缩、类型映射)
│   ├── falcon_native_server/    (原生协议服务器、会话、执行器桥接、nonce)
│   ├── falcon_raft/             (共识桩——不在生产路径上)
│   ├── falcon_proto/            (protobuf 定义 + tonic 代码生成)
│   ├── falcon_cluster/          (复制、故障转移、scatter/gather、epoch、迁移、supervisor)
│   ├── falcon_observability/    (指标、追踪、日志设置)
│   ├── falcon_server/           (主二进制 + 集成测试)
│   └── falcon_bench/            (YCSB 基准测试框架)
├── clients/
│   └── falcondb-jdbc/           (Java JDBC 驱动 — Maven 项目，HA 故障转移)
├── tools/
│   └── native-proto-spec/       (跨语言验证的 golden 测试向量)
├── bench_configs/              (M1 冻结基准测试配置)
├── docs/                       (路线图、RPO/RTO、协议兼容性、原生协议规范)
├── examples/                   (primary.toml, replica.toml)
└── scripts/                    (演示、故障转移、基准测试、CI 门禁、Windows 设置)
```

### 6.1 核心 Trait 草图

```rust
// StorageEngine — 可插拔存储后端
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    async fn get(&self, table: TableId, key: &[u8], txn: &TxnContext) -> Result<Option<OwnedRow>>;
    async fn scan(&self, table: TableId, range: ScanRange, txn: &TxnContext) -> Result<RowStream>;
    async fn put(&self, table: TableId, key: &[u8], row: OwnedRow, txn: &TxnContext) -> Result<()>;
    async fn delete(&self, table: TableId, key: &[u8], txn: &TxnContext) -> Result<()>;
    async fn create_table(&self, meta: TableMeta) -> Result<()>;
    async fn drop_table(&self, table: TableId) -> Result<()>;
}

// TxnManager — 事务生命周期
#[async_trait]
pub trait TxnManager: Send + Sync + 'static {
    async fn begin(&self, isolation: IsolationLevel) -> Result<TxnHandle>;
    async fn commit(&self, txn: TxnHandle) -> Result<()>;
    async fn abort(&self, txn: TxnHandle) -> Result<()>;
}

// Consensus — 可插拔共识（仅桩——不在生产路径上）
#[async_trait]
pub trait Consensus: Send + Sync + 'static {
    async fn propose(&self, shard: ShardId, entry: LogEntry) -> Result<()>;
    async fn add_member(&self, shard: ShardId, node: NodeId) -> Result<()>;
    async fn remove_member(&self, shard: ShardId, node: NodeId) -> Result<()>;
}

// Executor
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn execute(&self, plan: PhysicalPlan, txn: &TxnHandle) -> Result<ExecutionResult>;
}
```

### 6.2 错误策略

- 库 crate：`thiserror` 枚举（`StorageError`, `TxnError`, `ProtocolError` 等）
- 全部通过 `From` impl 汇集到 `common` 中的 `FalconError`
- 二进制（`main.rs`）：`anyhow` 用于顶层组装
- PG 错误映射：`FalconError → PgErrorResponse`（SQLSTATE 代码）

### 6.3 异步策略

- 运行时：`tokio` 多线程
- 所有 I/O（网络、WAL fsync）为异步
- CPU 密集型工作（解析、计划）：同步，仅在可测量到慢时用 `spawn_blocking` 包装
- 复制流：由 `tokio` 任务管理的长连接 gRPC 流

---

## 7. 性能目标与基准测试

### 7.1 MVP 目标（单节点、内存、无复制）

| 指标 | 目标 |
|------|------|
| 点读（PK 查询）QPS | ≥ 200K |
| 点读 P50 / P99 | < 0.2ms / < 1ms |
| 点写 QPS | ≥ 100K |
| 点写 P50 / P99 | < 0.5ms / < 2ms |
| 混合（50/50 读/写）QPS | ≥ 120K |
| 每 1M 行内存（8 列，平均 100B） | < 500MB |

### 7.2 集群目标（3 节点、WAL 复制）

| 指标 | 目标 |
|------|------|
| 写 QPS（单分片） | ≥ 50K |
| 写 P99 | < 5ms |
| 线性扩展因子（3→9 节点，3x 分片） | ≥ 2.5x |

### 7.3 基准测试计划

- **YCSB**：工作负载 A（50/50）、B（95/5）、C（100% 读）、F（读-修改-写）— MVP 使用 A+C
- **TPC-C**：延后到 P3（需要 JOIN）
- **自定义微基准测试**：PG 协议吞吐量、复制延迟、WAL fsync 延迟

### 7.4 热点处理（MVP）

- 检测：每分片 QPS 计数器；任一分片 > 3x 平均值时告警
- 缓解：手动分片分裂命令；自动延后到 P3

---

## 8. 测试、验证与里程碑

### 8.1 测试策略

| 级别 | 范围 | 工具 |
|------|------|------|
| **单元** | 解析器、执行算子、MVCC 可见性、GC 链、OCC 验证 | `#[test]` |
| **集成** | 完整 SQL 流水线：DDL、DML、连接、子查询、聚合、窗口函数 | `falcon_server/tests/integration_test.rs` |
| **集群** | 复制、故障转移、提升、scatter/gather、GC+复制组合 | `falcon_cluster/src/tests.rs` |
| **处理器** | SHOW 命令（gc_stats, replication_stats, txn_stats, txn_history） | `falcon_protocol_pg/src/handler.rs` |
| **基准** | YCSB 工作负载，快/慢路径，扩展，故障转移影响 | `falcon_bench` |
| **数据完整性** | 崩溃恢复、WAL 重放、GC+提升组合正确性 | 自动化测试 |

### 8.2 测试计数（3,654 个测试）

| Crate | 测试数 | 覆盖 |
|-------|--------|------|
| `falcon_cluster` | 1,057 | 复制、故障转移、scatter/gather、GC、准入、集群操作、熔断器、2PC、令牌桶、epoch、leader 租约、迁移、限流、supervisor、控制平面、企业安全、企业运维、确定性加固 |
| `falcon_storage` | 776 | MVCC、WAL、GC、索引、OCC、SI litmus、DDL 并发、恢复、检查点、二级索引、写集、WAL 观察者、列存、磁盘行存 |
| `falcon_server`（集成） | 421 | 完整 SQL 端到端、配置迁移、服务管理、CLI 子命令 |
| `falcon_common` | 254 | 配置验证、节点角色、datum 类型（含 Decimal）、schema、错误层级、RBAC、RLS、安全、一致性 |
| `falcon_protocol_pg` | 240 | SHOW 命令、错误路径、事务生命周期、语句超时、PgServer 配置、空闲超时、information_schema、视图、计划缓存 |
| `falcon_cli` | 201 | SQL 分割、CSV 解析、元命令解析、计时、历史、补全、集群/节点/事务/故障转移/策略命令、导出/导入 |
| `falcon_executor` | 192 | 算子执行、表达式求值、窗口函数、子查询、治理器 v2、优先级调度器、RBAC 强制 |
| `falcon_sql_frontend` | 149 | 解析、绑定、分析、视图展开、ALTER TABLE 重命名、谓词规范化、参数推断 |
| `falcon_txn` | 103 | 事务生命周期、OCC、统计、历史、死锁检测、READ ONLY 模式、超时、执行摘要 |
| `falcon_planner` | 89 | 计划生成、路由提示、分布式包装、视图/DDL 计划、成本模型 |
| `falcon_raft` | 53 | Raft 日志存储、状态机、propose/apply、快照、成员管理 |
| `falcon_segment_codec` | 53 | 段编解码、压缩、校验和 |
| `falcon_protocol_native` | 39 | 原生二进制协议编解码、压缩（LZ4）、类型映射、golden 向量 |
| `falcon_native_server` | 28 | 原生协议服务器、会话状态机、执行器桥接、nonce 防重放 |
| **总计** | **3,654**（通过） | `cargo clippy --workspace`: 0 警告 |

### 8.3 验收标准（M1）

1. ✅ `psql` 可以连接、运行 DDL、DML、事务
2. ✅ 基于 WAL 传输的主-副本复制
3. ✅ 可通过 fencing 协议执行提升/故障转移
4. ✅ txn_type/txn_path 不可绕过（硬不变量强制）
5. ✅ 通过 SHOW 命令可观测快速/慢速路径指标
6. ✅ 基准测试结果可复现（固定种子，CSV/JSON 导出）
7. ✅ README 可外部运行（6 个编号章节）
8. ✅ 崩溃恢复恢复所有已提交数据
9. ✅ MVCC GC 尊重复制安全和活动事务

### 8.4 里程碑

| 阶段 | 范围 | 状态 |
|------|------|------|
| **M1** | 稳定 OLTP，快/慢路径事务，WAL 复制，故障转移，GC，基准测试 | **已完成** |
| **M2** | gRPC WAL 流（tonic），多节点部署，WAL 观察者，副本应用 | 已完成 |
| **M3** | 生产加固：只读副本、优雅关机、健康检查、查询超时、连接限制 | **已完成** |
| **M4** | 查询优化（hash join、基于成本的计划器、表统计）、PG 协议加固（预处理语句）、EXPLAIN ANALYZE、慢查询日志、快照检查点 | **已完成** |
| **M5** | 目录查询（information_schema）、视图（CREATE/DROP/SELECT）、ALTER TABLE RENAME、查询计划缓存 | **已完成** |
| **M6** | JSONB 类型、关联子查询、FK 级联操作、ALTER COLUMN TYPE、递归 CTE | **已完成** |
| **M7** | 序列函数（CREATE/DROP SEQUENCE、nextval/currval/setval、SERIAL）、窗口函数 | **已完成** |
| **M8** | DATE 类型、COPY 命令 | **已完成** |
| **M1 收尾** | SQL 解析器/绑定器加固：参数化查询、谓词规范化、分片键推断 | **已完成** |

### 8.5 M2 架构 — gRPC WAL 流

```
主节点
┌───────────────────┐  WAL 观察者   ┌─────────────────┐  gRPC 服务  ┌──────────────────────┐
│  StorageEngine    │ ──回调──▶     │ ReplicationLog  │ ◀─read_from─ │ WalReplicationService│
│  (append_wal)     │               │  (append)       │              │   (tonic server)     │
└───────────────────┘               └─────────────────┘              └──────────┬───────────┘
                                                                               │
                                        SubscribeWal (gRPC stream)            │
                                        AckWal (gRPC unary)                   │
                                                                               │
副本节点                                                                       │
┌─────────────────┐   pull_wal_chunk   ┌──────────────────────┐                │
│  GrpcTransport  │ ──gRPC 调用──▶     │ WalReplicationClient │ ───────────────┘
│  (tonic client) │ ◀──WalChunk──     │   (generated)        │
└────────┬────────┘                    └──────────────────────┘
         │
         ▼
  apply_wal_record_to_engine()  ──▶  副本 StorageEngine
```

**Proto 服务**（`proto/falcon_replication.proto`）：

| RPC | 类型 | 用途 |
|-----|------|------|
| `SubscribeWal` | 服务端流 | 副本从 LSN 订阅；主节点推送 `WalChunkMessage` |
| `AckWal` | 一元 | 副本确认已应用 LSN 用于延迟追踪 |
| `GetCheckpoint` | 服务端流 | 新副本请求完整快照用于引导 |

**传输抽象**（`falcon_cluster::replication`）：

| Trait | 同步/异步 | 用途 |
|-------|-----------|------|
| `ReplicationTransport` | 同步 | 进程内测试（`InProcessTransport`, `ChannelTransport`） |
| `AsyncReplicationTransport` | 异步 | gRPC 网络传输（`GrpcTransport`）；同步上的 blanket impl |

**线格式**：`WalChunk` / `LsnWalRecord` 在 proto `record_payload` 字节字段中通过 serde 序列化为 JSON。Rust 类型是真实来源；proto 是信封。

**代码生成**：`cargo build -p falcon_cluster --features grpc-codegen`（需要 `protoc`）。生成代码到 `src/generated/`。

**M2 已交付**：
1. ✅ `protoc` v28.3 安装，tonic 代码生成产出 `falcon.replication.rs`
2. ✅ `GrpcTransport.pull_wal_chunk()` 连接到 tonic `WalReplicationClient`
3. ✅ `WalReplication` tonic server trait 在 `WalReplicationService` 上实现
4. ✅ `StorageEngine::set_wal_observer()` — 主节点所有 WAL 写入馈入 `ReplicationLog`
5. ✅ 副本拉取循环通过 `apply_wal_record_to_engine` 应用 WAL 记录
6. ✅ CLI 标志（`--role`, `--primary-endpoint`, `--grpc-addr`）+ 示例 TOML 配置
7. ✅ 端到端 gRPC 测试：tonic server + client over localhost

**M2 增强已交付**：
1. ✅ 真正的服务端流：`ReplicationLog::notify` 唤醒流任务；服务器保持 gRPC 流打开，WAL 追加时推送块
2. ✅ `GetCheckpoint` 实现：`snapshot_checkpoint_data()` + `CheckpointStreamer` 通过 gRPC 分块流传输
3. ✅ 连接缓存 + 指数退避：`GrpcTransport` 缓存 tonic `Channel`，副本循环使用可配置 `max_backoff_ms` 的指数退避
4. ✅ `subscribe_wal_stream()`：带自动断连重连的持久客户端流

---

## 9. 扩展标量函数

> **已移至 [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)** 以保持本文件可管理。
> 包含 260+ 数组矩阵和字符串编码函数规范。
