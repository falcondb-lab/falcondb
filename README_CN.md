# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center"><a href="README.md">English</a> | 简体中文</p>

<p align="center">
  <strong>兼容 PostgreSQL · 分布式 · 内存优先 · 确定性事务语义</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.3.0-blue" alt="Version" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="License" />
</p>

> FalconDB 是一个**兼容 PostgreSQL 协议的分布式内存优先 OLTP 数据库**，具备确定性事务语义。
> 已与 PostgreSQL、VoltDB、SingleStore 进行基准对比 — 详见 **[基准测试矩阵](benchmarks/README.md)**。
>
> - ✅ **低延迟** — 单分片快速路径提交完全绕过 2PC
> - ✅ **可证明的一致性** — 快照隔离下的 MVCC/OCC，CI 验证 ACID
> - ✅ **可运维** — 50+ SHOW 命令、Prometheus 指标、故障转移 CI 门控
> - ✅ **确定性** — 强化状态机、有界 in-doubt、幂等重试
> - ❌ 非 HTAP — 不支持分析型负载
> - ❌ 非完整 PG — [查看不支持列表](#not-supported)

FalconDB 提供快速/慢速路径事务的 OLTP、基于 WAL 的主-从复制（gRPC 流式）、
提升/故障转移、MVCC 垃圾回收以及可复现的基准测试。

### 确定性提交保证 (DCG)

> **如果 FalconDB 返回"已提交"，该事务将在任何单节点崩溃、
> 任何故障转移和任何恢复中存活 — 零例外。**

这是 FalconDB 的核心工程特性，称为**确定性提交保证 (DCG)**。
它不是配置选项 — 在 `LocalWalSync` 提交策略下这是默认行为。

- **亲自验证**: [falcondb-poc-dcg/](falcondb-poc-dcg/) — 一键演示：写入 1,000 订单 → kill -9 主节点 → 验证零数据丢失
- **基准对比**: [falcondb-poc-pgbench/](falcondb-poc-pgbench/) — 相同持久化设置下与 PostgreSQL 的 pgbench 对比
- **负载下崩溃**: [falcondb-poc-failover-under-load/](falcondb-poc-failover-under-load/) — 持续写入期间 kill -9 主节点 → 验证零数据丢失 + 自动恢复
- **内部观测**: [falcondb-poc-observability/](falcondb-poc-observability/) — 实时 Grafana 仪表盘、Prometheus 指标、运维控制
- **从 PG 迁移**: [falcondb-poc-migration/](falcondb-poc-migration/) — 仅更改连接字符串即可迁移 PostgreSQL 应用
- **灾难恢复**: [falcondb-poc-backup-pitr/](falcondb-poc-backup-pitr/) — 销毁数据库、从备份恢复、回放到精确秒、验证每行匹配

### 支持平台

| 平台 | 构建 | 测试 | 状态 |
|------|:----:|:----:|------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | 主要 CI 目标 |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | CI 目标 |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | 社区测试 |

**最低 Rust 版本**: **1.75** (`rust-version = "1.75"`)

### PG 协议兼容性

| 功能 | 状态 | 备注 |
|------|:----:|------|
| 简单查询协议 | ✅ | 单语句 + 多语句 |
| 扩展查询 (Parse/Bind/Execute) | ✅ | 预处理语句 + Portal |
| 认证: Trust | ✅ | 接受任意用户 |
| 认证: MD5 | ✅ | PG auth type 5 |
| 认证: SCRAM-SHA-256 | ✅ | PG 10+ 兼容 |
| 认证: 明文密码 | ✅ | PG auth type 3 |
| TLS/SSL | ✅ | SSLRequest → 配置后升级 |
| COPY IN/OUT | ✅ | Text 和 CSV 格式 |
| `psql` 12+ | ✅ | 完全测试 |
| `pgbench` (init + run) | ✅ | 内建脚本可用 |
| JDBC (pgjdbc 42.x) | ✅ | 测试 42.7+ |
| 取消请求 | ✅ | AtomicBool 轮询，50ms 延迟 |
| LISTEN/NOTIFY | ✅ | 内存广播中心 |
| 逻辑复制协议 | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### SQL 覆盖

| 类别 | 支持项 |
|------|--------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (含 ON CONFLICT, RETURNING, SELECT), UPDATE (含 FROM, RETURNING), DELETE (含 USING, RETURNING), COPY |
| **查询** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOIN (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL), 子查询 (标量/IN/EXISTS/关联), CTE (含 RECURSIVE), UNION/INTERSECT/EXCEPT, 窗口函数 |
| **聚合** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **类型** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL, 用户自定义 ENUM |
| **事务** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, 每事务超时, Read Committed, Snapshot Isolation |
| **函数** | 500+ 标量函数 (字符串, 数学, 日期/时间, 加密, JSON, 数组) |
| **全文搜索** | tsvector/tsquery 类型, `@@` 运算符, to_tsvector/to_tsquery/ts_rank/ts_headline 等 14 个 FTS 函数 |
| **可观测性** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE, pg_stat_statements |

### <a id="not-supported"></a>不支持 (v1.3)

以下功能**明确不在** v1.3 范围内。尝试使用会返回包含对应 SQLSTATE 代码的 `ErrorResponse`。

| 功能 | 错误码 | 错误信息 |
|------|--------|----------|
| ~~存储过程 / PL/pgSQL~~ | ✅ | **已实现** — `CREATE [OR REPLACE] FUNCTION ... LANGUAGE SQL/plpgsql`, `DROP FUNCTION`, `CALL`, PL/pgSQL: DECLARE, IF/ELSIF/ELSE, WHILE/FOR LOOP, RETURN, RAISE, PERFORM |
| ~~触发器~~ | ✅ | **已实现** — `CREATE TRIGGER`, `DROP TRIGGER [IF EXISTS]`, BEFORE/AFTER INSERT/UPDATE/DELETE, FOR EACH ROW/STATEMENT, PL/pgSQL 触发函数。通过 `[triggers]` 配置段启用 (`enabled = true`)。 |
| ~~物化视图~~ | ✅ | **已实现** — `CREATE MATERIALIZED VIEW ... AS SELECT`, `DROP MATERIALIZED VIEW [IF EXISTS]`, `REFRESH MATERIALIZED VIEW` |
| 外部数据包装器 (FDW) | `0A000` | `foreign data wrappers are not supported` |
| HTAP / ColumnStore 分析 | — | ColumnStore 存储 + 向量化 AGG 下推已实现；完整分析管道开发中 |
| ~~自动再均衡~~ | ✅ | **已实现** — 生产级 `RebalanceRunner`，策略驱动分片迁移、批量行传输、暂停/恢复、Prometheus 指标、Raft 感知再均衡、47 个混沌测试。通过 `[rebalance]` 配置段启用。 |
| ~~自定义类型 (JSONB 之外)~~ | ✅ | **已实现** — `CREATE TYPE name AS ENUM (...)`, `DROP TYPE [IF EXISTS]`，枚举值可作为列默认值及在表达式中使用。 |

### 可选功能（已实现，非默认构建路径）

| 功能 | 状态 | 说明 |
|------|:----:|------|
| Raft 共识复制 | 生产级 | `falcon_raft` — `SingleNodeConsensus`（默认无操作）或 `RaftConsensus`（多节点，`role = raft_member`）。32 个测试，leader 选举，故障转移，gRPC 传输。 |
| LSM-tree 存储引擎 | 默认 | `lsm/` — 默认启用（`default = ["lsm"]`）；Rowstore 和 LSM 均集成 USTM |
| RocksDB 存储引擎 | 实验性 | `rocksdb_table.rs` — 编译开关（`--features rocksdb`）；完整 MVCC 集成 |
| redb 存储引擎 | 实验性 | `redb_table.rs` — 编译开关（`--features redb`）；纯 Rust，零 C 依赖 |
| 自动分片再均衡 | 生产级 | `rebalancer.rs` + `raft_rebalance.rs` — 策略驱动后台再均衡，批量行迁移，暂停/恢复，Prometheus 指标，47 个混沌测试。通过 `[rebalance]` 配置段启用。 |

### 非默认功能（已实现，feature-gated 或尚未在默认生产路径上）

| 功能 | 状态 | 说明 |
|------|:----:|------|
| 磁盘 B-tree 行存储 | 实验性 | `disk_rowstore.rs` — 966 行，页式 B-tree 引擎，页分裂，LRU 缓冲池，叶链扫描，9 个测试。`--features disk_rowstore` |
| ColumnStore 分析 | 部分实现 | `columnstore.rs` 存储 + `executor_columnar.rs` 向量化 GROUP BY AGG 下推已实现；完整 scan/filter 管道开发中 |
| 透明数据加密 (TDE) | 已实现 | `encryption.rs` — 886 行，22 个测试。AES-256-GCM + PBKDF2 密钥派生，per-scope DEK，密钥轮换，重新加密。已接入 WAL + SST + engine。`--features encryption_tde` |
| 时间点恢复 (PITR) | 已实现 | `pitr.rs` — 598 行，9 个测试。WAL 归档器，基础备份管理，命名恢复点，4 种恢复目标（Latest/Time/LSN/XID）。`--features pitr` |
| 多租户资源隔离 | 实验性 | `resource_isolation.rs`（547 行）— I/O 令牌桶 + CPU 并发限制器。`tenant_registry.rs`（515 行）— 租户生命周期、QPS/内存/事务配额管控。`--features resource_isolation,tenant` |

---

## 1. 构建

```bash
# 前置条件: Rust 1.75+ (rustup), C/C++ 工具链 (Windows 用 MSVC, Linux/macOS 用 gcc/clang)

# 构建所有 crate (debug)
cargo build --workspace

# 构建 release (默认: Rowstore + LSM)
cargo build --release --workspace

# 构建 RocksDB 引擎 (Windows 需要 LLVM + MSVC)
cargo build --release -p falcon_server --features rocksdb

# 构建 redb 引擎 (纯 Rust, 无 C 依赖)
cargo build --release -p falcon_server --features redb

# 运行测试 (4,350+ 测试，跨 18 个 crate + 根集成测试)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 1.1 存储引擎

FalconDB 支持多种存储引擎。每张表可通过 `CREATE TABLE` 中的 `ENGINE=` 子句独立选择引擎。

### 引擎对比

| 引擎 | 存储方式 | 持久化 | 数据上限 | 适用场景 |
|------|----------|--------|----------|----------|
| **Rowstore** (默认) | 内存 (MVCC 版本链) | 仅 WAL (启用 WAL 时崩溃安全) | 受可用内存限制 | 低延迟 OLTP，热数据 |
| **LSM** | 磁盘 (LSM-Tree + WAL) | 完整磁盘持久化 | 受磁盘空间限制 | 大数据集，冷数据，持久化优先 |
| **RocksDB** | 磁盘 (RocksDB 库) | 完整磁盘持久化 | 受磁盘空间限制 | 生产级磁盘 OLTP，成熟的 LSM 压缩 |
| **redb** | 磁盘 (纯 Rust 嵌入式 KV) | 完整磁盘持久化 | 受磁盘空间限制 | 轻量嵌入式，零 C 依赖 |

### Rowstore（默认 — 内存引擎）

```sql
-- 以下两种写法等价:
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
CREATE TABLE users (id INT PRIMARY KEY, name TEXT) ENGINE=rowstore;
```

特性：
- 所有数据驻留内存（`DashMap` 中的 MVCC 版本链）
- 最快的读写延迟
- 仅在启用 WAL 时数据可在重启后存活
- 内存背压：可在 `falcon.toml` 的 `[memory]` 段配置软硬限制

### LSM（磁盘引擎）

默认启用（`lsm` feature 在默认 feature 集中）。

```sql
CREATE TABLE events (id BIGSERIAL PRIMARY KEY, payload TEXT) ENGINE=lsm;
```

### RocksDB（磁盘引擎，需要 feature 开关）

```bash
cargo build --release -p falcon_server --features rocksdb
```

```sql
CREATE TABLE orders (id BIGINT PRIMARY KEY, data TEXT) ENGINE=rocksdb;
```

### redb（磁盘引擎，需要 feature 开关）

```bash
cargo build --release -p falcon_server --features redb
```

```sql
CREATE TABLE logs (id BIGINT PRIMARY KEY, msg TEXT) ENGINE=redb;
```

### 混合引擎

同一数据库中不同表可使用不同引擎：

```sql
CREATE TABLE sessions (id INT PRIMARY KEY, token TEXT);              -- 内存，速度优先
CREATE TABLE audit_log (id BIGSERIAL PRIMARY KEY, event TEXT) ENGINE=lsm;  -- 磁盘，容量优先
CREATE TABLE trades (id BIGINT PRIMARY KEY, symbol TEXT) ENGINE=rocksdb;   -- RocksDB 持久化
```

---

## 2. 启动集群（主 + 从）

### 单节点（开发模式）

```bash
# 内存模式 (无 WAL, 最快)
cargo run -p falcon_server -- --no-wal

# 带 WAL 持久化
cargo run -p falcon_server -- --data-dir ./falcon_data

# 通过 psql 连接
psql -h 127.0.0.1 -p 5433 -U falcon
```

### 多节点部署（gRPC WAL 流式复制）

**通过配置文件**（推荐）:
```bash
# 主节点 — 接受写入，向从节点流式传输 WAL
cargo run -p falcon_server -- -c examples/primary.toml

# 从节点 — 从主节点接收 WAL，提供只读查询
cargo run -p falcon_server -- -c examples/replica.toml
```

**通过命令行参数:**
```bash
# 主节点 (PG 端口 5433, gRPC 端口 50051)
cargo run -p falcon_server -- --role primary --pg-addr 0.0.0.0:5433 \
  --grpc-addr 0.0.0.0:50051 --data-dir ./node1

# 从节点 (PG 端口 5434, 连接主节点 gRPC)
cargo run -p falcon_server -- --role replica --pg-addr 0.0.0.0:5434 \
  --primary-endpoint http://127.0.0.1:50051 --data-dir ./node2
```

### 编程式集群创建 (Rust API)

```rust
use falcon_cluster::replication::ShardReplicaGroup;

let mut group = ShardReplicaGroup::new(ShardId(0), &[schema]).unwrap();
group.ship_wal_record(wal_record);
group.catch_up_replica(0).unwrap();
```

---

## 3. 配置与复制

### 最小化 `falcon.toml`

```toml
[server]
pg_listen_addr = "0.0.0.0:5433"
admin_listen_addr = "0.0.0.0:8080"
node_id = 1

[storage]
wal_enabled = true
data_dir = "./falcon_data"

[replication]
commit_ack = "primary_durable"
```

生成完整默认配置：`cargo run -p falcon_server -- --print-default-config > falcon.toml`。
配置详情参见 [docs/OPERATIONS.md](docs/OPERATIONS.md)。

### 复制

- **提交确认**: 主节点 WAL fsync → 响应客户端
- **WAL 传输**: 带 LSN 范围 + CRC32 的 `WalChunk` 帧
- **确认追踪**: 从节点报告 `applied_lsn`；主节点从 `ack_lsn + 1` 恢复
- **从节点只读**: 未提升前拒绝写入

---

## 4. 提升 / 故障转移

### 提升操作

```rust
group.promote(replica_index).unwrap();
```

提升语义:
1. **隔离旧主节点** — 标记为 `read_only`，拒绝新写入
2. **追赶从节点** — 应用剩余 WAL 至最新 LSN
3. **角色交换** — 原子交换主从角色
4. **解除隔离** — 新主节点接受写入
5. **更新分片映射** — 将新写入路由至提升后的节点

```bash
# 运行故障转移演练
cargo run -p falcon_cluster --example failover_exercise

# 运行故障转移相关测试
cargo test -p falcon_cluster -- promote_fencing_tests
```

---

## 5. 运行基准测试

### 批量插入 + 查询 (百万行)

```bash
cargo build --release -p falcon_server -p falcon_bench

./target/release/falcon --no-wal &
./target/release/falcon_bench --bulk \
  --bulk-file benchmarks/bulk_insert_1m.sql \
  --bulk-host localhost --bulk-port 5433 \
  --bulk-sslmode disable --export text
```

**百万行基准结果** (vs PostgreSQL 16):

| 指标 | FalconDB | PostgreSQL | 比率 |
|------|----------|------------|------|
| 总计 (DDL + INSERT + 查询) | ~6.4s | ~6.1s | 1.05x |
| INSERT 阶段 (100 批 × 10K 行) | ~2.9s | ~5.4s | **0.54x (更快)** |
| 查询阶段 (COUNT, ORDER BY, 聚合) | ~2.8s | ~0.65s | 4.3x |
| 行/秒 (INSERT) | ~340K | ~185K | **1.84x (更快)** |

### YCSB 风格负载

```bash
cargo run -p falcon_bench -- --ops 10000
cargo run -p falcon_bench -- --ops 50000 --read-pct 80 --local-pct 90 --shards 4
cargo run -p falcon_bench -- --ops 10000 --export csv
```

---

## 6. 查看指标

通过 `psql -h 127.0.0.1 -p 5433 -U falcon` 连接后执行：

```sql
SHOW falcon.txn_stats;           -- 事务统计
SHOW falcon.txn_history;         -- 近期事务历史
SHOW falcon.txn;                 -- 活跃事务
SHOW falcon.gc_stats;            -- GC 统计
SHOW falcon.gc_safepoint;        -- GC 安全点诊断
SHOW falcon.replication_stats;   -- 复制/故障转移指标
SHOW falcon.scatter_stats;       -- 分散/收集执行统计
SHOW falcon.connections;         -- 连接数
SHOW falcon.plan_cache;          -- 查询计划缓存命中率
SHOW falcon.slow_queries;        -- 慢查询日志
SHOW falcon.table_stats;         -- 表统计信息
SHOW falcon.checkpoint_stats;    -- 检查点统计
SELECT * FROM pg_stat_statements; -- 语句级性能统计
```

---

## AI 查询优化器

FalconDB 内置**在线学习查询优化器**，叠加在规则优化器之上。无需外部服务，无冷启动延迟——模型训练充分前始终回退到规则优化计划。

### 工作流程

```
SQL → Binder → LogicalPlan
                    │
          extract_features()          ← 15 维特征向量
                    │
          generate_candidates()       ← 规则计划 + 备选计划
                    │
          AiOptimizer::select_plan()  ← 线性代价模型 (SGD)
                    │
             Executor 执行计划
                    │
          record_feedback(actual_us)  ← 在线 SGD 权重更新
```

### 特征向量（15 维）

| 索引 | 特征 |
|------|------|
| 0 | log₂(估计输出行数 + 1) |
| 1 | join 数量 |
| 2 | 过滤谓词数量 |
| 3 | 是否有 GROUP BY (0/1) |
| 4 | 是否有 ORDER BY (0/1) |
| 5 | 是否有 LIMIT (0/1) |
| 6 | 扫描表上是否有可用索引 (0/1) |
| 7 | log₂(最大表行数 + 1) |
| 8 | log₂(第二大表行数 + 1) |
| 9 | 跨表选择率估计 (0..1) |
| 10 | 投影列数 |
| 11 | 是否有聚合 (0/1) |
| 12 | 是否有 DISTINCT (0/1) |
| 13 | 子查询深度 |
| 14 | log₂(总字节数估计 + 1) |

### 代价模型

- **算法**：在线 SGD（随机梯度下降）+ L2 正则化
- **预测目标**：`log₂(执行时间_μs)`
- **计划类型偏置**：7 维 one-hot 编码（`SeqScan / IndexScan / IndexRangeScan / NestedLoopJoin / HashJoin / MergeSortJoin / Other`）
- **模型维度**：23 个权重（15 特征 + 7 计划类型 one-hot + 1 偏置项）
- **预热阈值**：50 个样本——达到阈值前回退到规则优化计划
- **查询指纹历史**：按查询形状跟踪平均执行时间，用于辅助统计

### 候选计划生成

当规则优化器选择 `IndexScan` 时，AI 层同时生成 `SeqScan` 备选计划，代价模型对两者评分并选择预测代价更低的方案。这使系统能够学习到索引统计信息陈旧时全表扫描更快的场景。

### 可观测性

```sql
-- 查看 AI 优化器诊断信息
SHOW AI STATS;
```

返回字段：

| 指标 | 说明 |
|------|------|
| `enabled` | AI 计划选择是否启用 |
| `samples_trained` | 已完成的 SGD 更新次数 |
| `model_ready` | 是否达到预热阈值（50 个样本） |
| `ema_mae_log2` | 指数加权平均绝对误差（log₂ 代价单位） |
| `query_fingerprints` | 已追踪的不同查询形状数量 |

### 启用 / 禁用

AI 计划选择**默认启用**。预热阈值未达到前无任何可观测影响——预热期间始终使用规则优化计划。

```sql
-- 优化器持续自我调优，无需手动配置。
-- 使用 SHOW AI STATS 监控训练进度。
SHOW AI STATS;
```

### 权重持久化

可通过 Rust API 导出/导入模型权重，实现跨重启学习：

```rust
// 导出权重用于持久化
let weights = executor.ai_optimizer.export_weights();

// 导入已保存的权重
executor.ai_optimizer.import_weights(weights);
```

---

## 架构

```
┌─────────────────────────────────────────────────────────┐
│  PG Wire Protocol (TCP)  │  Native Protocol (TCP/TLS)  │
├──────────────────────────┴─────────────────────────────┤
│         SQL 前端 (sqlparser-rs → Binder)                │
├────────────────────────────────────────────────────────┤
│         计划器 / CBO / AI 优化器 / 路由器                │
├────────────────────────────────────────────────────────┤
│         执行器 (行式 + 向量化 + 融合流式聚合)             │
├──────────────────┬─────────────────────────────────────┤
│   事务管理器      │   存储引擎                           │
│   (MVCC, OCC)    │   engine_tables: DashMap<TableHandle>│
│                  │     ├─ Rowstore(MemTable)  ← 快速    │
│                  │     ├─ Lsm / RocksDb / Redb          │
│                  │     └─ Columnstore (stub)             │
│                  ├─────────────────────────────────────┤
│                  │   USTM — 用户空间分层内存             │
│                  │   Hot(DRAM) │ Warm(LIRS-2) │ Cold    │
├──────────────────┴─────────────────────────────────────┤
│  集群 (ShardMap, 复制, 故障转移, Epoch)                  │
└────────────────────────────────────────────────────────┘
```

### Crate 结构

| Crate | 职责 |
|-------|------|
| `falcon_common` | 共享类型、错误、配置、数据类型 (含 Decimal)、Schema、RLS、RBAC |
| `falcon_storage` | 多引擎存储: Rowstore/LSM/RocksDB/redb; MVCC, 二级索引, WAL, GC, USTM, TDE, CDC |
| `falcon_txn` | 事务生命周期, OCC 验证, 时间戳分配, 死锁检测 |
| `falcon_sql_frontend` | SQL 解析 (sqlparser-rs) + 绑定/分析 |
| `falcon_planner` | 计划生成, 基于代价的优化器, AI 优化器 (在线 SGD 代价模型、候选计划选择、执行反馈), 路由提示, 分布式包装 |
| `falcon_executor` | 算子执行, 表达式求值, 查询治理器, 融合流式聚合, FTS 引擎, 向量化列存 AGG |
| `falcon_protocol_pg` | PostgreSQL 线协议编解码 + TCP 服务器 |
| `falcon_protocol_native` | FalconDB 原生二进制协议 |
| `falcon_native_server` | 原生协议服务器、会话管理、Nonce 防重放 |
| `falcon_raft` | Raft 共识 — 单节点默认 / 多节点生产 (`role = raft_member`) |
| `falcon_cluster` | 分片映射, 复制, 故障转移, scatter/gather, Epoch, 迁移, 监督器 |
| `falcon_observability` | Prometheus 指标, 结构化日志, pg_stat_statements |
| `falcon_proto` | Protobuf 定义 + tonic gRPC 代码生成 |
| `falcon_segment_codec` | 段级压缩 (Zstd, LZ4, 字典, CRC) |
| `falcon_enterprise` | 企业功能: 控制平面 HA, 安全 (AuthN/AuthZ, TLS 轮换), 运维自动化 |
| `falcon_server` | 主二进制文件，组装所有组件 |
| `falcon_cli` | 交互式 CLI — REPL, 集群管理, 导入/导出, 故障转移 |
| `falcon_bench` | YCSB 风格基准测试工具 |

### 客户端 SDK (`clients/`)

| SDK | 语言 | 功能 |
|-----|------|------|
| `falcondb-jdbc` | Java | JDBC 4.2, HA 故障转移, 连接池 |
| `falcondb-go` | Go | 原生协议, HA seed list, 连接池 |
| `falcondb-python` | Python | DB-API 2.0, HA 故障转移, 连接池 |
| `falcondb-node` | Node.js | 异步, TLS, HA 故障转移, TypeScript 类型 |

标准 PG 驱动 (psycopg2, pgx, node-postgres, pgjdbc, tokio-postgres, Npgsql) 也可通过 PG 线协议直连 5433 端口。

---

## 事务模型

- **LocalTxn（快速路径）**: 单分片事务在快照隔离下以 OCC 提交 — 无 2PC 开销
- **GlobalTxn（慢速路径）**: 跨分片事务使用 XA-2PC（prepare/commit）
- **统一提交入口**: `StorageEngine::commit_txn(txn_id, commit_ts, txn_type)`
- **硬性不变量**: 提交时校验 — LocalTxn 必须恰好 1 个分片且用快速路径，违反返回 `InternalError`

---

## MVCC 垃圾回收

- **安全点**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL 感知**: 不回收未提交或已中止的版本
- **复制安全**: 尊重从节点已应用的时间戳
- **无锁**: 每键修剪，无全局锁，无 stop-the-world
- **后台运行**: `GcRunner` 线程，可配置间隔
- **可观测**: `SHOW falcon.gc_stats`

---

## 快速开始

```bash
# 单机演示 (构建 + 启动 + SQL 冒烟测试 + 基准)
./scripts/demo_standalone.sh          # Linux/macOS/WSL
.\scripts\demo_standalone.ps1         # Windows

# 主从复制演示
./scripts/demo_replication.sh

# 端到端故障转移 (写入 → kill 主节点 → 提升 → 验证)
./scripts/e2e_two_node_failover.sh    # Linux/macOS/WSL
.\scripts\e2e_two_node_failover.ps1   # Windows
```

---

## 开发环境

### Windows

```powershell
.\scripts\setup_windows.ps1
```

### Linux / macOS

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
sudo apt install postgresql-client    # Debian/Ubuntu
brew install libpq                    # macOS
cargo build --workspace
```

---

## 路线图

所有里程碑至 v1.3 已发布。当前测试数: **4,380+**，跨 18 个 crate (432 个 `.rs` 文件, ~246K 行 Rust 代码)。

| 里程碑 | 亮点 |
|--------|------|
| **v0.1–v0.9** ✅ | OLTP 基础, WAL, 故障转移, gRPC, 安全, 混沌加固 |
| **v1.0** ✅ | LSM 引擎, SQL 完整性, 企业功能 (RLS/TDE/PITR/CDC), 分布式加固 |
| **v1.0.1–v1.0.3** ✅ | 零 panic, 故障转移×事务矩阵, 确定性与信任加固 |
| **v1.1–v1.2** ✅ | USTM 分层内存, 融合流式聚合, 接近 PG 查询对等 |
| **v1.3** ✅ | PL/pgSQL 触发器 (CREATE/DROP TRIGGER, BEFORE/AFTER, FOR EACH ROW/STATEMENT), Spring Boot/JDBC 兼容性改进, ColumnStore GC & truncate, `CREATE TYPE ... AS ENUM` 自定义枚举类型, AI 优化器集成 (在线 SGD 代价模型, `SHOW AI STATS`) |

### RPO / RTO

FalconDB 支持两种生产持久化策略：`local-fsync`（默认, RPO > 0）和 `sync-replica`（主节点等待从节点 WAL 确认, RPO ≈ 0）。详见 [docs/rpo_rto.md](docs/rpo_rto.md)。

---

## 文档

| 文档 | 说明 |
|------|------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 系统架构、crate 结构、数据流 |
| [docs/INSTALL.md](docs/INSTALL.md) | 安装与卸载 (Windows + Linux) |
| [docs/UPGRADE.md](docs/UPGRADE.md) | 升级、滚动升级与版本管理 |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | 服务管理、监控、配置管理 |
| [docs/cluster_ops.md](docs/cluster_ops.md) | 集群运维、2PC、故障转移运维手册 |
| [docs/security.md](docs/security.md) | 认证、RBAC、TLS、SQL 防火墙、审计 |
| [docs/observability.md](docs/observability.md) | 指标、SHOW 命令、SLA/SLO |
| [docs/failover_behavior.md](docs/failover_behavior.md) | 故障转移不变量、复制完整性 |
| [docs/pg_driver_compatibility.md](docs/pg_driver_compatibility.md) | PG 驱动兼容性与 JDBC 连接 |
| [docs/sql_compatibility.md](docs/sql_compatibility.md) | SQL 兼容性参考 |
| [docs/error_model.md](docs/error_model.md) | 统一错误模型、SQLSTATE 映射、重试提示 |
| [docs/rpo_rto.md](docs/rpo_rto.md) | 各持久化策略的 RPO/RTO 保证 |
| [docs/backup_restore.md](docs/backup_restore.md) | 备份与恢复 |
| [docs/production_checklist.md](docs/production_checklist.md) | 生产就绪检查清单 |
| [docs/cli.md](docs/cli.md) | CLI 参考 (falcon-cli) |
| [docs/design/](docs/design/) | 设计文档: MVCC, WAL, 分片, 向量化执行 |
| [docs/adr/](docs/adr/) | 架构决策记录 (ADR-001–007) |
| [CHANGELOG.md](CHANGELOG.md) | 语义化版本变更日志 (v0.1–v1.3) |

---

## 测试

```bash
# 运行全部测试 (4,380+)
cargo test --workspace

# 按 crate 运行
cargo test -p falcon_cluster   # 1,050+ 测试
cargo test -p falcon_storage   # 820+ 测试
cargo test -p falcon_server    # 420+ 测试
cargo test -p falcon_executor  # 320+ 测试
cargo test -p falcon_common    # 250+ 测试

# Lint
cargo clippy --workspace       # 必须 0 warning
```

---

## 许可证

Apache-2.0
