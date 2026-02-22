# FalconDB
<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="License" />
</p>

**兼容 PostgreSQL 协议的分布式内存 OLTP 数据库** — 使用 Rust 编写。

FalconDB 提供稳定的 OLTP 能力、快/慢路径事务、基于 WAL 的主从复制（gRPC 流式传输）、
主从切换/故障转移、MVCC 垃圾回收，以及可复现的基准测试。

> **[English README](README.md)** | 简体中文

### 支持平台

| 平台 | 构建 | 测试 | 状态 |
|------|:----:|:----:|------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | 主要 CI 目标 |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | CI 目标 |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | 社区测试 |

**最低 Rust 版本 (MSRV)**: Rust **1.75** (`rust-version = "1.75"` in `Cargo.toml`)

### PG 协议兼容性

| 功能 | 状态 | 备注 |
|------|:----:|------|
| 简单查询协议 | ✅ | 单语句 + 多语句 |
| 扩展查询 (Parse/Bind/Execute) | ✅ | 预编译语句 + Portal |
| 认证: Trust | ✅ | 接受任意用户 |
| 认证: MD5 | ✅ | PG auth type 5 |
| 认证: SCRAM-SHA-256 | ✅ | 兼容 PG 10+ |
| 认证: Password (明文) | ✅ | PG auth type 3 |
| TLS/SSL | ✅ | SSLRequest → 配置后自动升级 |
| COPY IN/OUT | ✅ | 支持 Text 和 CSV 格式 |
| `psql` 12+ | ✅ | 完整测试 |
| `pgbench` (初始化 + 运行) | ✅ | 内置脚本可用 |
| JDBC (pgjdbc 42.x) | ✅ | 已测试 42.7+ |
| 取消请求 | ⚠️ | 已接受，暂未执行 |
| LISTEN/NOTIFY | ❌ | 未实现 |
| 逻辑复制协议 | ❌ | 使用 gRPC 替代 |

详见 [docs/protocol_compatibility.md](docs/protocol_compatibility.md)。

### SQL 覆盖范围

| 类别 | 支持内容 |
|------|----------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (含 ON CONFLICT, RETURNING, SELECT), UPDATE (含 FROM, RETURNING), DELETE (含 USING, RETURNING), COPY |
| **查询** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOIN (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL), 子查询 (标量/IN/EXISTS/关联), CTE (含 RECURSIVE), UNION/INTERSECT/EXCEPT, 窗口函数 |
| **聚合** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **类型** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **事务** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, 事务级超时, Read Committed, Snapshot Isolation |
| **函数** | 500+ 标量函数 (字符串、数学、日期/时间、加密、JSON、数组) |
| **可观测性** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### 暂不支持

- 存储过程 / PL/pgSQL
- 触发器
- 物化视图
- 外部数据包装器 (FDW)
- 在线 Schema 变更 (并发索引构建)
- 自动再均衡 (仅支持手动分片拆分)
- 自定义类型 (JSONB 除外)
- 全文搜索 (tsvector/tsquery)

---

## 1. 构建

```bash
# 前置条件: Rust 1.75+ (rustup), C/C++ 工具链 (Windows 用 MSVC, Linux/macOS 用 gcc/clang)

# 构建所有 crate (debug)
cargo build --workspace

# 构建 release 版本
cargo build --release --workspace

# 运行测试 (15 个 crate + 根集成测试，共 2,239 个测试)
cargo test --workspace

# 代码检查
cargo clippy --workspace
```

---

## 2. 启动集群 (主节点 + 副本)

### 单节点 (开发模式)

```bash
# 纯内存模式 (无 WAL，最快)
cargo run -p falcon_server -- --no-wal

# 启用 WAL 持久化
cargo run -p falcon_server -- --data-dir ./falcon_data

# 通过 psql 连接
psql -h 127.0.0.1 -p 5433 -U falcon
```

### 多节点部署 (M2 — gRPC WAL 流式传输)

**通过配置文件** (推荐):
```bash
# 主节点 — 接受写入，将 WAL 流式传输到副本
cargo run -p falcon_server -- -c examples/primary.toml

# 副本 — 从主节点接收 WAL，提供只读查询
cargo run -p falcon_server -- -c examples/replica.toml
```

**通过命令行参数:**
```bash
# 主节点: 端口 5433, gRPC 端口 50051
cargo run -p falcon_server -- --role primary --pg-addr 0.0.0.0:5433 \
  --grpc-addr 0.0.0.0:50051 --data-dir ./node1

# 副本: 端口 5434, 连接主节点 gRPC
cargo run -p falcon_server -- --role replica --pg-addr 0.0.0.0:5434 \
  --primary-endpoint http://127.0.0.1:50051 --data-dir ./node2

# 第二个副本: 端口 5435
cargo run -p falcon_server -- --role replica --pg-addr 0.0.0.0:5435 \
  --primary-endpoint http://127.0.0.1:50051 --data-dir ./node3
```

> **注意**: M2 gRPC WAL 流式传输正在开发中。`--role` 参数已被接受，
> 但实际网络复制需要 `protoc` 和 tonic 代码生成
> (`cargo build -p falcon_cluster --features grpc-codegen`)。
> M1 进程内复制仍可通过 Rust API 使用。

### 编程式集群设置 (Rust API)

```rust
use falcon_cluster::replication::ShardReplicaGroup;

// 创建 1 主 + 1 副本，共享 schema
let mut group = ShardReplicaGroup::new(ShardId(0), &[schema]).unwrap();

// 将 WAL 记录从主节点发送到副本
group.ship_wal_record(wal_record);

// 追赶副本到最新 LSN
group.catch_up_replica(0).unwrap();
```

---

## 3. 主从配置

### 配置文件 (`falcon.toml`)

```toml
[server]
pg_listen_addr = "0.0.0.0:5433"   # PostgreSQL 协议监听地址
admin_listen_addr = "0.0.0.0:8080" # 管理/指标端点
node_id = 1                        # 唯一节点标识
max_connections = 1024              # 最大并发 PG 连接数

[storage]
memory_limit_bytes = 0              # 0 = 无限制
wal_enabled = true                  # 启用预写日志
data_dir = "./falcon_data"           # WAL 和检查点目录

[wal]
group_commit = true                 # 批量 WAL 写入以提高吞吐量
flush_interval_us = 1000            # 组提交刷新间隔 (微秒)
sync_mode = "fdatasync"             # "fsync", "fdatasync", 或 "none"
segment_size_bytes = 67108864       # WAL 段大小 (默认 64MB)

[gc]
enabled = true                      # 启用 MVCC 垃圾回收
interval_ms = 1000                  # GC 扫描间隔 (毫秒)
batch_size = 0                      # 0 = 无限制 (每周期扫描所有链)
min_chain_length = 2                # 跳过短于此长度的链

[replication]
commit_ack = "primary_durable"      # 方案 A: 提交确认 = 主节点 WAL fsync
                                    # RPO > 0 (已记录的权衡)
```

### 命令行选项

```
falcon [OPTIONS]

选项:
  -c, --config <FILE>        配置文件 [默认: falcon.toml]
      --pg-addr <ADDR>       PG 监听地址 (覆盖配置)
      --data-dir <DIR>       数据目录 (覆盖配置)
      --no-wal               禁用 WAL (纯内存模式)
      --metrics-addr <ADDR>  指标端点 [默认: 0.0.0.0:9090]
      --replica              以副本模式启动
      --primary-addr <ADDR>  复制的主节点地址
```

### 复制语义 (M1)

- **提交确认 (方案 A)**: `commit ack = 主节点 WAL 持久化 (fsync)`。
  主节点不等待副本确认即返回提交。
  在主节点故障且尚未复制时，RPO 可能 > 0。
- **WAL 传输**: `WalChunk` 帧包含 `start_lsn`、`end_lsn`、CRC32 校验和。
- **确认追踪**: 副本报告 `applied_lsn`；主节点追踪每个副本的确认 LSN，用于重连/从 `ack_lsn + 1` 恢复。
- **副本只读**: 副本以 `read_only` 模式启动，在提升前拒绝写入。

---

## 4. 主从切换 / 故障转移

### 提升操作

```rust
// 编程式 API
group.promote(replica_index).unwrap();
```

提升语义:
1. **隔离旧主节点** — 标记为 `read_only`，拒绝新写入。
2. **追赶副本** — 应用剩余 WAL 到最新 LSN。
3. **交换角色** — 原子性地交换主从角色。
4. **解除新主节点隔离** — 新主节点接受写入。
5. **更新分片映射** — 将新写入路由到提升后的节点。

### 故障转移演练 (端到端)

参见 `crates/falcon_cluster/examples/failover_exercise.rs`，包含完整示例:

1. 创建集群 (1 主 + 1 副本)
2. 在主节点写入测试数据
3. 复制到副本
4. 隔离 (终止) 主节点
5. 提升副本
6. 在新主节点写入新数据
7. 验证数据完整性 (已提交数据零丢失)

```bash
# 运行故障转移演练示例
cargo run -p falcon_cluster --example failover_exercise

# 运行故障转移相关测试
cargo test -p falcon_cluster -- promote_fencing_tests
cargo test -p falcon_cluster -- m1_full_lifecycle
```

### 故障转移可观测性

```sql
-- 查看故障转移/复制指标
SHOW falcon.replication_stats;
```

| 指标 | 描述 |
|------|------|
| `promote_count` | 已完成的提升操作总数 |
| `last_failover_time_ms` | 上次故障转移耗时 (毫秒) |

---

## 5. 运行基准测试

### YCSB 风格负载

```bash
# 默认: 10k 操作, 50% 读, 80% 本地事务, 4 分片
cargo run -p falcon_bench -- --ops 10000

# 自定义混合
cargo run -p falcon_bench -- --ops 50000 --read-pct 80 --local-pct 90 --shards 4

# 导出为 CSV 或 JSON
cargo run -p falcon_bench -- --ops 10000 --export csv
cargo run -p falcon_bench -- --ops 10000 --export json
```

### 快速路径 ON vs OFF 对比 (图表 2: p99 延迟)

```bash
cargo run -p falcon_bench -- --ops 10000 --compare --export csv
```

输出: TPS、提交计数、快速路径 vs 全局路径的 p50/p95/p99 延迟。

### 扩展性基准测试 (图表 1: TPS vs 分片数)

```bash
# 自动运行 1/2/4/8 分片配置
cargo run -p falcon_bench -- --scaleout --ops 5000 --export csv
```

### 故障转移基准测试 (图表 3: 前后延迟对比)

```bash
cargo run -p falcon_bench -- --failover --ops 10000 --export csv
```

### 基准测试参数

| 参数 | 默认值 | 描述 |
|------|--------|------|
| `--ops` | 10000 | 每次运行的总操作数 |
| `--read-pct` | 50 | 读取百分比 (0–100) |
| `--local-pct` | 80 | 本地 (单分片) 事务百分比 |
| `--shards` | 4 | 逻辑分片数 |
| `--record-count` | 1000 | 预加载行数 |
| `--isolation` | rc | `rc` (读已提交) 或 `si` (快照隔离) |
| `--export` | text | `text`、`csv` 或 `json` |

随机种子固定为 42 以确保可复现性。

---

## 6. 查看指标

### SQL 可观测性命令

通过 `psql -h 127.0.0.1 -p 5433 -U falcon` 连接后执行:

```sql
-- 事务统计 (提交/中止计数, 延迟百分位)
SHOW falcon.txn_stats;

-- 最近事务历史 (逐事务记录)
SHOW falcon.txn_history;

-- 活跃事务
SHOW falcon.txn;

-- GC 统计 (安全点, 回收版本/字节, 链长度)
SHOW falcon.gc_stats;

-- GC 安全点诊断 (长事务检测, 停滞指示)
SHOW falcon.gc_safepoint;

-- 复制/故障转移指标
SHOW falcon.replication_stats;

-- Scatter/Gather 执行统计
SHOW falcon.scatter_stats;
```

### `SHOW falcon.txn_stats` 输出

| 指标 | 描述 |
|------|------|
| `total_committed` | 已提交事务总数 |
| `fast_path_commits` | 快速路径提交 (LocalTxn) |
| `slow_path_commits` | 慢速路径提交 (GlobalTxn) |
| `total_aborted` | 已中止事务总数 |
| `occ_conflicts` | OCC 序列化冲突 |
| `constraint_violations` | 唯一约束违反 |
| `active_count` | 当前活跃事务数 |
| `fast_p50/p95/p99_us` | 快速路径提交延迟百分位 |
| `slow_p50/p95/p99_us` | 慢速路径提交延迟百分位 |

### `SHOW falcon.gc_stats` 输出

| 指标 | 描述 |
|------|------|
| `gc_safepoint_ts` | 当前 GC 水位时间戳 |
| `active_txn_count` | 阻塞 GC 的活跃事务数 |
| `oldest_txn_ts` | 最旧活跃事务时间戳 |
| `total_sweeps` | 已完成的 GC 扫描周期总数 |
| `reclaimed_version_count` | 已回收的 MVCC 版本总数 |
| `reclaimed_memory_bytes` | GC 释放的总字节数 |
| `last_sweep_duration_us` | 上次 GC 扫描耗时 |
| `max_chain_length` | 观察到的最长版本链 |

### `SHOW falcon.gc_safepoint` 输出

| 指标 | 描述 |
|------|------|
| `active_txn_count` | 阻塞 GC 的活跃事务数 |
| `longest_txn_age_us` | 最长运行活跃事务的年龄 (微秒) |
| `min_active_start_ts` | 最旧活跃事务的起始时间戳 |
| `current_ts` | 当前时间戳分配器值 |
| `stalled` | GC 安全点是否被长事务阻塞 |

---

## 支持的 SQL

### DDL

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    total FLOAT8 DEFAULT 0.0,
    CHECK (total >= 0)
);
DROP TABLE users;
DROP TABLE IF EXISTS users;
TRUNCATE TABLE users;

-- 索引
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);
DROP INDEX idx_name;

-- ALTER TABLE
ALTER TABLE users ADD COLUMN email TEXT;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users RENAME COLUMN name TO full_name;
ALTER TABLE users RENAME TO people;
ALTER TABLE users ALTER COLUMN age SET NOT NULL;
ALTER TABLE users ALTER COLUMN age DROP NOT NULL;
ALTER TABLE users ALTER COLUMN age SET DEFAULT 0;
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;

-- 序列
CREATE SEQUENCE user_id_seq START 1;
SELECT nextval('user_id_seq');
SELECT currval('user_id_seq');
SELECT setval('user_id_seq', 100);
```

### DML

```sql
-- INSERT (单行, 多行, DEFAULT, RETURNING, ON CONFLICT)
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users (name, age) VALUES ('Bob', 25), ('Eve', 22);
INSERT INTO users VALUES (1, 'Alice', 30) ON CONFLICT DO NOTHING;
INSERT INTO users VALUES (1, 'Alice', 30)
    ON CONFLICT (id) DO UPDATE SET name = excluded.name;
INSERT INTO users VALUES (2, 'Bob', 25) RETURNING *;
INSERT INTO users VALUES (3, 'Eve', 22) RETURNING id, name;
INSERT INTO orders SELECT id, name FROM staging;  -- INSERT ... SELECT

-- UPDATE (单表, 多表 FROM, RETURNING)
UPDATE users SET age = 31 WHERE id = 1;
UPDATE users SET age = 31 WHERE id = 1 RETURNING id, age;
UPDATE products SET price = p.new_price
    FROM price_updates p WHERE products.id = p.id;

-- DELETE (单表, 多表 USING, RETURNING)
DELETE FROM users WHERE id = 2;
DELETE FROM users WHERE id = 2 RETURNING *;
DELETE FROM employees USING terminated
    WHERE employees.id = terminated.emp_id;

-- COPY (stdin/stdout, CSV/text 格式)
COPY users FROM STDIN;
COPY users TO STDOUT WITH (FORMAT csv, HEADER true);
COPY (SELECT * FROM users WHERE age > 25) TO STDOUT;
```

### 查询

```sql
-- 基本 SELECT: 过滤、排序、分页
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10 OFFSET 5;
SELECT DISTINCT department FROM employees;

-- 表达式: CASE, COALESCE, NULLIF, CAST, BETWEEN, IN, LIKE/ILIKE
SELECT CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END FROM users;
SELECT COALESCE(nickname, name) FROM users;
SELECT * FROM users WHERE age BETWEEN 20 AND 30;
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE name ILIKE '%alice%';
SELECT CAST(age AS TEXT) FROM users;

-- 聚合与 GROUP BY / HAVING
SELECT dept, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary)
    FROM employees GROUP BY dept HAVING COUNT(*) > 5;
SELECT BOOL_AND(active), BOOL_OR(active) FROM users;
SELECT ARRAY_AGG(name) FROM users;

-- 窗口函数
SELECT name, salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC),
    RANK() OVER (ORDER BY salary DESC),
    DENSE_RANK() OVER (ORDER BY salary DESC),
    LAG(salary) OVER (ORDER BY salary),
    LEAD(salary) OVER (ORDER BY salary),
    SUM(salary) OVER (PARTITION BY dept)
FROM employees;

-- JOIN (INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, USING)
SELECT * FROM orders JOIN users ON orders.user_id = users.id;
SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id;
SELECT * FROM orders NATURAL JOIN users;
SELECT * FROM t1 JOIN t2 USING (id);

-- 子查询 (标量, IN, EXISTS, 关联)
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);
SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users;

-- 集合操作 (UNION, INTERSECT, EXCEPT — 含 ALL)
SELECT name FROM employees UNION SELECT name FROM contractors;
SELECT id FROM t1 INTERSECT SELECT id FROM t2;
SELECT id FROM t1 EXCEPT ALL SELECT id FROM t2;

-- CTE (WITH, 递归)
WITH active AS (SELECT * FROM users WHERE active = true)
SELECT * FROM active WHERE age > 25;
WITH RECURSIVE nums AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 10
) SELECT * FROM nums;

-- 数组
SELECT ARRAY[1, 2, 3];
SELECT arr[1] FROM t;
SELECT UNNEST(ARRAY[1, 2, 3]);
SELECT ARRAY_AGG(name) FROM users;

-- ANY / ALL 操作符
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3]);
SELECT * FROM users WHERE age > ALL(SELECT min_age FROM rules);

-- IS DISTINCT FROM
SELECT * FROM t WHERE a IS DISTINCT FROM b;

-- 事务
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 28);
COMMIT;  -- 或 ROLLBACK;

-- EXPLAIN
EXPLAIN SELECT * FROM users WHERE id = 1;

-- 可观测性
SHOW falcon.txn_stats;
SHOW falcon.gc_stats;
SHOW falcon.replication_stats;
```

### 支持的类型

| 类型 | PG 等价类型 |
|------|------------|
| `INT` / `INTEGER` | `integer` / `int4` |
| `BIGINT` | `bigint` / `int8` |
| `FLOAT8` / `DOUBLE PRECISION` | `double precision` |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `numeric` (i128 尾数 + u8 精度) |
| `TEXT` / `VARCHAR` | `text` / `varchar` |
| `BOOLEAN` | `boolean` |
| `TIMESTAMP` | `timestamp without time zone` |
| `DATE` | `date` |
| `SERIAL` | 自增 `int4` |
| `BIGSERIAL` | 自增 `int8` |
| `INT[]` / `TEXT[]` / ... | 一维数组 |

### 标量函数 (500+)

核心 PG 兼容函数包括: `UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`, `CONCAT`, `REPLACE`, `TRIM`, `LPAD`, `RPAD`, `LEFT`, `RIGHT`, `REVERSE`, `INITCAP`, `POSITION`, `SPLIT_PART`, `ABS`, `ROUND`, `CEIL`, `FLOOR`, `POWER`, `SQRT`, `LN`, `LOG`, `EXP`, `MOD`, `SIGN`, `PI`, `GREATEST`, `LEAST`, `TO_CHAR`, `TO_NUMBER`, `TO_DATE`, `TO_TIMESTAMP`, `NOW`, `CURRENT_DATE`, `CURRENT_TIME`, `DATE_TRUNC`, `DATE_PART`, `EXTRACT`, `AGE`, `MD5`, `SHA256`, `ENCODE`, `DECODE`, `GEN_RANDOM_UUID`, `RANDOM`, `REGEXP_REPLACE`, `REGEXP_MATCH`, `REGEXP_COUNT`, `STARTS_WITH`, `ENDS_WITH`, `PG_TYPEOF`，以及大量数组/字符串/数学/统计函数。

---

## 架构

```
┌─────────────────────────────────────────────────────────┐
│  PG 协议 (TCP)           │  原生协议 (TCP/TLS)          │
├──────────────────────────┴─────────────────────────────┤
│         SQL 前端 (sqlparser-rs → Binder)                │
├────────────────────────────────────────────────────────┤
│         计划器 / 路由器                                  │
├────────────────────────────────────────────────────────┤
│         执行器 (逐行执行, 表达式求值)                     │
├──────────────────┬─────────────────────────────────────┤
│   事务管理器      │   存储引擎                           │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)      │
├──────────────────┴─────────────────────────────────────┤
│  集群层 (分片映射, 复制, 故障转移, Epoch)                 │
└────────────────────────────────────────────────────────┘
```

### Crate 结构

| Crate | 职责 |
|-------|------|
| `falcon_common` | 共享类型、错误、配置、数据类型、Schema、RLS、RBAC |
| `falcon_storage` | 内存表、LSM 引擎、MVCC、索引、WAL、GC、TDE、分区、PITR、CDC |
| `falcon_txn` | 事务生命周期、OCC 验证、时间戳分配 |
| `falcon_sql_frontend` | SQL 解析 (sqlparser-rs) + 绑定/分析 |
| `falcon_planner` | 逻辑 → 物理计划、路由提示 |
| `falcon_executor` | 算子执行、表达式求值、查询治理 |
| `falcon_protocol_pg` | PostgreSQL 协议编解码 + TCP 服务器 |
| `falcon_protocol_native` | FalconDB 原生二进制协议 — 编解码、压缩、类型映射 |
| `falcon_native_server` | 原生协议服务器 — 会话管理、执行器桥接、Nonce 防重放 |
| `falcon_raft` | 共识 trait + 单节点桩实现 |
| `falcon_cluster` | 分片映射、复制、故障转移、Scatter/Gather、Epoch、迁移、Supervisor |
| `falcon_observability` | 指标 (Prometheus)、结构化日志、链路追踪 |
| `falcon_server` | 主二进制文件，组装所有组件 |
| `falcon_bench` | YCSB 风格基准测试工具 |

### Java JDBC 驱动 (`clients/falcondb-jdbc/`)

| 模块 | 职责 |
|------|------|
| `io.falcondb.jdbc` | JDBC Driver, Connection, Statement, PreparedStatement, ResultSet, DataSource |
| `io.falcondb.jdbc.protocol` | 原生协议线格式、TCP 连接、握手、认证 |
| `io.falcondb.jdbc.ha` | HA 感知故障转移: ClusterTopologyProvider, PrimaryResolver, FailoverRetryPolicy |

---

## 事务模型

- **LocalTxn (快速路径)**: 单分片事务在快照隔离下使用 OCC 提交 — 无 2PC 开销。`TxnContext.txn_path = Fast`。
- **GlobalTxn (慢速路径)**: 跨分片事务使用 XA-2PC (prepare/commit)。`TxnContext.txn_path = Slow`。
- **TxnContext**: 贯穿所有层，在提交时进行硬不变量验证:
  - LocalTxn → `involved_shards.len() == 1`，必须使用快速路径
  - GlobalTxn → 不得使用快速路径
  - 违反返回 `InternalError` (不仅仅是 debug_assert)
- **统一提交入口**: `StorageEngine::commit_txn(txn_id, commit_ts, txn_type)`。原始 `commit_txn_local`/`commit_txn_global` 仅为 `pub(crate)`。

---

## MVCC 垃圾回收

- **安全点**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL 感知**: 不回收未提交或已中止的版本
- **复制安全**: 尊重副本已应用的时间戳
- **无锁逐键**: 无全局锁，无 stop-the-world 暂停
- **后台运行器**: `GcRunner` 线程，可配置间隔
- **可观测性**: `SHOW falcon.gc_stats`

---

## 快速开始

### Linux / macOS / WSL

```bash
chmod +x scripts/demo_standalone.sh
./scripts/demo_standalone.sh
```

构建 FalconDB，启动单节点，通过 `psql` 运行 SQL 冒烟测试，
并执行快速基准测试 — 一条命令完成。

### Windows (PowerShell)

```powershell
.\scripts\demo_standalone.ps1
```

### 主从复制演示

```bash
chmod +x scripts/demo_replication.sh
./scripts/demo_replication.sh
```

启动主节点和副本 (gRPC)，写入数据，验证复制，显示复制指标。

### 端到端故障转移演示 (双节点，闭环)

```bash
# Linux / macOS / WSL
chmod +x scripts/e2e_two_node_failover.sh
./scripts/e2e_two_node_failover.sh

# Windows PowerShell
.\scripts\e2e_two_node_failover.ps1
```

完整闭环测试: 启动主节点 → 启动副本 → 写入数据 → 验证复制 →
终止主节点 → 提升副本 → 验证旧数据可读 → 写入新数据 → 输出 PASS/FAIL。
失败时打印每个节点最后 50 行日志及端口/PID 诊断信息。

---

## 开发环境设置

### Windows

```powershell
.\scripts\setup_windows.ps1
```

检查/安装: MSVC C++ 构建工具、Rust 工具链、protoc (内置)、
psql 客户端、git 换行符配置。详见 `scripts/setup_windows.ps1`。

### Linux / macOS

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# psql (用于测试)
sudo apt install postgresql-client    # Debian/Ubuntu
brew install libpq                    # macOS

# 构建
cargo build --workspace
```

### 默认配置模板

```bash
cargo run -p falcon_server -- --print-default-config > falcon.toml
```

---

## 路线图

| 阶段 | 范围 | 详情 |
|------|------|------|
| **v0.1–v0.4** ✅ | OLTP 基础、WAL、故障转移、gRPC 流式传输、TLS、列存、多租户 | 已发布 |
| **v0.4.x** ✅ | 生产加固: 错误模型、崩溃域、核心 crate unwrap=0 | 已发布 |
| **v0.5** ✅ | 可运维: 集群管理、再均衡、扩缩容、运维手册 | 已发布 |
| **v0.6** ✅ | 延迟可控 OLTP: 优先级调度、令牌桶、背压 | 已发布 |
| **v0.7** ✅ | 确定性 2PC: 决策日志、分层超时、慢分片追踪 | 已发布 |
| **v0.8** ✅ | 混沌就绪: 故障注入、网络分区、CPU/IO 抖动、可观测性 | 已发布 |
| **v0.9** ✅ | 生产候选: 安全加固、WAL 版本化、协议兼容、配置兼容 | 已发布 |
| **v1.0 Phase 1** ✅ | LSM 内核: 磁盘 OLTP、MVCC 编码、幂等性、TPC-B 基准测试 | 1,917 测试 |
| **v1.0 Phase 2** ✅ | SQL 完整性: DECIMAL、组合索引、RBAC、事务 READ ONLY、Governor v2 | 1,976 测试 |
| **v2.0 Phase 3** ✅ | 企业版: RLS、TDE、分区、PITR、CDC | 2,056 测试 |
| **存储加固** ✅ | WAL 恢复、压缩调度、内存预算、GC 安全点、故障注入 | 2,261 测试 |
| **分布式加固** ✅ | Epoch 隔离、Leader 租约、分片迁移、跨分片限流、Supervisor | +62 测试 |
| **原生协议** ✅ | FalconDB 原生二进制协议、Java JDBC 驱动、压缩、HA 故障转移 | 2,239 测试 |
| **v1.0.0** 📋 | 生产级数据库内核 — 所有门控通过 | [docs/roadmap.md](docs/roadmap.md) |

详见 [docs/roadmap.md](docs/roadmap.md) 了解每个里程碑的详细验收标准。

### RPO / RTO

FalconDB 支持三种持久化策略: `local-fsync` (默认, RPO > 0 可能)、
`quorum-ack` (RPO = 0, 需要多数派)、`all-ack` (最强, 延迟最高)。
详见 [docs/rpo_rto.md](docs/rpo_rto.md)。

---

## 文档

| 文档 | 描述 |
|------|------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 系统架构、Crate 结构、数据流 |
| [docs/roadmap.md](docs/roadmap.md) | 里程碑定义和验收标准 |
| [docs/rpo_rto.md](docs/rpo_rto.md) | 各持久化策略的 RPO/RTO 保证 |
| [docs/show_commands_schema.md](docs/show_commands_schema.md) | 所有 `SHOW falcon.*` 命令的稳定输出 Schema |
| [docs/protocol_compatibility.md](docs/protocol_compatibility.md) | PG 客户端兼容性矩阵 (psql, JDBC, pgbench) |
| [docs/feature_gap_analysis.md](docs/feature_gap_analysis.md) | 已知差距和改进方向 |
| [docs/error_model.md](docs/error_model.md) | 统一错误模型、SQLSTATE 映射、重试提示 |
| [docs/observability.md](docs/observability.md) | Prometheus 指标、SHOW 命令、慢查询日志 |
| [docs/production_readiness.md](docs/production_readiness.md) | 生产就绪检查清单 |
| [docs/production_readiness_report.md](docs/production_readiness_report.md) | 完整生产就绪审计报告 |
| [docs/ops_playbook.md](docs/ops_playbook.md) | 扩缩容、故障转移、滚动升级操作手册 |
| [docs/chaos_matrix.md](docs/chaos_matrix.md) | 30 个混沌场景及预期行为 |
| [docs/security.md](docs/security.md) | 安全特性、RBAC、SQL 防火墙、审计 |
| [docs/wire_compatibility.md](docs/wire_compatibility.md) | WAL/快照/协议/配置兼容性策略 |
| [docs/performance_baseline.md](docs/performance_baseline.md) | P99 延迟目标和基准测试方法论 |
| [docs/native_protocol.md](docs/native_protocol.md) | FalconDB 原生二进制协议规范 |
| [docs/native_protocol_compat.md](docs/native_protocol_compat.md) | 原生协议版本协商和特性标志 |
| [docs/perf_testing.md](docs/perf_testing.md) | 性能测试方法论和 CI 门控 |
| [CHANGELOG.md](CHANGELOG.md) | 语义化版本变更日志 (v0.1–v1.0) |

---

## 测试

```bash
# 运行所有测试 (共 2,239 个)
cargo test --workspace

# 按 crate 运行
cargo test -p falcon_storage          # 417 测试 (MVCC, WAL, GC, LSM, 索引, TDE, 分区, PITR, CDC, 恢复, 压缩调度)
cargo test -p falcon_cluster          # 485 测试 (复制, 故障转移, Scatter/Gather, 2PC, Epoch, 迁移, Supervisor, 限流)
cargo test -p falcon_server           # 372 测试 (SQL 端到端, 错误路径, SHOW 命令)
cargo test -p falcon_common           # 246 测试 (错误模型, 配置, RBAC, RoleCatalog, PrivilegeManager, Decimal, RLS)
cargo test -p falcon_executor         # 162 测试 (Governor v2, 优先级调度, 向量化, RBAC 执行)
cargo test -p falcon_sql_frontend     # 148 测试 (绑定器, 谓词规范化, 参数推断)
cargo test -p falcon_protocol_pg      # 180 测试 (SHOW 命令, 错误路径, 事务生命周期, Handler)
cargo test -p falcon_planner          # 89 测试 (路由提示, 分布式包装, 分片键推断)
cargo test -p falcon_txn              # 61 测试 (事务生命周期, OCC, 统计, READ ONLY, 超时, 执行摘要)
cargo test -p falcon_protocol_native  # 39 测试 (原生协议编解码, 压缩, 类型映射)
cargo test -p falcon_native_server    # 28 测试 (服务器, 会话, 执行器桥接, Nonce 防重放)
cargo test -p falcon_raft             # 12 测试 (共识 trait, 单节点桩)
cargo test --test integration_test    # 12 测试 (根集成: DDL, DML, RETURNING, 事务)

# 代码检查
cargo clippy --workspace       # 必须 0 警告
```

---

## 许可证

Apache-2.0
