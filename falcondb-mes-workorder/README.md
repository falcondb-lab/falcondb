# FalconDB MES — 工单 & 工序执行核心系统

# Manufacturing Execution System — Work Order & Operation Reporting

> **系统一旦返回"报工成功"，这条生产事实就永远不会因为系统故障而消失。**
>
> **Once the system confirms "report submitted", that production fact
> will never be lost — not to crashes, not to failover, not to anything.**

---

## 这个系统是什么？ (What Is This?)

一个简化但真实的 **MES 工单 + 工序报工系统**。

A simplified but real **Manufacturing Execution System** for work order
management and production operation reporting.

它不是数据库演示。它是一个**业务系统**，用 FalconDB 作为唯一数据库，
证明生产事实在任何故障后都不会丢失。

### 核心业务流程

```
工单创建 → 工序开工 → 工序报工 → 工序完成 → 质检通过 → 工单完成

Create    → Start Op  → Report   → Complete  → QA Pass  → Order Done
```

### 业务不变式 (Business Invariants)

| 不变式 | 含义 |
|--------|------|
| **工序状态单向推进** | 完成后不能回退到进行中 |
| **报工数量只增不减** | 累计产量单调递增 |
| **工单完成不可逆** | 标记完成后不能再变 |
| **宕机前后一致** | kill -9 后以上全部成立 |

---

## 快速开始 (Quick Start)

### 前置条件

| 工具 | 用途 |
|------|------|
| **FalconDB** | `cargo build -p falcon_server --release` |
| **psql** | PostgreSQL 客户端 |
| **Python 3.9+** | 后端 API 服务 |
| **pip** | `pip install -r backend/requirements.txt` |
| **curl** | 演示脚本调用 API |

### 一键运行

```bash
# 安装 Python 依赖
pip install -r backend/requirements.txt

# 1. 启动系统（数据库 + API 服务）
./scripts/start_cluster.sh

# 2. 运行完整演示（场景 A + B + C）
./scripts/run_demo.sh

# 3. 模拟宕机
./scripts/kill_primary.sh

# 4. 恢复
./scripts/promote_replica.sh

# 5. 验证：宕机前后数据完全一致
./scripts/verify_business_state.sh

# 清理
./scripts/cleanup.sh --all
```

Windows 用户使用 `.ps1` 版本。

---

## 三个演示场景

### 场景 A：正常生产 (Normal Production)

```
创建工单：电机 A100，计划 1000 台
  ↓
工序 1 (下料): 报工 4×250 = 1000 → 完成
工序 2 (焊接): 报工 4×250 = 1000 → 完成
工序 3 (组装): 报工 4×250 = 1000 → 完成
工序 4 (质检): 报工 4×250 = 1000 → 完成
  ↓
工单完成 ✓

验证结果: PASS
```

### 场景 B：生产中宕机 (Failover During Production)

```
创建工单：变速箱 B200，计划 500 台
  ↓
工序 1-2 完成（各 250 台）
工序 3 进行中（已报工 100 台）
  ↓
📸 记录当前状态: completed_qty=600, reported=600
  ↓
⚡ kill -9 数据库进程
  ↓
🔄 重启恢复
  ↓
📋 对比: completed_qty=600, reported=600

宕机前 = 宕机后 → PASS ✓
```

**这是核心演示：报工成功的数据在宕机后一条都没丢。**

### 场景 C：并发报工 (Concurrent Reporting)

```
10 个终端同时提交报工（各 60 台）
  ↓
预期总量: 600 台
实际总量: 600 台

不重复、不丢失、不回滚 → PASS ✓
```

---

## REST API

系统启动后，访问 `http://localhost:8000/docs` 查看完整 API 文档 (Swagger UI)。

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/work-orders` | 创建工单 + 工序 |
| `GET` | `/api/work-orders` | 查询所有工单 |
| `GET` | `/api/work-orders/{id}` | 查询工单详情 |
| `GET` | `/api/work-orders/{id}/operations` | 查询工序列表 |
| `POST` | `/api/operations/{id}/start` | 开工 |
| `POST` | `/api/operations/{id}/report` | **报工**（核心操作） |
| `POST` | `/api/operations/{id}/complete` | 完工 |
| `POST` | `/api/work-orders/{id}/complete` | 工单完成 |
| `GET` | `/api/work-orders/{id}/reports` | 查询报工记录 |
| `GET` | `/api/work-orders/{id}/state-log` | 工单状态审计 |
| `GET` | `/api/work-orders/{id}/verify` | **业务不变式验证** |

### 报工示例

```bash
curl -X POST http://localhost:8000/api/operations/1/report \
  -H "Content-Type: application/json" \
  -d '{"report_qty": 200, "reported_by": "张工"}'
```

```json
{
  "report_id": 1,
  "work_order_id": 1,
  "operation_id": 1,
  "report_qty": 200,
  "reported_by": "张工",
  "reported_at": "2026-03-01T14:23:15"
}
```

---

## 验证输出 (Verification Output)

```
══════════════════════════════════════════════════════
  FalconDB MES — 业务状态验证报告
══════════════════════════════════════════════════════

  工单 #2 (GEARBOX-B200)
  ────────────────────────────────────
  计划产量 (Planned):    500
  已报工产量 (Reported): 600
  已完成产量 (Completed):600
  工单状态 (Status):     IN_PROGRESS
  验证结果 (Result):     PASS

  Failover Comparison: IDENTICAL
  reported_qty: 600 → 600 (no change)
  completed_qty: 600 → 600 (no change)
  status: IN_PROGRESS → IN_PROGRESS (no change)

  ══════════════════════════════════════════════════
  最终结果 (Final Result): PASS
  所有生产事实在宕机后完整保留。
  All production facts survived the crash.
  ══════════════════════════════════════════════════
```

---

## 数据模型

### 四张核心表

```
work_order (生产工单)
  work_order_id, product_code, planned_qty, completed_qty, status, created_at

operation (工序定义)
  operation_id, work_order_id, seq_no, operation_name, status
  约束: 同一工单下 seq_no 唯一，只能按顺序推进

operation_report (报工事实账本 — 系统灵魂表)
  report_id, work_order_id, operation_id, report_qty, reported_by, reported_at
  规则: 只允许 INSERT，不允许 UPDATE/DELETE

work_order_state_log (工单状态变更历史)
  event_id, work_order_id, from_status, to_status, event_time
```

### 为什么 operation_report 是"灵魂表"？

因为每一条记录代表一个**不可否认的生产事实**：

> "某个操作员，在某个时间，在某道工序上，完成了多少产量。"

这条记录一旦写入，就不能修改、不能删除。
就像会计账本：只记录，不涂改。

---

## FalconDB 的作用

FalconDB 是这个系统的**唯一数据库**。

### 使用方式

- 所有写操作使用**显式事务** + **同步提交**
- 没有应用层缓存绕过数据库
- 没有"先返回成功、后异步落库"
- 每次报工 = 一个独立的事务

### 为什么选 FalconDB？

FalconDB 的**确定性提交保证 (DCG)** 意味着：

当系统返回"报工成功"时，这条记录已经被持久化。
即使数据库进程在下一毫秒被 kill -9 终止，
重启后这条记录仍然存在。

**对生产系统来说，这就是"可信"的定义。**

---

## 目录结构

```
falcondb-mes-workorder/
├── README.md                                ← 你在这里
├── schema/
│   └── init.sql                             ← 4 张表 + 约束 + 索引
├── backend/
│   ├── requirements.txt                     ← Python 依赖
│   ├── app.py                               ← FastAPI REST 服务
│   ├── models.py                            ← 请求/响应模型
│   ├── db.py                                ← 数据库连接层
│   └── invariants.py                        ← 业务不变式验证
├── scripts/
│   ├── start_cluster.sh / .ps1              ← 启动系统
│   ├── run_demo.sh / .ps1                   ← 运行完整演示
│   ├── kill_primary.sh / .ps1               ← 模拟宕机
│   ├── promote_replica.sh / .ps1            ← 恢复
│   ├── verify_business_state.sh / .ps1      ← 验证业务状态
│   └── cleanup.sh / .ps1                    ← 清理
├── output/                                  ← 验证结果
│   └── verification_report.txt
└── docs/
    └── explanation_for_customers.md          ← 给非技术人员的说明
```

---

## 成功标准 (Definition of Done)

| 条件 | 状态 |
|------|------|
| 可以在真实机器上运行 | ✅ |
| 可以现场 kill 数据库进程 | ✅ |
| Failover 后业务结果不变 | ✅ |
| 非技术人员能理解演示结果 | ✅ |
| 不使用数据库术语解释价值 | ✅ |

---

## 禁止出现的术语 (Forbidden Terms)

本系统的所有用户可见输出中不使用以下数据库术语：

- ~~WAL~~ → "永久记忆" / "事务日志"
- ~~MVCC~~ → 不提及
- ~~Raft~~ → 不提及
- ~~共识算法~~ → 不提及
- ~~LSN~~ → 不提及

**FalconDB 的价值不靠"数据库术语"，而靠"业务事实"。**

---

## 详细说明

更详细的非技术解释，请参阅 [docs/explanation_for_customers.md](docs/explanation_for_customers.md)。

---

## 配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `FALCON_BIN` | `target/release/falcon_server` | FalconDB 二进制路径 |
| `FALCON_HOST` | `127.0.0.1` | 数据库主机 |
| `FALCON_PORT` | `5433` | 数据库端口 |
| `FALCON_DB` | `mes_prod` | 数据库名 |
| `FALCON_USER` | `falcon` | 数据库用户 |
