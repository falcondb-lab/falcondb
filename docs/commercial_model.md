# FalconDB — Commercial Model & License Boundary

> This document defines what is free, what is paid, and why customers pay.
> It is a binding product decision, not a brainstorm.

---

## 1. License Structure

```
┌─────────────────────────────────────────────────────────┐
│                    FalconDB Core                         │
│                   Apache License 2.0                     │
│                                                          │
│  Storage Engine · MVCC · WAL · Replication · PG Wire     │
│  Backpressure · GC · Basic Monitoring · CLI              │
│                                                          │
│  100% open source. Forever.                              │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              FalconDB Financial Edition                   │
│              Commercial License (per-node/year)          │
│                                                          │
│  Ops Console · Advanced Replication · Compliance Pack    │
│  Priority Support · SLA Guarantee · Industry Configs     │
│                                                          │
│  Source-available. Requires license key.                  │
└─────────────────────────────────────────────────────────┘
```

## 2. Core (Apache 2.0) — Always Free

Everything a developer needs to build, test, and run FalconDB in production:

| Component | Included |
|-----------|----------|
| **Storage engine** | Memory-first MVCC, OCC, GC |
| **WAL** | Group commit, fdatasync, checkpoint |
| **Replication** | Async/sync streaming, promote, catch-up |
| **PG wire protocol** | libpq, JDBC, psycopg2 compatible |
| **Backpressure** | 4-tier governor (None/Soft/Hard/Emergency) |
| **Prometheus metrics** | 60+ built-in metrics |
| **Health endpoints** | `/health`, `/ready`, `/status` |
| **CLI** | `falcon` binary: start, version, config check |
| **Benchmarks** | Reproducible FalconDB vs PG comparison suite |
| **Documentation** | All architecture, ops, and API docs |

**Commitment**: The core will never be relicensed. Apache 2.0 is permanent.

## 3. Financial Edition (Commercial) — Paid

Features that matter to production financial deployments at scale:

### 3.1 Operations Console

| Feature | Description | Status |
|---------|------------|--------|
| **Web dashboard** | Real-time cluster health, memory, replication | Planned |
| **Alert management** | Configurable alert rules with email/Slack/PagerDuty | Planned |
| **Topology view** | Visual primary–replica–DR topology | Planned |
| **Config management** | Centralized config push to all nodes | Planned |
| **Audit log viewer** | Searchable, filterable audit trail | Planned |

### 3.2 Advanced Replication

| Feature | Description | Status |
|---------|------------|--------|
| **Multi-site DR** | Synchronous replication across data centers | Planned |
| **Cascading replicas** | Primary → Replica → Replica chain | Planned |
| **Read replicas** | Load-balanced read routing | Planned |
| **Online schema change** | Zero-downtime DDL with replication | Planned |

### 3.3 Compliance & Audit Pack

| Feature | Description | Status |
|---------|------------|--------|
| **Tamper-proof audit log** | HMAC-chained, SIEM-exportable | Built (v1.1) |
| **Data encryption at rest** | AES-256 with key rotation | Planned |
| **TLS certificate management** | Auto-rotation, expiry alerts | Built (v1.1) |
| **Regulatory report templates** | Pre-built for SEC, FCA, MAS | Planned |
| **Data retention policies** | Configurable TTL with audit | Planned |

### 3.4 Priority Support

| Tier | Response SLA | Includes |
|------|-------------|----------|
| **Standard** | 8h business hours | Email, knowledge base |
| **Premium** | 4h 24/7 | Slack, phone, dedicated engineer |
| **Critical** | 1h 24/7 | On-call, root cause analysis, hotfix |

## 4. Pricing Model

| Edition | Price | Billing |
|---------|-------|---------|
| **Core** | $0 | Forever free |
| **Financial Edition** | $X,000/node/year | Annual subscription |
| **Premium Support** | $X0,000/year | Annual contract |
| **PoC License** | $0 (30 days) | Evaluation |

*Exact pricing TBD based on market validation. Placeholder ranges:*
- *Financial Edition: $5K–$20K/node/year (vs Oracle $47.5K/core)*
- *Premium Support: $50K–$200K/year depending on cluster size*

## 5. Why Customers Pay

### 5.1 The Core Is Sufficient for Development

A trading desk engineer can:
- Build and test against FalconDB Core
- Run single-node production with full OLTP capability
- Use all benchmarks and evidence to validate fit

### 5.2 Financial Edition Is Required for Production at Scale

A production financial deployment needs:
- **Ops Console**: Because the CTO won't approve a database without a dashboard
- **Multi-site DR**: Because regulators require geographic redundancy
- **Compliance Pack**: Because auditors need tamper-proof logs and encryption
- **Priority Support**: Because a 3AM trading system outage needs a human, not a GitHub issue

### 5.3 The Value Equation

```
Oracle RAC (100 cores):     $4.75M license + $1.05M/year = $9.5M / 5 years
FalconDB Financial (10 nodes): $100K–$200K/year          = $0.5M–$1M / 5 years

Savings: $8.5M–$9M over 5 years
         + no vendor lock-in
         + PG wire compatible (migration cost ≈ 0)
```

## 6. What We Will NEVER Charge For

| Component | Reason |
|-----------|--------|
| Core storage engine | Foundation of trust; Apache 2.0 forever |
| Basic replication | Users must be able to run HA without paying |
| Prometheus metrics | Observability is a right, not a premium feature |
| PG wire protocol | Compatibility is a moat, not a paywall |
| Documentation | Knowledge should be free |
| Community support (GitHub) | Open source social contract |

## 7. Revenue Projections (Illustrative)

| Year | Customers | ARR/Customer | Total ARR |
|------|-----------|-------------|-----------|
| Y1 | 3 PoC → 1 paid | $100K | $100K |
| Y2 | 5 paid | $150K avg | $750K |
| Y3 | 15 paid | $200K avg | $3M |
| Y4 | 30 paid | $250K avg | $7.5M |

**Path to $10M ARR**: 40 financial institutions × $250K/year average.

## 8. Competitive Moat

| Moat | Description |
|------|------------|
| **Architecture** | Memory-first OLTP; not a feature PostgreSQL can bolt on |
| **Evidence** | Deterministic failover proven (not claimed); SLA backed by reproducible tests |
| **Switching cost** | PG wire means zero switching cost IN, high switching cost OUT (once in production) |
| **Industry depth** | Financial-specific defaults, compliance, and PoC playbook |
| **Open core trust** | Core is Apache 2.0; customers don't fear re-licensing |
