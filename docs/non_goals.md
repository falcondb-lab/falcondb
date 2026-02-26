# FalconDB — Non-Goals & Scope Declaration

> **Purpose**: Clearly state what FalconDB is NOT, so that CTOs, investors, and evaluators
> can quickly determine fit — and so that FalconDB is never mistakenly positioned as
> something it isn't.

---

## The One-Sentence Position

**FalconDB is a deterministic-commit OLTP transaction engine.** It guarantees that confirmed
writes survive crashes and failovers. It is not a general-purpose database platform.

---

## What FalconDB IS

| Property | Description |
|----------|-------------|
| **OLTP-only** | Optimized for high-frequency, low-latency transactional reads and writes |
| **Memory-first** | Primary data store is in-memory with WAL persistence for crash safety |
| **PG wire compatible** | Speaks PostgreSQL protocol — works with `psql`, `pgbench`, JDBC, standard ORMs |
| **Deterministic commit** | Once the client receives "committed", that data survives any single-node crash or failover |
| **Distributed** | Hash-sharded across nodes with WAL-shipping replication and automatic failover |

---

## What FalconDB is NOT

### 1. Not an HTAP / OLAP / Analytics Engine

FalconDB does not optimize for:
- Full table scans over millions of rows
- Columnar storage or vectorized execution
- Star schema / snowflake schema analytical queries
- Data warehouse workloads (ETL targets)
- BI tool integration (Tableau, Looker, Superset)

**Why**: Analytical workloads require columnar storage, batch-oriented execution, and
compression strategies that conflict with FalconDB's low-latency row-store design.
Adding OLAP would dilute the OLTP optimization and introduce complexity with no benefit
to our target users.

**Use instead**: ClickHouse, DuckDB, Snowflake, BigQuery, Apache Doris.

### 2. Not an Eventually-Consistent / AP System

FalconDB does not offer:
- Eventual consistency modes
- Last-write-wins conflict resolution
- CRDTs or merge-based replication
- Multi-master writes without coordination
- Tunable consistency (à la Cassandra)

**Why**: FalconDB's core value proposition is *deterministic commit* — "success means
success, forever." Offering weaker consistency modes would undermine the entire trust model
and create configuration foot-guns for users.

**Use instead**: Cassandra, DynamoDB, CouchDB, Riak.

### 3. Not a Full PostgreSQL Replacement

FalconDB does not support:
- Stored procedures / PL/pgSQL
- Triggers
- Materialized views
- Foreign data wrappers (FDW)
- Full-text search (tsvector / tsquery)
- Custom types (beyond JSONB)
- PostGIS / spatial extensions
- Logical replication slots for external consumers
- `pg_dump` / `pg_restore` binary compatibility

**Why**: Full PG compatibility requires implementing the entire PG extension ecosystem,
which is a multi-year, multi-team effort that distracts from the transaction engine.
FalconDB targets the **SQL subset that OLTP applications actually use** (INSERT, UPDATE,
DELETE, SELECT by PK/index, JOINs, transactions).

**Use instead**: PostgreSQL, Aurora PostgreSQL, AlloyDB.

### 4. Not a General-Purpose Data Platform

FalconDB does not provide:
- Built-in streaming / CDC (Kafka Connect, Debezium)
- Built-in caching layer (Redis replacement)
- Built-in search (Elasticsearch replacement)
- Built-in graph queries (Neo4j replacement)
- Machine learning / AI integration
- Multi-model storage

**Why**: Bundling non-transactional features into a transaction engine creates operational
complexity and attack surface. FalconDB stays in its lane.

**Use instead**: Purpose-built tools for each concern, connected via standard PG protocol.

### 5. Not Optimized for Datasets Larger Than RAM

FalconDB's default (and production) storage engine is an in-memory row store.

- Data size is bounded by available RAM (per shard)
- The experimental LSM engine exists for disk-backed tables but is not production-hardened
- There is no automatic tiering / spill-to-disk in the default engine
- Memory pressure triggers admission control (reject new writes), not transparent paging

**Why**: In-memory storage is what enables sub-millisecond commit latency. Transparent
paging would introduce unpredictable latency spikes that violate FalconDB's tail-latency
guarantees.

**Use instead**: PostgreSQL, CockroachDB, TiDB for datasets exceeding available RAM.

### 6. Not a Multi-Region / Geo-Distributed Database

FalconDB does not currently provide:
- Cross-region replication with bounded staleness
- Geo-partitioning (pin data to a region)
- Global transaction coordination across datacenters
- WAN-optimized replication protocol

**Why**: Geo-distribution introduces fundamental latency constraints (speed of light) that
conflict with FalconDB's low-latency commitment. Adding it would require a fundamentally
different replication and consensus architecture.

**Use instead**: CockroachDB, YugabyteDB, Spanner.

---

## Scope Boundary Summary

| Dimension | In Scope | Out of Scope |
|-----------|----------|--------------|
| **Workload** | OLTP (point reads, small txns, <100ms) | OLAP, ETL, batch, streaming |
| **Data size** | Fits in RAM (per-shard, up to ~64 GB) | Larger-than-RAM datasets |
| **Consistency** | Strong (Snapshot Isolation, deterministic commit) | Eventual, tunable, causal |
| **SQL** | DML + common DDL + 500+ scalar functions | Stored procs, triggers, extensions |
| **Deployment** | Single-region, 1–N nodes, same datacenter | Multi-region, geo-distributed |
| **Protocol** | PostgreSQL wire (Simple + Extended Query) | MySQL, MongoDB, custom wire |
| **Durability** | WAL fsync (configurable per-policy) | Synchronous disk + replicated storage |

---

## Why Non-Goals Matter

Stating non-goals is not a weakness — it is a **trust signal**.

1. **For investors**: A database that claims to do everything is a database that does nothing well.
   FalconDB's focus on deterministic OLTP is a defensible moat, not a limitation.

2. **For CTOs evaluating a PoC**: Knowing what FalconDB *won't* do saves weeks of evaluation
   time. If your workload is analytical, FalconDB is the wrong tool — and we'll tell you that
   upfront rather than let you discover it in production.

3. **For engineers**: Clear boundaries prevent scope creep and keep the codebase focused.
   Every feature added to FalconDB must pass the test: "Does this make deterministic OLTP
   transactions better?"

---

## How to Determine if FalconDB is Right for Your Use Case

**Good fit** ✅:
- Financial transaction processing (transfers, settlements, ledgers)
- Order management systems (state machines with strict consistency)
- Session stores with transactional guarantees
- Any workload where "the database said committed but the data is gone" is unacceptable

**Poor fit** ❌:
- Data warehousing / BI dashboards
- Log aggregation / time-series storage
- Content management / full-text search
- IoT telemetry ingestion (high volume, low consistency requirement)
- Machine learning feature stores

---

*This document is versioned with the codebase. Non-goals may evolve as FalconDB matures,
but the core position — deterministic OLTP, nothing else — is a permanent architectural decision.*
