# SQL & Protocol Boundary — GA Normative Reference

> **This document is normative.** Every supported feature has tests.
> Every unsupported feature returns an explicit error with SQLSTATE `0A000`.

---

## 1. Supported SQL (GA Whitelist)

### 1.1 DML

| Feature | Syntax | Test Reference |
|---------|--------|----------------|
| SELECT | `SELECT col, expr FROM t WHERE ...` | handler tests (232+) |
| INSERT | `INSERT INTO t (cols) VALUES (...)` | handler tests |
| UPDATE | `UPDATE t SET col = expr WHERE ...` | handler tests |
| DELETE | `DELETE FROM t WHERE ...` | handler tests |
| UPSERT | `INSERT ... ON CONFLICT (col) DO UPDATE SET ...` | `test_sql_upsert_on_conflict` |
| RETURNING | `INSERT/UPDATE/DELETE ... RETURNING col` | `test_sql_update_returning`, `test_sql_delete_returning` |
| Batch INSERT | `INSERT INTO t VALUES (...), (...), (...)` | handler tests |

### 1.2 Query Features

| Feature | Syntax | Test Reference |
|---------|--------|----------------|
| INNER JOIN | `SELECT ... FROM a JOIN b ON ...` | `test_sql_join_inner` |
| LEFT JOIN | `SELECT ... FROM a LEFT JOIN b ON ...` | `test_sql_join_left` |
| GROUP BY | `SELECT col, COUNT(*) FROM t GROUP BY col` | `test_sql_group_by_aggregate` |
| Aggregates | `COUNT, SUM, AVG, MIN, MAX` | `test_sql_group_by_aggregate` |
| ORDER BY | `SELECT ... ORDER BY col ASC/DESC` | `test_sql_order_by_limit` |
| LIMIT / OFFSET | `SELECT ... LIMIT n OFFSET m` | `test_sql_order_by_limit` |
| Subqueries | `SELECT ... WHERE col IN (SELECT ...)` | handler tests |
| WHERE predicates | `=, <>, <, >, <=, >=, AND, OR, NOT, IS NULL, LIKE, IN, BETWEEN` | handler tests |
| Aliases | `SELECT col AS alias FROM t AS alias` | handler tests |
| DISTINCT | `SELECT DISTINCT col FROM t` | handler tests |
| CASE/WHEN | `CASE WHEN cond THEN val ELSE val END` | handler tests |
| CAST | `CAST(expr AS type)` | handler tests |
| String functions | `UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, REPLACE` | handler tests |
| Math functions | `ABS, CEIL, FLOOR, ROUND, MOD` | handler tests |
| Date functions | `NOW(), CURRENT_TIMESTAMP, EXTRACT` | handler tests |
| COALESCE / NULLIF | `COALESCE(a, b)`, `NULLIF(a, b)` | handler tests |
| EXISTS | `WHERE EXISTS (SELECT ...)` | handler tests |

### 1.3 DDL

| Feature | Syntax | Test Reference |
|---------|--------|----------------|
| CREATE TABLE | `CREATE TABLE t (col type constraints)` | handler tests |
| DROP TABLE | `DROP TABLE [IF EXISTS] t` | handler tests |
| ALTER TABLE ADD COLUMN | `ALTER TABLE t ADD COLUMN col type` | handler tests |
| ALTER TABLE DROP COLUMN | `ALTER TABLE t DROP COLUMN col` | handler tests |
| ALTER TABLE ADD CONSTRAINT | `ALTER TABLE t ADD CONSTRAINT ...` | handler tests |
| CREATE INDEX | `CREATE INDEX idx ON t (col)` | handler tests |
| DROP INDEX | `DROP INDEX idx` | handler tests |
| CREATE VIEW | `CREATE VIEW v AS SELECT ...` | handler tests |
| DROP VIEW | `DROP VIEW v` | handler tests |
| CREATE DATABASE | `CREATE DATABASE db` | handler tests |
| DROP DATABASE | `DROP DATABASE db` | handler tests |
| CREATE SCHEMA | `CREATE SCHEMA s` | handler tests |
| DROP SCHEMA | `DROP SCHEMA s` | handler tests |

### 1.4 DCL / Role Management

| Feature | Syntax | Test Reference |
|---------|--------|----------------|
| CREATE ROLE | `CREATE ROLE name [LOGIN] [PASSWORD 'x'] [SUPERUSER]` | binder tests |
| ALTER ROLE | `ALTER ROLE name SET option` | binder tests |
| DROP ROLE | `DROP ROLE name` | binder tests |
| GRANT | `GRANT privilege ON object TO role` | binder tests |
| REVOKE | `REVOKE privilege ON object FROM role` | binder tests |

### 1.5 Transaction Control

| Feature | Syntax | Test Reference |
|---------|--------|----------------|
| BEGIN | `BEGIN [ISOLATION LEVEL ...]` | handler tests |
| COMMIT | `COMMIT` | handler tests |
| ROLLBACK | `ROLLBACK` | handler tests |
| Autocommit | Implicit per-statement | handler tests |
| Snapshot Isolation | `BEGIN ISOLATION LEVEL REPEATABLE READ` | `si_litmus_tests` (35 tests) |

### 1.6 Constraints

| Feature | Description |
|---------|-------------|
| PRIMARY KEY | Enforced, single/composite |
| NOT NULL | Enforced |
| UNIQUE | Enforced |
| CHECK | Enforced (SQLSTATE `23514`) |
| FOREIGN KEY | Parsed, enforcement TBD |
| DEFAULT | Supported |

### 1.7 Data Types

| Type | PG OID | Notes |
|------|--------|-------|
| `INT` / `INTEGER` / `INT4` | 23 | 32-bit signed |
| `BIGINT` / `INT8` | 20 | 64-bit signed |
| `REAL` / `FLOAT4` | 700 | 32-bit IEEE 754 |
| `DOUBLE PRECISION` / `FLOAT8` | 701 | 64-bit IEEE 754 |
| `TEXT` / `VARCHAR` | 25 | Variable-length UTF-8 |
| `BOOLEAN` | 16 | true/false |
| `TIMESTAMP` | 1114 | Microsecond precision |
| `DATE` | 1082 | Calendar date |
| `SERIAL` / `BIGSERIAL` | — | Auto-increment |
| `BYTEA` | 17 | Binary data |

### 1.8 Protocol

| Feature | Status |
|---------|--------|
| Simple Query protocol | Supported |
| Extended Query protocol (Parse/Bind/Execute) | Supported |
| COPY protocol | Not supported (SQLSTATE `0A000`) |
| Notification / LISTEN/NOTIFY | Not supported (SQLSTATE `0A000`) |

---

## 2. Not Supported (GA)

Every unsupported feature returns **SQLSTATE `0A000`** (`feature_not_supported`)
with a clear error message. No silent ignore.

| Feature | SQLSTATE | Error Message | Test |
|---------|----------|---------------|------|
| `CREATE TRIGGER` | `0A000` | `Unsupported feature: CREATE TRIGGER` | `test_unsupported_create_trigger_error` |
| `CREATE FUNCTION` / `CREATE PROCEDURE` | `0A000` | `Unsupported feature: CREATE FUNCTION` | `test_unsupported_create_function_error` |
| `CREATE EXTENSION` | `0A000` | `Unsupported feature: ...` | — |
| Stored procedures (`DO $$...$$`) | `0A000` | `Unsupported feature: ...` | — |
| `CREATE TYPE` (composite/enum) | `0A000` | `Unsupported feature: ...` | — |
| `LISTEN` / `NOTIFY` | `0A000` | `Unsupported feature: ...` | — |
| `COPY ... FROM/TO` | `0A000` | `Unsupported feature: ...` | — |
| Window functions (`OVER(...)`) | `0A000` | `Unsupported feature: ...` | — |
| CTEs (`WITH ... AS`) | `0A000` | `Unsupported feature: ...` | — |
| Materialized Views | `0A000` | `Unsupported feature: ...` | — |
| Full-text search (`tsvector`, `tsquery`) | `0A000` | `Unsupported feature: ...` | — |
| JSON/JSONB operators | `0A000` | `Unsupported feature: ...` | — |
| Array operators | `0A000` | `Unsupported feature: ...` | — |
| `SAVEPOINT` / `RELEASE` | `0A000` | `Unsupported feature: ...` | — |
| `PREPARE TRANSACTION` (2PC via SQL) | `0A000` | `Unsupported feature: ...` | — |
| Table inheritance | `0A000` | `Unsupported feature: ...` | — |
| Partitioned tables (declarative) | `0A000` | `Unsupported feature: ...` | — |
| Advisory locks | `0A000` | `Unsupported feature: ...` | — |
| Row-level security | `0A000` | `Unsupported feature: ...` | — |
| Logical replication slots | `0A000` | `Unsupported feature: ...` | — |
| `pg_catalog` system tables | Partial | Some `pg_type`, `pg_class` stubs for JDBC compatibility |

---

## 3. Error Classification

All errors follow the PostgreSQL SQLSTATE convention:

| Class | Range | Meaning | Example |
|-------|-------|---------|---------|
| `00` | Success | — | `00000` |
| `0A` | Feature not supported | Unsupported SQL | `0A000` |
| `22` | Data exception | Division by zero, overflow | `22012` |
| `23` | Integrity constraint | PK/UNIQUE/CHECK/FK violation | `23505`, `23514` |
| `25` | Invalid transaction state | Read-only violation | `25006` |
| `28` | Invalid authorization | Auth failed | `28P01` |
| `40` | Transaction rollback | Serialization failure, deadlock | `40001` |
| `42` | Syntax/access error | Parse error, unknown table | `42601`, `42P01` |
| `53` | Insufficient resources | OOM, WAL backlog, connection limit | `53200`, `53300` |
| `57` | Operator intervention | Replication lag, shutdown | `57P03` |
| `08` | Connection exception | Lost connection, protocol error | `08006`, `08P01` |
| `XX` | Internal error | Bug (should never occur) | `XX000` |

**Code**: `falcon_common::error::FalconError::pg_sqlstate()`

---

## 4. Invariants

| ID | Invariant |
|----|-----------|
| **SQL-1** | Every unsupported feature returns SQLSTATE `0A000`. No silent ignore, no silent truncation. |
| **SQL-2** | Every constraint violation returns the appropriate SQLSTATE (`23xxx`). |
| **SQL-3** | Every resource exhaustion returns SQLSTATE `53xxx` with retry guidance. |
| **SQL-4** | The supported SQL list above is the complete GA whitelist. Anything not listed is either unsupported (`0A000`) or a bug. |
