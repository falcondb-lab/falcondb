# SQL Compatibility Matrix — FalconDB v1.0.4

> **Status**: Frozen. This document is normative. Behavior not listed here is
> explicitly unsupported and MUST fail fast with SQLSTATE `0A000`.

---

## 1. Supported SQL Statements

| Category | Statement | Notes |
|----------|-----------|-------|
| **DDL** | `CREATE TABLE` | Column types, PK, UNIQUE, NOT NULL, CHECK, DEFAULT, FK |
| | `DROP TABLE` | Cascade not supported |
| | `TRUNCATE TABLE` | |
| | `ALTER TABLE ADD COLUMN` | |
| | `ALTER TABLE DROP COLUMN` | |
| | `CREATE INDEX` | B-tree only |
| | `DROP INDEX` | |
| **DML** | `INSERT INTO ... VALUES` | Single and multi-row |
| | `INSERT INTO ... SELECT` | |
| | `INSERT ... ON CONFLICT DO NOTHING` | |
| | `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) | |
| | `UPDATE ... SET ... WHERE` | |
| | `DELETE FROM ... WHERE` | |
| | `UPDATE ... RETURNING` | |
| | `DELETE ... RETURNING` | |
| **Query** | `SELECT` | Columns, expressions, aliases |
| | `WHERE` | All comparison operators |
| | `ORDER BY` | ASC/DESC, NULLS FIRST/LAST |
| | `LIMIT` / `OFFSET` | |
| | `GROUP BY` | Column refs and expressions |
| | `HAVING` | |
| | `DISTINCT` | |
| | `INNER JOIN` | |
| | `LEFT JOIN` / `LEFT OUTER JOIN` | |
| | `RIGHT JOIN` / `RIGHT OUTER JOIN` | |
| | `CROSS JOIN` | |
| | `Subqueries` | Scalar, EXISTS, IN |
| | `UNION` / `UNION ALL` | |
| | `CASE WHEN ... THEN ... ELSE ... END` | |
| | `CAST(expr AS type)` | |
| | `COALESCE` | |
| | `NULLIF` | |
| **Aggregates** | `COUNT(*)`, `COUNT(expr)` | |
| | `SUM`, `AVG`, `MIN`, `MAX` | |
| **Txn** | `BEGIN` / `START TRANSACTION` | |
| | `COMMIT` | |
| | `ROLLBACK` | |
| | `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` | |
| | `SET TRANSACTION ISOLATION LEVEL` | RC, SI, Serializable |
| | `SET TRANSACTION READ ONLY` / `READ WRITE` | |
| **Prepared** | `PREPARE name AS ...` | |
| | `EXECUTE name (params)` | |
| | `DEALLOCATE name` | |
| **Copy** | `COPY ... FROM STDIN` | CSV format |
| **System** | `SHOW` | falcon.* namespace |
| | `SET` / `RESET` | Session variables |
| | `EXPLAIN` / `EXPLAIN ANALYZE` | |
| | `DISCARD ALL` | |

## 2. Supported Data Types

| Type | PG OID | Notes |
|------|--------|-------|
| `BOOLEAN` | 16 | |
| `INT` / `INTEGER` / `INT4` | 23 | 32-bit |
| `BIGINT` / `INT8` | 20 | 64-bit |
| `SMALLINT` / `INT2` | 21 | Stored as INT4 |
| `FLOAT` / `FLOAT8` / `DOUBLE PRECISION` | 701 | 64-bit IEEE 754 |
| `REAL` / `FLOAT4` | 700 | Stored as FLOAT8 |
| `NUMERIC` / `DECIMAL` | 1700 | Arbitrary precision |
| `TEXT` | 25 | |
| `VARCHAR(n)` | 1043 | Length enforced |
| `CHAR(n)` | 1042 | Padded |
| `BYTEA` | 17 | |
| `DATE` | 1082 | |
| `TIME` | 1083 | Without timezone |
| `TIMESTAMP` | 1114 | Without timezone |
| `TIMESTAMPTZ` | 1184 | With timezone |
| `INTERVAL` | 1186 | |
| `UUID` | 2950 | |
| `JSONB` | 3802 | |
| `BOOLEAN[]`, `INT[]`, `TEXT[]` | — | 1-D arrays |
| `SERIAL` / `BIGSERIAL` | — | Auto-increment sugar |

## 3. Supported Operators

| Category | Operators |
|----------|-----------|
| **Comparison** | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=` |
| **Logical** | `AND`, `OR`, `NOT` |
| **Arithmetic** | `+`, `-`, `*`, `/`, `%` |
| **String** | `\|\|` (concat), `LIKE`, `ILIKE`, `~`, `~*` |
| **NULL** | `IS NULL`, `IS NOT NULL`, `COALESCE`, `NULLIF` |
| **Range** | `BETWEEN ... AND ...`, `IN (...)` |
| **Array** | `@>`, `<@`, `&&`, `\|\|` |
| **JSON** | `->`, `->>`, `#>`, `#>>` |

## 4. Supported Built-in Functions

| Category | Functions |
|----------|-----------|
| **String** | `length`, `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `substring`, `replace`, `concat`, `concat_ws`, `left`, `right`, `repeat`, `reverse`, `position`, `split_part`, `starts_with`, `encode`, `decode`, `md5`, `chr`, `ascii` |
| **Math** | `abs`, `ceil`, `floor`, `round`, `trunc`, `sqrt`, `power`, `mod`, `sign`, `random`, `greatest`, `least`, `pi`, `log`, `ln`, `exp` |
| **Date/Time** | `now()`, `current_timestamp`, `current_date`, `current_time`, `extract`, `date_trunc`, `date_part`, `age`, `to_char`, `to_timestamp`, `to_date` |
| **Type Cast** | `CAST(x AS type)`, `x::type` |
| **Aggregate** | `count`, `sum`, `avg`, `min`, `max`, `string_agg`, `array_agg`, `bool_and`, `bool_or` |
| **JSON** | `jsonb_extract_path`, `jsonb_extract_path_text`, `jsonb_typeof`, `jsonb_array_length`, `jsonb_object_keys`, `jsonb_build_object`, `jsonb_build_array` |
| **Array** | `array_length`, `array_upper`, `array_lower`, `unnest`, `array_append`, `array_remove`, `array_position` |
| **System** | `version()`, `current_user`, `current_database`, `pg_backend_pid`, `txid_current` |

## 5. Explicitly Unsupported (Rejected with SQLSTATE)

| Feature | SQLSTATE | Error Message |
|---------|----------|---------------|
| `CREATE TRIGGER` | `0A000` | Feature not supported: triggers |
| `CREATE FUNCTION` / `CREATE PROCEDURE` | `0A000` | Feature not supported: user-defined functions |
| `CREATE VIEW` | `0A000` | Feature not supported: views |
| `CREATE MATERIALIZED VIEW` | `0A000` | Feature not supported: materialized views |
| `CREATE EXTENSION` | `0A000` | Feature not supported: extensions |
| `LISTEN` / `NOTIFY` | `0A000` | Feature not supported: async notifications |
| `CURSOR` (server-side) | `0A000` | Feature not supported: server-side cursors |
| `FULL OUTER JOIN` | `0A000` | Feature not supported: full outer join |
| `LATERAL JOIN` | `0A000` | Feature not supported: lateral join |
| `WINDOW FUNCTIONS` (partial) | `0A000` | Feature not supported: window functions |
| `CTE` / `WITH` (recursive) | `0A000` | Feature not supported: recursive CTE |
| `GRANT` / `REVOKE` (SQL-level) | `0A000` | Feature not supported: SQL-level grants |
| `ALTER TABLE ADD CONSTRAINT` (post-create) | `0A000` | Feature not supported: add constraint after create |
| `VACUUM` / `ANALYZE` | `0A000` | Feature not supported: manual vacuum |
| `REINDEX` | `0A000` | Feature not supported: reindex |
| `CLUSTER` | `0A000` | Feature not supported: cluster |
| `TABLESPACE` | `0A000` | Feature not supported: tablespaces |
| `SEQUENCE` (explicit) | `0A000` | Feature not supported: explicit sequences |
| `DOMAIN` | `0A000` | Feature not supported: domains |
| `ENUM` types | `0A000` | Feature not supported: enum types |
| `RANGE` types | `0A000` | Feature not supported: range types |
| `COMPOSITE` types | `0A000` | Feature not supported: composite types |
| Multi-statement transactions over multiple TCP messages (pipeline) | — | Supported via extended query protocol |

## 6. Wire Protocol Compatibility

| Feature | Status |
|---------|--------|
| Simple query protocol | Supported |
| Extended query protocol (Parse/Bind/Execute) | Supported |
| `COPY FROM STDIN` (binary) | Not supported (CSV only) |
| SSL/TLS | Supported |
| SCRAM-SHA-256 auth | Supported |
| MD5 auth | Supported |
| `pg_catalog` system tables | Not supported |
| `information_schema` | Not supported |

## 7. Isolation Levels

| Level | Supported | Behavior |
|-------|-----------|----------|
| `READ UNCOMMITTED` | Maps to `READ COMMITTED` | PG-compatible |
| `READ COMMITTED` | Yes | Statement-level snapshot |
| `REPEATABLE READ` | Maps to `SNAPSHOT ISOLATION` | PG-compatible |
| `SERIALIZABLE` | Yes | OCC-based (SSI) |

## 8. Contract

- Any SQL that is **not listed in §1–§4** and is **not explicitly rejected in §5** is an **undefined behavior bug**.
- All unsupported SQL MUST fail with `SQLSTATE 0A000` and a human-readable message.
- No unsupported SQL may cause a panic, hang, or corrupt state.
- This matrix is frozen for v1.0.x. Changes require a minor version bump.
