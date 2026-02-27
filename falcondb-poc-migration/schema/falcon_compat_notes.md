# FalconDB PostgreSQL Compatibility Notes

This document records what works, what doesn't, and what to watch for
when migrating a PostgreSQL OLTP application to FalconDB.

**This is a living document. Update it after every migration attempt.**

---

## Supported SQL Features (Used by This Demo)

| Feature | Status | Notes |
|---------|--------|-------|
| `CREATE TABLE` | ✅ Supported | Including `IF NOT EXISTS` |
| `DROP TABLE` | ✅ Supported | Including `IF EXISTS` |
| `ALTER TABLE ADD COLUMN` | ✅ Supported | |
| `ALTER TABLE DROP COLUMN` | ✅ Supported | |
| `ALTER TABLE RENAME COLUMN` | ✅ Supported | |
| `ALTER TABLE RENAME TO` | ✅ Supported | |
| `ALTER TABLE ALTER COLUMN TYPE` | ✅ Supported | |
| `ALTER TABLE SET NOT NULL` | ✅ Supported | |
| `ALTER TABLE SET DEFAULT` | ✅ Supported | |
| `CREATE INDEX` | ✅ Supported | Single-column indexes |
| `DROP INDEX` | ✅ Supported | |
| `CREATE VIEW` | ✅ Supported | Including `OR REPLACE` |
| `DROP VIEW` | ✅ Supported | |
| `CREATE SEQUENCE` | ✅ Supported | |
| `nextval()` | ✅ Supported | Used in DEFAULT expressions |
| `INSERT` | ✅ Supported | Including `RETURNING`, `ON CONFLICT` |
| `UPDATE` | ✅ Supported | Including `WHERE`, `RETURNING` |
| `DELETE` | ✅ Supported | Including `WHERE` |
| `SELECT` | ✅ Supported | Including `JOIN`, `ORDER BY`, `LIMIT`, subqueries |
| `BEGIN / COMMIT / ROLLBACK` | ✅ Supported | Full ACID transactions |
| `NOW()` | ✅ Supported | |
| `COPY FROM / TO` | ✅ Supported | For bulk data migration |

## Supported Data Types (Used by This Demo)

| Type | Status | Notes |
|------|--------|-------|
| `BIGINT` | ✅ | Primary key, amounts, counts |
| `TEXT` | ✅ | Strings, status fields |
| `TIMESTAMP` | ✅ | `DEFAULT NOW()` works |
| `BOOLEAN` | ✅ | |
| `INT` / `INTEGER` | ✅ | Mapped to Int32 |
| `SMALLINT` | ✅ | |
| `REAL` / `FLOAT` | ✅ | |
| `DATE` | ✅ | |
| `TIME` | ✅ | |
| `INTERVAL` | ✅ | |
| `UUID` | ✅ | |
| `JSONB` | ✅ | |
| `BYTEA` | ✅ | |
| `NUMERIC` / `DECIMAL` | ✅ | |
| `BIGSERIAL` | ✅ | Translated to BIGINT + sequence |
| `SERIAL` | ✅ | Translated to INT + sequence |

---

## Known Incompatibilities

### Not Supported (Will Fail)

| Feature | Impact | Workaround |
|---------|--------|------------|
| `CREATE INDEX CONCURRENTLY` | Medium | Use regular `CREATE INDEX` (blocks writes briefly) |
| `FOREIGN KEY` constraints | Medium | Enforce in application logic; FalconDB parses but does not enforce FK |
| `CHECK` constraints | Low | Enforce in application logic |
| `UNIQUE` constraints (as table constraint) | Low | Use `UNIQUE` index instead |
| `EXCLUDE` constraints | Low | Not common in OLTP apps |
| `TRIGGER` / `CREATE TRIGGER` | High | Must move trigger logic to application |
| `STORED PROCEDURES` (`CREATE FUNCTION` in PL/pgSQL) | High | Must rewrite as application code |
| `LISTEN / NOTIFY` | Medium | Use application-level pub/sub |
| `MATERIALIZED VIEW` | Low | Use regular view or application cache |
| `PARTITION BY` (declarative partitioning) | Medium | Use application-level sharding or FalconDB native sharding |
| `ENUM` types | Low | Use TEXT with application validation |
| `DOMAIN` types | Low | Use base type |
| `EXTENSION` (e.g., pg_trgm, PostGIS) | Varies | Not available; use application-level alternatives |
| `GRANT / REVOKE` (row-level security) | Low | Use FalconDB auth model |
| Multi-column indexes | Low | Create separate single-column indexes |
| Expression indexes | Low | Not supported yet |
| `pg_catalog` system tables | Low | FalconDB has its own `SHOW` commands |

### Syntax Differences

| PostgreSQL | FalconDB | Notes |
|-----------|----------|-------|
| `::type` cast | ✅ Works | Standard PostgreSQL cast syntax supported |
| `$1, $2` parameters | ✅ Works | Extended query protocol supported |
| `\d`, `\dt` (psql) | ⚠️ Partial | `SHOW TABLES`, `SHOW SEQUENCES` available |
| `information_schema` | ⚠️ Limited | Not all views available |

---

## Migration Decision Checklist

Before migrating, check:

1. **Does the app use triggers?** → Must move to application code
2. **Does the app use stored procedures?** → Must rewrite
3. **Does the app rely on FOREIGN KEY enforcement?** → Must add app-level checks
4. **Does the app use PostgreSQL extensions?** → Must find alternatives
5. **Does the app use LISTEN/NOTIFY?** → Must use external pub/sub
6. **Does the app use only standard SQL DML?** → Likely works as-is

---

## What "DROP-in Replacement" Actually Means

FalconDB is a drop-in replacement for **standard OLTP SQL workloads**:
- INSERT, UPDATE, DELETE, SELECT with JOINs
- Transactions (BEGIN/COMMIT/ROLLBACK)
- Indexes and sequences
- Standard PostgreSQL wire protocol (psycopg2, JDBC, libpq all work)

FalconDB is **not** a drop-in replacement for:
- Applications deeply dependent on PostgreSQL-specific features (PL/pgSQL, triggers, extensions)
- Analytics workloads using window functions extensively
- Applications using PostgreSQL as a message queue (LISTEN/NOTIFY)
