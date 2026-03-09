# Migration Report

**Source**: PostgreSQL 127.0.0.1:5432/shop_demo
**Target**: FalconDB 127.0.0.1:5433/shop_demo
**Date**: 2026-03-04 05:24:17 UTC

## Schema Migration

| Metric | Count |
|--------|-------|
| Applied | 9 |
| Failed | 0 |

### Object Details

| Object | Status | Notes |
|--------|--------|-------|
| CREATE SEQUENCE customers_id_seq | Applied | |
| CREATE SEQUENCE orders_id_seq | Applied | |
| CREATE SEQUENCE payments_id_seq | Applied | |
| CREATE SEQUENCE order_items_id_seq | Applied | |
| CREATE TABLE customers | Applied | |
| CREATE TABLE orders | Applied | |
| CREATE TABLE order_items | Applied | |
| CREATE TABLE payments | Applied | |
| CREATE VIEW order_summary | Applied | |

### Method

1. Schema exported from PostgreSQL using `pg_dump --schema-only`
2. Applied to FalconDB using curated `postgres_schema.sql`
3. All CREATE SEQUENCE + DEFAULT nextval() recognized as dynamic defaults

---

## Data Migration

**Date**: 2026-03-04 05:24:17 UTC

### Row Count Comparison

| Table | PostgreSQL | FalconDB | Status |
|-------|-----------|----------|--------|
| customers | 5 | 5 | Match |
| orders | 9 | 9 | Match |
| order_items | 18 | 18 | Match |
| payments | 7 | 7 | Match |

### Method

1. Seed data applied via `data/sample_seed.sql`
2. `setval()` calls correctly advance sequence counters
3. Row counts verified to match exactly

---

## Smoke Test

  Workflow               PostgreSQL     FalconDB       Match
  ----------------------------------------------------------------
  create_order           PASS           PASS           YES
  update_order           PASS           PASS           YES
  query_order            PASS           PASS           YES
  list_recent_orders     PASS           PASS           YES
  ----------------------------------------------------------------
  All workflows match.

---

## What Worked

- CREATE TABLE with DEFAULT, NOT NULL, PRIMARY KEY
- CREATE SEQUENCE + DEFAULT nextval('seq_name')
- setval() to advance sequences after bulk insert
- CREATE INDEX (single-column)
- CREATE VIEW with JOIN
- INSERT...RETURNING
- UPDATE...WHERE with subquery
- SELECT with JOIN, ORDER BY, LIMIT
- BEGIN / COMMIT (implicit via psycopg2 context manager)
- NOW() function
- psycopg2 driver (standard PG wire protocol)
- Parameterized queries

## Known Incompatibilities

See `output/incompatibilities.json` for full details.
