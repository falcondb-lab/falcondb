# FalconDB — PostgreSQL-Compatible Migration & Drop-in Replacement

> **This demo shows how an existing PostgreSQL application can be moved to
> FalconDB with minimal effort, and exactly what to expect during migration.**

Adopting a new database is not just a technical decision — it's a risk decision.
This PoC answers the question every team asks before committing:

> "How hard is it to move our existing app, and what breaks?"

---

## What This Demo Does

1. Start a real **PostgreSQL** instance with a realistic OLTP schema
2. Populate it with sample e-commerce data (customers, orders, payments)
3. Run a **standard Python application** (using psycopg2) against PostgreSQL
4. **Migrate the schema** to FalconDB — record what works, what doesn't
5. **Migrate the data** — verify row counts match
6. **Change only the connection string** — no code changes, no SQL rewrites
7. Run the **exact same application** against FalconDB
8. Compare results side by side

The application uses `BEGIN/COMMIT`, `INSERT...RETURNING`, `JOIN`, `ORDER BY`,
`LIMIT` — the same SQL any real OLTP app would use.

---

## Run It

### Prerequisites

| Tool | Purpose |
|------|---------|
| **PostgreSQL** | Source database (`psql`, `pg_dump`, `pg_ctl`) |
| **FalconDB** | Target database (`cargo build -p falcon_server --release`) |
| **Python 3** | Demo application |
| **psycopg2** | `pip install psycopg2-binary` |

### Step-by-Step

```bash
# 1. Start PostgreSQL
./scripts/start_postgres.sh

# 2. Initialize the sample database (schema + data)
./scripts/init_sample_app_db.sh

# 3. Start FalconDB
./scripts/start_falcondb.sh

# 4. Migrate schema to FalconDB
./scripts/migrate_schema.sh

# 5. Migrate data to FalconDB
./scripts/migrate_data.sh

# 6. See what changed (spoiler: only the config)
./scripts/switch_app_to_falcon.sh

# 7. Run the smoke test against BOTH databases
./scripts/run_app_smoke_test.sh

# 8. Cleanup
./scripts/cleanup.sh
```

Windows users: use the `.ps1` variants of each script.

---

## What the Smoke Test Shows

The smoke test runs the same four workflows against both databases:

```
Workflow               PostgreSQL    FalconDB      Match
------------------------------------------------------------
create_order           PASS          PASS          YES
update_order           PASS          PASS          YES
query_order            PASS          PASS          YES
list_recent_orders     PASS          PASS          YES
------------------------------------------------------------
All workflows match.
```

**The application did not change.** Only the config file changed:
- `host` stayed the same
- `port` changed from 5432 to 5433
- `user` changed from `postgres` to `falcon`
- **No SQL was rewritten. No driver was changed. No code was modified.**

---

## How Much Effort Did Migration Take?

| Step | Effort | Tools |
|------|--------|-------|
| Export schema | ~1 second | `pg_dump --schema-only` |
| Apply schema to FalconDB | ~1 second | `psql -f postgres_schema.sql` |
| Export data | ~1 second | `pg_dump --data-only --inserts` |
| Import data to FalconDB | ~1 second | `psql -f sample_seed.sql` |
| Switch application | Change 2 config values | Edit host + port |
| Code changes | **Zero** | |

---

## What Worked Out of the Box

- `CREATE TABLE` with `DEFAULT`, `NOT NULL`, `PRIMARY KEY`
- `CREATE INDEX` (single-column)
- `CREATE SEQUENCE` + `nextval()`
- `CREATE VIEW` with `JOIN`
- `INSERT...RETURNING`
- `UPDATE...WHERE`
- `SELECT` with `JOIN`, `ORDER BY`, `LIMIT`
- `BEGIN` / `COMMIT` / `ROLLBACK`
- `NOW()` function
- `psycopg2` driver (standard PostgreSQL wire protocol)
- Parameterized queries (`%s` placeholders)
- All standard OLTP data types (BIGINT, TEXT, TIMESTAMP)

---

## What Did Not Work (and Why)

See [output/incompatibilities.json](output/incompatibilities.json) for the full list.

The most important ones:

| Feature | Impact | Status |
|---------|--------|--------|
| **Triggers** | High | Not supported — must move to app code |
| **Stored Procedures (PL/pgSQL)** | High | Not supported — must rewrite |
| **FOREIGN KEY enforcement** | Medium | Parsed but not enforced |
| **CREATE INDEX CONCURRENTLY** | Medium | Use regular CREATE INDEX |
| **LISTEN / NOTIFY** | Medium | Use external pub/sub |
| **Declarative Partitioning** | Medium | Use FalconDB sharding |
| **CHECK constraints** | Low | Validate in application |
| **ENUM types** | Low | Use TEXT |

**We do not hide these.** They are documented, categorized by impact,
and include suggested workarounds.

---

## Migration Risk Assessment

### Low Risk (proceed with confidence)
Applications that use:
- Standard SQL DML (INSERT, UPDATE, DELETE, SELECT)
- Transactions (BEGIN/COMMIT)
- Indexes and sequences
- Standard PostgreSQL client libraries (psycopg2, JDBC, libpq, node-postgres)

### Medium Risk (migration needed for some features)
Applications that use:
- Foreign key constraints (move enforcement to app)
- Basic partitioning (consider FalconDB sharding)
- LISTEN/NOTIFY (switch to external pub/sub)

### High Risk (significant rewrite required)
Applications that heavily depend on:
- PL/pgSQL stored procedures
- Database triggers
- PostgreSQL-specific extensions (PostGIS, pg_trgm)

---

## Compatibility Boundary

For detailed compatibility notes, see [schema/falcon_compat_notes.md](schema/falcon_compat_notes.md).

FalconDB is a drop-in replacement for **standard OLTP SQL workloads**.
It is not a drop-in for applications deeply coupled to PostgreSQL internals.

**We are transparent about this boundary** because migration trust comes
from honesty, not from "100% compatible" marketing.

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FALCON_BIN` | `target/release/falcon_server` | Path to FalconDB binary |
| `FALCON_PORT` | `5433` | FalconDB listen port |
| `FALCON_USER` | `falcon` | FalconDB user |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_USER` | `postgres` | PostgreSQL user |

---

## Output Files

| File | Content |
|------|---------|
| `output/migration_report.md` | Schema + data migration results |
| `output/smoke_test_results.txt` | Side-by-side workflow comparison |
| `output/incompatibilities.json` | Known incompatibilities with impact/workaround |
| `output/pg_schema_dump.sql` | Raw pg_dump schema for audit |
| `output/pg_data_dump.sql` | Raw pg_dump data for audit |
| `output/smoke_pg.json` | PostgreSQL workflow results (JSON) |
| `output/smoke_falcon.json` | FalconDB workflow results (JSON) |

---

## What This PoC Does NOT Claim

- ❌ "100% PostgreSQL compatible"
- ❌ "Zero migration risk"
- ❌ "Just works for everything"

## What This PoC DOES Prove

- ✅ Standard OLTP apps work by changing only the connection string
- ✅ Schema and data migration is straightforward and fast
- ✅ Incompatibilities are known, documented, and manageable
- ✅ You can assess migration risk in minutes, not weeks

---

## Directory Structure

```
falcondb-poc-migration/
├── README.md                           ← You are here
├── schema/
│   ├── postgres_schema.sql             ← OLTP schema (customers, orders, payments)
│   └── falcon_compat_notes.md          ← What works, what doesn't, workarounds
├── data/
│   └── sample_seed.sql                 ← Realistic e-commerce seed data
├── app/
│   ├── README.md                       ← App documentation
│   ├── demo_app.py                     ← Standard psycopg2 app (no FalconDB code)
│   ├── app.conf.postgres               ← Connection config for PostgreSQL
│   └── app.conf.falcon                 ← Connection config for FalconDB
├── scripts/
│   ├── start_postgres.sh / .ps1        ← Start PostgreSQL
│   ├── start_falcondb.sh / .ps1        ← Start FalconDB
│   ├── init_sample_app_db.sh / .ps1    ← Load schema + data into PostgreSQL
│   ├── migrate_schema.sh / .ps1        ← Schema: PostgreSQL → FalconDB
│   ├── migrate_data.sh / .ps1          ← Data: PostgreSQL → FalconDB
│   ├── switch_app_to_falcon.sh / .ps1  ← Show what changes (just the config)
│   ├── run_app_smoke_test.sh / .ps1    ← Run app against both, compare
│   └── cleanup.sh / .ps1               ← Stop everything
└── output/
    ├── migration_report.md             ← Generated: schema + data migration
    ├── smoke_test_results.txt          ← Generated: workflow comparison
    └── incompatibilities.json          ← Known incompatibilities
```
