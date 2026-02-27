# Demo Application

A simple e-commerce OLTP application using **standard psycopg2** (PostgreSQL client).

**No FalconDB-specific code.** The same Python file runs against PostgreSQL and FalconDB.

## Requirements

```bash
pip install psycopg2-binary
```

## Usage

```bash
# Run against PostgreSQL
python demo_app.py --config app.conf.postgres --workflow all

# Run against FalconDB (same code, different config)
python demo_app.py --config app.conf.falcon --workflow all

# Run a single workflow
python demo_app.py --config app.conf.falcon --workflow create_order

# JSON output (for automated comparison)
python demo_app.py --config app.conf.falcon --workflow all --json
```

## Workflows

| Workflow | What It Does | SQL Used |
|----------|-------------|----------|
| `create_order` | INSERT order + items + payment in one transaction | INSERT, RETURNING, BEGIN/COMMIT |
| `update_order` | Update order and payment status | UPDATE, WHERE, NOW() |
| `query_order` | Fetch order with customer info | SELECT, JOIN, WHERE |
| `list_recent_orders` | Dashboard query: recent orders | SELECT, JOIN, ORDER BY DESC, LIMIT |

## Key Point

The **only difference** between running on PostgreSQL and FalconDB is the config file:
- `app.conf.postgres` → host=127.0.0.1, port=5432
- `app.conf.falcon` → host=127.0.0.1, port=5433

No code changes. No SQL rewrites. No special drivers.
