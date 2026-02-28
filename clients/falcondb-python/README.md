# FalconDB Python SDK

Native protocol driver for [FalconDB](https://github.com/falcondb/falcondb) with
HA failover, connection pooling, and DB-API 2.0–inspired cursor interface.

## Installation

```bash
pip install falcondb
```

Or from source:

```bash
cd clients/falcondb-python
pip install -e ".[dev]"
```

## Quick Start

```python
import falcondb

# Simple connection
with falcondb.connect(host="localhost", port=6543, user="falcon") as conn:
    cur = conn.execute("SELECT id, name FROM users WHERE id = 42")
    row = cur.fetchone()
    print(row)  # [42, 'Alice']
```

## Connection Pool

```python
from falcondb import ConnectionPool

pool = ConnectionPool(
    host="localhost", port=6543,
    user="falcon", password="secret",
    min_size=2, max_size=20,
)

with pool.connection() as conn:
    cur = conn.execute("INSERT INTO orders (id, total) VALUES (1, 99.95)")
    print(cur.rowcount)  # 1
```

## HA Failover

```python
from falcondb import FailoverConnection

conn = FailoverConnection(
    seeds=[("node1", 6543), ("node2", 6543), ("node3", 6543)],
    database="mydb",
    user="falcon",
    password="secret",
    max_retries=3,
)

# Automatic failover on NOT_LEADER / FENCED_EPOCH
cur = conn.execute("SELECT * FROM orders")
for row in cur:
    print(row)
```

## Cursor Interface

```python
with falcondb.connect() as conn:
    cur = conn.cursor()
    cur.execute("SELECT id, name, email FROM users")

    # DB-API 2.0 description
    for col in cur.description:
        print(col[0], col[1])  # name, type_id

    # Fetch methods
    row = cur.fetchone()
    rows = cur.fetchmany(10)
    rest = cur.fetchall()

    # Iteration
    cur.execute("SELECT * FROM logs")
    for row in cur:
        print(row)
```

## Transactions

```python
with falcondb.connect(autocommit=False) as conn:
    conn.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    conn.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    conn.commit()
```

## Error Handling

```python
from falcondb import FalconError, ProgrammingError, OperationalError

try:
    conn.execute("SELET 1")  # typo
except ProgrammingError as e:
    print(e.error_code)  # 1000
    print(e.sqlstate)    # 42601
    print(e.retryable)   # False

try:
    conn.execute("INSERT INTO t VALUES (1)")
except OperationalError as e:
    if e.retryable:
        # Safe to retry (e.g., serialization conflict)
        pass
```

## Error Hierarchy

| Exception | Error Codes | Retryable |
|-----------|------------|-----------|
| `ProgrammingError` | 1000–1999 | ❌ |
| `OperationalError` | 2000–2999 | ✅ (NOT_LEADER, FENCED_EPOCH, SERIALIZATION_CONFLICT) |
| `InternalError` | 3000–3999 | ✅ (TIMEOUT, OVERLOADED) / ❌ (INTERNAL_ERROR) |
| `AuthenticationError` | 4000–4001 | ❌ |

## Requirements

- Python 3.9+
- No external dependencies (stdlib only)

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## License

Apache-2.0
