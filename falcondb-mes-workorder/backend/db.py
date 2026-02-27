"""
FalconDB MES — Database connection layer.

All connections go through FalconDB's PostgreSQL wire protocol (port 5433).
Every write uses an explicit transaction with synchronous COMMIT.
No caching. No async-write-behind. What the database confirms is truth.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

DB_HOST = os.getenv("FALCON_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("FALCON_PORT", "5433"))
DB_USER = os.getenv("FALCON_USER", "falcon")
DB_NAME = os.getenv("FALCON_DB", "mes_prod")


def get_connection():
    """Create a new database connection. Autocommit OFF by default."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        dbname=DB_NAME,
        options="-c synchronous_commit=on",
    )


@contextmanager
def transaction():
    """Context manager that yields (conn, cursor) inside an explicit transaction.

    On success: COMMIT.
    On exception: ROLLBACK, then re-raise.
    Always closes the connection.
    """
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield conn, cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@contextmanager
def read_cursor():
    """Read-only cursor for queries."""
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()
        conn.close()
