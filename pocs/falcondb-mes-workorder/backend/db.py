"""
FalconDB MES — Database connection layer.

Uses simple query protocol (no parameterized queries) for full
FalconDB compatibility. Every confirmed write is durable.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

DB_HOST = os.getenv("FALCON_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("FALCON_PORT", "5433"))
DB_USER = os.getenv("FALCON_USER", "falcon")
DB_NAME = os.getenv("FALCON_DB", "mes_prod")


def _escape(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def sql(template, *args):
    """Format SQL with embedded values for simple query protocol."""
    if not args:
        return template
    return template % tuple(_escape(a) for a in args)


def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        dbname=DB_NAME,
    )
    conn.autocommit = True
    return conn


@contextmanager
def get_cursor():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()
        conn.close()
