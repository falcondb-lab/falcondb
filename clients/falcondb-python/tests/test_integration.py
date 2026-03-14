"""
FalconDB Python SDK — Integration Tests

These tests run against a live FalconDB instance. They are skipped
automatically when FALCON_TEST_HOST is not set.

Usage:
    FALCON_TEST_HOST=127.0.0.1 FALCON_TEST_PORT=5443 pytest tests/test_integration.py -v

Environment variables:
    FALCON_TEST_HOST  — FalconDB host (required to run)
    FALCON_TEST_PORT  — PG wire port (default: 5443)
    FALCON_TEST_USER  — Username     (default: falcon)
    FALCON_TEST_PASS  — Password     (default: "")
    FALCON_TEST_DB    — Database     (default: falcon)
"""

import os
import time
import pytest

SKIP = not os.environ.get("FALCON_TEST_HOST")
REASON = "FALCON_TEST_HOST not set — skipping integration tests"

if not SKIP:
    from falcondb import Connection, ConnectionPool


def _cfg():
    return {
        "host": os.environ.get("FALCON_TEST_HOST", "127.0.0.1"),
        "port": int(os.environ.get("FALCON_TEST_PORT", "5443")),
        "user": os.environ.get("FALCON_TEST_USER", "falcon"),
        "password": os.environ.get("FALCON_TEST_PASS", ""),
        "database": os.environ.get("FALCON_TEST_DB", "falcon"),
    }


@pytest.fixture
def conn():
    c = Connection(**_cfg())
    c.connect()
    yield c
    c.close()


@pytest.fixture
def table_name():
    return f"_sdk_integ_{int(time.time() * 1000)}"


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=REASON)
class TestConnectionLifecycle:

    def test_connect_and_ping(self, conn):
        assert conn.ping(timeout_ms=3000) is True

    def test_select_one(self, conn):
        rows = conn.query("SELECT 1 AS val")
        assert len(rows) >= 1
        assert rows[0]["val"] == 1 or rows[0][0] == 1

    def test_parameterised_query(self, conn):
        rows = conn.query("SELECT $1::int + $2::int AS sum", [3, 4])
        assert len(rows) >= 1
        s = rows[0].get("sum", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
        assert s == 7


# ---------------------------------------------------------------------------
# DDL and DML
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=REASON)
class TestDDLDML:

    def test_create_insert_select_drop(self, conn, table_name):
        conn.query(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
        try:
            conn.query(f"INSERT INTO {table_name} (id, name) VALUES (1, 'alice')")
            rows = conn.query(f"SELECT name FROM {table_name} WHERE id = 1")
            assert len(rows) == 1
            name = rows[0].get("name", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
            assert name == "alice"

            conn.query(f"UPDATE {table_name} SET name = 'bob' WHERE id = 1")
            rows = conn.query(f"SELECT name FROM {table_name} WHERE id = 1")
            name = rows[0].get("name", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
            assert name == "bob"

            conn.query(f"DELETE FROM {table_name} WHERE id = 1")
            rows = conn.query(f"SELECT count(*) AS cnt FROM {table_name}")
            cnt = rows[0].get("cnt", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
            assert int(cnt) == 0
        finally:
            conn.query(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=REASON)
class TestTransactions:

    def test_commit(self, conn, table_name):
        conn.query(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val INT)")
        try:
            conn.query("BEGIN")
            conn.query(f"INSERT INTO {table_name} (id, val) VALUES (1, 100)")
            conn.query("COMMIT")
            rows = conn.query(f"SELECT val FROM {table_name} WHERE id = 1")
            val = rows[0].get("val", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
            assert val == 100
        finally:
            conn.query(f"DROP TABLE IF EXISTS {table_name}")

    def test_rollback(self, conn, table_name):
        conn.query(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val INT)")
        try:
            conn.query(f"INSERT INTO {table_name} (id, val) VALUES (1, 100)")
            conn.query("BEGIN")
            conn.query(f"UPDATE {table_name} SET val = 999 WHERE id = 1")
            conn.query("ROLLBACK")
            rows = conn.query(f"SELECT val FROM {table_name} WHERE id = 1")
            val = rows[0].get("val", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
            assert val == 100
        finally:
            conn.query(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=REASON)
class TestConnectionPool:

    def test_acquire_release(self):
        pool = ConnectionPool(**_cfg(), min_size=1, max_size=3)
        try:
            c = pool.acquire()
            assert c.ping(timeout_ms=3000) is True
            pool.release(c)
        finally:
            pool.close()

    def test_with_connection(self):
        pool = ConnectionPool(**_cfg(), min_size=1, max_size=3)
        try:
            def work(c):
                return c.query("SELECT 42 AS answer")

            rows = pool.with_connection(work)
            ans = rows[0].get("answer", rows[0][0]) if isinstance(rows[0], dict) else rows[0][0]
            assert ans == 42
        finally:
            pool.close()


# ---------------------------------------------------------------------------
# SHOW commands
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=REASON)
class TestShowCommands:

    def test_show_txn_stats(self, conn):
        rows = conn.query("SHOW falcon.txn_stats")
        assert rows is not None

    def test_show_connections(self, conn):
        rows = conn.query("SHOW falcon.connections")
        assert rows is not None

    def test_show_health_score(self, conn):
        rows = conn.query("SHOW falcon.health_score")
        assert rows is not None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=REASON)
class TestErrors:

    def test_invalid_sql(self, conn):
        with pytest.raises(Exception):
            conn.query("SELECTTTT")

    def test_nonexistent_table(self, conn):
        with pytest.raises(Exception):
            conn.query("SELECT * FROM _nonexistent_table_xyz_12345")
