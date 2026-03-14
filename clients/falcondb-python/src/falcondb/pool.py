"""FalconDB connection pool with health checking."""

from __future__ import annotations

import threading
import time
from typing import Any

from falcondb.connection import Connection


class ConnectionPool:
    """Thread-safe connection pool for FalconDB.

    Usage::

        pool = ConnectionPool(host="localhost", port=6543, min_size=2, max_size=10)
        conn = pool.acquire()
        try:
            cur = conn.execute("SELECT 1")
            print(cur.fetchone())
        finally:
            pool.release(conn)

    Or as a context manager::

        with pool.connection() as conn:
            conn.execute("INSERT INTO t VALUES (1, 'a')")
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6543,
        database: str = "falcon",
        user: str = "falcon",
        password: str = "",
        min_size: int = 1,
        max_size: int = 10,
        connect_timeout: float = 10.0,
        ssl_enabled: bool = False,
        ssl_verify: bool = True,
        autocommit: bool = True,
        health_check_interval: float = 30.0,
    ) -> None:
        self._connect_kwargs: dict[str, Any] = dict(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=connect_timeout,
            ssl_enabled=ssl_enabled,
            ssl_verify=ssl_verify,
            autocommit=autocommit,
        )
        self._min_size = min_size
        self._max_size = max_size
        self._health_check_interval = health_check_interval

        self._lock = threading.Lock()
        self._idle: list[_PooledConnection] = []
        self._in_use: set[int] = set()
        self._total_created = 0
        self._closed = False

        # Pre-fill minimum connections
        for _ in range(min_size):
            self._idle.append(self._create_pooled())

    # ── Public API ───────────────────────────────────────────────────

    def acquire(self) -> Connection:
        """Acquire a connection from the pool."""
        with self._lock:
            if self._closed:
                raise RuntimeError("pool is closed")

            # Try to reuse an idle connection
            while self._idle:
                pc = self._idle.pop()
                if self._is_healthy(pc):
                    self._in_use.add(id(pc.conn))
                    return pc.conn
                else:
                    self._safe_close(pc.conn)
                    self._total_created -= 1

            # Create a new connection if under limit
            if len(self._in_use) < self._max_size:
                pc = self._create_pooled()
                self._in_use.add(id(pc.conn))
                return pc.conn

        raise RuntimeError(
            f"pool exhausted: {len(self._in_use)}/{self._max_size} connections in use"
        )

    def release(self, conn: Connection) -> None:
        """Return a connection to the pool."""
        with self._lock:
            self._in_use.discard(id(conn))
            if self._closed or conn.closed:
                self._safe_close(conn)
                self._total_created -= 1
            else:
                self._idle.append(_PooledConnection(conn, time.monotonic()))

    def connection(self) -> _ConnectionContext:
        """Context manager that acquires and releases a connection."""
        return _ConnectionContext(self)

    def close(self) -> None:
        """Close all connections and shut down the pool."""
        with self._lock:
            self._closed = True
            for pc in self._idle:
                self._safe_close(pc.conn)
            self._idle.clear()
            self._total_created = 0

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._idle) + len(self._in_use)

    @property
    def idle_count(self) -> int:
        with self._lock:
            return len(self._idle)

    @property
    def in_use_count(self) -> int:
        with self._lock:
            return len(self._in_use)

    # ── Internal ─────────────────────────────────────────────────────

    def _create_pooled(self) -> _PooledConnection:
        conn = Connection(**self._connect_kwargs)
        self._total_created += 1
        return _PooledConnection(conn, time.monotonic())

    def _is_healthy(self, pc: _PooledConnection) -> bool:
        if pc.conn.closed:
            return False
        # Skip health check if recently used
        elapsed = time.monotonic() - pc.last_used
        if elapsed < self._health_check_interval:
            return True
        return pc.conn.ping(timeout=2.0)

    @staticmethod
    def _safe_close(conn: Connection) -> None:
        try:
            conn.close()
        except Exception:
            pass

    def __enter__(self) -> ConnectionPool:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class _PooledConnection:
    __slots__ = ("conn", "last_used")

    def __init__(self, conn: Connection, last_used: float) -> None:
        self.conn = conn
        self.last_used = last_used


class _ConnectionContext:
    __slots__ = ("_pool", "_conn")

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool
        self._conn: Connection | None = None

    def __enter__(self) -> Connection:
        self._conn = self._pool.acquire()
        return self._conn

    def __exit__(self, *exc: Any) -> None:
        if self._conn is not None:
            self._pool.release(self._conn)
            self._conn = None
