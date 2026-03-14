"""FalconDB HA failover — automatic primary discovery and reconnection."""

from __future__ import annotations

import logging
import time
from typing import Any

from falcondb.connection import Connection
from falcondb.cursor import Cursor
from falcondb.errors import (
    ERR_FENCED_EPOCH,
    ERR_NOT_LEADER,
    ERR_READ_ONLY,
    ERR_SERIALIZATION_CONFLICT,
    ERR_TIMEOUT,
    ERR_OVERLOADED,
    FalconError,
)

logger = logging.getLogger("falcondb.ha")

# Error codes that trigger failover
_FAILOVER_CODES = frozenset({ERR_NOT_LEADER, ERR_FENCED_EPOCH, ERR_READ_ONLY})

# Error codes that trigger retry (same node)
_RETRY_CODES = frozenset({ERR_SERIALIZATION_CONFLICT, ERR_TIMEOUT, ERR_OVERLOADED})


class FailoverConnection:
    """Connection wrapper with automatic failover across seed nodes.

    When a query fails with NOT_LEADER or FENCED_EPOCH, the driver
    automatically reconnects to the next seed node and retries.

    Usage::

        conn = FailoverConnection(
            seeds=[("node1", 6543), ("node2", 6543), ("node3", 6543)],
            database="mydb",
            user="falcon",
            password="secret",
        )
        cur = conn.execute("SELECT * FROM orders WHERE id = 42")
        print(cur.fetchone())
    """

    def __init__(
        self,
        seeds: list[tuple[str, int]],
        *,
        database: str = "falcon",
        user: str = "falcon",
        password: str = "",
        connect_timeout: float = 10.0,
        ssl_enabled: bool = False,
        ssl_verify: bool = True,
        autocommit: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
        retry_max_delay: float = 5.0,
    ) -> None:
        if not seeds:
            raise ValueError("at least one seed node is required")

        self._seeds = list(seeds)
        self._connect_kwargs: dict[str, Any] = dict(
            database=database,
            user=user,
            password=password,
            connect_timeout=connect_timeout,
            ssl_enabled=ssl_enabled,
            ssl_verify=ssl_verify,
            autocommit=autocommit,
        )
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay

        self._conn: Connection | None = None
        self._current_seed_idx = 0
        self._closed = False

        self._connect_to_any()

    # ── Public API ───────────────────────────────────────────────────

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def server_epoch(self) -> int:
        return self._conn.server_epoch if self._conn else 0

    @property
    def server_node_id(self) -> int:
        return self._conn.server_node_id if self._conn else 0

    def execute(self, sql: str, params: list[Any] | None = None) -> Cursor:
        """Execute with automatic failover and retry."""
        return self._with_retry(lambda c: c.execute(sql, params))

    def commit(self) -> None:
        self._with_retry(lambda c: c.commit() or c.cursor())

    def rollback(self) -> None:
        self._with_retry(lambda c: c.rollback() or c.cursor())

    def ping(self, timeout: float = 5.0) -> bool:
        if self._conn is None:
            return False
        return self._conn.ping(timeout)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    def __enter__(self) -> FailoverConnection:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ── Internal ─────────────────────────────────────────────────────

    def _with_retry(self, fn: Any) -> Any:
        """Execute fn(conn) with failover and retry logic."""
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            if self._closed:
                raise FalconError("connection is closed")

            if self._conn is None or self._conn.closed:
                self._connect_to_any()

            try:
                return fn(self._conn)
            except FalconError as e:
                last_error = e
                if e.error_code in _FAILOVER_CODES:
                    logger.warning(
                        "failover triggered (code=%d, epoch=%d), reconnecting...",
                        e.error_code,
                        e.server_epoch,
                    )
                    self._close_current()
                    self._advance_seed()
                    delay = min(
                        self._retry_base_delay * (2**attempt),
                        self._retry_max_delay,
                    )
                    time.sleep(delay)
                elif e.error_code in _RETRY_CODES:
                    logger.info(
                        "retryable error (code=%d), attempt %d/%d",
                        e.error_code,
                        attempt + 1,
                        self._max_retries,
                    )
                    delay = min(
                        self._retry_base_delay * (2**attempt),
                        self._retry_max_delay,
                    )
                    time.sleep(delay)
                else:
                    raise
            except Exception as e:
                last_error = e
                logger.warning("connection error: %s, reconnecting...", e)
                self._close_current()
                self._advance_seed()

        raise last_error or FalconError("max retries exhausted")

    def _connect_to_any(self) -> None:
        """Try each seed node until one connects."""
        errors: list[str] = []
        for _ in range(len(self._seeds)):
            host, port = self._seeds[self._current_seed_idx]
            try:
                self._conn = Connection(
                    host=host, port=port, **self._connect_kwargs
                )
                logger.info("connected to %s:%d (epoch=%d)", host, port, self._conn.server_epoch)
                return
            except Exception as e:
                errors.append(f"{host}:{port} -> {e}")
                self._advance_seed()

        raise FalconError(
            f"cannot connect to any seed node: {'; '.join(errors)}"
        )

    def _advance_seed(self) -> None:
        self._current_seed_idx = (self._current_seed_idx + 1) % len(self._seeds)

    def _close_current(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
