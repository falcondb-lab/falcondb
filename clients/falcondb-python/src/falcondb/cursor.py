"""FalconDB Cursor — DB-API 2.0 inspired interface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from falcondb.connection import Connection


@dataclass(frozen=True, slots=True)
class ColumnMeta:
    """Metadata for a single result column."""

    name: str
    type_id: int
    nullable: bool
    precision: int
    scale: int


class Cursor:
    """A cursor for executing queries and iterating results."""

    def __init__(self, connection: Connection) -> None:
        self._conn = connection
        self._columns: list[ColumnMeta] = []
        self._rows: list[list[Any]] = []
        self._rows_affected: int = -1
        self._pos: int = 0
        self._closed: bool = False

    # ── Properties ───────────────────────────────────────────────────

    @property
    def description(self) -> list[tuple[str, int, None, None, int, int, bool]] | None:
        """DB-API 2.0 compatible description."""
        if not self._columns:
            return None
        return [
            (c.name, c.type_id, None, None, c.precision, c.scale, c.nullable)
            for c in self._columns
        ]

    @property
    def rowcount(self) -> int:
        return self._rows_affected

    @property
    def columns(self) -> list[ColumnMeta]:
        return self._columns

    # ── Execution ────────────────────────────────────────────────────

    def execute(self, sql: str, params: list[Any] | None = None) -> Cursor:
        """Execute a SQL statement."""
        self._check_open()
        self._columns, self._rows, self._rows_affected = (
            self._conn._execute_query(sql, params)
        )
        self._pos = 0
        return self

    def executemany(self, sql: str, param_sets: list[list[Any]]) -> None:
        """Execute a SQL statement against each parameter set."""
        self._check_open()
        for params in param_sets:
            self.execute(sql, params)

    # ── Fetch ────────────────────────────────────────────────────────

    def fetchone(self) -> list[Any] | None:
        """Fetch the next row, or None if exhausted."""
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int = 100) -> list[list[Any]]:
        """Fetch up to *size* rows."""
        self._check_open()
        end = min(self._pos + size, len(self._rows))
        rows = self._rows[self._pos : end]
        self._pos = end
        return rows

    def fetchall(self) -> list[list[Any]]:
        """Fetch all remaining rows."""
        self._check_open()
        rows = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rows

    # ── Iteration ────────────────────────────────────────────────────

    def __iter__(self) -> Iterator[list[Any]]:
        self._check_open()
        return iter(self._rows)

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise Exception("cursor is closed")
