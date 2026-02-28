"""FalconDB Python SDK — native protocol driver with HA failover."""

from falcondb.connection import connect, Connection
from falcondb.cursor import Cursor
from falcondb.pool import ConnectionPool
from falcondb.errors import (
    FalconError,
    OperationalError,
    ProgrammingError,
    IntegrityError,
    AuthenticationError,
    RetryableError,
)
from falcondb.ha import FailoverConnection

__version__ = "0.2.0"

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "ConnectionPool",
    "FailoverConnection",
    "FalconError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "AuthenticationError",
    "RetryableError",
]
