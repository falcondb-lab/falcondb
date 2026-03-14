"""FalconDB error hierarchy — mirrors native protocol error codes."""

from __future__ import annotations


class FalconError(Exception):
    """Base exception for all FalconDB errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int = 0,
        sqlstate: str = "",
        retryable: bool = False,
        server_epoch: int = 0,
    ):
        super().__init__(message)
        self.error_code = error_code
        self.sqlstate = sqlstate
        self.retryable = retryable
        self.server_epoch = server_epoch


class ProgrammingError(FalconError):
    """SQL syntax or invalid parameter errors (1000–1999)."""


class OperationalError(FalconError):
    """Cluster / operational errors (2000–2999)."""


class IntegrityError(FalconError):
    """Constraint violations."""


class InternalError(FalconError):
    """Server internal errors (3000–3999)."""


class AuthenticationError(FalconError):
    """Authentication failure (4000–4999)."""


class RetryableError(FalconError):
    """Wrapper indicating the operation can be retried."""


# Error code ranges
_CODE_RANGES: list[tuple[int, int, type[FalconError]]] = [
    (1000, 1999, ProgrammingError),
    (2000, 2999, OperationalError),
    (3000, 3999, InternalError),
    (4000, 4001, AuthenticationError),
]

# Well-known error codes
ERR_SYNTAX_ERROR = 1000
ERR_INVALID_PARAM = 1001
ERR_NOT_LEADER = 2000
ERR_FENCED_EPOCH = 2001
ERR_READ_ONLY = 2002
ERR_SERIALIZATION_CONFLICT = 2003
ERR_INTERNAL_ERROR = 3000
ERR_TIMEOUT = 3001
ERR_OVERLOADED = 3002
ERR_AUTH_FAILED = 4000
ERR_PERMISSION_DENIED = 4001

_RETRYABLE_CODES = frozenset(
    {
        ERR_NOT_LEADER,
        ERR_FENCED_EPOCH,
        ERR_READ_ONLY,
        ERR_SERIALIZATION_CONFLICT,
        ERR_TIMEOUT,
        ERR_OVERLOADED,
    }
)


def make_error(
    message: str,
    error_code: int,
    sqlstate: str = "",
    server_epoch: int = 0,
) -> FalconError:
    """Create the appropriate FalconError subclass from an error code."""
    retryable = error_code in _RETRYABLE_CODES
    kwargs = dict(
        error_code=error_code,
        sqlstate=sqlstate,
        retryable=retryable,
        server_epoch=server_epoch,
    )
    for lo, hi, cls in _CODE_RANGES:
        if lo <= error_code <= hi:
            return cls(message, **kwargs)
    return FalconError(message, **kwargs)
