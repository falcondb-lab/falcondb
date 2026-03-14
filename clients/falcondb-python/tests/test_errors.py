"""Tests for the error hierarchy and classification."""

from falcondb.errors import (
    FalconError,
    ProgrammingError,
    OperationalError,
    InternalError,
    AuthenticationError,
    make_error,
    ERR_SYNTAX_ERROR,
    ERR_NOT_LEADER,
    ERR_FENCED_EPOCH,
    ERR_INTERNAL_ERROR,
    ERR_AUTH_FAILED,
    ERR_SERIALIZATION_CONFLICT,
    ERR_TIMEOUT,
)


class TestMakeError:
    def test_syntax_error(self):
        e = make_error("bad sql", ERR_SYNTAX_ERROR, "42601")
        assert isinstance(e, ProgrammingError)
        assert e.error_code == ERR_SYNTAX_ERROR
        assert e.sqlstate == "42601"
        assert not e.retryable

    def test_not_leader(self):
        e = make_error("not leader", ERR_NOT_LEADER, "F0001", server_epoch=5)
        assert isinstance(e, OperationalError)
        assert e.retryable
        assert e.server_epoch == 5

    def test_fenced_epoch(self):
        e = make_error("stale", ERR_FENCED_EPOCH)
        assert isinstance(e, OperationalError)
        assert e.retryable

    def test_serialization_conflict(self):
        e = make_error("conflict", ERR_SERIALIZATION_CONFLICT)
        assert isinstance(e, OperationalError)
        assert e.retryable

    def test_timeout(self):
        e = make_error("timeout", ERR_TIMEOUT)
        assert isinstance(e, InternalError)
        assert e.retryable

    def test_internal_error(self):
        e = make_error("bug", ERR_INTERNAL_ERROR)
        assert isinstance(e, InternalError)
        assert not e.retryable

    def test_auth_failed(self):
        e = make_error("bad password", ERR_AUTH_FAILED, "28P01")
        assert isinstance(e, AuthenticationError)
        assert not e.retryable

    def test_unknown_code(self):
        e = make_error("unknown", 9999)
        assert isinstance(e, FalconError)
        assert not isinstance(e, ProgrammingError)

    def test_error_str(self):
        e = make_error("parse error", ERR_SYNTAX_ERROR)
        assert "parse error" in str(e)
