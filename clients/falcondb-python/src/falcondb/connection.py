"""FalconDB native protocol connection."""

from __future__ import annotations

import os
import socket
import ssl
import struct
from typing import Any

from falcondb.errors import AuthenticationError, FalconError, make_error
from falcondb.wire import (
    AUTH_PASSWORD,
    FEATURE_BATCH_INGEST,
    FEATURE_BINARY_PARAMS,
    FEATURE_EPOCH_FENCING,
    FEATURE_PIPELINE,
    FEATURE_TLS,
    MAX_FRAME_SIZE,
    MSG_AUTH_FAIL,
    MSG_AUTH_OK,
    MSG_AUTH_REQUEST,
    MSG_AUTH_RESPONSE,
    MSG_BATCH_REQUEST,
    MSG_BATCH_RESPONSE,
    MSG_CLIENT_HELLO,
    MSG_DISCONNECT,
    MSG_ERROR_RESPONSE,
    MSG_PING,
    MSG_PONG,
    MSG_QUERY_REQUEST,
    MSG_QUERY_RESPONSE,
    MSG_SERVER_HELLO,
    SESSION_AUTOCOMMIT,
    WireReader,
    WireWriter,
    build_frame,
    decode_row,
    encode_row,
)
from falcondb.cursor import ColumnMeta, Cursor


def connect(
    host: str = "localhost",
    port: int = 6543,
    database: str = "falcon",
    user: str = "falcon",
    password: str = "",
    *,
    connect_timeout: float = 10.0,
    ssl_enabled: bool = False,
    ssl_verify: bool = True,
    autocommit: bool = True,
) -> Connection:
    """Create a new connection to a FalconDB server."""
    return Connection(
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


class Connection:
    """A single connection to a FalconDB server using the native protocol."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6543,
        database: str = "falcon",
        user: str = "falcon",
        password: str = "",
        connect_timeout: float = 10.0,
        ssl_enabled: bool = False,
        ssl_verify: bool = True,
        autocommit: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._autocommit = autocommit
        self._ssl_enabled = ssl_enabled
        self._ssl_verify = ssl_verify

        self._sock: socket.socket | None = None
        self._closed = False
        self._request_id = 0
        self._server_epoch: int = 0
        self._server_node_id: int = 0
        self._negotiated_features: int = 0

        self._connect(connect_timeout)

    # ── Public API ───────────────────────────────────────────────────

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._autocommit = value

    @property
    def server_epoch(self) -> int:
        return self._server_epoch

    @property
    def server_node_id(self) -> int:
        return self._server_node_id

    @property
    def closed(self) -> bool:
        return self._closed

    def cursor(self) -> Cursor:
        """Create a new cursor bound to this connection."""
        self._check_open()
        return Cursor(self)

    def execute(self, sql: str, params: list[Any] | None = None) -> Cursor:
        """Shorthand: create a cursor, execute, and return it."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self) -> None:
        """Commit the current transaction."""
        self._check_open()
        self._execute_simple("COMMIT")

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._check_open()
        self._execute_simple("ROLLBACK")

    def ping(self, timeout: float = 5.0) -> bool:
        """Send a Ping and wait for Pong."""
        try:
            self._check_open()
            self._send_frame(MSG_PING, b"")
            old_timeout = self._sock.gettimeout()  # type: ignore[union-attr]
            self._sock.settimeout(timeout)  # type: ignore[union-attr]
            try:
                msg_type, _ = self._recv_frame()
                return msg_type == MSG_PONG
            finally:
                self._sock.settimeout(old_timeout)  # type: ignore[union-attr]
        except Exception:
            return False

    def close(self) -> None:
        """Graceful disconnect."""
        if not self._closed and self._sock is not None:
            self._closed = True
            try:
                self._send_frame(MSG_DISCONNECT, b"")
                self._sock.settimeout(1.0)
                try:
                    self._recv_frame()
                except Exception:
                    pass
            finally:
                self._sock.close()
                self._sock = None

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        if not self._closed:
            self.close()

    # ── Internal: query execution ────────────────────────────────────

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _execute_query(
        self, sql: str, params: list[Any] | None = None
    ) -> tuple[list[ColumnMeta], list[list[Any]], int]:
        """Execute a query and return (columns, rows, rows_affected)."""
        self._check_open()
        req_id = self._next_request_id()
        session_flags = SESSION_AUTOCOMMIT if self._autocommit else 0

        w = WireWriter()
        w.u64(req_id)
        w.u64(self._server_epoch)
        w.string_u32(sql)
        # No binary params for now (simplified SDK)
        w.u16(0)
        w.u32(session_flags)

        self._send_frame(MSG_QUERY_REQUEST, w.to_bytes())
        return self._read_query_response()

    def _execute_simple(self, sql: str) -> int:
        """Execute a statement and return rows_affected."""
        _, _, affected = self._execute_query(sql)
        return affected

    def _read_query_response(
        self,
    ) -> tuple[list[ColumnMeta], list[list[Any]], int]:
        """Read a QueryResponse or ErrorResponse."""
        msg_type, payload = self._recv_frame()

        if msg_type == MSG_ERROR_RESPONSE:
            self._raise_error(payload)

        if msg_type != MSG_QUERY_RESPONSE:
            raise FalconError(
                f"unexpected response type: 0x{msg_type:02x}",
                error_code=0,
            )

        reader = WireReader(payload)
        _req_id = reader.u64()
        num_cols = reader.u16()

        columns: list[ColumnMeta] = []
        col_types: list[int] = []
        for _ in range(num_cols):
            name = reader.string_u16()
            type_id = reader.u8()
            nullable = reader.u8() != 0
            precision = reader.u16()
            scale = reader.u16()
            columns.append(ColumnMeta(name, type_id, nullable, precision, scale))
            col_types.append(type_id)

        num_rows = reader.u32()
        rows: list[list[Any]] = []
        for _ in range(num_rows):
            rows.append(decode_row(reader, col_types))

        rows_affected = reader.u64()
        return columns, rows, rows_affected

    # ── Internal: connection setup ───────────────────────────────────

    def _connect(self, timeout: float) -> None:
        """Establish TCP connection, handshake, and authenticate."""
        self._sock = socket.create_connection(
            (self._host, self._port), timeout=timeout
        )
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self._handshake()

        if self._ssl_enabled and (self._negotiated_features & FEATURE_TLS):
            self._upgrade_tls()

        self._authenticate()

    def _handshake(self) -> None:
        w = WireWriter()
        w.u16(0)  # version major
        w.u16(1)  # version minor
        features = (
            FEATURE_BATCH_INGEST
            | FEATURE_PIPELINE
            | FEATURE_EPOCH_FENCING
            | FEATURE_BINARY_PARAMS
        )
        if self._ssl_enabled:
            features |= FEATURE_TLS
        w.u64(features)
        w.string_u16("falcondb-python/0.2")
        w.string_u16(self._database)
        w.string_u16(self._user)
        w.raw(os.urandom(16))  # nonce
        w.u16(0)  # num_params

        self._send_frame(MSG_CLIENT_HELLO, w.to_bytes())

        # Read ServerHello
        msg_type, payload = self._recv_frame()
        if msg_type == MSG_ERROR_RESPONSE:
            self._raise_error(payload)
        if msg_type != MSG_SERVER_HELLO:
            raise FalconError(f"expected ServerHello, got 0x{msg_type:02x}")

        reader = WireReader(payload)
        _server_major = reader.u16()
        _server_minor = reader.u16()
        self._negotiated_features = reader.u64()
        self._server_epoch = reader.u64()
        self._server_node_id = reader.u64()
        _server_nonce = reader.raw(16)
        num_params = reader.u16()
        for _ in range(num_params):
            reader.string_u16()  # key
            reader.string_u16()  # value

        # Read AuthRequest
        msg_type, payload = self._recv_frame()
        if msg_type != MSG_AUTH_REQUEST:
            raise FalconError(f"expected AuthRequest, got 0x{msg_type:02x}")
        reader = WireReader(payload)
        _auth_method = reader.u8()
        # Consume remaining challenge bytes
        if reader.remaining > 0:
            reader.raw(reader.remaining)

    def _authenticate(self) -> None:
        w = WireWriter()
        w.u8(AUTH_PASSWORD)
        w.raw(self._password.encode("utf-8"))
        self._send_frame(MSG_AUTH_RESPONSE, w.to_bytes())

        msg_type, payload = self._recv_frame()
        if msg_type == MSG_AUTH_OK:
            return
        if msg_type == MSG_AUTH_FAIL:
            reason = ""
            if payload:
                reader = WireReader(payload)
                reason = reader.string_u16()
            raise AuthenticationError(
                f"authentication failed: {reason}",
                error_code=4000,
                sqlstate="28P01",
            )
        raise FalconError(f"unexpected auth response: 0x{msg_type:02x}")

    def _upgrade_tls(self) -> None:
        ctx = ssl.create_default_context()
        if not self._ssl_verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self._sock = ctx.wrap_socket(self._sock, server_hostname=self._host)  # type: ignore[arg-type]

    # ── Internal: framing ────────────────────────────────────────────

    def _send_frame(self, msg_type: int, payload: bytes) -> None:
        frame = build_frame(msg_type, payload)
        self._sock.sendall(frame)  # type: ignore[union-attr]

    def _recv_frame(self) -> tuple[int, bytes]:
        header = self._recv_exact(5)
        msg_type = header[0]
        length = struct.unpack_from("<I", header, 1)[0]
        if length > MAX_FRAME_SIZE:
            raise FalconError(f"frame too large: {length}")
        payload = self._recv_exact(length) if length > 0 else b""
        return msg_type, payload

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))  # type: ignore[union-attr]
            if not chunk:
                raise FalconError("connection closed by server")
            buf.extend(chunk)
        return bytes(buf)

    def _raise_error(self, payload: bytes) -> None:
        reader = WireReader(payload)
        _req_id = reader.u64()
        error_code = reader.u32()
        sqlstate = reader.raw(5).decode("ascii")
        _retryable = reader.u8()
        server_epoch = reader.u64()
        message = reader.string_u16()
        raise make_error(message, error_code, sqlstate, server_epoch)

    def _check_open(self) -> None:
        if self._closed or self._sock is None:
            raise FalconError("connection is closed")
