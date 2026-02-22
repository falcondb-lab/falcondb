package io.falcondb.jdbc.protocol;

import java.io.*;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.falcondb.jdbc.protocol.WireFormat.*;

/**
 * Low-level native protocol connection. Handles framing, handshake, auth,
 * and request/response exchange over a TCP socket.
 */
public class NativeConnection implements Closeable {

    private final Socket socket;
    private final OutputStream out;
    private final InputStream in;
    private final AtomicLong requestIdSeq = new AtomicLong(1);

    private long serverEpoch;
    private long serverNodeId;
    private long negotiatedFeatures;
    private boolean authenticated;
    private boolean closed;

    public NativeConnection(String host, int port, String database, String user,
                            String password, int connectTimeoutMs) throws IOException {
        this.socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        socket.setTcpNoDelay(true);
        this.out = new BufferedOutputStream(socket.getOutputStream(), 65536);
        this.in = new BufferedInputStream(socket.getInputStream(), 65536);

        handshake(database, user);
        authenticate(password);
    }

    private void handshake(String database, String user) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(256);
        // version
        writeU16(payload, 0); // major
        writeU16(payload, 1); // minor
        // feature flags
        writeU64(payload, FEATURE_BATCH_INGEST | FEATURE_PIPELINE | FEATURE_EPOCH_FENCING | FEATURE_BINARY_PARAMS);
        // client_name
        writeStringU16(payload, "falcondb-jdbc/0.1");
        // database
        writeStringU16(payload, database);
        // user
        writeStringU16(payload, user);
        // nonce (16 bytes)
        byte[] nonce = new byte[16];
        new Random().nextBytes(nonce);
        payload.write(nonce);
        // params: 0
        writeU16(payload, 0);

        writeFrame(out, MSG_CLIENT_HELLO, payload.toByteArray());

        // Read ServerHello
        int[] header = readFrameHeader(in);
        if (header[0] == MSG_ERROR_RESPONSE) {
            readErrorAndThrow(header[1]);
        }
        if (header[0] != MSG_SERVER_HELLO) {
            throw new IOException("expected ServerHello, got 0x" + Integer.toHexString(header[0]));
        }
        // Parse ServerHello
        int serverMajor = readU16(in);
        int serverMinor = readU16(in);
        this.negotiatedFeatures = readU64(in);
        this.serverEpoch = readU64(in);
        this.serverNodeId = readU64(in);
        byte[] serverNonce = new byte[16];
        readFully(in, serverNonce);
        int numParams = readU16(in);
        for (int i = 0; i < numParams; i++) {
            readStringU16(in); // key
            readStringU16(in); // value
        }

        // Read AuthRequest
        header = readFrameHeader(in);
        if (header[0] != MSG_AUTH_REQUEST) {
            throw new IOException("expected AuthRequest, got 0x" + Integer.toHexString(header[0]));
        }
        int authMethod = readU8(in);
        // Read remaining challenge bytes
        int challengeLen = header[1] - 1;
        if (challengeLen > 0) {
            byte[] challenge = new byte[challengeLen];
            readFully(in, challenge);
        }
    }

    private void authenticate(String password) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(64);
        payload.write(AUTH_PASSWORD);
        byte[] cred = password.getBytes(StandardCharsets.UTF_8);
        payload.write(cred);

        writeFrame(out, MSG_AUTH_RESPONSE, payload.toByteArray());

        int[] header = readFrameHeader(in);
        if (header[0] == MSG_AUTH_OK) {
            // consume any payload (should be 0)
            if (header[1] > 0) {
                byte[] skip = new byte[header[1]];
                readFully(in, skip);
            }
            this.authenticated = true;
        } else if (header[0] == MSG_AUTH_FAIL) {
            String reason = "";
            if (header[1] > 0) {
                ByteArrayInputStream bais = new ByteArrayInputStream(readPayload(header[1]));
                reason = readStringU16(bais);
            }
            throw new IOException("authentication failed: " + reason);
        } else {
            throw new IOException("unexpected response to auth: 0x" + Integer.toHexString(header[0]));
        }
    }

    /**
     * Execute a SQL query and return the parsed response.
     */
    public QueryResult executeQuery(String sql, int sessionFlags) throws IOException {
        long reqId = requestIdSeq.getAndIncrement();

        ByteArrayOutputStream payload = new ByteArrayOutputStream(256);
        writeU64(payload, reqId);
        writeU64(payload, serverEpoch);
        writeStringU32(payload, sql);
        writeU16(payload, 0); // no params
        writeU32(payload, sessionFlags);

        writeFrame(out, MSG_QUERY_REQUEST, payload.toByteArray());

        return readQueryResponse(reqId);
    }

    /**
     * Execute a batch of parameter sets against a SQL template.
     */
    public BatchResult executeBatch(String sql, byte[] columnTypes,
                                    List<Object[]> paramSets, int options) throws IOException {
        long reqId = requestIdSeq.getAndIncrement();

        ByteArrayOutputStream payload = new ByteArrayOutputStream(4096);
        writeU64(payload, reqId);
        writeU64(payload, serverEpoch);
        writeStringU32(payload, sql);
        writeU16(payload, columnTypes.length);
        payload.write(columnTypes);
        writeU32(payload, paramSets.size());
        for (Object[] row : paramSets) {
            encodeRow(payload, columnTypes, row);
        }
        writeU32(payload, options);

        writeFrame(out, MSG_BATCH_REQUEST, payload.toByteArray());

        return readBatchResponse(reqId);
    }

    /**
     * Send a ping and wait for pong.
     */
    public boolean ping(int timeoutMs) throws IOException {
        int oldTimeout = socket.getSoTimeout();
        try {
            socket.setSoTimeout(timeoutMs);
            writeFrame(out, MSG_PING, new byte[0]);
            int[] header = readFrameHeader(in);
            if (header[1] > 0) {
                byte[] skip = new byte[header[1]];
                readFully(in, skip);
            }
            return header[0] == MSG_PONG;
        } catch (IOException e) {
            return false;
        } finally {
            socket.setSoTimeout(oldTimeout);
        }
    }

    /**
     * Graceful disconnect.
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                writeFrame(out, MSG_DISCONNECT, new byte[0]);
                // Try to read DisconnectAck but don't block
                socket.setSoTimeout(1000);
                try {
                    readFrameHeader(in);
                } catch (IOException ignored) {}
            } finally {
                socket.close();
            }
        }
    }

    public boolean isClosed() {
        return closed || socket.isClosed();
    }

    public long getServerEpoch() { return serverEpoch; }
    public long getServerNodeId() { return serverNodeId; }
    public long getNegotiatedFeatures() { return negotiatedFeatures; }

    // ── Internal helpers ─────────────────────────────────────────────

    private QueryResult readQueryResponse(long expectedReqId) throws IOException {
        int[] header = readFrameHeader(in);
        byte[] payload = readPayload(header[1]);
        ByteArrayInputStream bais = new ByteArrayInputStream(payload);

        if (header[0] == MSG_ERROR_RESPONSE) {
            ErrorInfo err = parseError(bais);
            throw new FalconSQLException(err.message, err.sqlstate, err.errorCode, err.retryable);
        }
        if (header[0] != MSG_QUERY_RESPONSE) {
            throw new IOException("expected QueryResponse, got 0x" + Integer.toHexString(header[0]));
        }

        long reqId = readU64(bais);
        int numCols = readU16(bais);
        ColumnMeta[] columns = new ColumnMeta[numCols];
        for (int i = 0; i < numCols; i++) {
            String name = readStringU16(bais);
            byte typeId = (byte) readU8(bais);
            boolean nullable = readU8(bais) != 0;
            int precision = readU16(bais);
            int scale = readU16(bais);
            columns[i] = new ColumnMeta(name, typeId, nullable, precision, scale);
        }

        byte[] colTypes = new byte[numCols];
        for (int i = 0; i < numCols; i++) colTypes[i] = columns[i].typeId;

        int numRows = readU32(bais);
        List<Object[]> rows = new ArrayList<>(numRows);
        for (int r = 0; r < numRows; r++) {
            rows.add(decodeRow(bais, colTypes));
        }
        long rowsAffected = readU64(bais);

        return new QueryResult(columns, rows, rowsAffected);
    }

    private BatchResult readBatchResponse(long expectedReqId) throws IOException {
        int[] header = readFrameHeader(in);
        byte[] payload = readPayload(header[1]);
        ByteArrayInputStream bais = new ByteArrayInputStream(payload);

        if (header[0] == MSG_ERROR_RESPONSE) {
            ErrorInfo err = parseError(bais);
            throw new FalconSQLException(err.message, err.sqlstate, err.errorCode, err.retryable);
        }
        if (header[0] != MSG_BATCH_RESPONSE) {
            throw new IOException("expected BatchResponse, got 0x" + Integer.toHexString(header[0]));
        }

        long reqId = readU64(bais);
        int numCounts = readU32(bais);
        long[] counts = new long[numCounts];
        for (int i = 0; i < numCounts; i++) {
            counts[i] = readI64(bais);
        }
        int hasError = readU8(bais);
        ErrorInfo error = null;
        if (hasError != 0) {
            error = parseError(bais);
        }
        return new BatchResult(counts, error);
    }

    private byte[] readPayload(int length) throws IOException {
        byte[] buf = new byte[length];
        readFully(in, buf);
        return buf;
    }

    private void readErrorAndThrow(int payloadLen) throws IOException {
        byte[] payload = readPayload(payloadLen);
        ByteArrayInputStream bais = new ByteArrayInputStream(payload);
        ErrorInfo err = parseError(bais);
        throw new FalconSQLException(err.message, err.sqlstate, err.errorCode, err.retryable);
    }

    private ErrorInfo parseError(InputStream in) throws IOException {
        long reqId = readU64(in);
        int errorCode = readU32(in);
        byte[] sqlstateBytes = new byte[5];
        readFully(in, sqlstateBytes);
        String sqlstate = new String(sqlstateBytes, StandardCharsets.US_ASCII);
        boolean retryable = readU8(in) != 0;
        long srvEpoch = readU64(in);
        String message = readStringU16(in);
        return new ErrorInfo(reqId, errorCode, sqlstate, retryable, srvEpoch, message);
    }

    private Object[] decodeRow(InputStream in, byte[] colTypes) throws IOException {
        int numCols = colTypes.length;
        int bitmapBytes = (numCols + 7) / 8;
        byte[] bitmap = new byte[bitmapBytes];
        readFully(in, bitmap);

        Object[] values = new Object[numCols];
        for (int i = 0; i < numCols; i++) {
            boolean isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0;
            if (isNull) {
                values[i] = null;
            } else {
                values[i] = decodeValue(in, colTypes[i]);
            }
        }
        return values;
    }

    private Object decodeValue(InputStream in, byte typeId) throws IOException {
        switch (typeId) {
            case TYPE_BOOLEAN:   return readU8(in) != 0;
            case TYPE_INT32:     return readI32(in);
            case TYPE_INT64:     return readI64(in);
            case TYPE_FLOAT64:   return readF64(in);
            case TYPE_TEXT:      return readStringU32(in);
            case TYPE_TIMESTAMP: return readI64(in);
            case TYPE_DATE:      return readI32(in);
            case TYPE_JSONB:     return readStringU32(in);
            case TYPE_TIME:      return readI64(in);
            case TYPE_BYTEA: {
                int len = readU32(in);
                byte[] data = new byte[len];
                readFully(in, data);
                return data;
            }
            default:
                // For unsupported types, read as text if possible
                return readStringU32(in);
        }
    }

    private void encodeRow(OutputStream out, byte[] colTypes, Object[] values) throws IOException {
        int numCols = colTypes.length;
        int bitmapBytes = (numCols + 7) / 8;
        byte[] bitmap = new byte[bitmapBytes];
        for (int i = 0; i < numCols; i++) {
            if (values[i] == null) {
                bitmap[i / 8] |= (1 << (i % 8));
            }
        }
        out.write(bitmap);
        for (int i = 0; i < numCols; i++) {
            if (values[i] != null) {
                encodeValue(out, colTypes[i], values[i]);
            }
        }
    }

    private void encodeValue(OutputStream out, byte typeId, Object value) throws IOException {
        switch (typeId) {
            case TYPE_BOOLEAN:
                out.write(((Boolean) value) ? 1 : 0);
                break;
            case TYPE_INT32:
                writeI32(out, ((Number) value).intValue());
                break;
            case TYPE_INT64:
                writeI64(out, ((Number) value).longValue());
                break;
            case TYPE_FLOAT64:
                writeI64(out, Double.doubleToLongBits(((Number) value).doubleValue()));
                break;
            case TYPE_TEXT:
            case TYPE_JSONB:
                writeStringU32(out, value.toString());
                break;
            case TYPE_TIMESTAMP:
            case TYPE_TIME:
                writeI64(out, ((Number) value).longValue());
                break;
            case TYPE_DATE:
                writeI32(out, ((Number) value).intValue());
                break;
            case TYPE_BYTEA:
                byte[] bytes = (byte[]) value;
                writeU32(out, bytes.length);
                out.write(bytes);
                break;
            default:
                writeStringU32(out, value.toString());
                break;
        }
    }

    // ── Inner types ──────────────────────────────────────────────────

    public static class ColumnMeta {
        public final String name;
        public final byte typeId;
        public final boolean nullable;
        public final int precision;
        public final int scale;

        public ColumnMeta(String name, byte typeId, boolean nullable, int precision, int scale) {
            this.name = name;
            this.typeId = typeId;
            this.nullable = nullable;
            this.precision = precision;
            this.scale = scale;
        }
    }

    public static class QueryResult {
        public final ColumnMeta[] columns;
        public final List<Object[]> rows;
        public final long rowsAffected;

        public QueryResult(ColumnMeta[] columns, List<Object[]> rows, long rowsAffected) {
            this.columns = columns;
            this.rows = rows;
            this.rowsAffected = rowsAffected;
        }
    }

    public static class BatchResult {
        public final long[] counts;
        public final ErrorInfo error;

        public BatchResult(long[] counts, ErrorInfo error) {
            this.counts = counts;
            this.error = error;
        }
    }

    public static class ErrorInfo {
        public final long requestId;
        public final int errorCode;
        public final String sqlstate;
        public final boolean retryable;
        public final long serverEpoch;
        public final String message;

        public ErrorInfo(long requestId, int errorCode, String sqlstate,
                         boolean retryable, long serverEpoch, String message) {
            this.requestId = requestId;
            this.errorCode = errorCode;
            this.sqlstate = sqlstate;
            this.retryable = retryable;
            this.serverEpoch = serverEpoch;
            this.message = message;
        }
    }
}
