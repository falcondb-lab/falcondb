package io.falcondb.jdbc.protocol;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Low-level encode/decode for the FalconDB Native Protocol wire format.
 * All multi-byte integers are little-endian.
 */
public final class WireFormat {

    // Message type tags
    public static final byte MSG_CLIENT_HELLO    = 0x01;
    public static final byte MSG_SERVER_HELLO    = 0x02;
    public static final byte MSG_AUTH_REQUEST    = 0x03;
    public static final byte MSG_AUTH_RESPONSE   = 0x04;
    public static final byte MSG_AUTH_OK         = 0x05;
    public static final byte MSG_AUTH_FAIL       = 0x06;
    public static final byte MSG_QUERY_REQUEST   = 0x10;
    public static final byte MSG_QUERY_RESPONSE  = 0x11;
    public static final byte MSG_ERROR_RESPONSE  = 0x12;
    public static final byte MSG_BATCH_REQUEST   = 0x13;
    public static final byte MSG_BATCH_RESPONSE  = 0x14;
    public static final byte MSG_PING            = 0x20;
    public static final byte MSG_PONG            = 0x21;
    public static final byte MSG_DISCONNECT      = 0x30;
    public static final byte MSG_DISCONNECT_ACK  = 0x31;

    // Type IDs
    public static final byte TYPE_NULL      = 0x00;
    public static final byte TYPE_BOOLEAN   = 0x01;
    public static final byte TYPE_INT32     = 0x02;
    public static final byte TYPE_INT64     = 0x03;
    public static final byte TYPE_FLOAT64   = 0x04;
    public static final byte TYPE_TEXT      = 0x05;
    public static final byte TYPE_TIMESTAMP = 0x06;
    public static final byte TYPE_DATE      = 0x07;
    public static final byte TYPE_JSONB     = 0x08;
    public static final byte TYPE_DECIMAL   = 0x09;
    public static final byte TYPE_TIME      = 0x0A;
    public static final byte TYPE_INTERVAL  = 0x0B;
    public static final byte TYPE_UUID      = 0x0C;
    public static final byte TYPE_BYTEA     = 0x0D;
    public static final byte TYPE_ARRAY     = 0x0E;

    // Feature flags
    public static final long FEATURE_COMPRESSION_LZ4  = 1L;
    public static final long FEATURE_COMPRESSION_ZSTD = 1L << 1;
    public static final long FEATURE_BATCH_INGEST     = 1L << 2;
    public static final long FEATURE_PIPELINE         = 1L << 3;
    public static final long FEATURE_EPOCH_FENCING    = 1L << 4;
    public static final long FEATURE_TLS              = 1L << 5;
    public static final long FEATURE_BINARY_PARAMS    = 1L << 6;

    // Auth methods
    public static final byte AUTH_PASSWORD     = 0;
    public static final byte AUTH_TOKEN        = 1;

    // Session flags
    public static final int SESSION_AUTOCOMMIT = 1;
    public static final int SESSION_READ_ONLY  = 1 << 1;

    // Error codes
    public static final int ERR_SYNTAX_ERROR           = 1000;
    public static final int ERR_INVALID_PARAM          = 1001;
    public static final int ERR_NOT_LEADER             = 2000;
    public static final int ERR_FENCED_EPOCH           = 2001;
    public static final int ERR_READ_ONLY              = 2002;
    public static final int ERR_SERIALIZATION_CONFLICT = 2003;
    public static final int ERR_INTERNAL_ERROR         = 3000;
    public static final int ERR_TIMEOUT                = 3001;
    public static final int ERR_OVERLOADED             = 3002;
    public static final int ERR_AUTH_FAILED            = 4000;
    public static final int ERR_PERMISSION_DENIED      = 4001;

    // Max frame size: 64 MiB
    public static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;
    public static final int FRAME_HEADER_SIZE = 5;

    private WireFormat() {}

    // ── Write helpers ────────────────────────────────────────────────

    public static void writeFrame(OutputStream out, byte msgType, byte[] payload) throws IOException {
        out.write(msgType);
        writeU32(out, payload.length);
        out.write(payload);
        out.flush();
    }

    public static void writeU16(OutputStream out, int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >> 8) & 0xFF);
    }

    public static void writeU32(OutputStream out, int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >> 8) & 0xFF);
        out.write((v >> 16) & 0xFF);
        out.write((v >> 24) & 0xFF);
    }

    public static void writeU64(OutputStream out, long v) throws IOException {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >> (i * 8)) & 0xFF));
        }
    }

    public static void writeI32(OutputStream out, int v) throws IOException {
        writeU32(out, v);
    }

    public static void writeI64(OutputStream out, long v) throws IOException {
        writeU64(out, v);
    }

    public static void writeStringU16(OutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeU16(out, bytes.length);
        out.write(bytes);
    }

    public static void writeStringU32(OutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeU32(out, bytes.length);
        out.write(bytes);
    }

    // ── Read helpers ─────────────────────────────────────────────────

    public static void readFully(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n < 0) throw new IOException("unexpected end of stream");
            off += n;
        }
    }

    public static int readU8(InputStream in) throws IOException {
        int v = in.read();
        if (v < 0) throw new IOException("unexpected end of stream");
        return v;
    }

    public static int readU16(InputStream in) throws IOException {
        byte[] b = new byte[2];
        readFully(in, b);
        return (b[0] & 0xFF) | ((b[1] & 0xFF) << 8);
    }

    public static int readU32(InputStream in) throws IOException {
        byte[] b = new byte[4];
        readFully(in, b);
        return (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) |
               ((b[2] & 0xFF) << 16) | ((b[3] & 0xFF) << 24);
    }

    public static long readU64(InputStream in) throws IOException {
        byte[] b = new byte[8];
        readFully(in, b);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= ((long) (b[i] & 0xFF)) << (i * 8);
        }
        return v;
    }

    public static int readI32(InputStream in) throws IOException {
        return readU32(in);
    }

    public static long readI64(InputStream in) throws IOException {
        return readU64(in);
    }

    public static double readF64(InputStream in) throws IOException {
        return Double.longBitsToDouble(readI64(in));
    }

    public static String readStringU16(InputStream in) throws IOException {
        int len = readU16(in);
        byte[] buf = new byte[len];
        readFully(in, buf);
        return new String(buf, StandardCharsets.UTF_8);
    }

    public static String readStringU32(InputStream in) throws IOException {
        int len = readU32(in);
        byte[] buf = new byte[len];
        readFully(in, buf);
        return new String(buf, StandardCharsets.UTF_8);
    }

    // ── Frame read ───────────────────────────────────────────────────

    /**
     * Read a complete frame header. Returns [msgType, payloadLength].
     */
    public static int[] readFrameHeader(InputStream in) throws IOException {
        int msgType = readU8(in);
        int length = readU32(in);
        if (length > MAX_FRAME_SIZE) {
            throw new IOException("frame too large: " + length);
        }
        return new int[]{msgType, length};
    }

    /**
     * Map a native type_id to java.sql.Types constant.
     */
    public static int typeIdToSqlType(byte typeId) {
        switch (typeId) {
            case TYPE_BOOLEAN:   return java.sql.Types.BOOLEAN;
            case TYPE_INT32:     return java.sql.Types.INTEGER;
            case TYPE_INT64:     return java.sql.Types.BIGINT;
            case TYPE_FLOAT64:   return java.sql.Types.DOUBLE;
            case TYPE_TEXT:      return java.sql.Types.VARCHAR;
            case TYPE_TIMESTAMP: return java.sql.Types.TIMESTAMP;
            case TYPE_DATE:      return java.sql.Types.DATE;
            case TYPE_JSONB:     return java.sql.Types.VARCHAR;
            case TYPE_DECIMAL:   return java.sql.Types.DECIMAL;
            case TYPE_TIME:      return java.sql.Types.TIME;
            case TYPE_INTERVAL:  return java.sql.Types.VARCHAR;
            case TYPE_UUID:      return java.sql.Types.VARCHAR;
            case TYPE_BYTEA:     return java.sql.Types.BINARY;
            case TYPE_ARRAY:     return java.sql.Types.ARRAY;
            default:             return java.sql.Types.OTHER;
        }
    }

    /**
     * Map a native type_id to a SQL type name.
     */
    public static String typeIdToName(byte typeId) {
        switch (typeId) {
            case TYPE_BOOLEAN:   return "BOOLEAN";
            case TYPE_INT32:     return "INTEGER";
            case TYPE_INT64:     return "BIGINT";
            case TYPE_FLOAT64:   return "DOUBLE";
            case TYPE_TEXT:      return "TEXT";
            case TYPE_TIMESTAMP: return "TIMESTAMP";
            case TYPE_DATE:      return "DATE";
            case TYPE_JSONB:     return "JSONB";
            case TYPE_DECIMAL:   return "DECIMAL";
            case TYPE_TIME:      return "TIME";
            case TYPE_INTERVAL:  return "INTERVAL";
            case TYPE_UUID:      return "UUID";
            case TYPE_BYTEA:     return "BYTEA";
            case TYPE_ARRAY:     return "ARRAY";
            default:             return "UNKNOWN";
        }
    }

    /**
     * Returns true if the error code indicates the client should retry.
     */
    public static boolean isRetryable(int errorCode) {
        return errorCode == ERR_NOT_LEADER
            || errorCode == ERR_FENCED_EPOCH
            || errorCode == ERR_READ_ONLY
            || errorCode == ERR_SERIALIZATION_CONFLICT
            || errorCode == ERR_TIMEOUT
            || errorCode == ERR_OVERLOADED;
    }
}
