package io.falcondb.jdbc.protocol;

import java.sql.SQLException;

/**
 * SQLException subclass carrying FalconDB native error metadata.
 */
public class FalconSQLException extends SQLException {

    private final int nativeErrorCode;
    private final boolean retryable;

    public FalconSQLException(String message, String sqlState, int nativeErrorCode, boolean retryable) {
        super(message, sqlState, nativeErrorCode);
        this.nativeErrorCode = nativeErrorCode;
        this.retryable = retryable;
    }

    public int getNativeErrorCode() {
        return nativeErrorCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
