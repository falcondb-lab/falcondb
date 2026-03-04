package io.falcondb.proc;

public class ProcException extends Exception {
    private final String sqlState;
    private final int vendorCode;

    public ProcException(String message, String sqlState) {
        super(message);
        this.sqlState = sqlState;
        this.vendorCode = 0;
    }

    public ProcException(String message, String sqlState, Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState;
        this.vendorCode = 0;
    }

    public ProcException(String message, String sqlState, int vendorCode, Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState;
        this.vendorCode = vendorCode;
    }

    public String getSqlState() { return sqlState; }
    public int getVendorCode() { return vendorCode; }

    public static ProcException fromSqlException(java.sql.SQLException e) {
        return new ProcException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
    }

    public static ProcException notFound(String procName) {
        return new ProcException("Procedure not found: " + procName, "42883");
    }
}
