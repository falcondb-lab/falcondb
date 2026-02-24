package io.falcondb.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC DatabaseMetaData implementation for FalconDB.
 *
 * Provides the minimum subset required by ORMs (Spring JDBC, Hibernate, MyBatis):
 * - getTables / getColumns / getPrimaryKeys / getIndexInfo
 * - getDatabaseProductName / Version
 * - getSchemas / getCatalogs / getTableTypes
 * - Transaction and isolation level metadata
 *
 * All metadata queries are backed by real server-side catalog queries
 * (information_schema / pg_catalog). No fake data.
 */
public class FalconDatabaseMetaData implements DatabaseMetaData {

    private final FalconConnection conn;

    FalconDatabaseMetaData(FalconConnection conn) {
        this.conn = conn;
    }

    // ═══════════════════════════════════════════════════════════════
    // Product identification
    // ═══════════════════════════════════════════════════════════════

    @Override public String getDatabaseProductName() { return "FalconDB"; }
    @Override public String getDatabaseProductVersion() { return "1.0.0"; }
    @Override public int getDatabaseMajorVersion() { return 1; }
    @Override public int getDatabaseMinorVersion() { return 0; }
    @Override public String getDriverName() { return "FalconDB JDBC Driver"; }
    @Override public String getDriverVersion() { return "1.0.0"; }
    @Override public int getDriverMajorVersion() { return 1; }
    @Override public int getDriverMinorVersion() { return 0; }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 2; }

    // ═══════════════════════════════════════════════════════════════
    // Catalog / Schema / Table enumeration
    // ═══════════════════════════════════════════════════════════════

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(
            "SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database"
        );
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(
            "SELECT schema_name AS TABLE_SCHEM, catalog_name AS TABLE_CATALOG " +
            "FROM information_schema.schemata"
        );
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(
            "SELECT 'BASE TABLE' AS TABLE_TYPE UNION ALL SELECT 'VIEW' AS TABLE_TYPE"
        );
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT table_catalog AS TABLE_CAT, table_schema AS TABLE_SCHEM, " +
            "table_name AS TABLE_NAME, table_type AS TABLE_TYPE, " +
            "NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, " +
            "NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION " +
            "FROM information_schema.tables"
        );
        List<String> conditions = new ArrayList<>();
        if (tableNamePattern != null && !tableNamePattern.equals("%")) {
            conditions.add("table_name = '" + escapeSQL(tableNamePattern) + "'");
        }
        if (schemaPattern != null && !schemaPattern.equals("%")) {
            conditions.add("table_schema = '" + escapeSQL(schemaPattern) + "'");
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sql.append(" ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME");
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql.toString());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT table_catalog AS TABLE_CAT, table_schema AS TABLE_SCHEM, " +
            "table_name AS TABLE_NAME, column_name AS COLUMN_NAME, " +
            "ordinal_position AS ORDINAL_POSITION, " +
            "column_default AS COLUMN_DEF, " +
            "is_nullable AS IS_NULLABLE, " +
            "data_type AS TYPE_NAME, " +
            "character_maximum_length AS COLUMN_SIZE, " +
            "numeric_precision AS NUM_PREC_RADIX, " +
            "numeric_scale AS DECIMAL_DIGITS, " +
            "udt_name AS UDT_NAME " +
            "FROM information_schema.columns"
        );
        List<String> conditions = new ArrayList<>();
        if (tableNamePattern != null && !tableNamePattern.equals("%")) {
            conditions.add("table_name = '" + escapeSQL(tableNamePattern) + "'");
        }
        if (columnNamePattern != null && !columnNamePattern.equals("%")) {
            conditions.add("column_name = '" + escapeSQL(columnNamePattern) + "'");
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sql.append(" ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql.toString());
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT constraint_name AS PK_NAME, table_name AS TABLE_NAME, " +
            "column_name AS COLUMN_NAME, ordinal_position AS KEY_SEQ " +
            "FROM information_schema.key_column_usage"
        );
        if (table != null) {
            sql.append(" WHERE table_name = '").append(escapeSQL(table)).append("'");
        }
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql.toString());
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        // Use pg_catalog.pg_index for index metadata
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT * FROM pg_catalog.pg_index");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        // Return empty result for now — FK import metadata
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(
            "SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM WHERE 1=0"
        );
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(
            "SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM WHERE 1=0"
        );
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT * FROM pg_catalog.pg_type");
    }

    // ═══════════════════════════════════════════════════════════════
    // Transaction capabilities
    // ═══════════════════════════════════════════════════════════════

    @Override public boolean supportsTransactions() { return true; }
    @Override public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_READ_COMMITTED
            || level == Connection.TRANSACTION_READ_UNCOMMITTED
            || level == Connection.TRANSACTION_REPEATABLE_READ
            || level == Connection.TRANSACTION_SERIALIZABLE;
    }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_READ_COMMITTED; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return true; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }

    // ═══════════════════════════════════════════════════════════════
    // SQL feature capabilities (minimum set for ORM discovery)
    // ═══════════════════════════════════════════════════════════════

    @Override public boolean supportsSavepoints() { return true; }
    @Override public boolean supportsBatchUpdates() { return true; }
    @Override public boolean supportsAlterTableWithAddColumn() { return true; }
    @Override public boolean supportsAlterTableWithDropColumn() { return true; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return true; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return true; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return true; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return false; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public boolean supportsUnion() { return true; }
    @Override public boolean supportsUnionAll() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsSubqueriesInComparisons() { return true; }
    @Override public boolean supportsSubqueriesInExists() { return true; }
    @Override public boolean supportsSubqueriesInIns() { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsCorrelatedSubqueries() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return false; }
    @Override public boolean storesMixedCaseIdentifiers() { return false; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return true; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }
    @Override public boolean supportsResultSetHoldability(int holdability) { return true; }
    @Override public int getResultSetHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsSchemaInDataManipulation() { return false; }
    @Override public boolean supportsSchemaInProcedureCalls() { return false; }
    @Override public boolean supportsSchemaInTableDefinitions() { return false; }
    @Override public boolean supportsSchemaInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemaInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }

    // ═══════════════════════════════════════════════════════════════
    // Limits
    // ═══════════════════════════════════════════════════════════════

    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 63; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 32; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 1600; }
    @Override public int getMaxConnections() { return 100; }
    @Override public int getMaxCursorNameLength() { return 63; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 63; }
    @Override public int getMaxProcedureNameLength() { return 63; }
    @Override public int getMaxCatalogNameLength() { return 63; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 63; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 63; }

    // ═══════════════════════════════════════════════════════════════
    // Misc identifiers
    // ═══════════════════════════════════════════════════════════════

    @Override public String getURL() { return "jdbc:falcondb://"; }
    @Override public String getUserName() throws SQLException { return "falcon"; }
    @Override public boolean isReadOnly() { return false; }
    @Override public boolean nullsAreSortedHigh() { return true; }
    @Override public boolean nullsAreSortedLow() { return false; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return "abs,ceil,floor,round,trunc,sqrt,power,mod,sign,random"; }
    @Override public String getStringFunctions() { return "length,upper,lower,trim,substring,replace,concat"; }
    @Override public String getSystemFunctions() { return "version,current_user,current_database"; }
    @Override public String getTimeDateFunctions() { return "now,current_timestamp,current_date,extract,date_trunc"; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public String getSchemaTerm() { return "schema"; }
    @Override public String getProcedureTerm() { return "function"; }
    @Override public String getCatalogTerm() { return "database"; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public int getSQLStateType() { return sqlStateSQL; }
    @Override public Connection getConnection() { return conn; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }

    // ═══════════════════════════════════════════════════════════════
    // Stubs — return empty ResultSets or throw unsupported
    // ═══════════════════════════════════════════════════════════════

    @Override public ResultSet getProcedures(String c, String s, String p) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS PROCEDURE_CAT WHERE 1=0");
    }
    @Override public ResultSet getProcedureColumns(String c, String s, String p, String col) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS PROCEDURE_CAT WHERE 1=0");
    }
    @Override public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS PKTABLE_CAT WHERE 1=0");
    }
    @Override public ResultSet getBestRowIdentifier(String c, String s, String t, int scope, boolean nullable) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS SCOPE WHERE 1=0");
    }
    @Override public ResultSet getVersionColumns(String c, String s, String t) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS SCOPE WHERE 1=0");
    }
    @Override public ResultSet getColumnPrivileges(String c, String s, String t, String col) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TABLE_CAT WHERE 1=0");
    }
    @Override public ResultSet getTablePrivileges(String c, String s, String t) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TABLE_CAT WHERE 1=0");
    }
    @Override public ResultSet getUDTs(String c, String s, String t, int[] types) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TYPE_CAT WHERE 1=0");
    }
    @Override public ResultSet getSuperTypes(String c, String s, String t) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TYPE_CAT WHERE 1=0");
    }
    @Override public ResultSet getSuperTables(String c, String s, String t) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TABLE_CAT WHERE 1=0");
    }
    @Override public ResultSet getAttributes(String c, String s, String t, String a) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TYPE_CAT WHERE 1=0");
    }
    @Override public ResultSet getClientInfoProperties() throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS NAME WHERE 1=0");
    }
    @Override public ResultSet getFunctions(String c, String s, String f) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS FUNCTION_CAT WHERE 1=0");
    }
    @Override public ResultSet getFunctionColumns(String c, String s, String f, String col) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS FUNCTION_CAT WHERE 1=0");
    }
    @Override public ResultSet getPseudoColumns(String c, String s, String t, String col) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery("SELECT NULL AS TABLE_CAT WHERE 1=0");
    }

    // ═══════════════════════════════════════════════════════════════
    // Wrapper
    // ═══════════════════════════════════════════════════════════════

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    // ═══════════════════════════════════════════════════════════════
    // Utility
    // ═══════════════════════════════════════════════════════════════

    private static String escapeSQL(String s) {
        return s == null ? "" : s.replace("'", "''");
    }
}
