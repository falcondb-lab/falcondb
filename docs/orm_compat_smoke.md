# ORM Compatibility Smoke Test — FalconDB v1.x

> Date: 2026-02-24  
> Status: **Verified** (server-side catalog queries tested)

---

## 1. Supported Introspection Surface

FalconDB implements the following catalog/introspection surfaces that ORMs
and JDBC `DatabaseMetaData` rely on:

### information_schema

| View | Status | Tested |
|------|--------|--------|
| `information_schema.tables` | ✅ Full | `test_information_schema_tables` |
| `information_schema.columns` | ✅ Full (15 columns incl. udt_name, identity) | `test_information_schema_columns` |
| `information_schema.schemata` | ✅ Full | `test_information_schema_schemata` |
| `information_schema.table_constraints` | ✅ PK/UNIQUE/FK/CHECK | `test_information_schema_table_constraints` |
| `information_schema.key_column_usage` | ✅ PK/UNIQUE keys | `test_information_schema_key_column_usage` |

### pg_catalog

| Table | Status | Use Case |
|-------|--------|----------|
| `pg_type` | ✅ 14 builtin types, filter by oid/typname | JDBC type mapping |
| `pg_namespace` | ✅ pg_catalog/public/information_schema | Schema discovery |
| `pg_class` | ✅ Table listing with oid/relname/relkind | ORM table detection |
| `pg_attribute` | ✅ Column details per table | `\d table` in psql |
| `pg_index` | ✅ PK + UNIQUE indexes | Index introspection |
| `pg_constraint` | ✅ PK/UNIQUE/FK/CHECK with conkey/confkey | Constraint discovery |
| `pg_database` | ✅ Single database listing | Connection validation |
| `pg_settings` | ✅ 12 parameters | GUC queries |
| `pg_am` | ✅ btree/hash | Index method queries |
| `pg_stat_user_tables` | ✅ Stub (zero stats) | Hibernate/Spring startup probe |
| `pg_statio_user_tables` | ✅ Stub (zero I/O stats) | ORM startup probe |
| `pg_description` | ✅ Empty (no comments) | ORM startup probe |
| `pg_proc` | ✅ Empty (no UDFs) | ORM function introspection |
| `pg_enum` | ✅ Empty (no enums) | ORM enum introspection |

### JDBC DatabaseMetaData

| Method | Status | Server Query |
|--------|--------|-------------|
| `getDatabaseProductName()` | ✅ "FalconDB" | — |
| `getDatabaseProductVersion()` | ✅ "1.0.0" | — |
| `getTables()` | ✅ | `information_schema.tables` |
| `getColumns()` | ✅ | `information_schema.columns` |
| `getPrimaryKeys()` | ✅ | `information_schema.key_column_usage` |
| `getIndexInfo()` | ✅ | `pg_catalog.pg_index` |
| `getSchemas()` | ✅ | `information_schema.schemata` |
| `getCatalogs()` | ✅ | `pg_catalog.pg_database` |
| `getTableTypes()` | ✅ | BASE TABLE / VIEW |
| `getTypeInfo()` | ✅ | `pg_catalog.pg_type` |
| `supportsTransactions()` | ✅ true | — |
| `supportsSavepoints()` | ✅ true | — |

---

## 2. Spring JDBC Smoke Test

### Prerequisites
- FalconDB running on `localhost:5432`
- JDBC driver JAR in classpath

### Test Steps

```java
// 1. Connect
Connection conn = DriverManager.getConnection(
    "jdbc:falcondb://localhost:5432/falcon", "falcon", "");

// 2. Create table
Statement stmt = conn.createStatement();
stmt.execute("CREATE TABLE spring_test (id INT PRIMARY KEY, name TEXT, active BOOLEAN)");

// 3. Insert
stmt.execute("INSERT INTO spring_test VALUES (1, 'alice', true)");
stmt.execute("INSERT INTO spring_test VALUES (2, 'bob', false)");

// 4. Query
ResultSet rs = stmt.executeQuery("SELECT * FROM spring_test ORDER BY id");
assert rs.next() && rs.getInt("id") == 1;
assert rs.next() && rs.getString("name").equals("bob");

// 5. Metadata
DatabaseMetaData meta = conn.getMetaData();
assert meta.getDatabaseProductName().equals("FalconDB");
ResultSet tables = meta.getTables(null, null, "spring_test", null);
assert tables.next();
ResultSet cols = meta.getColumns(null, null, "spring_test", null);
int colCount = 0;
while (cols.next()) colCount++;
assert colCount == 3;

// 6. Cleanup
stmt.execute("DROP TABLE spring_test");
conn.close();
```

### Expected Result
All assertions pass. No `SQLFeatureNotSupportedException` thrown.

---

## 3. Hibernate Smoke Test

### Minimal Configuration

```properties
hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect
hibernate.connection.driver_class=io.falcondb.jdbc.FalconDriver
hibernate.connection.url=jdbc:falcondb://localhost:5432/falcon
hibernate.connection.username=falcon
hibernate.connection.password=
hibernate.hbm2ddl.auto=validate
```

### Expected Behavior
- Hibernate connects successfully
- `SessionFactory` creation queries `pg_catalog.pg_type`, `pg_namespace`, `pg_settings`
- `pg_stat_user_tables` returns empty rows (no startup failure)
- Schema validation via `information_schema.columns` succeeds
- Basic CRUD (save/load/delete) works

### Known Limitations
- `hibernate.hbm2ddl.auto=create` may fail for advanced DDL (triggers, sequences with custom properties)
- Use `validate` or `none` for `hbm2ddl.auto`
- No stored procedure support (`pg_proc` returns empty)

---

## 4. MyBatis Smoke Test

### Minimal Configuration

```xml
<configuration>
  <environments default="falcon">
    <environment id="falcon">
      <transactionManager type="JDBC"/>
      <dataSource type="POOLED">
        <property name="driver" value="io.falcondb.jdbc.FalconDriver"/>
        <property name="url" value="jdbc:falcondb://localhost:5432/falcon"/>
        <property name="username" value="falcon"/>
        <property name="password" value=""/>
      </dataSource>
    </environment>
  </environments>
</configuration>
```

### Expected Behavior
- Connection pool initializes successfully
- Mapper XML with `SELECT`, `INSERT`, `UPDATE`, `DELETE` works
- `#{param}` parameter binding uses prepared statements
- Transaction commit/rollback works

---

## 5. Verification Commands

```bash
# Server-side catalog tests (Rust)
cargo test -p falcon_protocol_pg -- "information_schema"
cargo test -p falcon_protocol_pg -- "pg_type"
cargo test -p falcon_protocol_pg -- "pg_namespace"
cargo test -p falcon_protocol_pg -- "pg_database"
cargo test -p falcon_protocol_pg -- "pg_settings"
cargo test -p falcon_protocol_pg -- "pg_index"
cargo test -p falcon_protocol_pg -- "pg_constraint"

# JDBC driver tests
mvn -f clients/falcondb-jdbc/pom.xml test
```
