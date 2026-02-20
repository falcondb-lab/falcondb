use cedar_common::error::SqlError;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Parse a SQL string into one or more AST statements using sqlparser-rs.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, SqlError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))
}
