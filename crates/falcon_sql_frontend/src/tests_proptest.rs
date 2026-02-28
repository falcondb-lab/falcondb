#[cfg(test)]
mod sql_parser_proptest {
    use crate::parser::parse_sql;
    use proptest::prelude::*;

    // ── Strategy: generate valid SQL identifiers ────────────────────────

    fn sql_identifier() -> impl Strategy<Value = String> {
        prop::string::string_regex("[a-z][a-z0-9_]{0,15}")
            .unwrap()
            .prop_filter("must not be empty", |s| !s.is_empty())
    }

    fn sql_table_name() -> impl Strategy<Value = String> {
        sql_identifier().prop_map(|s| format!("t_{s}"))
    }

    fn sql_column_name() -> impl Strategy<Value = String> {
        sql_identifier().prop_map(|s| format!("c_{s}"))
    }

    fn sql_type() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("INT"),
            Just("INTEGER"),
            Just("BIGINT"),
            Just("SMALLINT"),
            Just("TEXT"),
            Just("VARCHAR(255)"),
            Just("BOOLEAN"),
            Just("REAL"),
            Just("DOUBLE PRECISION"),
            Just("SERIAL"),
        ]
    }

    // ── Property: SELECT round-trip ─────────────────────────────────────
    // parse(sql) succeeds → to_string() → parse again → same AST

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn select_parses_and_round_trips(
            table in sql_table_name(),
            col in sql_column_name(),
        ) {
            let sql = format!("SELECT {col} FROM {table}");
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");
            let stmts = stmts.unwrap();
            prop_assert_eq!(stmts.len(), 1);

            // Round-trip: AST → string → parse again
            let reparsed_sql = stmts[0].to_string();
            let stmts2 = parse_sql(&reparsed_sql);
            prop_assert!(stmts2.is_ok(), "re-parse failed for: {reparsed_sql}");
            let stmts2 = stmts2.unwrap();
            prop_assert_eq!(
                stmts[0].to_string(),
                stmts2[0].to_string(),
                "round-trip mismatch for original: {}",
                sql,
            );
        }
    }

    // ── Property: SELECT with WHERE round-trip ──────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn select_where_round_trips(
            table in sql_table_name(),
            col in sql_column_name(),
            val in -1_000_000i64..1_000_000,
        ) {
            let sql = format!("SELECT * FROM {table} WHERE {col} = {val}");
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");

            let reparsed = parse_sql(&stmts.unwrap()[0].to_string());
            prop_assert!(reparsed.is_ok());
        }
    }

    // ── Property: INSERT round-trip ─────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn insert_round_trips(
            table in sql_table_name(),
            col in sql_column_name(),
            val in -1_000_000i64..1_000_000,
        ) {
            let sql = format!("INSERT INTO {table} ({col}) VALUES ({val})");
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");

            let reparsed_sql = stmts.unwrap()[0].to_string();
            let stmts2 = parse_sql(&reparsed_sql);
            prop_assert!(stmts2.is_ok(), "re-parse failed for: {reparsed_sql}");
        }
    }

    // ── Property: CREATE TABLE round-trip ────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn create_table_round_trips(
            table in sql_table_name(),
            col1 in sql_column_name(),
            type1 in sql_type(),
            col2_suffix in 0u32..1000,
        ) {
            let col2 = format!("c_{col2_suffix}");
            let sql = format!(
                "CREATE TABLE {table} ({col1} {type1} PRIMARY KEY, {col2} TEXT)"
            );
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");

            let reparsed_sql = stmts.unwrap()[0].to_string();
            let stmts2 = parse_sql(&reparsed_sql);
            prop_assert!(stmts2.is_ok(), "re-parse failed for: {reparsed_sql}");
        }
    }

    // ── Property: UPDATE round-trip ─────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn update_round_trips(
            table in sql_table_name(),
            set_col in sql_column_name(),
            where_col_suffix in 0u32..1000,
            set_val in -1_000_000i64..1_000_000,
            where_val in 1i64..1_000_000,
        ) {
            let where_col = format!("c_{where_col_suffix}");
            let sql = format!(
                "UPDATE {table} SET {set_col} = {set_val} WHERE {where_col} = {where_val}"
            );
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");

            let reparsed = parse_sql(&stmts.unwrap()[0].to_string());
            prop_assert!(reparsed.is_ok());
        }
    }

    // ── Property: DELETE round-trip ──────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn delete_round_trips(
            table in sql_table_name(),
            col in sql_column_name(),
            val in 1i64..1_000_000,
        ) {
            let sql = format!("DELETE FROM {table} WHERE {col} = {val}");
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");

            let reparsed = parse_sql(&stmts.unwrap()[0].to_string());
            prop_assert!(reparsed.is_ok());
        }
    }

    // ── Property: string literals with escaping ─────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn string_literal_round_trips(
            table in sql_table_name(),
            col in sql_column_name(),
            // Printable ASCII without single-quote to avoid SQL injection edge cases
            text in "[a-zA-Z0-9 _]{1,30}",
        ) {
            let sql = format!("INSERT INTO {table} ({col}) VALUES ('{text}')");
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");

            let reparsed = parse_sql(&stmts.unwrap()[0].to_string());
            prop_assert!(reparsed.is_ok());
        }
    }

    // ── Property: multi-statement semicolons ─────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn multi_statement_parse(
            t1 in sql_table_name(),
            t2 in sql_table_name(),
        ) {
            let sql = format!("SELECT 1 FROM {t1}; SELECT 2 FROM {t2}");
            let stmts = parse_sql(&sql);
            prop_assert!(stmts.is_ok(), "parse failed for: {sql}");
            prop_assert_eq!(stmts.unwrap().len(), 2, "expected 2 statements");
        }
    }
}
