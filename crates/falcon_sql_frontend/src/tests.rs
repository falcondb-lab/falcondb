#[cfg(test)]
mod binder_tests {
    use crate::binder::Binder;
    use crate::parser::parse_sql;
    use crate::types::*;
    use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    /// Build a simple catalog with a "users" table: id INT PK, name TEXT, age INT
    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.add_table(TableSchema {
            id: TableId(1),
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "age".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        });
        catalog
    }

    fn bind_sql(sql: &str) -> Result<BoundStatement, falcon_common::error::SqlError> {
        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql(sql).unwrap();
        binder.bind(&stmts[0])
    }

    // ---- CREATE TABLE ----

    #[test]
    fn test_bind_create_table() {
        let result = bind_sql("CREATE TABLE orders (oid INT PRIMARY KEY, amount INT)");
        let stmt = result.unwrap();
        match stmt {
            BoundStatement::CreateTable(ct) => {
                assert_eq!(ct.schema.name, "orders");
                assert_eq!(ct.schema.columns.len(), 2);
                assert_eq!(ct.schema.columns[0].name, "oid");
                assert!(ct.schema.columns[0].is_primary_key);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_bind_create_table_if_not_exists() {
        let result = bind_sql("CREATE TABLE IF NOT EXISTS users (id INT)");
        let stmt = result.unwrap();
        match stmt {
            BoundStatement::CreateTable(ct) => {
                assert!(ct.if_not_exists);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    // ---- DROP TABLE ----

    #[test]
    fn test_bind_drop_table() {
        let stmt = bind_sql("DROP TABLE users").unwrap();
        match stmt {
            BoundStatement::DropTable(dt) => {
                assert_eq!(dt.table_name, "users");
                assert!(!dt.if_exists);
            }
            _ => panic!("Expected DropTable"),
        }
    }

    #[test]
    fn test_bind_drop_table_if_exists() {
        let stmt = bind_sql("DROP TABLE IF EXISTS nonexistent").unwrap();
        match stmt {
            BoundStatement::DropTable(dt) => {
                assert!(dt.if_exists);
            }
            _ => panic!("Expected DropTable"),
        }
    }

    // ---- INSERT ----

    #[test]
    fn test_bind_insert_values() {
        let stmt = bind_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)").unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                assert_eq!(ins.table_id, TableId(1));
                assert_eq!(ins.columns.len(), 3);
                assert_eq!(ins.rows.len(), 1);
                assert_eq!(ins.rows[0].len(), 3);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_bind_insert_partial_columns() {
        let stmt = bind_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                assert_eq!(ins.columns.len(), 2);
                assert_eq!(ins.columns[0], 0); // id
                assert_eq!(ins.columns[1], 1); // name
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_bind_insert_unknown_table() {
        let result = bind_sql("INSERT INTO nonexistent (id) VALUES (1)");
        assert!(result.is_err());
    }

    // ---- SELECT ----

    #[test]
    fn test_bind_select_star() {
        let stmt = bind_sql("SELECT * FROM users").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 3); // id, name, age
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_select_columns() {
        let stmt = bind_sql("SELECT name, age FROM users").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 2);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_select_with_where() {
        let stmt = bind_sql("SELECT id FROM users WHERE age > 20").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_select_with_limit() {
        let stmt = bind_sql("SELECT * FROM users LIMIT 10").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(10));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_select_with_order_by() {
        let stmt = bind_sql("SELECT * FROM users ORDER BY age DESC").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(!sel.order_by[0].asc);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_select_count() {
        let stmt = bind_sql("SELECT COUNT(*) FROM users").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                match &sel.projections[0] {
                    BoundProjection::Aggregate(AggFunc::Count, _, _, _, _) => {}
                    other => panic!("Expected Count aggregate, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_select_unknown_column() {
        let result = bind_sql("SELECT nonexistent FROM users");
        assert!(result.is_err());
    }

    #[test]
    fn test_bind_select_unknown_table() {
        let result = bind_sql("SELECT * FROM nonexistent");
        assert!(result.is_err());
    }

    // ---- UPDATE ----

    #[test]
    fn test_bind_update() {
        let stmt = bind_sql("UPDATE users SET name = 'Charlie' WHERE id = 1").unwrap();
        match stmt {
            BoundStatement::Update(upd) => {
                assert_eq!(upd.table_id, TableId(1));
                assert_eq!(upd.assignments.len(), 1);
                assert!(upd.filter.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    // ---- DELETE ----

    #[test]
    fn test_bind_delete() {
        let stmt = bind_sql("DELETE FROM users WHERE id = 1").unwrap();
        match stmt {
            BoundStatement::Delete(del) => {
                assert_eq!(del.table_id, TableId(1));
                assert!(del.filter.is_some());
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_bind_delete_no_filter() {
        let stmt = bind_sql("DELETE FROM users").unwrap();
        match stmt {
            BoundStatement::Delete(del) => {
                assert!(del.filter.is_none());
            }
            _ => panic!("Expected Delete"),
        }
    }

    // ---- Expression binding ----

    #[test]
    fn test_bind_scalar_function() {
        let stmt = bind_sql("SELECT UPPER(name) FROM users").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                match &sel.projections[0] {
                    BoundProjection::Expr(BoundExpr::Function { func, args }, _) => {
                        assert!(matches!(func, ScalarFunc::Upper));
                        assert_eq!(args.len(), 1);
                    }
                    other => panic!("Expected Function expr, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_nested_expression() {
        let stmt = bind_sql("SELECT age + 1 FROM users WHERE age > 10 AND name = 'Alice'").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                assert!(sel.filter.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_is_null() {
        let stmt = bind_sql("SELECT * FROM users WHERE name IS NULL").unwrap();
        match stmt {
            BoundStatement::Select(sel) => match sel.filter.as_ref().unwrap() {
                BoundExpr::IsNull(_) => {}
                other => panic!("Expected IsNull, got {:?}", other),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_like() {
        let stmt = bind_sql("SELECT * FROM users WHERE name LIKE '%alice%'").unwrap();
        match stmt {
            BoundStatement::Select(sel) => match sel.filter.as_ref().unwrap() {
                BoundExpr::Like { negated, .. } => {
                    assert!(!negated);
                }
                other => panic!("Expected Like, got {:?}", other),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_between() {
        let stmt = bind_sql("SELECT * FROM users WHERE age BETWEEN 20 AND 30").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                // BETWEEN should produce an AND expression
                assert!(sel.filter.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_in_list() {
        let stmt = bind_sql("SELECT * FROM users WHERE age IN (20, 25, 30)").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- GROUP BY / HAVING ----

    #[test]
    fn test_bind_group_by() {
        let stmt = bind_sql("SELECT age, COUNT(*) FROM users GROUP BY age").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(!sel.group_by.is_empty());
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- DISTINCT ----

    #[test]
    fn test_bind_distinct() {
        let stmt = bind_sql("SELECT DISTINCT name FROM users").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.distinct, DistinctMode::All);
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- EXPLAIN ----

    #[test]
    fn test_bind_explain() {
        let stmt = bind_sql("EXPLAIN SELECT * FROM users").unwrap();
        assert!(matches!(stmt, BoundStatement::Explain(_)));
    }

    #[test]
    fn test_bind_explain_analyze() {
        let stmt = bind_sql("EXPLAIN ANALYZE SELECT * FROM users").unwrap();
        assert!(matches!(stmt, BoundStatement::ExplainAnalyze(_)));
    }
}

#[cfg(test)]
mod resolve_function_tests {
    use crate::resolve_function::resolve_scalar_func;
    use crate::types::ScalarFunc;

    #[test]
    fn test_resolve_basic_string_functions() {
        assert!(matches!(
            resolve_scalar_func("UPPER"),
            Some(ScalarFunc::Upper)
        ));
        assert!(matches!(
            resolve_scalar_func("LOWER"),
            Some(ScalarFunc::Lower)
        ));
        assert!(matches!(
            resolve_scalar_func("TRIM"),
            Some(ScalarFunc::Trim)
        ));
        assert!(matches!(
            resolve_scalar_func("CONCAT"),
            Some(ScalarFunc::Concat)
        ));
        assert!(matches!(
            resolve_scalar_func("REPLACE"),
            Some(ScalarFunc::Replace)
        ));
    }

    #[test]
    fn test_resolve_aliases() {
        assert!(matches!(
            resolve_scalar_func("LENGTH"),
            Some(ScalarFunc::Length)
        ));
        assert!(matches!(
            resolve_scalar_func("CHAR_LENGTH"),
            Some(ScalarFunc::Length)
        ));
        assert!(matches!(
            resolve_scalar_func("CHARACTER_LENGTH"),
            Some(ScalarFunc::Length)
        ));
        assert!(matches!(
            resolve_scalar_func("CEIL"),
            Some(ScalarFunc::Ceil)
        ));
        assert!(matches!(
            resolve_scalar_func("CEILING"),
            Some(ScalarFunc::Ceil)
        ));
        assert!(matches!(
            resolve_scalar_func("POWER"),
            Some(ScalarFunc::Power)
        ));
        assert!(matches!(
            resolve_scalar_func("POW"),
            Some(ScalarFunc::Power)
        ));
    }

    #[test]
    fn test_resolve_math_functions() {
        assert!(matches!(resolve_scalar_func("ABS"), Some(ScalarFunc::Abs)));
        assert!(matches!(
            resolve_scalar_func("ROUND"),
            Some(ScalarFunc::Round)
        ));
        assert!(matches!(
            resolve_scalar_func("FLOOR"),
            Some(ScalarFunc::Floor)
        ));
        assert!(matches!(
            resolve_scalar_func("SQRT"),
            Some(ScalarFunc::Sqrt)
        ));
        assert!(matches!(
            resolve_scalar_func("CBRT"),
            Some(ScalarFunc::Cbrt)
        ));
        assert!(matches!(resolve_scalar_func("PI"), Some(ScalarFunc::Pi)));
        assert!(matches!(resolve_scalar_func("SIN"), Some(ScalarFunc::Sin)));
        assert!(matches!(resolve_scalar_func("COS"), Some(ScalarFunc::Cos)));
    }

    #[test]
    fn test_resolve_array_functions() {
        assert!(matches!(
            resolve_scalar_func("ARRAY_LENGTH"),
            Some(ScalarFunc::ArrayLength)
        ));
        assert!(matches!(
            resolve_scalar_func("ARRAY_APPEND"),
            Some(ScalarFunc::ArrayAppend)
        ));
        assert!(matches!(
            resolve_scalar_func("ARRAY_CAT"),
            Some(ScalarFunc::ArrayCat)
        ));
        assert!(matches!(
            resolve_scalar_func("ARRAY_SORT"),
            Some(ScalarFunc::ArraySort)
        ));
    }

    #[test]
    fn test_resolve_unknown() {
        assert!(resolve_scalar_func("NONEXISTENT_FUNCTION").is_none());
        assert!(resolve_scalar_func("").is_none());
    }

    #[test]
    fn test_resolve_special_functions_not_in_resolver() {
        // NOW, DATE_TRUNC, EXTRACT are handled specially in the binder, not here
        assert!(resolve_scalar_func("NOW").is_none());
        assert!(resolve_scalar_func("CURRENT_TIMESTAMP").is_none());
        assert!(resolve_scalar_func("DATE_TRUNC").is_none());
        assert!(resolve_scalar_func("DATE_PART").is_none());
        assert!(resolve_scalar_func("EXTRACT").is_none());
    }
}

#[cfg(test)]
mod parameter_tests {
    use crate::binder::Binder;
    use crate::parser::parse_sql;
    use crate::types::*;
    use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.add_table(TableSchema {
            id: TableId(1),
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        });
        catalog
    }

    #[test]
    fn test_parameter_placeholder_binds() {
        let catalog = test_catalog();
        let stmts = parse_sql("SELECT * FROM users WHERE id = $1;").unwrap();
        let mut binder = Binder::new(catalog);
        let bound = binder.bind(&stmts[0]).unwrap();
        match bound {
            BoundStatement::Select(sel) => {
                // Filter should contain a Parameter(1) node
                let filter = sel.filter.as_ref().expect("should have filter");
                assert!(
                    contains_parameter(filter, 1),
                    "Filter should contain $1 parameter"
                );
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_multiple_parameters() {
        let catalog = test_catalog();
        let stmts = parse_sql("SELECT * FROM users WHERE id = $1 AND name = $2;").unwrap();
        let mut binder = Binder::new(catalog);
        let bound = binder.bind(&stmts[0]).unwrap();
        match bound {
            BoundStatement::Select(sel) => {
                let filter = sel.filter.as_ref().expect("should have filter");
                assert!(contains_parameter(filter, 1), "Filter should contain $1");
                assert!(contains_parameter(filter, 2), "Filter should contain $2");
            }
            _ => panic!("Expected Select"),
        }
    }

    fn contains_parameter(expr: &BoundExpr, idx: usize) -> bool {
        match expr {
            BoundExpr::Parameter(i) => *i == idx,
            BoundExpr::BinaryOp { left, right, .. } => {
                contains_parameter(left, idx) || contains_parameter(right, idx)
            }
            BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) => {
                contains_parameter(inner, idx)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod normalize_tests {
    use crate::normalize::{from_cnf_conjuncts, normalize_expr, to_cnf_conjuncts};
    use crate::types::*;
    use falcon_common::datum::Datum;

    #[test]
    fn test_between_expansion() {
        // col BETWEEN 1 AND 10  → col >= 1 AND col <= 10
        let expr = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            high: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            negated: false,
        };
        let normalized = normalize_expr(&expr);
        // Should be a BinaryOp AND
        match &normalized {
            BoundExpr::BinaryOp {
                op: BinOp::And,
                left,
                right,
            } => {
                match left.as_ref() {
                    BoundExpr::BinaryOp {
                        op: BinOp::GtEq, ..
                    } => {}
                    other => panic!("Expected >= on left, got {:?}", other),
                }
                match right.as_ref() {
                    BoundExpr::BinaryOp {
                        op: BinOp::LtEq, ..
                    } => {}
                    other => panic!("Expected <= on right, got {:?}", other),
                }
            }
            other => panic!("Expected AND, got {:?}", other),
        }
    }

    #[test]
    fn test_not_between_expansion() {
        // col NOT BETWEEN 1 AND 10  → col < 1 OR col > 10
        let expr = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            high: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            negated: true,
        };
        let normalized = normalize_expr(&expr);
        match &normalized {
            BoundExpr::BinaryOp {
                op: BinOp::Or,
                left,
                right,
            } => {
                match left.as_ref() {
                    BoundExpr::BinaryOp { op: BinOp::Lt, .. } => {}
                    other => panic!("Expected < on left, got {:?}", other),
                }
                match right.as_ref() {
                    BoundExpr::BinaryOp { op: BinOp::Gt, .. } => {}
                    other => panic!("Expected > on right, got {:?}", other),
                }
            }
            other => panic!("Expected OR, got {:?}", other),
        }
    }

    #[test]
    fn test_constant_folding_arithmetic() {
        // 2 + 3  → 5
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(2))),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::Literal(Datum::Int32(3))),
        };
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Int32(5)) => {}
            other => panic!("Expected Literal(5), got {:?}", other),
        }
    }

    #[test]
    fn test_constant_folding_boolean() {
        // true AND false  → false
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal(Datum::Boolean(true))),
            op: BinOp::And,
            right: Box::new(BoundExpr::Literal(Datum::Boolean(false))),
        };
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(false)) => {}
            other => panic!("Expected Literal(false), got {:?}", other),
        }
    }

    #[test]
    fn test_constant_folding_not() {
        // NOT true  → false
        let expr = BoundExpr::Not(Box::new(BoundExpr::Literal(Datum::Boolean(true))));
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(false)) => {}
            other => panic!("Expected Literal(false), got {:?}", other),
        }
    }

    #[test]
    fn test_constant_folding_string_concat() {
        // 'hello' || ' world'  → 'hello world'
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal(Datum::Text("hello".into()))),
            op: BinOp::StringConcat,
            right: Box::new(BoundExpr::Literal(Datum::Text(" world".into()))),
        };
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Text(s)) if s == "hello world" => {}
            other => panic!("Expected 'hello world', got {:?}", other),
        }
    }

    #[test]
    fn test_cnf_extraction() {
        // (a AND b) AND c  → [a, b, c]
        let a = BoundExpr::ColumnRef(0);
        let b = BoundExpr::ColumnRef(1);
        let c = BoundExpr::ColumnRef(2);
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            }),
            op: BinOp::And,
            right: Box::new(c),
        };
        let conjuncts = to_cnf_conjuncts(&expr);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn test_cnf_roundtrip() {
        let a = BoundExpr::Literal(Datum::Boolean(true));
        let b = BoundExpr::Literal(Datum::Boolean(false));
        let conjuncts = vec![a.clone(), b.clone()];
        let reconstructed = from_cnf_conjuncts(&conjuncts);
        assert!(reconstructed.is_some());
        // Should be a AND b
        match reconstructed.unwrap() {
            BoundExpr::BinaryOp { op: BinOp::And, .. } => {}
            other => panic!("Expected AND, got {:?}", other),
        }
    }

    #[test]
    fn test_cnf_empty() {
        let result = from_cnf_conjuncts(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_is_null_constant_fold() {
        // IS NULL on literal NULL  → true
        let expr = BoundExpr::IsNull(Box::new(BoundExpr::Literal(Datum::Null)));
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(true)) => {}
            other => panic!("Expected true, got {:?}", other),
        }
    }

    #[test]
    fn test_is_not_null_constant_fold() {
        // IS NOT NULL on literal 5  → true
        let expr = BoundExpr::IsNotNull(Box::new(BoundExpr::Literal(Datum::Int32(5))));
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(true)) => {}
            other => panic!("Expected true, got {:?}", other),
        }
    }

    #[test]
    fn test_is_not_distinct_from_constant_fold_both_null() {
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::Literal(Datum::Null)),
            right: Box::new(BoundExpr::Literal(Datum::Null)),
        };
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(true)) => {}
            other => panic!("Expected true, got {:?}", other),
        }
    }

    #[test]
    fn test_is_not_distinct_from_constant_fold_one_null() {
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::Literal(Datum::Int32(5))),
            right: Box::new(BoundExpr::Literal(Datum::Null)),
        };
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(false)) => {}
            other => panic!("Expected false, got {:?}", other),
        }
    }

    #[test]
    fn test_is_not_distinct_from_constant_fold_equal() {
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::Literal(Datum::Int32(42))),
            right: Box::new(BoundExpr::Literal(Datum::Int32(42))),
        };
        let folded = normalize_expr(&expr);
        match &folded {
            BoundExpr::Literal(Datum::Boolean(true)) => {}
            other => panic!("Expected true, got {:?}", other),
        }
    }
}

#[cfg(test)]
mod param_type_inference_tests {
    use crate::normalize::infer_param_types;
    use crate::types::*;
    use falcon_common::datum::Datum;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn users_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "age".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn test_infer_eq_comparison() {
        // id = $1  → $1 is Int32
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Parameter(1)),
        };
        let types = infer_param_types(&expr, &users_schema());
        assert_eq!(types.get(&1), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_reversed_comparison() {
        // $1 = name  → $1 is Text
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Parameter(1)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        let types = infer_param_types(&expr, &users_schema());
        assert_eq!(types.get(&1), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_multiple_params() {
        // id = $1 AND name = $2
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(0)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Parameter(1)),
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(1)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Parameter(2)),
            }),
        };
        let types = infer_param_types(&expr, &users_schema());
        assert_eq!(types.get(&1), Some(&DataType::Int32));
        assert_eq!(types.get(&2), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_from_in_list() {
        // id IN ($1, $2)
        let expr = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![BoundExpr::Parameter(1), BoundExpr::Parameter(2)],
            negated: false,
        };
        let types = infer_param_types(&expr, &users_schema());
        assert_eq!(types.get(&1), Some(&DataType::Int32));
        assert_eq!(types.get(&2), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_from_between() {
        // age BETWEEN $1 AND $2
        let expr = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(2)),
            low: Box::new(BoundExpr::Parameter(1)),
            high: Box::new(BoundExpr::Parameter(2)),
            negated: false,
        };
        let types = infer_param_types(&expr, &users_schema());
        assert_eq!(types.get(&1), Some(&DataType::Int32));
        assert_eq!(types.get(&2), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_no_params() {
        // id = 5 (no parameters)
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Int32(5))),
        };
        let types = infer_param_types(&expr, &users_schema());
        assert!(types.is_empty());
    }
}

#[cfg(test)]
mod equality_set_tests {
    use crate::normalize::extract_equality_sets;
    use crate::types::*;
    use falcon_common::datum::Datum;

    #[test]
    fn test_in_list_equality_set() {
        // col0 IN (1, 2, 3)
        let expr = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![
                BoundExpr::Literal(Datum::Int32(1)),
                BoundExpr::Literal(Datum::Int32(2)),
                BoundExpr::Literal(Datum::Int32(3)),
            ],
            negated: false,
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].column_idx, 0);
        assert_eq!(sets[0].values.len(), 3);
        assert!(!sets[0].negated);
    }

    #[test]
    fn test_eq_literal_equality_set() {
        // col0 = 42
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Int32(42))),
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].column_idx, 0);
        assert_eq!(sets[0].values, vec![Datum::Int32(42)]);
    }

    #[test]
    fn test_reversed_eq_literal() {
        // 42 = col0
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(42))),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(0)),
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].column_idx, 0);
    }

    #[test]
    fn test_combined_and_equality_sets() {
        // col0 = 1 AND col1 IN (10, 20)
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(0)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::InList {
                expr: Box::new(BoundExpr::ColumnRef(1)),
                list: vec![
                    BoundExpr::Literal(Datum::Int32(10)),
                    BoundExpr::Literal(Datum::Int32(20)),
                ],
                negated: false,
            }),
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 2);
    }

    #[test]
    fn test_not_in_negated() {
        // col0 NOT IN (1, 2)
        let expr = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![
                BoundExpr::Literal(Datum::Int32(1)),
                BoundExpr::Literal(Datum::Int32(2)),
            ],
            negated: true,
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 1);
        assert!(sets[0].negated);
    }

    #[test]
    fn test_no_equality_set_for_gt() {
        // col0 > 5 (not an equality)
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Gt,
            right: Box::new(BoundExpr::Literal(Datum::Int32(5))),
        };
        let sets = extract_equality_sets(&expr);
        assert!(sets.is_empty());
    }

    #[test]
    fn test_is_not_distinct_from_equality_set() {
        // col0 IS NOT DISTINCT FROM 42
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::Literal(Datum::Int32(42))),
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].column_idx, 0);
        assert_eq!(sets[0].values, vec![Datum::Int32(42)]);
        assert!(!sets[0].negated);
    }

    #[test]
    fn test_is_not_distinct_from_reversed() {
        // 42 IS NOT DISTINCT FROM col0
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::Literal(Datum::Int32(42))),
            right: Box::new(BoundExpr::ColumnRef(0)),
        };
        let sets = extract_equality_sets(&expr);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].column_idx, 0);
    }
}

#[cfg(test)]
mod volatile_tests {
    use crate::normalize::{expr_has_volatile, is_volatile};
    use crate::types::*;

    #[test]
    fn test_random_is_volatile() {
        assert!(is_volatile(&ScalarFunc::Random));
    }

    #[test]
    fn test_now_is_volatile() {
        assert!(is_volatile(&ScalarFunc::Now));
    }

    #[test]
    fn test_gen_random_uuid_is_volatile() {
        assert!(is_volatile(&ScalarFunc::GenRandomUuid));
    }

    #[test]
    fn test_upper_is_not_volatile() {
        assert!(!is_volatile(&ScalarFunc::Upper));
    }

    #[test]
    fn test_abs_is_not_volatile() {
        assert!(!is_volatile(&ScalarFunc::Abs));
    }

    #[test]
    fn test_expr_with_volatile_function() {
        let expr = BoundExpr::Function {
            func: ScalarFunc::Random,
            args: vec![],
        };
        assert!(expr_has_volatile(&expr));
    }

    #[test]
    fn test_expr_volatile_in_comparison() {
        // col > random()
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Gt,
            right: Box::new(BoundExpr::Function {
                func: ScalarFunc::Random,
                args: vec![],
            }),
        };
        assert!(expr_has_volatile(&expr));
    }

    #[test]
    fn test_expr_no_volatile() {
        // col = upper('hello')
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Function {
                func: ScalarFunc::Upper,
                args: vec![BoundExpr::Literal(falcon_common::datum::Datum::Text(
                    "hello".into(),
                ))],
            }),
        };
        assert!(!expr_has_volatile(&expr));
    }

    #[test]
    fn test_literal_not_volatile() {
        let expr = BoundExpr::Literal(falcon_common::datum::Datum::Int32(42));
        assert!(!expr_has_volatile(&expr));
    }

    #[test]
    fn test_volatile_nested_in_case() {
        // CASE WHEN true THEN random() ELSE 0 END
        let expr = BoundExpr::Case {
            operand: None,
            conditions: vec![BoundExpr::Literal(falcon_common::datum::Datum::Boolean(
                true,
            ))],
            results: vec![BoundExpr::Function {
                func: ScalarFunc::Random,
                args: vec![],
            }],
            else_result: Some(Box::new(BoundExpr::Literal(
                falcon_common::datum::Datum::Int32(0),
            ))),
        };
        assert!(expr_has_volatile(&expr));
    }
}

// ===========================================================================
// New feature tests: VALUES clause, IS TRUE/FALSE, IS [NOT] DISTINCT FROM,
// COPY (query) TO, RETURNING compound identifiers, window positional refs
// ===========================================================================

#[cfg(test)]
mod new_feature_tests {
    use crate::binder::Binder;
    use crate::parser::parse_sql;
    use crate::types::*;
    use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.add_table(TableSchema {
            id: TableId(1),
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "age".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        });
        catalog
    }

    fn bind_sql(sql: &str) -> Result<BoundStatement, falcon_common::error::SqlError> {
        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql(sql).unwrap();
        binder.bind(&stmts[0])
    }

    // ---- VALUES clause ----

    #[test]
    fn test_values_clause_basic() {
        let stmt = bind_sql("VALUES (1, 'hello'), (2, 'world')").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.virtual_rows.len(), 2);
                assert_eq!(sel.projections.len(), 2);
                assert_eq!(sel.table_name, "__values__");
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_values_clause_single_row() {
        let stmt = bind_sql("VALUES (42)").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.virtual_rows.len(), 1);
                assert_eq!(sel.projections.len(), 1);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_values_clause_with_null() {
        let stmt = bind_sql("VALUES (1, NULL), (2, 'abc')").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.virtual_rows.len(), 2);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_values_clause_with_limit() {
        let stmt = bind_sql("VALUES (1), (2), (3) LIMIT 2").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.virtual_rows.len(), 3);
                assert_eq!(sel.limit, Some(2));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_values_clause_negative_number() {
        let stmt = bind_sql("VALUES (-5, 'neg')").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.virtual_rows.len(), 1);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_values_clause_mismatched_columns_error() {
        let result = bind_sql("VALUES (1, 2), (3)");
        assert!(result.is_err());
    }

    // ---- IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE ----

    #[test]
    fn test_bind_is_true() {
        let stmt = bind_sql("SELECT * FROM users WHERE (age > 20) IS TRUE").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_is_false() {
        let stmt = bind_sql("SELECT * FROM users WHERE (age > 20) IS FALSE").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_is_not_true() {
        let stmt = bind_sql("SELECT * FROM users WHERE (age > 20) IS NOT TRUE").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
                // IS NOT TRUE wraps in BoundExpr::Not
                match sel.filter.as_ref().unwrap() {
                    BoundExpr::Not(_) => {}
                    other => panic!("Expected Not wrapper, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_is_not_false() {
        let stmt = bind_sql("SELECT * FROM users WHERE (age > 20) IS NOT FALSE").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
                match sel.filter.as_ref().unwrap() {
                    BoundExpr::Not(_) => {}
                    other => panic!("Expected Not wrapper, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- IS DISTINCT FROM / IS NOT DISTINCT FROM ----

    #[test]
    fn test_bind_is_not_distinct_from() {
        let stmt = bind_sql("SELECT * FROM users WHERE age IS NOT DISTINCT FROM 25").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
                match sel.filter.as_ref().unwrap() {
                    BoundExpr::IsNotDistinctFrom { .. } => {}
                    other => panic!("Expected IsNotDistinctFrom, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_is_distinct_from() {
        let stmt = bind_sql("SELECT * FROM users WHERE age IS DISTINCT FROM 25").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(sel.filter.is_some());
                // IS DISTINCT FROM = NOT (IS NOT DISTINCT FROM)
                match sel.filter.as_ref().unwrap() {
                    BoundExpr::Not(inner) => match inner.as_ref() {
                        BoundExpr::IsNotDistinctFrom { .. } => {}
                        other => panic!("Expected IsNotDistinctFrom inside Not, got {:?}", other),
                    },
                    other => panic!("Expected Not wrapper, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- COPY (query) TO STDOUT ----

    #[test]
    fn test_bind_copy_query_to() {
        let stmt = bind_sql("COPY (SELECT id, name FROM users) TO STDOUT").unwrap();
        match stmt {
            BoundStatement::CopyQueryTo {
                query,
                csv,
                delimiter,
                header,
                ..
            } => {
                assert!(!csv);
                assert_eq!(delimiter, '\t');
                assert!(!header);
                assert_eq!(query.projections.len(), 2);
            }
            _ => panic!("Expected CopyQueryTo"),
        }
    }

    #[test]
    fn test_bind_copy_query_to_csv() {
        let stmt = bind_sql("COPY (SELECT * FROM users) TO STDOUT WITH (FORMAT csv, HEADER true)")
            .unwrap();
        match stmt {
            BoundStatement::CopyQueryTo {
                csv,
                header,
                delimiter,
                ..
            } => {
                assert!(csv);
                assert!(header);
                assert_eq!(delimiter, ',');
            }
            _ => panic!("Expected CopyQueryTo"),
        }
    }

    #[test]
    fn test_bind_copy_query_from_unsupported() {
        // Parser or binder should reject COPY (query) FROM  — either way it must error
        let catalog = test_catalog();
        let mut binder = Binder::new(catalog);
        let parse_result = parse_sql("COPY (SELECT * FROM users) FROM STDIN");
        if let Ok(stmts) = parse_result {
            let bind_result = binder.bind(&stmts[0]);
            assert!(bind_result.is_err());
        }
        // If parse itself fails, that's also acceptable
    }

    // ---- RETURNING with compound identifier ----

    #[test]
    fn test_bind_insert_returning_compound_identifier() {
        let stmt = bind_sql(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) RETURNING users.id",
        )
        .unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                assert_eq!(ins.returning.len(), 1);
                assert!(matches!(&ins.returning[0].0, BoundExpr::ColumnRef(0)));
                // id is column 0
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_bind_delete_returning_star() {
        let stmt = bind_sql("DELETE FROM users WHERE id = 1 RETURNING *").unwrap();
        match stmt {
            BoundStatement::Delete(del) => {
                assert_eq!(del.returning.len(), 3); // all columns
                assert!(matches!(&del.returning[0].0, BoundExpr::ColumnRef(0)));
                assert!(matches!(&del.returning[1].0, BoundExpr::ColumnRef(1)));
                assert!(matches!(&del.returning[2].0, BoundExpr::ColumnRef(2)));
            }
            _ => panic!("Expected Delete"),
        }
    }

    // ---- GROUP BY positional on expression projection ----

    #[test]
    fn test_bind_group_by_positional_on_expression() {
        let stmt = bind_sql("SELECT age + 1, COUNT(*) FROM users GROUP BY 1").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert!(!sel.group_by.is_empty());
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- ON CONFLICT DO UPDATE with excluded references ----

    #[test]
    fn test_bind_on_conflict_do_nothing() {
        let stmt = bind_sql(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) ON CONFLICT DO NOTHING",
        )
        .unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                assert!(matches!(ins.on_conflict, Some(OnConflictAction::DoNothing)));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_bind_on_conflict_do_update_excluded() {
        let stmt = bind_sql(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, age = excluded.age",
        )
        .unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                match &ins.on_conflict {
                    Some(OnConflictAction::DoUpdate(assignments)) => {
                        assert_eq!(assignments.len(), 2);
                        // name is col 1, age is col 2
                        assert_eq!(assignments[0].0, 1);
                        assert_eq!(assignments[1].0, 2);
                        // excluded.name should resolve to ColumnRef(3+1=4)  — but
                        // since schema has 3 cols, excluded offset is 3
                        // excluded.name = ColumnRef(3+1) via CompoundIdentifier
                        // Actually: schema find_column returns first match by name,
                        // so excluded.name resolves through alias offset
                    }
                    other => panic!("Expected DoUpdate, got {:?}", other),
                }
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_bind_on_conflict_do_update_simple() {
        let stmt = bind_sql(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) \
             ON CONFLICT (id) DO UPDATE SET name = 'Bob'",
        )
        .unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                match &ins.on_conflict {
                    Some(OnConflictAction::DoUpdate(assignments)) => {
                        assert_eq!(assignments.len(), 1);
                        assert_eq!(assignments[0].0, 1); // name is col 1
                    }
                    other => panic!("Expected DoUpdate, got {:?}", other),
                }
            }
            _ => panic!("Expected Insert"),
        }
    }

    // ---- ANY / ALL operators ----

    #[test]
    fn test_bind_any_op() {
        let stmt = bind_sql("SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                let filter = sel.filter.as_ref().expect("expected filter");
                match filter {
                    BoundExpr::AnyOp {
                        left,
                        compare_op,
                        right,
                    } => {
                        assert!(matches!(left.as_ref(), BoundExpr::ColumnRef(0)));
                        assert_eq!(*compare_op, BinOp::Eq);
                        assert!(matches!(right.as_ref(), BoundExpr::ArrayLiteral(_)));
                    }
                    other => panic!("Expected AnyOp, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_all_op() {
        let stmt = bind_sql("SELECT * FROM users WHERE age > ALL(ARRAY[10, 20])").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                let filter = sel.filter.as_ref().expect("expected filter");
                match filter {
                    BoundExpr::AllOp {
                        left,
                        compare_op,
                        right,
                    } => {
                        assert!(matches!(left.as_ref(), BoundExpr::ColumnRef(2)));
                        assert_eq!(*compare_op, BinOp::Gt);
                        assert!(matches!(right.as_ref(), BoundExpr::ArrayLiteral(_)));
                    }
                    other => panic!("Expected AllOp, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_any_op_neq() {
        let stmt = bind_sql("SELECT * FROM users WHERE id <> ANY(ARRAY[1])").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                let filter = sel.filter.as_ref().expect("expected filter");
                match filter {
                    BoundExpr::AnyOp { compare_op, .. } => {
                        assert_eq!(*compare_op, BinOp::NotEq);
                    }
                    other => panic!("Expected AnyOp, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- Array slice ----

    #[test]
    fn test_bind_array_slice() {
        // Standalone array slice: (ARRAY[10,20,30])[1:2]
        let stmt = bind_sql("SELECT (ARRAY[10,20,30])[1:2]").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                match &sel.projections[0] {
                    BoundProjection::Expr(
                        BoundExpr::ArraySlice {
                            array,
                            lower,
                            upper,
                        },
                        _,
                    ) => {
                        assert!(matches!(array.as_ref(), BoundExpr::ArrayLiteral(_)));
                        assert!(lower.is_some());
                        assert!(upper.is_some());
                    }
                    other => panic!("Expected ArraySlice projection, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- UNNEST table function ----

    #[test]
    fn test_bind_unnest_table_function() {
        let stmt = bind_sql("SELECT * FROM UNNEST(ARRAY[10, 20, 30]) AS u").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                // Should have 1 projection (the "unnest" column)
                assert_eq!(sel.visible_projection_count, 1);
                // Should have a CTE backing the unnest virtual table
                assert!(!sel.ctes.is_empty());
            }
            _ => panic!("Expected Select"),
        }
    }

    // ---- Expression-based RETURNING ----

    #[test]
    fn test_bind_returning_expression() {
        let stmt = bind_sql(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) RETURNING id, age + 1",
        )
        .unwrap();
        match stmt {
            BoundStatement::Insert(ins) => {
                assert_eq!(ins.returning.len(), 2);
                // First: simple column ref
                assert!(matches!(&ins.returning[0].0, BoundExpr::ColumnRef(0)));
                assert_eq!(ins.returning[0].1, "id");
                // Second: expression (age + 1)
                assert!(matches!(&ins.returning[1].0, BoundExpr::BinaryOp { .. }));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_bind_returning_star() {
        let stmt = bind_sql("DELETE FROM users WHERE id = 1 RETURNING *").unwrap();
        match stmt {
            BoundStatement::Delete(del) => {
                // * expands to all 3 columns: id, name, age
                assert_eq!(del.returning.len(), 3);
                assert!(matches!(&del.returning[0].0, BoundExpr::ColumnRef(0)));
                assert!(matches!(&del.returning[1].0, BoundExpr::ColumnRef(1)));
                assert!(matches!(&del.returning[2].0, BoundExpr::ColumnRef(2)));
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_bind_returning_with_alias() {
        let stmt =
            bind_sql("UPDATE users SET age = 31 WHERE id = 1 RETURNING name AS updated_name")
                .unwrap();
        match stmt {
            BoundStatement::Update(upd) => {
                assert_eq!(upd.returning.len(), 1);
                assert!(matches!(&upd.returning[0].0, BoundExpr::ColumnRef(1)));
                assert_eq!(upd.returning[0].1, "updated_name");
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_limit_cast_expression() {
        let stmt = bind_sql("SELECT * FROM users LIMIT CAST(5 AS INT)").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(5));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_limit_arithmetic_expression() {
        let stmt = bind_sql("SELECT * FROM users LIMIT 2 + 3").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(5));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_limit_nested_parens() {
        let stmt = bind_sql("SELECT * FROM users LIMIT (10)").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(10));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_limit_null_means_unlimited() {
        let stmt = bind_sql("SELECT * FROM users LIMIT NULL").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(usize::MAX));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_offset_cast_expression() {
        let stmt = bind_sql("SELECT * FROM users LIMIT 10 OFFSET CAST(3 AS INT)").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(10));
                assert_eq!(sel.offset, Some(3));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_offset_arithmetic() {
        let stmt = bind_sql("SELECT * FROM users LIMIT 10 OFFSET 2 * 5").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.offset, Some(10));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_update_from() {
        let mut catalog = test_catalog();
        // Add a second table for FROM clause
        catalog.add_table(TableSchema {
            id: TableId(2),
            name: "orders".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "amount".into(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        });

        let sql = "UPDATE users SET age = orders.amount FROM orders WHERE users.id = orders.id";
        let stmts = parse_sql(sql).unwrap();
        let mut binder = Binder::new(catalog);
        let stmt = binder.bind(&stmts[0]).unwrap();
        match stmt {
            BoundStatement::Update(upd) => {
                assert!(upd.from_table.is_some());
                let ft = upd.from_table.unwrap();
                assert_eq!(ft.table_name.to_lowercase(), "orders");
                assert!(upd.filter.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_bind_delete_using() {
        let mut catalog = test_catalog();
        catalog.add_table(TableSchema {
            id: TableId(2),
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        });

        let sql = "DELETE FROM users USING orders WHERE users.id = orders.id";
        let stmts = parse_sql(sql).unwrap();
        let mut binder = Binder::new(catalog);
        let stmt = binder.bind(&stmts[0]).unwrap();
        match stmt {
            BoundStatement::Delete(del) => {
                assert!(del.using_table.is_some());
                let ut = del.using_table.unwrap();
                assert_eq!(ut.table_name.to_lowercase(), "orders");
                assert!(del.filter.is_some());
            }
            _ => panic!("Expected Delete"),
        }
    }

    // ---- DISTINCT ON ----

    #[test]
    fn test_bind_distinct_on_single_column() {
        let stmt =
            bind_sql("SELECT DISTINCT ON (name) name, age FROM users ORDER BY name, age").unwrap();
        match stmt {
            BoundStatement::Select(sel) => match &sel.distinct {
                DistinctMode::On(indices) => {
                    assert_eq!(indices.len(), 1);
                }
                other => panic!("Expected DistinctMode::On, got {:?}", other),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_distinct_on_multiple_columns() {
        let stmt =
            bind_sql("SELECT DISTINCT ON (name, age) name, age, id FROM users ORDER BY name, age")
                .unwrap();
        match stmt {
            BoundStatement::Select(sel) => match &sel.distinct {
                DistinctMode::On(indices) => {
                    assert_eq!(indices.len(), 2);
                }
                other => panic!("Expected DistinctMode::On, got {:?}", other),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_bind_distinct_none() {
        let stmt = bind_sql("SELECT name FROM users").unwrap();
        match stmt {
            BoundStatement::Select(sel) => {
                assert_eq!(sel.distinct, DistinctMode::None);
            }
            _ => panic!("Expected Select"),
        }
    }
}
