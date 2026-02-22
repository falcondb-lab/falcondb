#[cfg(test)]
mod eval_tests {
    use crate::expr_engine::ExprEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::error::ExecutionError;
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn eval_expr(expr: &BoundExpr, row: &OwnedRow) -> Result<Datum, ExecutionError> {
        ExprEngine::eval_row(expr, row)
    }

    fn eval_filter(expr: &BoundExpr, row: &OwnedRow) -> Result<bool, ExecutionError> {
        ExprEngine::eval_filter(expr, row)
    }

    #[test]
    fn test_literal() {
        let r = row(vec![]);
        assert_eq!(
            eval_expr(&BoundExpr::Literal(Datum::Int32(42)), &r).unwrap(),
            Datum::Int32(42)
        );
        assert!(matches!(
            eval_expr(&BoundExpr::Literal(Datum::Null), &r).unwrap(),
            Datum::Null
        ));
        assert_eq!(
            eval_expr(&BoundExpr::Literal(Datum::Text("hello".into())), &r).unwrap(),
            Datum::Text("hello".into())
        );
    }

    #[test]
    fn test_column_ref() {
        let r = row(vec![
            Datum::Int32(10),
            Datum::Text("abc".into()),
            Datum::Boolean(true),
        ]);
        assert_eq!(
            eval_expr(&BoundExpr::ColumnRef(0), &r).unwrap(),
            Datum::Int32(10)
        );
        assert_eq!(
            eval_expr(&BoundExpr::ColumnRef(1), &r).unwrap(),
            Datum::Text("abc".into())
        );
        assert_eq!(
            eval_expr(&BoundExpr::ColumnRef(2), &r).unwrap(),
            Datum::Boolean(true)
        );
    }

    #[test]
    fn test_column_ref_out_of_bounds() {
        let r = row(vec![Datum::Int32(1)]);
        assert!(eval_expr(&BoundExpr::ColumnRef(5), &r).is_err());
    }

    #[test]
    fn test_comparison_ops() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(20)]);
        let col0 = BoundExpr::ColumnRef(0);
        let col1 = BoundExpr::ColumnRef(1);

        let eq = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Eq,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&eq, &r).unwrap(), Datum::Boolean(false));

        let lt = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Lt,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&lt, &r).unwrap(), Datum::Boolean(true));

        let gt = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Gt,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&gt, &r).unwrap(), Datum::Boolean(false));

        let lte = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::LtEq,
            right: Box::new(col0.clone()),
        };
        assert_eq!(eval_expr(&lte, &r).unwrap(), Datum::Boolean(true));

        let neq = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::NotEq,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&neq, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_arithmetic() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(3)]);
        let col0 = BoundExpr::ColumnRef(0);
        let col1 = BoundExpr::ColumnRef(1);

        let plus = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Plus,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&plus, &r).unwrap(), Datum::Int64(13));

        let minus = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Minus,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&minus, &r).unwrap(), Datum::Int64(7));

        let mul = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Multiply,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&mul, &r).unwrap(), Datum::Int64(30));

        let div = BoundExpr::BinaryOp {
            left: Box::new(col0.clone()),
            op: BinOp::Divide,
            right: Box::new(col1.clone()),
        };
        assert_eq!(eval_expr(&div, &r).unwrap(), Datum::Int64(3));
    }

    #[test]
    fn test_division_by_zero() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(0)]);
        let div = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Divide,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(eval_expr(&div, &r).is_err());
    }

    #[test]
    fn test_float_arithmetic() {
        let r = row(vec![Datum::Float64(10.5), Datum::Float64(2.0)]);
        let plus = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&plus, &r).unwrap(), Datum::Float64(12.5));
    }

    #[test]
    fn test_mixed_int_float() {
        let r = row(vec![Datum::Int32(10), Datum::Float64(2.5)]);
        let mul = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Multiply,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&mul, &r).unwrap(), Datum::Float64(25.0));
    }

    #[test]
    fn test_boolean_and_or() {
        let r = row(vec![Datum::Boolean(true), Datum::Boolean(false)]);

        let and = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::And,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&and, &r).unwrap(), Datum::Boolean(false));

        let or = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Or,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&or, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_not() {
        let r = row(vec![Datum::Boolean(true)]);
        let not = BoundExpr::Not(Box::new(BoundExpr::ColumnRef(0)));
        assert_eq!(eval_expr(&not, &r).unwrap(), Datum::Boolean(false));

        let r2 = row(vec![Datum::Null]);
        let not_null = BoundExpr::Not(Box::new(BoundExpr::ColumnRef(0)));
        assert!(matches!(eval_expr(&not_null, &r2).unwrap(), Datum::Null));
    }

    #[test]
    fn test_is_null_is_not_null() {
        let r = row(vec![Datum::Null, Datum::Int32(5)]);

        let is_null_0 = BoundExpr::IsNull(Box::new(BoundExpr::ColumnRef(0)));
        assert_eq!(eval_expr(&is_null_0, &r).unwrap(), Datum::Boolean(true));

        let is_null_1 = BoundExpr::IsNull(Box::new(BoundExpr::ColumnRef(1)));
        assert_eq!(eval_expr(&is_null_1, &r).unwrap(), Datum::Boolean(false));

        let is_not_null_0 = BoundExpr::IsNotNull(Box::new(BoundExpr::ColumnRef(0)));
        assert_eq!(
            eval_expr(&is_not_null_0, &r).unwrap(),
            Datum::Boolean(false)
        );

        let is_not_null_1 = BoundExpr::IsNotNull(Box::new(BoundExpr::ColumnRef(1)));
        assert_eq!(eval_expr(&is_not_null_1, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_null_propagation() {
        let r = row(vec![Datum::Null, Datum::Int32(5)]);

        // NULL + 5 = NULL
        let plus = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(matches!(eval_expr(&plus, &r).unwrap(), Datum::Null));

        // NULL = 5 => NULL
        let eq = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(matches!(eval_expr(&eq, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn test_three_valued_logic_and() {
        let r = row(vec![Datum::Boolean(false), Datum::Null]);
        // FALSE AND NULL = FALSE
        let and = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::And,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&and, &r).unwrap(), Datum::Boolean(false));

        let r2 = row(vec![Datum::Boolean(true), Datum::Null]);
        // TRUE AND NULL = NULL
        let and2 = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::And,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(matches!(eval_expr(&and2, &r2).unwrap(), Datum::Null));
    }

    #[test]
    fn test_three_valued_logic_or() {
        let r = row(vec![Datum::Boolean(true), Datum::Null]);
        // TRUE OR NULL = TRUE
        let or = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Or,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&or, &r).unwrap(), Datum::Boolean(true));

        let r2 = row(vec![Datum::Boolean(false), Datum::Null]);
        // FALSE OR NULL = NULL
        let or2 = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Or,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(matches!(eval_expr(&or2, &r2).unwrap(), Datum::Null));
    }

    #[test]
    fn test_eval_filter() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(5)]);

        let gt = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Gt,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(eval_filter(&gt, &r).unwrap());

        let lt = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Lt,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(!eval_filter(&lt, &r).unwrap());

        // NULL filter => false (row doesn't pass)
        let null_r = row(vec![Datum::Null, Datum::Int32(5)]);
        assert!(!eval_filter(&gt, &null_r).unwrap());
    }

    #[test]
    fn test_nested_expression() {
        // (col0 + col1) > 15
        let r = row(vec![Datum::Int32(10), Datum::Int32(8)]);
        let sum = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        let gt = BoundExpr::BinaryOp {
            left: Box::new(sum),
            op: BinOp::Gt,
            right: Box::new(BoundExpr::Literal(Datum::Int64(15))),
        };
        assert_eq!(eval_expr(&gt, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_string_comparison() {
        let r = row(vec![
            Datum::Text("apple".into()),
            Datum::Text("banana".into()),
        ]);
        let lt = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Lt,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&lt, &r).unwrap(), Datum::Boolean(true));

        let eq = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Text("apple".into()))),
        };
        assert_eq!(eval_expr(&eq, &r).unwrap(), Datum::Boolean(true));
    }

    // --- LIKE tests ---

    #[test]
    fn test_like_exact() {
        let r = row(vec![Datum::Text("hello".into())]);
        let like = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("hello".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_like_percent() {
        let r = row(vec![Datum::Text("hello world".into())]);
        let like = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("hello%".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like, &r).unwrap(), Datum::Boolean(true));

        let like2 = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("%world".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like2, &r).unwrap(), Datum::Boolean(true));

        let like3 = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("%lo w%".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like3, &r).unwrap(), Datum::Boolean(true));

        let like4 = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("xyz%".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like4, &r).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_like_underscore() {
        let r = row(vec![Datum::Text("cat".into())]);
        let like = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("c_t".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like, &r).unwrap(), Datum::Boolean(true));

        let like2 = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("c__".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like2, &r).unwrap(), Datum::Boolean(true));

        let like3 = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("__".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&like3, &r).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_not_like() {
        let r = row(vec![Datum::Text("hello".into())]);
        let nlike = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("world".into()))),
            negated: true,
            case_insensitive: false,
        };
        assert_eq!(eval_expr(&nlike, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_like_null() {
        let r = row(vec![Datum::Null]);
        let like = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            pattern: Box::new(BoundExpr::Literal(Datum::Text("%".into()))),
            negated: false,
            case_insensitive: false,
        };
        assert!(matches!(eval_expr(&like, &r).unwrap(), Datum::Null));
    }

    // --- BETWEEN tests ---

    #[test]
    fn test_between() {
        let r = row(vec![Datum::Int32(5)]);
        let between = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            high: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            negated: false,
        };
        assert_eq!(eval_expr(&between, &r).unwrap(), Datum::Boolean(true));

        let r2 = row(vec![Datum::Int32(15)]);
        assert_eq!(eval_expr(&between, &r2).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_between_boundary() {
        let r = row(vec![Datum::Int32(1)]);
        let between = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            high: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            negated: false,
        };
        assert_eq!(eval_expr(&between, &r).unwrap(), Datum::Boolean(true));

        let r2 = row(vec![Datum::Int32(10)]);
        assert_eq!(eval_expr(&between, &r2).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_not_between() {
        let r = row(vec![Datum::Int32(15)]);
        let nb = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            high: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            negated: true,
        };
        assert_eq!(eval_expr(&nb, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_between_null() {
        let r = row(vec![Datum::Null]);
        let between = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            high: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            negated: false,
        };
        assert!(matches!(eval_expr(&between, &r).unwrap(), Datum::Null));
    }

    // --- IN tests ---

    #[test]
    fn test_in_list() {
        let r = row(vec![Datum::Int32(3)]);
        let inl = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![
                BoundExpr::Literal(Datum::Int32(1)),
                BoundExpr::Literal(Datum::Int32(2)),
                BoundExpr::Literal(Datum::Int32(3)),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&inl, &r).unwrap(), Datum::Boolean(true));

        let r2 = row(vec![Datum::Int32(5)]);
        assert_eq!(eval_expr(&inl, &r2).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_not_in_list() {
        let r = row(vec![Datum::Int32(5)]);
        let ninl = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![
                BoundExpr::Literal(Datum::Int32(1)),
                BoundExpr::Literal(Datum::Int32(2)),
            ],
            negated: true,
        };
        assert_eq!(eval_expr(&ninl, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_in_list_null() {
        let r = row(vec![Datum::Null]);
        let inl = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![BoundExpr::Literal(Datum::Int32(1))],
            negated: false,
        };
        assert!(matches!(eval_expr(&inl, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn test_in_list_strings() {
        let r = row(vec![Datum::Text("b".into())]);
        let inl = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![
                BoundExpr::Literal(Datum::Text("a".into())),
                BoundExpr::Literal(Datum::Text("b".into())),
                BoundExpr::Literal(Datum::Text("c".into())),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&inl, &r).unwrap(), Datum::Boolean(true));
    }

    // --- CAST tests ---

    #[test]
    fn test_cast_int_to_text() {
        let r = row(vec![Datum::Int32(42)]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "text".into(),
        };
        assert_eq!(eval_expr(&cast, &r).unwrap(), Datum::Text("42".into()));
    }

    #[test]
    fn test_cast_text_to_int() {
        let r = row(vec![Datum::Text("123".into())]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "integer".into(),
        };
        assert_eq!(eval_expr(&cast, &r).unwrap(), Datum::Int32(123));
    }

    #[test]
    fn test_cast_float_to_int() {
        let r = row(vec![Datum::Float64(3.7)]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "int".into(),
        };
        assert_eq!(eval_expr(&cast, &r).unwrap(), Datum::Int32(3));
    }

    #[test]
    fn test_cast_int_to_float() {
        let r = row(vec![Datum::Int32(10)]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "float8".into(),
        };
        assert_eq!(eval_expr(&cast, &r).unwrap(), Datum::Float64(10.0));
    }

    #[test]
    fn test_cast_text_to_bool() {
        let r = row(vec![Datum::Text("true".into())]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "boolean".into(),
        };
        assert_eq!(eval_expr(&cast, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_cast_null() {
        let r = row(vec![Datum::Null]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "int".into(),
        };
        assert!(matches!(eval_expr(&cast, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn test_cast_bad_text_to_int() {
        let r = row(vec![Datum::Text("abc".into())]);
        let cast = BoundExpr::Cast {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            target_type: "integer".into(),
        };
        assert!(eval_expr(&cast, &r).is_err());
    }

    // --- Modulo test ---

    #[test]
    fn test_modulo() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(3)]);
        let modulo = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Modulo,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&modulo, &r).unwrap(), Datum::Int64(1));
    }

    #[test]
    fn test_modulo_by_zero() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(0)]);
        let modulo = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Modulo,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert!(eval_expr(&modulo, &r).is_err());
    }

    // --- IS NOT DISTINCT FROM tests ---

    #[test]
    fn test_is_not_distinct_from_both_null() {
        let r = row(vec![Datum::Null, Datum::Null]);
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_is_not_distinct_from_one_null() {
        let r = row(vec![Datum::Int32(5), Datum::Null]);
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_is_not_distinct_from_equal_values() {
        let r = row(vec![Datum::Int32(5), Datum::Int32(5)]);
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_is_not_distinct_from_different_values() {
        let r = row(vec![Datum::Int32(5), Datum::Int32(10)]);
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_is_distinct_from_via_not() {
        // IS DISTINCT FROM = NOT (IS NOT DISTINCT FROM)
        let r = row(vec![Datum::Null, Datum::Int32(5)]);
        let expr = BoundExpr::Not(Box::new(BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::ColumnRef(1)),
        }));
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    // ── ANY / ALL operator tests ──────────────────────────────────────

    #[test]
    fn test_any_eq_match() {
        // 2 = ANY(ARRAY[1,2,3]) → true
        let r = row(vec![]);
        let expr = BoundExpr::AnyOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(2))),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Int32(2),
                Datum::Int32(3),
            ]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_any_eq_no_match() {
        // 5 = ANY(ARRAY[1,2,3]) → false
        let r = row(vec![]);
        let expr = BoundExpr::AnyOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(5))),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Int32(2),
                Datum::Int32(3),
            ]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_any_gt() {
        // 2 > ANY(ARRAY[1,3,5]) → true (2 > 1)
        let r = row(vec![]);
        let expr = BoundExpr::AnyOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(2))),
            compare_op: BinOp::Gt,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Int32(3),
                Datum::Int32(5),
            ]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_any_with_null_element() {
        // 5 = ANY(ARRAY[1, NULL, 3]) → NULL (no match, but NULL present)
        let r = row(vec![]);
        let expr = BoundExpr::AnyOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(5))),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Null,
                Datum::Int32(3),
            ]))),
        };
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn test_any_null_left() {
        // NULL = ANY(ARRAY[1,2]) → NULL
        let r = row(vec![]);
        let expr = BoundExpr::AnyOp {
            left: Box::new(BoundExpr::Literal(Datum::Null)),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![Datum::Int32(1)]))),
        };
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn test_all_eq_all_match() {
        // 1 = ALL(ARRAY[1,1,1]) → true
        let r = row(vec![]);
        let expr = BoundExpr::AllOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Int32(1),
                Datum::Int32(1),
            ]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_all_eq_partial() {
        // 1 = ALL(ARRAY[1,2,1]) → false
        let r = row(vec![]);
        let expr = BoundExpr::AllOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Int32(2),
                Datum::Int32(1),
            ]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_all_gt() {
        // 10 > ALL(ARRAY[1,2,3]) → true
        let r = row(vec![]);
        let expr = BoundExpr::AllOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(10))),
            compare_op: BinOp::Gt,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(1),
                Datum::Int32(2),
                Datum::Int32(3),
            ]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    #[test]
    fn test_all_empty_array() {
        // 1 = ALL(ARRAY[]) → true (vacuously)
        let r = row(vec![]);
        let expr = BoundExpr::AllOp {
            left: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            compare_op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Array(vec![]))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    // ── Array slice tests ─────────────────────────────────────────────

    #[test]
    fn test_array_slice_basic() {
        // ARRAY[10,20,30,40,50][2:4] → ARRAY[20,30,40]
        let r = row(vec![]);
        let expr = BoundExpr::ArraySlice {
            array: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(10),
                Datum::Int32(20),
                Datum::Int32(30),
                Datum::Int32(40),
                Datum::Int32(50),
            ]))),
            lower: Some(Box::new(BoundExpr::Literal(Datum::Int32(2)))),
            upper: Some(Box::new(BoundExpr::Literal(Datum::Int32(4)))),
        };
        assert_eq!(
            eval_expr(&expr, &r).unwrap(),
            Datum::Array(vec![Datum::Int32(20), Datum::Int32(30), Datum::Int32(40)])
        );
    }

    #[test]
    fn test_array_slice_open_lower() {
        // ARRAY[10,20,30][:2] → ARRAY[10,20]
        let r = row(vec![]);
        let expr = BoundExpr::ArraySlice {
            array: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(10),
                Datum::Int32(20),
                Datum::Int32(30),
            ]))),
            lower: None,
            upper: Some(Box::new(BoundExpr::Literal(Datum::Int32(2)))),
        };
        assert_eq!(
            eval_expr(&expr, &r).unwrap(),
            Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)])
        );
    }

    #[test]
    fn test_array_slice_open_upper() {
        // ARRAY[10,20,30][2:] → ARRAY[20,30]
        let r = row(vec![]);
        let expr = BoundExpr::ArraySlice {
            array: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(10),
                Datum::Int32(20),
                Datum::Int32(30),
            ]))),
            lower: Some(Box::new(BoundExpr::Literal(Datum::Int32(2)))),
            upper: None,
        };
        assert_eq!(
            eval_expr(&expr, &r).unwrap(),
            Datum::Array(vec![Datum::Int32(20), Datum::Int32(30)])
        );
    }

    #[test]
    fn test_array_slice_null_array() {
        let r = row(vec![]);
        let expr = BoundExpr::ArraySlice {
            array: Box::new(BoundExpr::Literal(Datum::Null)),
            lower: Some(Box::new(BoundExpr::Literal(Datum::Int32(1)))),
            upper: Some(Box::new(BoundExpr::Literal(Datum::Int32(2)))),
        };
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn test_array_slice_out_of_range() {
        // ARRAY[10,20][5:10] → empty array
        let r = row(vec![]);
        let expr = BoundExpr::ArraySlice {
            array: Box::new(BoundExpr::Literal(Datum::Array(vec![
                Datum::Int32(10),
                Datum::Int32(20),
            ]))),
            lower: Some(Box::new(BoundExpr::Literal(Datum::Int32(5)))),
            upper: Some(Box::new(BoundExpr::Literal(Datum::Int32(10)))),
        };
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Array(vec![]));
    }
}

// ── Parameterized SQL kernel tests ──────────────────────────────────
#[cfg(test)]
mod param_tests {
    use crate::eval::{eval_expr_with_params, substitute_params_expr};
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::error::ExecutionError;
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn empty_row() -> OwnedRow {
        OwnedRow::new(vec![])
    }

    // ── eval_expr_with_params ──

    #[test]
    fn param_literal_substitution() {
        let params = vec![Datum::Int32(42)];
        let expr = BoundExpr::Parameter(1);
        let result = eval_expr_with_params(&expr, &empty_row(), &params).unwrap();
        assert_eq!(result, Datum::Int32(42));
    }

    #[test]
    fn param_in_binary_op() {
        // $1 + $2 where $1=10, $2=20 → 30
        let params = vec![Datum::Int32(10), Datum::Int32(20)];
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Parameter(1)),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::Parameter(2)),
        };
        let result = eval_expr_with_params(&expr, &empty_row(), &params).unwrap();
        assert_eq!(result, Datum::Int32(30));
    }

    #[test]
    fn param_with_column_ref() {
        // col0 = $1 where col0=42, $1=42 → true
        let row = OwnedRow::new(vec![Datum::Int32(42)]);
        let params = vec![Datum::Int32(42)];
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Parameter(1)),
        };
        let result = eval_expr_with_params(&expr, &row, &params).unwrap();
        assert_eq!(result, Datum::Boolean(true));
    }

    #[test]
    fn param_null_value() {
        let params = vec![Datum::Null];
        let expr = BoundExpr::Parameter(1);
        let result = eval_expr_with_params(&expr, &empty_row(), &params).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn param_text_type() {
        let params = vec![Datum::Text("hello".into())];
        let expr = BoundExpr::Parameter(1);
        let result = eval_expr_with_params(&expr, &empty_row(), &params).unwrap();
        assert_eq!(result, Datum::Text("hello".into()));
    }

    #[test]
    fn param_missing_index_errors() {
        // $2 with only 1 param → error
        let params = vec![Datum::Int32(1)];
        let expr = BoundExpr::Parameter(2);
        let result = eval_expr_with_params(&expr, &empty_row(), &params);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::ParamMissing(2)
        ));
    }

    #[test]
    fn param_zero_index_errors() {
        // $0 is invalid
        let params = vec![Datum::Int32(1)];
        let expr = BoundExpr::Parameter(0);
        let result = eval_expr_with_params(&expr, &empty_row(), &params);
        assert!(result.is_err());
    }

    #[test]
    fn param_no_params_errors() {
        // BoundExpr::Parameter without any params → error
        let expr = BoundExpr::Parameter(1);
        let result = eval_expr_with_params(&expr, &empty_row(), &[]);
        assert!(result.is_err());
    }

    // ── substitute_params_expr ──

    #[test]
    fn subst_simple_parameter() {
        let params = vec![Datum::Int64(99)];
        let expr = BoundExpr::Parameter(1);
        let result = substitute_params_expr(&expr, &params).unwrap();
        assert!(matches!(result, BoundExpr::Literal(Datum::Int64(99))));
    }

    #[test]
    fn subst_nested_binary_op() {
        // $1 > $2 → Literal(10) > Literal(5)
        let params = vec![Datum::Int32(10), Datum::Int32(5)];
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Parameter(1)),
            op: BinOp::Gt,
            right: Box::new(BoundExpr::Parameter(2)),
        };
        let result = substitute_params_expr(&expr, &params).unwrap();
        match result {
            BoundExpr::BinaryOp { left, right, .. } => {
                assert!(matches!(*left, BoundExpr::Literal(Datum::Int32(10))));
                assert!(matches!(*right, BoundExpr::Literal(Datum::Int32(5))));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn subst_leaves_literals_unchanged() {
        let params = vec![Datum::Int32(1)];
        let expr = BoundExpr::Literal(Datum::Text("hello".into()));
        let result = substitute_params_expr(&expr, &params).unwrap();
        assert!(matches!(result, BoundExpr::Literal(Datum::Text(ref s)) if s == "hello"));
    }

    #[test]
    fn subst_missing_param_errors() {
        let params = vec![Datum::Int32(1)];
        let expr = BoundExpr::Parameter(3);
        let result = substitute_params_expr(&expr, &params);
        assert!(result.is_err());
    }

    #[test]
    fn subst_in_cast() {
        // CAST($1 AS text) → CAST(Literal(42) AS text)
        let params = vec![Datum::Int32(42)];
        let expr = BoundExpr::Cast {
            expr: Box::new(BoundExpr::Parameter(1)),
            target_type: "text".into(),
        };
        let result = substitute_params_expr(&expr, &params).unwrap();
        match result {
            BoundExpr::Cast { expr: inner, .. } => {
                assert!(matches!(*inner, BoundExpr::Literal(Datum::Int32(42))));
            }
            _ => panic!("Expected Cast"),
        }
    }

    #[test]
    fn subst_in_coalesce() {
        // COALESCE($1, $2)
        let params = vec![Datum::Null, Datum::Text("fallback".into())];
        let expr = BoundExpr::Coalesce(vec![BoundExpr::Parameter(1), BoundExpr::Parameter(2)]);
        let result = substitute_params_expr(&expr, &params).unwrap();
        match result {
            BoundExpr::Coalesce(args) => {
                assert_eq!(args.len(), 2);
                assert!(matches!(&args[0], BoundExpr::Literal(Datum::Null)));
                assert!(matches!(&args[1], BoundExpr::Literal(Datum::Text(s)) if s == "fallback"));
            }
            _ => panic!("Expected Coalesce"),
        }
    }

    #[test]
    fn subst_in_list() {
        // $1 IN ($2, $3)
        let params = vec![Datum::Int32(5), Datum::Int32(5), Datum::Int32(10)];
        let expr = BoundExpr::InList {
            expr: Box::new(BoundExpr::Parameter(1)),
            list: vec![BoundExpr::Parameter(2), BoundExpr::Parameter(3)],
            negated: false,
        };
        let substituted = substitute_params_expr(&expr, &params).unwrap();
        // Evaluate the substituted expression
        let result = crate::eval::eval_expr(&substituted, &empty_row()).unwrap();
        assert_eq!(result, Datum::Boolean(true));
    }
}

#[cfg(test)]
mod rbac_enforcement_tests {
    use crate::Executor;
    use falcon_common::error::{ExecutionError, FalconError};
    use falcon_common::security::{
        ObjectRef, ObjectType, Privilege, PrivilegeManager, Role, RoleCatalog, RoleId,
    };
    use std::sync::{Arc, RwLock};

    fn setup_rbac() -> (Arc<RwLock<RoleCatalog>>, Arc<RwLock<PrivilegeManager>>) {
        let mut catalog = RoleCatalog::new();
        let reader_role = Role::new_user(
            RoleId(100),
            "reader".into(),
            falcon_common::tenant::TenantId(0),
        );
        catalog.add_role(reader_role);

        let writer_role = Role::new_user(
            RoleId(200),
            "writer".into(),
            falcon_common::tenant::TenantId(0),
        );
        catalog.add_role(writer_role);

        let mut priv_mgr = PrivilegeManager::new();

        let users_obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: obj_hash("users"),
            object_name: "users".into(),
        };
        priv_mgr.grant(
            RoleId(100),
            Privilege::Select,
            users_obj.clone(),
            RoleId(0),
            false,
        );
        priv_mgr.grant(RoleId(200), Privilege::Insert, users_obj, RoleId(0), false);

        let public_obj = ObjectRef {
            object_type: ObjectType::Schema,
            object_id: obj_hash("public"),
            object_name: "public".into(),
        };
        priv_mgr.grant(RoleId(200), Privilege::Create, public_obj, RoleId(0), false);

        (
            Arc::new(RwLock::new(catalog)),
            Arc::new(RwLock::new(priv_mgr)),
        )
    }

    fn obj_hash(name: &str) -> u64 {
        let mut h: u64 = 5381;
        for b in name.bytes() {
            h = h.wrapping_mul(33).wrapping_add(b as u64);
        }
        h
    }

    fn make_executor(role: RoleId) -> Executor {
        let storage = Arc::new(falcon_storage::engine::StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(falcon_txn::TxnManager::new(storage.clone()));
        let (catalog, priv_mgr) = setup_rbac();
        let mut exec = Executor::new(storage, txn_mgr);
        exec.set_rbac(catalog, priv_mgr, role);
        exec
    }

    #[test]
    fn test_reader_cannot_insert() {
        let exec = make_executor(RoleId(100));
        let result = exec.check_privilege_public(Privilege::Insert, ObjectType::Table, "users");
        assert!(result.is_err());
        match result.unwrap_err() {
            FalconError::Execution(ExecutionError::InsufficientPrivilege(msg)) => {
                assert!(msg.contains("permission denied"), "msg={}", msg);
                assert!(msg.contains("users"), "msg should mention table: {}", msg);
            }
            other => panic!("expected InsufficientPrivilege, got: {:?}", other),
        }
    }

    #[test]
    fn test_reader_cannot_update() {
        let exec = make_executor(RoleId(100));
        let result = exec.check_privilege_public(Privilege::Update, ObjectType::Table, "users");
        assert!(result.is_err());
        match result.unwrap_err() {
            FalconError::Execution(ExecutionError::InsufficientPrivilege(_)) => {}
            other => panic!("expected InsufficientPrivilege, got: {:?}", other),
        }
    }

    #[test]
    fn test_reader_cannot_delete() {
        let exec = make_executor(RoleId(100));
        let result = exec.check_privilege_public(Privilege::Delete, ObjectType::Table, "users");
        assert!(result.is_err());
        match result.unwrap_err() {
            FalconError::Execution(ExecutionError::InsufficientPrivilege(_)) => {}
            other => panic!("expected InsufficientPrivilege, got: {:?}", other),
        }
    }

    #[test]
    fn test_reader_cannot_create_table() {
        let exec = make_executor(RoleId(100));
        let result = exec.check_privilege_public(Privilege::Create, ObjectType::Schema, "public");
        assert!(result.is_err());
        match result.unwrap_err() {
            FalconError::Execution(ExecutionError::InsufficientPrivilege(_)) => {}
            other => panic!("expected InsufficientPrivilege, got: {:?}", other),
        }
    }

    #[test]
    fn test_writer_can_insert_on_granted_table() {
        let exec = make_executor(RoleId(200));
        let result = exec.check_privilege_public(Privilege::Insert, ObjectType::Table, "users");
        assert!(result.is_ok(), "writer should have INSERT on users");
    }

    #[test]
    fn test_writer_cannot_select_without_grant() {
        let exec = make_executor(RoleId(200));
        let result = exec.check_privilege_public(Privilege::Select, ObjectType::Table, "users");
        assert!(result.is_err(), "writer should NOT have SELECT on users");
    }

    #[test]
    fn test_error_is_pg_compatible_sqlstate() {
        let exec = make_executor(RoleId(100));
        let err = exec
            .check_privilege_public(Privilege::Insert, ObjectType::Table, "users")
            .unwrap_err();
        let sqlstate = err.pg_sqlstate();
        assert_eq!(sqlstate, "42501", "should return PG SQLSTATE 42501");
    }
}
