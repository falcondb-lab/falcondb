/// Extended expression evaluation tests for falcon_executor.
///
/// Covers gaps not addressed by tests.rs:
///   - CASE WHEN (searched + simple + ELSE + NULL branch)
///   - COALESCE / NULLIF
///   - ScalarFunc calls (string / math / utility)
///   - ArrayLiteral construction + ArrayIndex access
///   - Deep nested expression evaluation
///   - GtEq / negative arithmetic / Int64 edge cases
///   - CompiledExpr wrapper
///   - eval_having_filter
///   - eval_filter error / non-boolean path

// ── CASE WHEN tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod case_when_tests {
    use crate::eval::eval_expr;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    fn col(i: usize) -> BoundExpr {
        BoundExpr::ColumnRef(i)
    }

    // CASE WHEN col0 > 10 THEN 'high' WHEN col0 > 5 THEN 'mid' ELSE 'low' END
    fn case_searched(col_idx: usize) -> BoundExpr {
        BoundExpr::Case {
            operand: None,
            conditions: vec![
                BoundExpr::BinaryOp {
                    left: Box::new(col(col_idx)),
                    op: BinOp::Gt,
                    right: Box::new(lit(Datum::Int32(10))),
                },
                BoundExpr::BinaryOp {
                    left: Box::new(col(col_idx)),
                    op: BinOp::Gt,
                    right: Box::new(lit(Datum::Int32(5))),
                },
            ],
            results: vec![
                lit(Datum::Text("high".into())),
                lit(Datum::Text("mid".into())),
            ],
            else_result: Some(Box::new(lit(Datum::Text("low".into())))),
        }
    }

    #[test]
    fn searched_case_first_branch() {
        let r = row(vec![Datum::Int32(20)]);
        assert_eq!(
            eval_expr(&case_searched(0), &r).unwrap(),
            Datum::Text("high".into())
        );
    }

    #[test]
    fn searched_case_second_branch() {
        let r = row(vec![Datum::Int32(7)]);
        assert_eq!(
            eval_expr(&case_searched(0), &r).unwrap(),
            Datum::Text("mid".into())
        );
    }

    #[test]
    fn searched_case_else_branch() {
        let r = row(vec![Datum::Int32(3)]);
        assert_eq!(
            eval_expr(&case_searched(0), &r).unwrap(),
            Datum::Text("low".into())
        );
    }

    #[test]
    fn searched_case_no_else_returns_null() {
        let expr = BoundExpr::Case {
            operand: None,
            conditions: vec![BoundExpr::BinaryOp {
                left: Box::new(col(0)),
                op: BinOp::Eq,
                right: Box::new(lit(Datum::Int32(99))),
            }],
            results: vec![lit(Datum::Text("found".into()))],
            else_result: None,
        };
        let r = row(vec![Datum::Int32(1)]);
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    // Simple CASE: CASE col0 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END
    #[test]
    fn simple_case_first_match() {
        let expr = BoundExpr::Case {
            operand: Some(Box::new(col(0))),
            conditions: vec![lit(Datum::Int32(1)), lit(Datum::Int32(2))],
            results: vec![
                lit(Datum::Text("one".into())),
                lit(Datum::Text("two".into())),
            ],
            else_result: Some(Box::new(lit(Datum::Text("other".into())))),
        };
        let r = row(vec![Datum::Int32(1)]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Text("one".into()));
    }

    #[test]
    fn simple_case_second_match() {
        let expr = BoundExpr::Case {
            operand: Some(Box::new(col(0))),
            conditions: vec![lit(Datum::Int32(1)), lit(Datum::Int32(2))],
            results: vec![
                lit(Datum::Text("one".into())),
                lit(Datum::Text("two".into())),
            ],
            else_result: Some(Box::new(lit(Datum::Text("other".into())))),
        };
        let r = row(vec![Datum::Int32(2)]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Text("two".into()));
    }

    #[test]
    fn simple_case_else() {
        let expr = BoundExpr::Case {
            operand: Some(Box::new(col(0))),
            conditions: vec![lit(Datum::Int32(1))],
            results: vec![lit(Datum::Text("one".into()))],
            else_result: Some(Box::new(lit(Datum::Text("other".into())))),
        };
        let r = row(vec![Datum::Int32(99)]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Text("other".into()));
    }

    // Short-circuit: second WHEN should not be evaluated once first matches
    #[test]
    fn searched_case_short_circuits() {
        // CASE WHEN col0 = 1 THEN 10 WHEN col0 = 1 THEN 20 END
        // Both conditions match, but first result wins.
        let expr = BoundExpr::Case {
            operand: None,
            conditions: vec![
                BoundExpr::BinaryOp {
                    left: Box::new(col(0)),
                    op: BinOp::Eq,
                    right: Box::new(lit(Datum::Int32(1))),
                },
                BoundExpr::BinaryOp {
                    left: Box::new(col(0)),
                    op: BinOp::Eq,
                    right: Box::new(lit(Datum::Int32(1))),
                },
            ],
            results: vec![lit(Datum::Int64(10)), lit(Datum::Int64(20))],
            else_result: None,
        };
        let r = row(vec![Datum::Int32(1)]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int64(10));
    }

    // CASE with NULL condition — NULL condition is not true, falls through
    #[test]
    fn searched_case_null_condition_falls_through() {
        let expr = BoundExpr::Case {
            operand: None,
            conditions: vec![
                // NULL IS NOT a boolean true, so this branch should be skipped
                BoundExpr::BinaryOp {
                    left: Box::new(lit(Datum::Null)),
                    op: BinOp::Eq,
                    right: Box::new(lit(Datum::Int32(1))),
                },
            ],
            results: vec![lit(Datum::Text("matched".into()))],
            else_result: Some(Box::new(lit(Datum::Text("fallback".into())))),
        };
        let r = row(vec![]);
        assert_eq!(
            eval_expr(&expr, &r).unwrap(),
            Datum::Text("fallback".into())
        );
    }

    // Result expression may itself be complex
    #[test]
    fn case_result_is_expression() {
        // CASE WHEN true THEN col0 * 2 END
        let expr = BoundExpr::Case {
            operand: None,
            conditions: vec![lit(Datum::Boolean(true))],
            results: vec![BoundExpr::BinaryOp {
                left: Box::new(col(0)),
                op: BinOp::Multiply,
                right: Box::new(lit(Datum::Int32(2))),
            }],
            else_result: None,
        };
        let r = row(vec![Datum::Int32(7)]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int64(14));
    }
}

// ── COALESCE / NULLIF tests ────────────────────────────────────────────────

#[cfg(test)]
mod coalesce_nullif_tests {
    use crate::eval::eval_expr;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::BoundExpr;

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    fn col(i: usize) -> BoundExpr {
        BoundExpr::ColumnRef(i)
    }

    #[test]
    fn coalesce_first_non_null() {
        let expr = BoundExpr::Coalesce(vec![lit(Datum::Null), lit(Datum::Int32(42))]);
        let r = row(vec![]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int32(42));
    }

    #[test]
    fn coalesce_first_value_non_null() {
        let expr = BoundExpr::Coalesce(vec![lit(Datum::Int32(10)), lit(Datum::Int32(99))]);
        let r = row(vec![]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int32(10));
    }

    #[test]
    fn coalesce_all_null_returns_null() {
        let expr = BoundExpr::Coalesce(vec![lit(Datum::Null), lit(Datum::Null), lit(Datum::Null)]);
        let r = row(vec![]);
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn coalesce_column_ref_non_null() {
        // COALESCE(col0, col1) where col0=NULL, col1=7
        let expr = BoundExpr::Coalesce(vec![col(0), col(1)]);
        let r = row(vec![Datum::Null, Datum::Int32(7)]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int32(7));
    }

    #[test]
    fn coalesce_single_element() {
        let expr = BoundExpr::Coalesce(vec![lit(Datum::Text("hello".into()))]);
        let r = row(vec![]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Text("hello".into()));
    }

    #[test]
    fn coalesce_single_null() {
        let expr = BoundExpr::Coalesce(vec![lit(Datum::Null)]);
        let r = row(vec![]);
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    // NULLIF(a, b) = CASE WHEN a = b THEN NULL ELSE a END
    // This is a SQL standard expression rewritten as a Case node.
    fn make_nullif(a: BoundExpr, b: BoundExpr) -> BoundExpr {
        use falcon_sql_frontend::types::BinOp;
        BoundExpr::Case {
            operand: None,
            conditions: vec![BoundExpr::BinaryOp {
                left: Box::new(a.clone()),
                op: BinOp::Eq,
                right: Box::new(b),
            }],
            results: vec![lit(Datum::Null)],
            else_result: Some(Box::new(a)),
        }
    }

    #[test]
    fn nullif_equal_returns_null() {
        let expr = make_nullif(lit(Datum::Int32(5)), lit(Datum::Int32(5)));
        let r = row(vec![]);
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    #[test]
    fn nullif_unequal_returns_first() {
        let expr = make_nullif(lit(Datum::Int32(5)), lit(Datum::Int32(3)));
        let r = row(vec![]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int32(5));
    }

    #[test]
    fn nullif_first_is_null_returns_null() {
        // NULLIF(NULL, 5): NULL = 5 is NULL (not true), so ELSE NULL
        let expr = make_nullif(lit(Datum::Null), lit(Datum::Int32(5)));
        let r = row(vec![]);
        // NULL = 5 → Datum::Null (not Boolean(true)), so else branch → Datum::Null
        assert!(matches!(eval_expr(&expr, &r).unwrap(), Datum::Null));
    }

    // COALESCE inside CASE
    #[test]
    fn coalesce_inside_case_result() {
        let expr = BoundExpr::Case {
            operand: None,
            conditions: vec![lit(Datum::Boolean(true))],
            results: vec![BoundExpr::Coalesce(vec![
                lit(Datum::Null),
                lit(Datum::Int32(99)),
            ])],
            else_result: None,
        };
        let r = row(vec![]);
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int32(99));
    }
}

// ── ScalarFunc tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod scalar_func_tests {
    use crate::eval::eval_expr;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::{BoundExpr, ScalarFunc};

    fn row0() -> OwnedRow {
        OwnedRow::new(vec![])
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    fn func(f: ScalarFunc, args: Vec<BoundExpr>) -> BoundExpr {
        BoundExpr::Function { func: f, args }
    }

    // ── String functions ──

    #[test]
    fn upper_basic() {
        let expr = func(ScalarFunc::Upper, vec![lit(Datum::Text("hello".into()))]);
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Text("HELLO".into())
        );
    }

    #[test]
    fn lower_basic() {
        let expr = func(ScalarFunc::Lower, vec![lit(Datum::Text("WORLD".into()))]);
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Text("world".into())
        );
    }

    #[test]
    fn length_ascii() {
        let expr = func(ScalarFunc::Length, vec![lit(Datum::Text("hello".into()))]);
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(5));
    }

    #[test]
    fn length_empty_string() {
        let expr = func(ScalarFunc::Length, vec![lit(Datum::Text("".into()))]);
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(0));
    }

    #[test]
    fn length_null_returns_null() {
        let expr = func(ScalarFunc::Length, vec![lit(Datum::Null)]);
        assert!(matches!(eval_expr(&expr, &row0()).unwrap(), Datum::Null));
    }

    #[test]
    fn concat_two_strings() {
        let expr = func(
            ScalarFunc::Concat,
            vec![
                lit(Datum::Text("foo".into())),
                lit(Datum::Text("bar".into())),
            ],
        );
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Text("foobar".into())
        );
    }

    #[test]
    fn concat_with_null_skips_null() {
        // PostgreSQL CONCAT() ignores NULL arguments
        let expr = func(
            ScalarFunc::Concat,
            vec![
                lit(Datum::Text("foo".into())),
                lit(Datum::Null),
                lit(Datum::Text("bar".into())),
            ],
        );
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Text("foobar".into())
        );
    }

    #[test]
    fn trim_leading_trailing() {
        let expr = func(ScalarFunc::Trim, vec![lit(Datum::Text("  hi  ".into()))]);
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Text("hi".into()));
    }

    #[test]
    fn substring_basic() {
        // SUBSTRING('hello', 2, 3) → 'ell'
        let expr = func(
            ScalarFunc::Substring,
            vec![
                lit(Datum::Text("hello".into())),
                lit(Datum::Int32(2)),
                lit(Datum::Int32(3)),
            ],
        );
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Text("ell".into())
        );
    }

    #[test]
    fn replace_basic() {
        let expr = func(
            ScalarFunc::Replace,
            vec![
                lit(Datum::Text("hello world".into())),
                lit(Datum::Text("world".into())),
                lit(Datum::Text("Rust".into())),
            ],
        );
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Text("hello Rust".into())
        );
    }

    #[test]
    fn upper_null_returns_null() {
        let expr = func(ScalarFunc::Upper, vec![lit(Datum::Null)]);
        assert!(matches!(eval_expr(&expr, &row0()).unwrap(), Datum::Null));
    }

    // ── Math functions ──

    #[test]
    fn abs_positive() {
        let expr = func(ScalarFunc::Abs, vec![lit(Datum::Int32(5))]);
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(5));
    }

    #[test]
    fn abs_negative() {
        let expr = func(ScalarFunc::Abs, vec![lit(Datum::Int32(-7))]);
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(7));
    }

    #[test]
    fn abs_float() {
        let expr = func(ScalarFunc::Abs, vec![lit(Datum::Float64(-3.14))]);
        let result = eval_expr(&expr, &row0()).unwrap();
        if let Datum::Float64(v) = result {
            assert!((v - 3.14).abs() < 1e-10);
        } else {
            panic!("expected Float64, got {:?}", result);
        }
    }

    #[test]
    fn round_float() {
        let expr = func(ScalarFunc::Round, vec![lit(Datum::Float64(3.6))]);
        let result = eval_expr(&expr, &row0()).unwrap();
        if let Datum::Float64(v) = result {
            assert!((v - 4.0).abs() < 1e-10);
        } else {
            panic!("expected Float64, got {:?}", result);
        }
    }

    #[test]
    fn ceil_float() {
        let expr = func(ScalarFunc::Ceil, vec![lit(Datum::Float64(2.1))]);
        let result = eval_expr(&expr, &row0()).unwrap();
        if let Datum::Float64(v) = result {
            assert!((v - 3.0).abs() < 1e-10);
        } else {
            panic!("expected Float64, got {:?}", result);
        }
    }

    #[test]
    fn floor_float() {
        let expr = func(ScalarFunc::Floor, vec![lit(Datum::Float64(2.9))]);
        let result = eval_expr(&expr, &row0()).unwrap();
        if let Datum::Float64(v) = result {
            assert!((v - 2.0).abs() < 1e-10);
        } else {
            panic!("expected Float64, got {:?}", result);
        }
    }

    #[test]
    fn sqrt_basic() {
        let expr = func(ScalarFunc::Sqrt, vec![lit(Datum::Float64(9.0))]);
        let result = eval_expr(&expr, &row0()).unwrap();
        if let Datum::Float64(v) = result {
            assert!((v - 3.0).abs() < 1e-10);
        } else {
            panic!("expected Float64, got {:?}", result);
        }
    }

    #[test]
    fn greatest_picks_max() {
        let expr = func(
            ScalarFunc::Greatest,
            vec![
                lit(Datum::Int32(3)),
                lit(Datum::Int32(7)),
                lit(Datum::Int32(1)),
            ],
        );
        // Greatest ignores NULLs and returns max non-null
        let result = eval_expr(&expr, &row0()).unwrap();
        assert_eq!(result, Datum::Int32(7));
    }

    #[test]
    fn least_picks_min() {
        let expr = func(
            ScalarFunc::Least,
            vec![
                lit(Datum::Int32(3)),
                lit(Datum::Int32(7)),
                lit(Datum::Int32(1)),
            ],
        );
        let result = eval_expr(&expr, &row0()).unwrap();
        assert_eq!(result, Datum::Int32(1));
    }
}

// ── ArrayLiteral / ArrayIndex tests ───────────────────────────────────────

#[cfg(test)]
mod array_literal_index_tests {
    use crate::eval::eval_expr;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::BoundExpr;

    fn row0() -> OwnedRow {
        OwnedRow::new(vec![])
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    #[test]
    fn array_literal_construction() {
        let expr = BoundExpr::ArrayLiteral(vec![
            lit(Datum::Int32(10)),
            lit(Datum::Int32(20)),
            lit(Datum::Int32(30)),
        ]);
        assert_eq!(
            eval_expr(&expr, &row0()).unwrap(),
            Datum::Array(vec![Datum::Int32(10), Datum::Int32(20), Datum::Int32(30)])
        );
    }

    #[test]
    fn array_literal_empty() {
        let expr = BoundExpr::ArrayLiteral(vec![]);
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Array(vec![]));
    }

    #[test]
    fn array_literal_with_nulls() {
        let expr = BoundExpr::ArrayLiteral(vec![
            lit(Datum::Int32(1)),
            lit(Datum::Null),
            lit(Datum::Int32(3)),
        ]);
        let result = eval_expr(&expr, &row0()).unwrap();
        match result {
            Datum::Array(elems) => {
                assert_eq!(elems.len(), 3);
                assert_eq!(elems[0], Datum::Int32(1));
                assert!(elems[1].is_null());
                assert_eq!(elems[2], Datum::Int32(3));
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn array_index_first_element() {
        // ARRAY[10,20,30][1] → 10  (1-indexed)
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(lit(Datum::Array(vec![
                Datum::Int32(10),
                Datum::Int32(20),
                Datum::Int32(30),
            ]))),
            index: Box::new(lit(Datum::Int32(1))),
        };
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(10));
    }

    #[test]
    fn array_index_last_element() {
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(lit(Datum::Array(vec![
                Datum::Int32(10),
                Datum::Int32(20),
                Datum::Int32(30),
            ]))),
            index: Box::new(lit(Datum::Int32(3))),
        };
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(30));
    }

    #[test]
    fn array_index_out_of_bounds_returns_null() {
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(lit(Datum::Array(vec![Datum::Int32(1)]))),
            index: Box::new(lit(Datum::Int32(5))),
        };
        assert!(matches!(eval_expr(&expr, &row0()).unwrap(), Datum::Null));
    }

    #[test]
    fn array_index_zero_returns_null() {
        // PostgreSQL arrays are 1-indexed; index 0 is out of range
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(lit(Datum::Array(vec![Datum::Int32(1), Datum::Int32(2)]))),
            index: Box::new(lit(Datum::Int32(0))),
        };
        assert!(matches!(eval_expr(&expr, &row0()).unwrap(), Datum::Null));
    }

    #[test]
    fn array_index_null_array_returns_null() {
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(lit(Datum::Null)),
            index: Box::new(lit(Datum::Int32(1))),
        };
        assert!(matches!(eval_expr(&expr, &row0()).unwrap(), Datum::Null));
    }

    #[test]
    fn array_index_null_index_returns_null() {
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(lit(Datum::Array(vec![Datum::Int32(1)]))),
            index: Box::new(lit(Datum::Null)),
        };
        assert!(matches!(eval_expr(&expr, &row0()).unwrap(), Datum::Null));
    }

    #[test]
    fn array_literal_then_index() {
        // ARRAY[5,10,15][2] → 10
        let arr = BoundExpr::ArrayLiteral(vec![
            lit(Datum::Int32(5)),
            lit(Datum::Int32(10)),
            lit(Datum::Int32(15)),
        ]);
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(arr),
            index: Box::new(lit(Datum::Int32(2))),
        };
        assert_eq!(eval_expr(&expr, &row0()).unwrap(), Datum::Int32(10));
    }
}

// ── Deep nested expression tests ──────────────────────────────────────────

#[cfg(test)]
mod nested_expr_tests {
    use crate::eval::eval_expr;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    fn col(i: usize) -> BoundExpr {
        BoundExpr::ColumnRef(i)
    }

    fn add(l: BoundExpr, r: BoundExpr) -> BoundExpr {
        BoundExpr::BinaryOp {
            left: Box::new(l),
            op: BinOp::Plus,
            right: Box::new(r),
        }
    }

    fn mul(l: BoundExpr, r: BoundExpr) -> BoundExpr {
        BoundExpr::BinaryOp {
            left: Box::new(l),
            op: BinOp::Multiply,
            right: Box::new(r),
        }
    }

    fn gt(l: BoundExpr, r: BoundExpr) -> BoundExpr {
        BoundExpr::BinaryOp {
            left: Box::new(l),
            op: BinOp::Gt,
            right: Box::new(r),
        }
    }

    fn and(l: BoundExpr, r: BoundExpr) -> BoundExpr {
        BoundExpr::BinaryOp {
            left: Box::new(l),
            op: BinOp::And,
            right: Box::new(r),
        }
    }

    // (col0 + col1) * (col2 - 1) > 20
    #[test]
    fn three_level_arithmetic_comparison() {
        let r = row(vec![Datum::Int32(3), Datum::Int32(4), Datum::Int32(5)]);
        // (3 + 4) * (5 - 1) = 7 * 4 = 28 > 20 → true
        let sum = add(col(0), col(1));
        let diff = BoundExpr::BinaryOp {
            left: Box::new(col(2)),
            op: BinOp::Minus,
            right: Box::new(lit(Datum::Int32(1))),
        };
        let product = mul(sum, diff);
        let cmp = gt(product, lit(Datum::Int64(20)));
        assert_eq!(eval_expr(&cmp, &r).unwrap(), Datum::Boolean(true));
    }

    // NOT (col0 > 5 AND col1 < 10)
    #[test]
    fn negated_compound_condition() {
        let r = row(vec![Datum::Int32(7), Datum::Int32(8)]);
        let cond = and(
            gt(col(0), lit(Datum::Int32(5))),
            BoundExpr::BinaryOp {
                left: Box::new(col(1)),
                op: BinOp::Lt,
                right: Box::new(lit(Datum::Int32(10))),
            },
        );
        let expr = BoundExpr::Not(Box::new(cond));
        // (7 > 5 AND 8 < 10) = true; NOT true = false
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(false));
    }

    // col0 * col0 + col1 * col1  (Pythagorean sum of squares)
    #[test]
    fn sum_of_squares() {
        let r = row(vec![Datum::Int32(3), Datum::Int32(4)]);
        // 3*3 + 4*4 = 9 + 16 = 25
        let sq0 = mul(col(0), col(0));
        let sq1 = mul(col(1), col(1));
        let total = add(sq0, sq1);
        assert_eq!(eval_expr(&total, &r).unwrap(), Datum::Int64(25));
    }

    // IS NULL inside AND
    #[test]
    fn is_null_inside_and() {
        let r = row(vec![Datum::Null, Datum::Int32(5)]);
        // (col0 IS NULL) AND (col1 > 3) → true AND true → true
        let expr = and(
            BoundExpr::IsNull(Box::new(col(0))),
            gt(col(1), lit(Datum::Int32(3))),
        );
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Boolean(true));
    }

    // COALESCE inside arithmetic
    #[test]
    fn coalesce_in_arithmetic() {
        let r = row(vec![Datum::Null, Datum::Int32(10)]);
        // COALESCE(col0, col1) + 5 = 10 + 5 = 15
        let coalesce = BoundExpr::Coalesce(vec![col(0), col(1)]);
        let expr = add(coalesce, lit(Datum::Int32(5)));
        assert_eq!(eval_expr(&expr, &r).unwrap(), Datum::Int64(15));
    }

    // Four levels of nesting: ((col0 + 1) * 2 - col1) > 0
    #[test]
    fn four_level_nested_expression() {
        let r = row(vec![Datum::Int32(5), Datum::Int32(3)]);
        // ((5 + 1) * 2 - 3) = (6 * 2 - 3) = 12 - 3 = 9 > 0 → true
        let inner = add(col(0), lit(Datum::Int32(1)));
        let scaled = mul(inner, lit(Datum::Int32(2)));
        let diff = BoundExpr::BinaryOp {
            left: Box::new(scaled),
            op: BinOp::Minus,
            right: Box::new(col(1)),
        };
        let cmp = gt(diff, lit(Datum::Int64(0)));
        assert_eq!(eval_expr(&cmp, &r).unwrap(), Datum::Boolean(true));
    }
}

// ── GtEq / negative / Int64 edge cases ────────────────────────────────────

#[cfg(test)]
mod numeric_edge_tests {
    use crate::eval::eval_expr;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    fn binop(l: Datum, op: BinOp, r_val: Datum) -> Datum {
        let r = row(vec![l.clone(), r_val.clone()]);
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        eval_expr(&expr, &r).unwrap()
    }

    #[test]
    fn gte_equal() {
        assert_eq!(
            binop(Datum::Int32(5), BinOp::GtEq, Datum::Int32(5)),
            Datum::Boolean(true)
        );
    }

    #[test]
    fn gte_greater() {
        assert_eq!(
            binop(Datum::Int32(6), BinOp::GtEq, Datum::Int32(5)),
            Datum::Boolean(true)
        );
    }

    #[test]
    fn gte_less() {
        assert_eq!(
            binop(Datum::Int32(4), BinOp::GtEq, Datum::Int32(5)),
            Datum::Boolean(false)
        );
    }

    #[test]
    fn negative_addition() {
        assert_eq!(
            binop(Datum::Int32(-3), BinOp::Plus, Datum::Int32(-7)),
            Datum::Int64(-10)
        );
    }

    #[test]
    fn negative_subtraction() {
        assert_eq!(
            binop(Datum::Int32(-5), BinOp::Minus, Datum::Int32(3)),
            Datum::Int64(-8)
        );
    }

    #[test]
    fn negative_multiplication() {
        assert_eq!(
            binop(Datum::Int32(-4), BinOp::Multiply, Datum::Int32(3)),
            Datum::Int64(-12)
        );
    }

    #[test]
    fn negative_divide() {
        assert_eq!(
            binop(Datum::Int32(-9), BinOp::Divide, Datum::Int32(3)),
            Datum::Int64(-3)
        );
    }

    #[test]
    fn int64_arithmetic() {
        let r = row(vec![
            Datum::Int64(1_000_000_000_i64),
            Datum::Int64(2_000_000_000_i64),
        ]);
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        assert_eq!(
            eval_expr(&expr, &r).unwrap(),
            Datum::Int64(3_000_000_000_i64)
        );
    }

    #[test]
    fn int64_comparison() {
        assert_eq!(
            binop(Datum::Int64(i64::MAX), BinOp::Gt, Datum::Int64(0)),
            Datum::Boolean(true)
        );
    }

    #[test]
    fn float64_comparison_gte() {
        assert_eq!(
            binop(Datum::Float64(3.14), BinOp::GtEq, Datum::Float64(3.14)),
            Datum::Boolean(true)
        );
    }

    #[test]
    fn float64_negative_divide() {
        let r = row(vec![Datum::Float64(-10.0), Datum::Float64(4.0)]);
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Divide,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        if let Datum::Float64(v) = eval_expr(&expr, &r).unwrap() {
            assert!((v - (-2.5)).abs() < 1e-10);
        } else {
            panic!("expected Float64");
        }
    }

    #[test]
    fn zero_comparison_operators() {
        assert_eq!(
            binop(Datum::Int32(0), BinOp::Eq, Datum::Int32(0)),
            Datum::Boolean(true)
        );
        assert_eq!(
            binop(Datum::Int32(0), BinOp::Lt, Datum::Int32(1)),
            Datum::Boolean(true)
        );
        assert_eq!(
            binop(Datum::Int32(0), BinOp::GtEq, Datum::Int32(0)),
            Datum::Boolean(true)
        );
    }

    #[test]
    fn cast_bool_to_int() {
        let expr = BoundExpr::Cast {
            expr: Box::new(lit(Datum::Boolean(true))),
            target_type: "integer".into(),
        };
        assert_eq!(eval_expr(&expr, &row(vec![])).unwrap(), Datum::Int32(1));

        let expr2 = BoundExpr::Cast {
            expr: Box::new(lit(Datum::Boolean(false))),
            target_type: "int".into(),
        };
        assert_eq!(eval_expr(&expr2, &row(vec![])).unwrap(), Datum::Int32(0));
    }

    #[test]
    fn cast_int64_to_text() {
        let expr = BoundExpr::Cast {
            expr: Box::new(lit(Datum::Int64(9876543210_i64))),
            target_type: "text".into(),
        };
        assert_eq!(
            eval_expr(&expr, &row(vec![])).unwrap(),
            Datum::Text("9876543210".into())
        );
    }

    #[test]
    fn cast_text_to_bigint() {
        let expr = BoundExpr::Cast {
            expr: Box::new(lit(Datum::Text("9876543210".into()))),
            target_type: "bigint".into(),
        };
        assert_eq!(
            eval_expr(&expr, &row(vec![])).unwrap(),
            Datum::Int64(9876543210_i64)
        );
    }
}

// ── CompiledExpr / eval_having_filter tests ───────────────────────────────

#[cfg(test)]
mod compiled_expr_tests {
    use crate::eval::eval_having_filter;
    use crate::expr_engine::{compile, CompiledExpr};
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    #[test]
    fn compiled_expr_eval_literal() {
        let expr = lit(Datum::Int32(42));
        let compiled = compile(&expr);
        let r = row(vec![]);
        assert_eq!(compiled.eval_row(&r).unwrap(), Datum::Int32(42));
    }

    #[test]
    fn compiled_expr_eval_column_ref() {
        let expr = BoundExpr::ColumnRef(1);
        let compiled = CompiledExpr::compile(&expr);
        let r = row(vec![Datum::Text("a".into()), Datum::Int32(99)]);
        assert_eq!(compiled.eval_row(&r).unwrap(), Datum::Int32(99));
    }

    #[test]
    fn compiled_expr_as_expr_roundtrip() {
        let expr = BoundExpr::BinaryOp {
            left: Box::new(lit(Datum::Int32(3))),
            op: BinOp::Plus,
            right: Box::new(lit(Datum::Int32(4))),
        };
        let compiled = compile(&expr);
        // as_expr() returns the original expression
        assert!(matches!(compiled.as_expr(), BoundExpr::BinaryOp { .. }));
    }

    #[test]
    fn eval_having_filter_true() {
        // HAVING col0 > 5: passes a group containing rows all with col0=10
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Gt,
            right: Box::new(lit(Datum::Int32(5))),
        };
        let r1 = row(vec![Datum::Int32(10)]);
        let r2 = row(vec![Datum::Int32(15)]);
        let group: Vec<&OwnedRow> = vec![&r1, &r2];
        assert!(eval_having_filter(&expr, &group).unwrap());
    }

    #[test]
    fn eval_having_filter_false() {
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Gt,
            right: Box::new(lit(Datum::Int32(100))),
        };
        let r1 = row(vec![Datum::Int32(10)]);
        let group: Vec<&OwnedRow> = vec![&r1];
        assert!(!eval_having_filter(&expr, &group).unwrap());
    }

    #[test]
    fn eval_having_filter_null_is_false() {
        // HAVING col0 = NULL → NULL evaluates as false
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(lit(Datum::Null)),
        };
        let r1 = row(vec![Datum::Int32(5)]);
        let group: Vec<&OwnedRow> = vec![&r1];
        assert!(!eval_having_filter(&expr, &group).unwrap());
    }
}

// ── eval_filter edge cases ─────────────────────────────────────────────────

#[cfg(test)]
mod eval_filter_edge_tests {
    use crate::eval::eval_filter;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::{BinOp, BoundExpr};

    fn row(values: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(values)
    }

    fn lit(d: Datum) -> BoundExpr {
        BoundExpr::Literal(d)
    }

    #[test]
    fn filter_true_literal() {
        let r = row(vec![]);
        assert!(eval_filter(&lit(Datum::Boolean(true)), &r).unwrap());
    }

    #[test]
    fn filter_false_literal() {
        let r = row(vec![]);
        assert!(!eval_filter(&lit(Datum::Boolean(false)), &r).unwrap());
    }

    #[test]
    fn filter_null_is_false() {
        let r = row(vec![]);
        assert!(!eval_filter(&lit(Datum::Null), &r).unwrap());
    }

    #[test]
    fn filter_compound_and_both_true() {
        let r = row(vec![Datum::Int32(10), Datum::Int32(5)]);
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(0)),
                op: BinOp::Gt,
                right: Box::new(lit(Datum::Int32(7))),
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(1)),
                op: BinOp::Lt,
                right: Box::new(lit(Datum::Int32(8))),
            }),
        };
        assert!(eval_filter(&expr, &r).unwrap());
    }

    #[test]
    fn filter_is_not_null_passes() {
        let r = row(vec![Datum::Int32(1)]);
        let expr = BoundExpr::IsNotNull(Box::new(BoundExpr::ColumnRef(0)));
        assert!(eval_filter(&expr, &r).unwrap());
    }

    #[test]
    fn filter_is_null_fails() {
        let r = row(vec![Datum::Int32(1)]);
        let expr = BoundExpr::IsNull(Box::new(BoundExpr::ColumnRef(0)));
        assert!(!eval_filter(&expr, &r).unwrap());
    }

    // BETWEEN as filter
    #[test]
    fn filter_between_in_range() {
        let r = row(vec![Datum::Int32(5)]);
        let expr = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(lit(Datum::Int32(1))),
            high: Box::new(lit(Datum::Int32(10))),
            negated: false,
        };
        assert!(eval_filter(&expr, &r).unwrap());
    }

    #[test]
    fn filter_between_out_of_range() {
        let r = row(vec![Datum::Int32(15)]);
        let expr = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(lit(Datum::Int32(1))),
            high: Box::new(lit(Datum::Int32(10))),
            negated: false,
        };
        assert!(!eval_filter(&expr, &r).unwrap());
    }
}
