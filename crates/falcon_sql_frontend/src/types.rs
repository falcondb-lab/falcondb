use falcon_common::datum::Datum;
use falcon_common::schema::TableSchema;
use falcon_common::types::TableId;

/// A fully bound and resolved statement ready for planning.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    DropTable(BoundDropTable),
    AlterTable(BoundAlterTable),
    Insert(BoundInsert),
    Update(BoundUpdate),
    Delete(BoundDelete),
    Select(BoundSelect),
    Explain(Box<BoundStatement>),
    ExplainAnalyze(Box<BoundStatement>),
    Truncate { table_name: String },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_indices: Vec<usize>,
        unique: bool,
    },
    DropIndex {
        index_name: String,
    },
    CreateView {
        name: String,
        query_sql: String,
        or_replace: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    Begin,
    Commit,
    Rollback,
    ShowTxnStats,
    ShowNodeRole,
    ShowWalStats,
    ShowConnections,
    RunGc,
    Analyze { table_name: String },
    ShowTableStats { table_name: Option<String> },
    CreateSequence { name: String, start: i64 },
    DropSequence { name: String, if_exists: bool },
    ShowSequences,
    /// COPY table FROM STDIN
    CopyFrom {
        table_id: TableId,
        schema: TableSchema,
        columns: Vec<usize>,
        /// true = CSV, false = text
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
    /// COPY table TO STDOUT
    CopyTo {
        table_id: TableId,
        schema: TableSchema,
        columns: Vec<usize>,
        /// true = CSV, false = text
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
    /// COPY (query) TO STDOUT
    CopyQueryTo {
        query: Box<BoundSelect>,
        /// true = CSV, false = text
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
}

#[derive(Debug, Clone)]
pub struct BoundCreateTable {
    pub schema: TableSchema,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct BoundDropTable {
    pub table_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn(falcon_common::schema::ColumnDef),
    DropColumn(String),
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
    AlterColumnType { column_name: String, new_type: falcon_common::types::DataType },
    AlterColumnSetNotNull { column_name: String },
    AlterColumnDropNotNull { column_name: String },
    AlterColumnSetDefault { column_name: String, default_expr: BoundExpr },
    AlterColumnDropDefault { column_name: String },
}

#[derive(Debug, Clone)]
pub struct BoundAlterTable {
    pub table_name: String,
    pub ops: Vec<AlterTableOp>,
}

#[derive(Debug, Clone)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate(Vec<(usize, BoundExpr)>), // (col_idx, expr) assignments
}

#[derive(Debug, Clone)]
pub struct BoundInsert {
    pub table_id: TableId,
    pub table_name: String,
    pub schema: TableSchema,
    pub columns: Vec<usize>, // indices into table schema
    pub rows: Vec<Vec<BoundExpr>>,
    /// INSERT ... SELECT source query (if present, rows will be empty)
    pub source_select: Option<BoundSelect>,
    /// RETURNING clause projections (expression, alias)
    pub returning: Vec<(BoundExpr, String)>,
    /// ON CONFLICT action
    pub on_conflict: Option<OnConflictAction>,
}

/// Info about a secondary table in UPDATE...FROM or DELETE...USING.
#[derive(Debug, Clone)]
pub struct BoundFromTable {
    pub table_id: TableId,
    pub table_name: String,
    pub schema: TableSchema,
    /// Column offset in the combined schema.
    pub col_offset: usize,
}

#[derive(Debug, Clone)]
pub struct BoundUpdate {
    pub table_id: TableId,
    pub table_name: String,
    pub schema: TableSchema,
    pub assignments: Vec<(usize, BoundExpr)>, // (column_idx, new_value_expr)
    pub filter: Option<BoundExpr>,
    /// RETURNING clause projections (expression, alias)
    pub returning: Vec<(BoundExpr, String)>,
    /// UPDATE ... FROM secondary_table WHERE ...
    pub from_table: Option<BoundFromTable>,
}

#[derive(Debug, Clone)]
pub struct BoundDelete {
    pub table_id: TableId,
    pub table_name: String,
    pub schema: TableSchema,
    pub filter: Option<BoundExpr>,
    /// RETURNING clause projections (expression, alias)
    pub returning: Vec<(BoundExpr, String)>,
    /// DELETE ... USING secondary_table WHERE ...
    pub using_table: Option<BoundFromTable>,
}

#[derive(Debug, Clone)]
pub struct BoundCte {
    pub name: String,
    pub table_id: TableId,
    pub select: BoundSelect,
    /// For recursive CTEs: the recursive member (anchor is `select`).
    pub recursive_select: Option<Box<BoundSelect>>,
}

#[derive(Debug, Clone)]
pub struct BoundSelect {
    pub table_id: TableId,
    pub table_name: String,
    pub schema: TableSchema,
    pub projections: Vec<BoundProjection>,
    /// Number of user-visible projections (rest are hidden ORDER BY columns)
    pub visible_projection_count: usize,
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<usize>, // column indices in source table
    /// GROUPING SETS: each inner Vec is one grouping set (column indices).
    /// Empty outer Vec = plain GROUP BY (use `group_by` field).
    /// Non-empty = run aggregation once per set and UNION ALL results.
    pub grouping_sets: Vec<Vec<usize>>,
    pub having: Option<BoundExpr>,
    pub order_by: Vec<BoundOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// SELECT DISTINCT / DISTINCT ON (...)
    pub distinct: DistinctMode,
    /// JOIN clauses (empty = single-table scan)
    pub joins: Vec<BoundJoin>,
    /// CTEs (WITH ... AS)
    pub ctes: Vec<BoundCte>,
    /// Set operations (UNION/INTERSECT/EXCEPT [ALL])
    pub unions: Vec<(BoundSelect, SetOpKind, bool)>,
    /// Inline virtual rows (for GENERATE_SERIES etc.)
    pub virtual_rows: Vec<falcon_common::datum::OwnedRow>,
}

/// How DISTINCT is applied to a SELECT.
#[derive(Debug, Clone, PartialEq)]
pub enum DistinctMode {
    /// No deduplication.
    None,
    /// DISTINCT 鈥?remove fully duplicate rows.
    All,
    /// DISTINCT ON (expr, ...) 鈥?keep first row per distinct-on key group.
    /// The Vec contains projection indices for the DISTINCT ON expressions.
    On(Vec<usize>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone)]
pub struct BoundJoin {
    pub join_type: JoinType,
    pub right_table_id: TableId,
    pub right_table_name: String,
    pub right_schema: TableSchema,
    /// Offset of right table columns in the combined row
    pub right_col_offset: usize,
    pub condition: Option<BoundExpr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

#[derive(Debug, Clone)]
pub enum BoundProjection {
    /// A column reference: (column_index_in_table, alias)
    Column(usize, String),
    /// An aggregate: (function, arg_expr, alias, distinct, filter)
    Aggregate(AggFunc, Option<BoundExpr>, String, bool, Option<Box<BoundExpr>>),
    /// A wildcard (*) 鈥?expanded during binding.
    Expr(BoundExpr, String),
    /// A window function: (function, partition_by_cols, order_by, alias)
    Window(BoundWindowFunc),
}

#[derive(Debug, Clone)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg(String), // separator
    BoolAnd,
    BoolOr,
    ArrayAgg,
    // ── Statistical aggregates (PostgreSQL) ──
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    // ── Two-argument statistical aggregates ──
    Corr,
    CovarPop,
    CovarSamp,
    RegrSlope,
    RegrIntercept,
    RegrR2,
    RegrCount,
    RegrAvgX,
    RegrAvgY,
    RegrSXX,
    RegrSYY,
    RegrSXY,
    // ── Ordered-set aggregates ──
    PercentileCont(f64),   // fraction
    PercentileDisc(f64),   // fraction
    Mode,
    // ── Bit aggregates ──
    BitAndAgg,
    BitOrAgg,
    BitXorAgg,
}

#[derive(Debug, Clone)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Ntile(i64),                   // NTILE(n) 鈥?divide into n buckets
    Lag(usize, i64),              // LAG(col_idx, offset)
    Lead(usize, i64),             // LEAD(col_idx, offset)
    FirstValue(usize),            // FIRST_VALUE(col_idx)
    LastValue(usize),             // LAST_VALUE(col_idx)
    PercentRank,                  // PERCENT_RANK()
    CumeDist,                     // CUME_DIST()
    NthValue(usize, i64),         // NTH_VALUE(col_idx, n)
    Agg(AggFunc, Option<usize>),  // aggregate over window (func, col_idx)
}

/// Window frame boundary specification.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// N PRECEDING
    Preceding(u64),
    /// CURRENT ROW
    CurrentRow,
    /// N FOLLOWING
    Following(u64),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
}

/// Window frame mode.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameMode {
    Rows,
    Range,
    Groups,
}

/// Window frame specification.
#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

impl Default for WindowFrame {
    /// Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    /// (entire partition 鈥?used when no ORDER BY and no explicit frame).
    fn default() -> Self {
        Self {
            mode: WindowFrameMode::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::UnboundedFollowing,
        }
    }
}

impl WindowFrame {
    /// Default frame when ORDER BY is present but no explicit frame:
    /// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    pub fn default_with_order_by() -> Self {
        Self {
            mode: WindowFrameMode::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        }
    }

    /// Returns true if this frame covers the entire partition.
    pub fn is_full_partition(&self) -> bool {
        self.start == WindowFrameBound::UnboundedPreceding
            && self.end == WindowFrameBound::UnboundedFollowing
    }
}

#[derive(Debug, Clone)]
pub struct BoundWindowFunc {
    pub func: WindowFunc,
    pub partition_by: Vec<usize>,       // column indices for PARTITION BY
    pub order_by: Vec<BoundOrderBy>,    // ORDER BY within the window
    pub frame: WindowFrame,             // window frame specification
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum ScalarFunc {
    // 鈹€鈹€ String functions (PG-standard) 鈹€鈹€
    Upper,
    Lower,
    Length,
    Substring,
    Concat,
    ConcatWs,
    Trim,
    Btrim,
    Ltrim,
    Rtrim,
    Replace,
    Position,
    Lpad,
    Rpad,
    Left,
    Right,
    Repeat,
    Reverse,
    Initcap,
    Translate,
    Split,
    Overlay,
    StartsWith,
    EndsWith,
    Chr,
    Ascii,
    QuoteLiteral,
    QuoteIdent,
    QuoteNullable,
    BitLength,
    OctetLength,
    Format,
    SubstrCount,
    Normalize,
    ParseIdent,
    // 鈹€鈹€ Math functions (PG-standard) 鈹€鈹€
    Abs,
    Round,
    Ceil,
    Ceiling,
    Floor,
    Power,
    Sqrt,
    Sign,
    Trunc,
    Ln,
    Log,
    Exp,
    Pi,
    Mod,
    Degrees,
    Radians,
    Greatest,
    Least,
    Cbrt,
    Factorial,
    Gcd,
    Lcm,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Log10,
    Log2,
    Cot,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    WidthBucket,
    Div,
    Scale,
    Isfinite,
    Isinf,
    Isnan,
    TrimScale,
    MinScale,
    TruncPrecision,
    RoundPrecision,
    Clamp,
    // 鈹€鈹€ Date/Time functions (PG-standard) 鈹€鈹€
    Now,
    CurrentDate,
    CurrentTime,
    Extract,
    DateTrunc,
    ToChar,
    Age,
    MakeDate,
    MakeTimestamp,
    ToTimestamp,
    ClockTimestamp,
    StatementTimestamp,
    Timeofday,
    ToDate,
    DateAdd,
    DateSubtract,
    DateDiff,
    EpochFromTimestamp,
    MakeInterval,
    // 鈹€鈹€ Array functions (PG-standard) 鈹€鈹€
    ArrayLength,
    Cardinality,
    ArrayPosition,
    ArrayPositions,
    ArrayAppend,
    ArrayPrepend,
    ArrayRemove,
    ArrayReplace,
    ArrayCat,
    ArrayToString,
    StringToArray,
    ArrayUpper,
    ArrayLower,
    ArrayDims,
    ArrayFill,
    ArrayReverse,
    ArrayDistinct,
    ArraySort,
    ArrayContains,
    ArrayOverlap,
    ArraySlice,
    ArrayNdims,
    Unnest,
    StringToTable,
    ArrayToJson,
    ArrayGenerate,
    ArraySample,
    ArrayShuffle,
    ArrayIntersect,
    ArrayExcept,
    ArrayCompact,
    ArrayFlatten,
    ArrayRepeat,
    ArrayRotate,
    ArrayToSet,
    ArrayZip,
    ArrayMin,
    ArrayMax,
    ArraySum,
    ArrayAvg,
    ArrayEvery,
    ArraySome,
    OverlayArray,
    // 鈹€鈹€ Regex functions (PG-standard) 鈹€鈹€
    RegexpReplace,
    RegexpMatch,
    RegexpCount,
    RegexpSubstr,
    RegexpSplitToArray,
    RegexpInstr,
    RegexpLike,
    RegexpEscape,
    // 鈹€鈹€ Crypto/encoding functions (PG-standard) 鈹€鈹€
    Md5,
    Sha256,
    Encode,
    Decode,
    ToHex,
    Hashtext,
    Crc32,
    // 鈹€鈹€ Bit operations (PG-standard) 鈹€鈹€
    BitXor,
    BitAnd,
    BitOr,
    BitCount,
    BitNot,
    // 鈹€鈹€ Utility functions (PG-standard) 鈹€鈹€
    PgTypeof,
    ToNumber,
    Random,
    GenRandomUuid,
    NumNonnulls,
    NumNulls,
    // 鈹€鈹€ Fuzzy string matching (PG fuzzystrmatch extension) 鈹€鈹€
    Levenshtein,
    Soundex,
    Difference,
    Similarity,
    HammingDistance,
    // JSONB functions
    JsonbBuildObject,
    JsonbBuildArray,
    JsonbTypeof,
    JsonbArrayLength,
    JsonbExtractPath,
    JsonbExtractPathText,
    JsonbObjectKeys,
    JsonbPretty,
    JsonbStripNulls,
    JsonbSetPath,
    ToJsonb,
    JsonbAgg,
    JsonbConcat,
    JsonbDeleteKey,
    JsonbDeletePath,
    JsonbPopulateRecord,
    JsonbArrayElements,
    JsonbArrayElementsText,
    JsonbEach,
    JsonbEachText,
    RowToJson,
    // ── Generated extended functions (pattern-based) ──
    /// ARRAY_MATRIX_{ROW|COLUMN}_{operation}{suffix}(arr, rows, cols, idx)
    ArrayMatrixFunc(String),
    /// STRING_{name}_ENCODE(fields...) / STRING_{name}_DECODE(line, idx)
    StringEncodingFunc { name: String, encode: bool },
}

#[derive(Debug, Clone)]
pub struct BoundOrderBy {
    pub column_idx: usize, // index in output projections
    pub asc: bool,
}

/// Bound expression tree.
#[derive(Debug, Clone)]
pub enum BoundExpr {
    /// A literal value.
    Literal(Datum),
    /// A column reference (column index in the source table).
    ColumnRef(usize),
    /// Binary operation.
    BinaryOp {
        left: Box<BoundExpr>,
        op: BinOp,
        right: Box<BoundExpr>,
    },
    /// Unary NOT.
    Not(Box<BoundExpr>),
    /// IS NULL check.
    IsNull(Box<BoundExpr>),
    /// IS NOT NULL check.
    IsNotNull(Box<BoundExpr>),
    /// IS NOT DISTINCT FROM (NULL-safe equality).
    IsNotDistinctFrom { left: Box<BoundExpr>, right: Box<BoundExpr> },
    /// LIKE / ILIKE pattern match.
    Like { expr: Box<BoundExpr>, pattern: Box<BoundExpr>, negated: bool, case_insensitive: bool },
    /// BETWEEN low AND high.
    Between { expr: Box<BoundExpr>, low: Box<BoundExpr>, high: Box<BoundExpr>, negated: bool },
    /// IN (list).
    InList { expr: Box<BoundExpr>, list: Vec<BoundExpr>, negated: bool },
    /// CAST(expr AS type) 鈥?target stored as string for simplicity.
    Cast { expr: Box<BoundExpr>, target_type: String },
    /// CASE WHEN ... THEN ... ELSE ... END
    Case {
        operand: Option<Box<BoundExpr>>,
        conditions: Vec<BoundExpr>,
        results: Vec<BoundExpr>,
        else_result: Option<Box<BoundExpr>>,
    },
    /// COALESCE(a, b, ...)
    Coalesce(Vec<BoundExpr>),
    /// Scalar function call: UPPER, LOWER, LENGTH, SUBSTRING, CONCAT
    Function { func: ScalarFunc, args: Vec<BoundExpr> },
    /// Scalar subquery 鈥?returns a single value: `(SELECT MAX(id) FROM t)`.
    ScalarSubquery(Box<BoundSelect>),
    /// IN subquery: `expr IN (SELECT col FROM t)`.
    InSubquery { expr: Box<BoundExpr>, subquery: Box<BoundSelect>, negated: bool },
    /// EXISTS subquery: `EXISTS (SELECT 1 FROM t WHERE ...)`.
    Exists { subquery: Box<BoundSelect>, negated: bool },
    /// Inline aggregate in HAVING expressions: evaluated per-group.
    AggregateExpr { func: AggFunc, arg: Option<Box<BoundExpr>>, distinct: bool },
    /// ARRAY literal: ARRAY[1, 2, 3]
    ArrayLiteral(Vec<BoundExpr>),
    /// Array subscript: arr[index] (1-indexed)
    ArrayIndex { array: Box<BoundExpr>, index: Box<BoundExpr> },
    /// Reference to a column in the outer query (correlated subquery).
    /// The usize is the column index in the outer query's schema.
    OuterColumnRef(usize),
    /// nextval('sequence_name')
    SequenceNextval(String),
    /// currval('sequence_name')
    SequenceCurrval(String),
    /// setval('sequence_name', value)
    SequenceSetval(String, i64),
    /// Parameter placeholder: $1, $2, ... (1-indexed).
    /// Used in prepared statements / parameterized queries.
    Parameter(usize),
    /// expr op ANY(array_or_subquery) 鈥?true if comparison holds for any element.
    AnyOp { left: Box<BoundExpr>, compare_op: BinOp, right: Box<BoundExpr> },
    /// expr op ALL(array_or_subquery) 鈥?true if comparison holds for all elements.
    AllOp { left: Box<BoundExpr>, compare_op: BinOp, right: Box<BoundExpr> },
    /// Array slice: arr[lower:upper] (1-indexed, inclusive bounds, PostgreSQL semantics).
    ArraySlice { array: Box<BoundExpr>, lower: Option<Box<BoundExpr>>, upper: Option<Box<BoundExpr>> },
    /// GROUPING(col1, col2, ...) 鈥?returns bitmask indicating which columns are
    /// super-aggregate NULLs in a GROUPING SETS / CUBE / ROLLUP result.
    /// Each usize is a column index in the source table.
    Grouping(Vec<usize>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    StringConcat,
    // JSONB operators
    JsonArrow,       // -> (get object field/array element as jsonb)
    JsonArrowText,   // ->> (get object field/array element as text)
    JsonHashArrow,   // #> (get nested path as jsonb)
    JsonHashArrowText, // #>> (get nested path as text)
    JsonContains,    // @> (left contains right)
    JsonContainedBy, // <@ (left contained by right)
    JsonExists,      // ? (key exists in jsonb)
}
