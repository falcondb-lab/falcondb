//! AI-driven query optimizer for FalconDB.
//!
//! Implements a lightweight, self-contained learned cost model that sits
//! on top of the existing rule-based optimizer. The AI layer:
//!
//! 1. **Extracts features** from `LogicalPlan` + table statistics into a
//!    fixed-length numeric vector.
//! 2. **Predicts execution cost** using an online-learning linear model
//!    (stochastic gradient descent with L2 regularization).
//! 3. **Selects the best plan** among multiple optimizer-generated candidates
//!    (e.g., SeqScan vs IndexScan, NL-join vs Hash-join).
//! 4. **Collects execution feedback** to improve the model over time.
//!
//! The model is purely in-process and does not require external services.
//! Weights are stored in an `Arc<Mutex<...>>` and can be persisted via
//! `save`/`load` for cross-restart learning.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use falcon_common::types::TableId;

use crate::cost::{IndexedColumns, TableStatsMap};
use crate::logical_plan::LogicalPlan;
use crate::plan::PhysicalPlan;

// ── Feature vector layout ────────────────────────────────────────────────────
//
// Index  Feature
// -----  -------
//  0     log2(estimated output rows + 1)
//  1     join count
//  2     filter predicate count
//  3     has GROUP BY (0/1)
//  4     has ORDER BY (0/1)
//  5     has LIMIT (0/1)
//  6     index scan available (0/1)
//  7     log2(largest table rows + 1)
//  8     log2(second-largest table rows + 1)
//  9     cross-table selectivity estimate (0..1)
// 10     projection column count
// 11     has aggregation (0/1)
// 12     has distinct (0/1)
// 13     subquery depth
// 14     log2(total bytes estimate + 1)

pub const FEATURE_DIM: usize = 15;

pub type FeatureVec = [f64; FEATURE_DIM];

// ── Plan type enumeration (for candidate labeling) ───────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlanKind {
    SeqScan,
    IndexScan,
    IndexRangeScan,
    NestedLoopJoin,
    HashJoin,
    MergeSortJoin,
    IndexNestedLoopJoin,
    Other,
}

impl PlanKind {
    pub fn from_physical(plan: &PhysicalPlan) -> Self {
        match plan {
            PhysicalPlan::SeqScan { .. } => PlanKind::SeqScan,
            PhysicalPlan::IndexScan { .. } => PlanKind::IndexScan,
            PhysicalPlan::IndexRangeScan { .. } => PlanKind::IndexRangeScan,
            PhysicalPlan::NestedLoopJoin { .. } => PlanKind::NestedLoopJoin,
            PhysicalPlan::HashJoin { .. } => PlanKind::HashJoin,
            PhysicalPlan::MergeSortJoin { .. } => PlanKind::MergeSortJoin,
            PhysicalPlan::IndexNestedLoopJoin { .. } => PlanKind::IndexNestedLoopJoin,
            _ => PlanKind::Other,
        }
    }

    /// One-hot offset in the feature dimension extension for plan-kind bias.
    fn bias_idx(self) -> usize {
        match self {
            PlanKind::SeqScan => 0,
            PlanKind::IndexScan => 1,
            PlanKind::IndexRangeScan => 2,
            PlanKind::NestedLoopJoin => 3,
            PlanKind::HashJoin => 4,
            PlanKind::MergeSortJoin => 5,
            PlanKind::IndexNestedLoopJoin => 6,
            PlanKind::Other => 7,
        }
    }
}

// ── Feature extraction ───────────────────────────────────────────────────────

/// Context for feature extraction.
pub struct FeatureContext<'a> {
    pub stats: &'a TableStatsMap,
    pub indexes: &'a IndexedColumns,
}

/// Extract a feature vector from a logical plan + stats.
pub fn extract_features(plan: &LogicalPlan, ctx: &FeatureContext<'_>) -> FeatureVec {
    let mut f = [0.0f64; FEATURE_DIM];
    let mut state = ExtractionState::default();
    walk_plan(plan, &mut state, ctx);

    // Estimated output rows
    f[0] = (state.estimated_rows as f64 + 1.0).log2();
    // Join count
    f[1] = state.join_count as f64;
    // Filter predicate count
    f[2] = state.filter_count as f64;
    // Has GROUP BY
    f[3] = if state.has_group_by { 1.0 } else { 0.0 };
    // Has ORDER BY
    f[4] = if state.has_order_by { 1.0 } else { 0.0 };
    // Has LIMIT
    f[5] = if state.has_limit { 1.0 } else { 0.0 };
    // Index available on any scanned table
    f[6] = if state.index_available { 1.0 } else { 0.0 };
    // Largest table
    let mut sorted_rows: Vec<u64> = state.table_rows.values().copied().collect();
    sorted_rows.sort_unstable_by(|a, b| b.cmp(a));
    f[7] = (sorted_rows.first().copied().unwrap_or(0) as f64 + 1.0).log2();
    f[8] = (sorted_rows.get(1).copied().unwrap_or(0) as f64 + 1.0).log2();
    // Cross-table selectivity
    f[9] = state.selectivity.clamp(0.0, 1.0);
    // Projection columns
    f[10] = state.projection_count as f64;
    // Has aggregation
    f[11] = if state.has_agg { 1.0 } else { 0.0 };
    // Has distinct
    f[12] = if state.has_distinct { 1.0 } else { 0.0 };
    // Subquery depth
    f[13] = state.subquery_depth as f64;
    // Total bytes estimate (avg 100 bytes/row)
    let total_bytes: u64 = state.table_rows.values().sum::<u64>() * 100;
    f[14] = (total_bytes as f64 + 1.0).log2();

    f
}

#[derive(Default)]
struct ExtractionState {
    estimated_rows: u64,
    join_count: usize,
    filter_count: usize,
    has_group_by: bool,
    has_order_by: bool,
    has_limit: bool,
    index_available: bool,
    table_rows: HashMap<TableId, u64>,
    selectivity: f64,
    projection_count: usize,
    has_agg: bool,
    has_distinct: bool,
    subquery_depth: usize,
}

fn walk_plan(plan: &LogicalPlan, s: &mut ExtractionState, ctx: &FeatureContext<'_>) {
    match plan {
        LogicalPlan::Scan {
            table_id, schema, ..
        } => {
            let rows = ctx.stats.get(table_id).map_or(1000, |st| st.row_count);
            s.table_rows.insert(*table_id, rows);
            s.estimated_rows = s.estimated_rows.max(rows);
            if ctx.indexes.contains_key(table_id) {
                s.index_available = true;
            }
            s.projection_count = s.projection_count.max(schema.columns.len());
        }
        LogicalPlan::Filter { input, predicate } => {
            s.filter_count += count_predicates(predicate);
            s.selectivity *= crate::cost::estimate_selectivity(predicate, None);
            walk_plan(input, s, ctx);
        }
        LogicalPlan::Project {
            input, projections, ..
        } => {
            s.projection_count = s.projection_count.max(projections.len());
            walk_plan(input, s, ctx);
        }
        LogicalPlan::Aggregate {
            input, group_by, ..
        } => {
            s.has_agg = true;
            s.has_group_by = !group_by.is_empty();
            walk_plan(input, s, ctx);
        }
        LogicalPlan::Sort { input, order_by } => {
            s.has_order_by = !order_by.is_empty();
            walk_plan(input, s, ctx);
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset: _,
        } => {
            s.has_limit = limit.is_some();
            walk_plan(input, s, ctx);
        }
        LogicalPlan::Distinct { input, .. } => {
            s.has_distinct = true;
            walk_plan(input, s, ctx);
        }
        LogicalPlan::Join { left, right, .. } => {
            s.join_count += 1;
            walk_plan(left, s, ctx);
            walk_plan(right, s, ctx);
        }
        LogicalPlan::MultiJoin { base, joins } => {
            s.join_count += joins.len();
            walk_plan(base, s, ctx);
        }
        LogicalPlan::SetOp { left, right, .. } => {
            walk_plan(left, s, ctx);
            walk_plan(right, s, ctx);
        }
        LogicalPlan::WithCtes { input, .. } => {
            s.subquery_depth += 1;
            walk_plan(input, s, ctx);
        }
        // DML / DDL leaf nodes — no sub-plan to walk
        _ => {}
    }

    // Default selectivity 1.0 if not set
    if s.selectivity == 0.0 {
        s.selectivity = 1.0;
    }
}

fn count_predicates(expr: &falcon_sql_frontend::types::BoundExpr) -> usize {
    use falcon_sql_frontend::types::BoundExpr;
    match expr {
        BoundExpr::BinaryOp { left, op, right } => {
            use falcon_sql_frontend::types::BinOp;
            match op {
                BinOp::And | BinOp::Or => count_predicates(left) + count_predicates(right),
                _ => 1,
            }
        }
        BoundExpr::Not(inner) => count_predicates(inner),
        _ => 1,
    }
}

// ── Online linear cost model ─────────────────────────────────────────────────
//
// Predicts log2(execution_time_us) from feature vector using a linear model
// with per-plan-kind bias weights. Model is updated via SGD on each feedback.

/// Extended feature dimension: base features + plan-kind one-hot (7) + bias.
const MODEL_DIM: usize = FEATURE_DIM + 7 + 1;

#[derive(Debug, Clone)]
pub struct CostModelWeights {
    /// Weight vector w ∈ ℝ^MODEL_DIM
    pub w: Vec<f64>,
    /// Number of training samples seen
    pub n_samples: u64,
    /// Exponentially-weighted mean absolute error (for monitoring)
    pub ema_mae: f64,
}

impl Default for CostModelWeights {
    fn default() -> Self {
        // Initialize with small positive weights biased toward row count (f[0])
        let mut w = vec![0.0f64; MODEL_DIM];
        w[0] = 1.0; // row count has strongest signal
        w[1] = 0.5; // joins are expensive
                    // Bias term (last weight) = 10 (reasonable initial log2 cost ~2^10 = 1ms)
        w[MODEL_DIM - 1] = 10.0;
        Self {
            w,
            n_samples: 0,
            ema_mae: 0.0,
        }
    }
}

impl CostModelWeights {
    pub fn predict(&self, features: &FeatureVec, kind: PlanKind) -> f64 {
        let x = Self::build_input(features, kind);
        x.iter().zip(self.w.iter()).map(|(xi, wi)| xi * wi).sum()
    }

    /// SGD update: given actual log2(exec_time_us), update weights.
    pub fn update(&mut self, features: &FeatureVec, kind: PlanKind, actual_log2_cost: f64) {
        const LR: f64 = 1e-4;
        const L2: f64 = 1e-5;
        const GRAD_CLIP: f64 = 1.0;

        let x = Self::build_input(features, kind);
        let pred = x
            .iter()
            .zip(self.w.iter())
            .map(|(xi, wi)| xi * wi)
            .sum::<f64>();
        let err = (pred - actual_log2_cost).clamp(-GRAD_CLIP, GRAD_CLIP);

        // w_i ← w_i - lr * (err * x_i + L2 * w_i)
        for (wi, xi) in self.w.iter_mut().zip(x.iter()) {
            *wi -= LR * (err * xi + L2 * *wi);
        }
        self.n_samples += 1;

        let alpha = 0.05;
        self.ema_mae = (1.0 - alpha) * self.ema_mae + alpha * err.abs();
    }

    fn build_input(features: &FeatureVec, kind: PlanKind) -> Vec<f64> {
        let mut x = Vec::with_capacity(MODEL_DIM);
        x.extend_from_slice(features);
        // One-hot for plan kind
        for i in 0..7 {
            x.push(if i == kind.bias_idx() { 1.0 } else { 0.0 });
        }
        // Bias term
        x.push(1.0);
        x
    }
}

// ── Query fingerprint ────────────────────────────────────────────────────────

/// A compact identifier for a query shape, used to group feedback records.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryFingerprint {
    /// Number of tables in the query
    pub table_count: usize,
    /// Number of join conditions
    pub join_count: usize,
    /// Whether filter predicates are present
    pub has_filter: bool,
    /// Whether GROUP BY is present
    pub has_group_by: bool,
}

impl QueryFingerprint {
    pub fn from_features(f: &FeatureVec) -> Self {
        Self {
            table_count: (f[7] as usize).max(1),
            join_count: f[1] as usize,
            has_filter: f[2] > 0.0,
            has_group_by: f[3] > 0.0,
        }
    }
}

// ── Execution feedback ───────────────────────────────────────────────────────

/// A single execution feedback record.
#[derive(Debug, Clone)]
pub struct FeedbackRecord {
    pub features: FeatureVec,
    pub plan_kind: PlanKind,
    /// Actual execution time in microseconds.
    pub actual_us: u64,
    /// Estimated rows (from model prediction at plan time)
    pub predicted_log2_cost: f64,
}

// ── AI Optimizer ─────────────────────────────────────────────────────────────

/// The main AI optimizer. Wraps a shared cost model and provides plan selection
/// and feedback APIs. Cheaply cloneable (Arc-wrapped internals).
#[derive(Clone)]
pub struct AiOptimizer {
    inner: Arc<Mutex<AiOptimizerInner>>,
}

struct AiOptimizerInner {
    weights: CostModelWeights,
    /// Per-fingerprint execution history: (sum_us, count) for fallback stats.
    history: HashMap<QueryFingerprint, (u64, u64)>,
    /// Minimum samples before trusting model predictions.
    min_samples: u64,
    /// Whether AI plan selection is currently enabled.
    enabled: bool,
}

impl Default for AiOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl AiOptimizer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AiOptimizerInner {
                weights: CostModelWeights::default(),
                history: HashMap::new(),
                min_samples: 50,
                enabled: true,
            })),
        }
    }

    /// Disable AI plan selection (fall back to rule-based only).
    pub fn set_enabled(&self, enabled: bool) {
        self.inner.lock().unwrap().enabled = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.lock().unwrap().enabled
    }

    /// Number of training samples seen so far.
    pub fn sample_count(&self) -> u64 {
        self.inner.lock().unwrap().weights.n_samples
    }

    /// Current exponentially-weighted MAE (in log2 cost units).
    pub fn ema_mae(&self) -> f64 {
        self.inner.lock().unwrap().weights.ema_mae
    }

    /// Select the best plan from a list of candidates.
    ///
    /// Returns the index of the selected candidate and the predicted cost.
    /// Falls back to returning index 0 if AI is disabled or insufficiently trained.
    pub fn select_plan(&self, candidates: &[(PhysicalPlan, FeatureVec)]) -> (usize, f64) {
        if candidates.is_empty() {
            return (0, f64::MAX);
        }
        if candidates.len() == 1 {
            let (plan, features) = &candidates[0];
            let kind = PlanKind::from_physical(plan);
            let cost = self.inner.lock().unwrap().weights.predict(features, kind);
            return (0, cost);
        }

        let inner = self.inner.lock().unwrap();
        if !inner.enabled || inner.weights.n_samples < inner.min_samples {
            // Not enough training data — use rule-based default (index 0)
            let (plan, features) = &candidates[0];
            let kind = PlanKind::from_physical(plan);
            let cost = inner.weights.predict(features, kind);
            return (0, cost);
        }

        let mut best_idx = 0;
        let mut best_cost = f64::MAX;
        for (i, (plan, features)) in candidates.iter().enumerate() {
            let kind = PlanKind::from_physical(plan);
            let cost = inner.weights.predict(features, kind);
            if cost < best_cost {
                best_cost = cost;
                best_idx = i;
            }
        }
        (best_idx, best_cost)
    }

    /// Record actual execution feedback and update model weights.
    pub fn record_feedback(&self, record: FeedbackRecord) {
        let actual_log2 = (record.actual_us as f64 + 1.0).log2();
        let fp = QueryFingerprint::from_features(&record.features);

        let mut inner = self.inner.lock().unwrap();

        // Update per-fingerprint history
        let entry = inner.history.entry(fp).or_insert((0, 0));
        entry.0 += record.actual_us;
        entry.1 += 1;

        // SGD update
        inner
            .weights
            .update(&record.features, record.plan_kind, actual_log2);
    }

    /// Predict cost for a single (features, plan_kind) pair.
    pub fn predict_cost(&self, features: &FeatureVec, kind: PlanKind) -> f64 {
        self.inner.lock().unwrap().weights.predict(features, kind)
    }

    /// Export current model weights for persistence.
    pub fn export_weights(&self) -> CostModelWeights {
        self.inner.lock().unwrap().weights.clone()
    }

    /// Import model weights (e.g., loaded from disk).
    pub fn import_weights(&self, weights: CostModelWeights) {
        self.inner.lock().unwrap().weights = weights;
    }

    /// Average execution time for a given query fingerprint (microseconds).
    /// Returns `None` if no history available.
    pub fn avg_exec_us(&self, fp: &QueryFingerprint) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.history.get(fp).map(|(sum, n)| sum / (*n).max(1))
    }

    /// Diagnostic summary for EXPLAIN output.
    pub fn diagnostics(&self) -> AiOptimizerDiagnostics {
        let inner = self.inner.lock().unwrap();
        AiOptimizerDiagnostics {
            enabled: inner.enabled,
            n_samples: inner.weights.n_samples,
            ema_mae: inner.weights.ema_mae,
            n_fingerprints: inner.history.len(),
            model_ready: inner.weights.n_samples >= inner.min_samples,
        }
    }
}

/// Summary of the AI optimizer state for EXPLAIN / monitoring.
#[derive(Debug, Clone)]
pub struct AiOptimizerDiagnostics {
    pub enabled: bool,
    pub n_samples: u64,
    pub ema_mae: f64,
    pub n_fingerprints: usize,
    pub model_ready: bool,
}

impl std::fmt::Display for AiOptimizerDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AI Optimizer: {} | samples={} | ema_mae={:.3} | fingerprints={} | ready={}",
            if self.enabled { "ON" } else { "OFF" },
            self.n_samples,
            self.ema_mae,
            self.n_fingerprints,
            self.model_ready,
        )
    }
}

// ── Plan candidate generation ─────────────────────────────────────────────────
//
// Given a logical plan + context, generate alternative physical plan candidates
// that the AI can score. Currently produces up to 2 candidates for SELECT:
//   1. The rule-optimized plan (default)
//   2. A forced SeqScan (useful when indexes have stale statistics)

/// Generate candidate physical plans for AI selection.
/// Returns a `Vec<(PhysicalPlan, FeatureVec)>` with the rule-based default first (index 0).
///
/// When an IndexScan is the default, a SeqScan fallback is added as candidate 1.
/// When a SeqScan is the default but indexes exist on the scanned table, an
/// IndexScan probe is NOT synthesized (we can't invent predicates), but the
/// feature vector records index availability so the model learns the tradeoff.
pub fn generate_candidates(
    default_plan: PhysicalPlan,
    features: FeatureVec,
    _stats: &TableStatsMap,
    _indexes: &IndexedColumns,
) -> Vec<(PhysicalPlan, FeatureVec)> {
    let mut candidates = Vec::with_capacity(2);

    // If the default is an IndexScan, also offer a SeqScan alternative
    match &default_plan {
        PhysicalPlan::IndexScan {
            table_id,
            schema,
            projections,
            visible_projection_count,
            filter,
            group_by,
            grouping_sets,
            having,
            order_by,
            limit,
            offset,
            distinct,
            ctes,
            unions,
            virtual_rows,
            for_lock,
            index_value: _,
            index_col: _,
        } => {
            // Candidate 0: the IndexScan (default)
            candidates.push((default_plan.clone(), features));
            // Candidate 1: SeqScan with the same filter (no index)
            let mut seq_features = features;
            seq_features[6] = 0.0; // mark as no-index path
            let seq = PhysicalPlan::SeqScan {
                table_id: *table_id,
                schema: schema.clone(),
                projections: projections.clone(),
                visible_projection_count: *visible_projection_count,
                filter: filter.clone(),
                group_by: group_by.clone(),
                grouping_sets: grouping_sets.clone(),
                having: having.clone(),
                order_by: order_by.clone(),
                limit: *limit,
                offset: *offset,
                distinct: distinct.clone(),
                ctes: ctes.clone(),
                unions: unions.clone(),
                virtual_rows: virtual_rows.clone(),
                for_lock: *for_lock,
            };
            candidates.push((seq, seq_features));
        }
        PhysicalPlan::IndexRangeScan {
            table_id,
            schema,
            projections,
            visible_projection_count,
            filter,
            group_by,
            grouping_sets,
            having,
            order_by,
            limit,
            offset,
            distinct,
            ctes,
            unions,
            virtual_rows,
            for_lock,
            ..
        } => {
            candidates.push((default_plan.clone(), features));
            let mut seq_features = features;
            seq_features[6] = 0.0;
            let seq = PhysicalPlan::SeqScan {
                table_id: *table_id,
                schema: schema.clone(),
                projections: projections.clone(),
                visible_projection_count: *visible_projection_count,
                filter: filter.clone(),
                group_by: group_by.clone(),
                grouping_sets: grouping_sets.clone(),
                having: having.clone(),
                order_by: order_by.clone(),
                limit: *limit,
                offset: *offset,
                distinct: distinct.clone(),
                ctes: ctes.clone(),
                unions: unions.clone(),
                virtual_rows: virtual_rows.clone(),
                for_lock: *for_lock,
            };
            candidates.push((seq, seq_features));
        }
        _ => {
            candidates.push((default_plan, features));
        }
    }

    candidates
}

/// Extract approximate features from a PhysicalPlan (for feedback recording).
/// Less precise than `extract_features` (no LogicalPlan/stats), but captures
/// the plan shape so the model can associate execution times with plan kinds.
pub fn extract_features_from_physical(plan: &PhysicalPlan) -> FeatureVec {
    let mut f = [0.0f64; FEATURE_DIM];
    match plan {
        PhysicalPlan::SeqScan {
            schema,
            filter,
            group_by,
            order_by,
            limit,
            distinct,
            projections,
            ..
        }
        | PhysicalPlan::IndexScan {
            schema,
            filter,
            group_by,
            order_by,
            limit,
            distinct,
            projections,
            ..
        }
        | PhysicalPlan::IndexRangeScan {
            schema,
            filter,
            group_by,
            order_by,
            limit,
            distinct,
            projections,
            ..
        } => {
            let cols = schema.columns.len() as f64;
            f[0] = (cols * 100.0 + 1.0).log2(); // rough row estimate
            f[2] = if filter.is_some() { 1.0 } else { 0.0 };
            f[3] = if !group_by.is_empty() { 1.0 } else { 0.0 };
            f[4] = if !order_by.is_empty() { 1.0 } else { 0.0 };
            f[5] = if limit.is_some() { 1.0 } else { 0.0 };
            f[6] = if matches!(
                plan,
                PhysicalPlan::IndexScan { .. } | PhysicalPlan::IndexRangeScan { .. }
            ) {
                1.0
            } else {
                0.0
            };
            f[10] = projections.len() as f64;
            f[12] = if !matches!(
                distinct,
                falcon_sql_frontend::types::DistinctMode::None
            ) {
                1.0
            } else {
                0.0
            };
        }
        PhysicalPlan::HashJoin { joins, .. }
        | PhysicalPlan::NestedLoopJoin { joins, .. }
        | PhysicalPlan::MergeSortJoin { joins, .. }
        | PhysicalPlan::IndexNestedLoopJoin { joins, .. } => {
            f[1] = joins.len() as f64;
        }
        _ => {}
    }
    f
}

// ── Serialization support ────────────────────────────────────────────────────

impl CostModelWeights {
    /// Serialize weights to a compact binary format (little-endian f64 array).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.w.len() * 8 + 16);
        out.extend_from_slice(&(self.w.len() as u64).to_le_bytes());
        for &wi in &self.w {
            out.extend_from_slice(&wi.to_le_bytes());
        }
        out.extend_from_slice(&self.n_samples.to_le_bytes());
        out.extend_from_slice(&self.ema_mae.to_le_bytes());
        out
    }

    /// Deserialize from bytes produced by `to_bytes`. Returns `None` on error.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let len = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let required = 8 + len * 8 + 16;
        if data.len() < required {
            return None;
        }
        let mut w = Vec::with_capacity(len);
        for i in 0..len {
            let off = 8 + i * 8;
            w.push(f64::from_le_bytes(data[off..off + 8].try_into().ok()?));
        }
        let n_off = 8 + len * 8;
        let n_samples = u64::from_le_bytes(data[n_off..n_off + 8].try_into().ok()?);
        let mae_off = n_off + 8;
        let ema_mae = f64::from_le_bytes(data[mae_off..mae_off + 8].try_into().ok()?);
        Some(Self {
            w,
            n_samples,
            ema_mae,
        })
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::LogicalPlan;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn simple_schema(tid: u64) -> TableSchema {
        TableSchema {
            id: TableId(tid),
            name: format!("t{}", tid),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
                max_length: None,
            }],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    fn scan_plan(tid: u64) -> LogicalPlan {
        LogicalPlan::Scan {
            table_id: TableId(tid),
            schema: simple_schema(tid),
            virtual_rows: vec![],
        }
    }

    #[test]
    fn test_feature_extraction_scan() {
        let plan = scan_plan(1);
        let mut stats = TableStatsMap::new();
        stats.insert(
            TableId(1),
            crate::cost::TableStatsInfo {
                table_id: TableId(1),
                row_count: 10_000,
                columns: vec![],
            },
        );
        let indexes = IndexedColumns::new();
        let ctx = FeatureContext {
            stats: &stats,
            indexes: &indexes,
        };
        let f = extract_features(&plan, &ctx);
        // f[7] = log2(10001) ≈ 13.3
        assert!(f[7] > 13.0 && f[7] < 14.0, "f[7]={}", f[7]);
        // No joins
        assert_eq!(f[1], 0.0);
        // No index
        assert_eq!(f[6], 0.0);
    }

    #[test]
    fn test_feature_extraction_with_index() {
        let plan = scan_plan(2);
        let stats = TableStatsMap::new();
        let mut indexes = IndexedColumns::new();
        indexes.insert(TableId(2), vec![0]);
        let ctx = FeatureContext {
            stats: &stats,
            indexes: &indexes,
        };
        let f = extract_features(&plan, &ctx);
        assert_eq!(f[6], 1.0, "should detect index");
    }

    #[test]
    fn test_feature_extraction_join() {
        let left = scan_plan(1);
        let right = scan_plan(2);
        let plan = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_info: falcon_sql_frontend::types::BoundJoin {
                right_table_id: TableId(2),
                right_table_name: "t2".into(),
                right_schema: simple_schema(2),
                join_type: falcon_sql_frontend::types::JoinType::Inner,
                condition: None,
                right_col_offset: 1,
            },
        };
        let stats = TableStatsMap::new();
        let indexes = IndexedColumns::new();
        let ctx = FeatureContext {
            stats: &stats,
            indexes: &indexes,
        };
        let f = extract_features(&plan, &ctx);
        assert_eq!(f[1], 1.0, "should count 1 join");
    }

    #[test]
    fn test_model_predict_and_update() {
        let mut w = CostModelWeights::default();
        // Use small, normalized features to avoid gradient issues
        let features = [
            1.0, 0.5, 0.2, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.5, 0.3, 1.0, 0.0, 0.0, 1.0,
        ];
        let kind = PlanKind::HashJoin;
        let target = 15.0f64;
        let initial_pred = w.predict(&features, kind);
        // After many updates the model should move toward target
        for _ in 0..5000 {
            w.update(&features, kind, target);
        }
        let final_pred = w.predict(&features, kind);
        let initial_err = (initial_pred - target).abs();
        let final_err = (final_pred - target).abs();
        assert!(
            final_err < initial_err,
            "model should move toward target: initial_err={:.2} final_err={:.2}",
            initial_err,
            final_err,
        );
    }

    #[test]
    fn test_optimizer_select_plan_single_candidate() {
        let opt = AiOptimizer::new();
        let schema = simple_schema(1);
        let plan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema,
            projections: vec![],
            visible_projection_count: 0,
            filter: None,
            group_by: vec![],
            grouping_sets: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            distinct: falcon_sql_frontend::types::DistinctMode::None,
            ctes: vec![],
            unions: vec![],
            virtual_rows: vec![],
            for_lock: falcon_sql_frontend::types::RowLockMode::None,
        };
        let features = [0.0f64; FEATURE_DIM];
        let (idx, _cost) = opt.select_plan(&[(plan, features)]);
        assert_eq!(idx, 0);
    }

    #[test]
    fn test_optimizer_feedback_updates_sample_count() {
        let opt = AiOptimizer::new();
        let features = [0.0f64; FEATURE_DIM];
        opt.record_feedback(FeedbackRecord {
            features,
            plan_kind: PlanKind::SeqScan,
            actual_us: 5000,
            predicted_log2_cost: 12.0,
        });
        assert_eq!(opt.sample_count(), 1);
    }

    #[test]
    fn test_diagnostics_display() {
        let opt = AiOptimizer::new();
        let diag = opt.diagnostics();
        let s = diag.to_string();
        assert!(s.contains("AI Optimizer"), "got: {}", s);
        assert!(s.contains("samples=0"));
    }

    #[test]
    fn test_weight_serialization_roundtrip() {
        let mut w = CostModelWeights::default();
        w.n_samples = 42;
        w.ema_mae = 1.23;
        w.w[0] = 3.14;
        let bytes = w.to_bytes();
        let w2 = CostModelWeights::from_bytes(&bytes).expect("deserialization failed");
        assert_eq!(w2.n_samples, 42);
        assert!((w2.ema_mae - 1.23).abs() < 1e-10);
        assert!((w2.w[0] - 3.14).abs() < 1e-10);
    }

    #[test]
    fn test_query_fingerprint_from_features() {
        let mut f = [0.0f64; FEATURE_DIM];
        f[1] = 2.0; // 2 joins
        f[2] = 3.0; // 3 filters
        f[3] = 1.0; // has group by
        let fp = QueryFingerprint::from_features(&f);
        assert_eq!(fp.join_count, 2);
        assert!(fp.has_filter);
        assert!(fp.has_group_by);
    }
}
