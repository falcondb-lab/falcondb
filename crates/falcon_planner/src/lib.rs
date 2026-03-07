pub mod ai_optimizer;
pub mod cost;
pub mod logical_plan;
pub mod optimizer;
pub mod plan;
pub mod planner;
#[cfg(test)]
mod tests;

pub use ai_optimizer::{
    AiOptimizer, AiOptimizerDiagnostics, FeatureVec, FeedbackRecord, PlanKind, QueryFingerprint,
    FEATURE_DIM,
};
pub use cost::{
    estimate_selectivity, index_scan_cost, prefer_index_scan, seq_scan_cost, ColumnStatsInfo,
    IndexedColumns, TableRowCounts, TableStatsInfo, TableStatsMap,
};
pub use plan::*;
pub use planner::{global_ai_optimizer, Planner};
