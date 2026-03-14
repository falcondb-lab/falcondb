pub mod deadlock;
pub mod lock_manager;
pub mod manager;

#[cfg(test)]
mod tests;

pub use manager::{
    GcSafepointInfo, LatencyStats, PercentileSet, PriorityLatencyStats, SavepointEntry,
    SlaViolationStats, SlowPathMode, TxnClassification, TxnExecSummary, TxnHandle, TxnManager,
    TxnOutcome, TxnRecord, TxnState, TxnStatsSnapshot,
};
pub use lock_manager::{LockMode, LockTableSnapshot, RowKey, TxnLockSet};

// Re-export from falcon_common for convenience
pub use falcon_common::types::{TxnPath, TxnType};
