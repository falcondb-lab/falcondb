//! # Module Status: STUB (feature-gated, default OFF)
//! Minimal type stubs so `StorageEngine` compiles without `pitr` feature.

/// Stub WAL archiver — not available in v1.0 default build.
#[derive(Debug, Default)]
pub struct WalArchiver;

impl WalArchiver {
    pub fn new() -> Self {
        Self
    }
    pub fn disabled() -> Self {
        Self
    }
}
