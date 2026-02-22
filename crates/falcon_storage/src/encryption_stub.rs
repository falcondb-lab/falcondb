//! # Module Status: STUB (feature-gated, default OFF)
//! Minimal type stubs so `StorageEngine` compiles without `encryption_tde` feature.

/// Stub key manager — not available in v1.0 default build.
#[derive(Debug, Default)]
pub struct KeyManager;

impl KeyManager {
    pub fn new() -> Self {
        Self
    }
    pub fn disabled() -> Self {
        Self
    }
}
