//! Minimal type stubs so `StorageEngine` compiles without `encryption_tde` feature.

#[derive(Debug, Default)]
pub struct KeyManager;

impl KeyManager {
    pub fn new() -> Self {
        Self
    }
    pub fn disabled() -> Self {
        Self
    }
    pub fn is_enabled(&self) -> bool {
        false
    }
    pub fn dek_count(&self) -> usize {
        0
    }
    pub fn list_deks(&self) -> Vec<DekMeta> {
        vec![]
    }
}

pub type DekId = u64;

#[derive(Debug, Clone, Copy)]
pub enum EncryptionScope {
    Wal,
    Data,
    Backup,
    Table(u64),
    Sst(u64),
}

pub const NONCE_LEN: usize = 12;
pub const GCM_TAG_LEN: usize = 16;

#[derive(Debug, Clone)]
pub struct DekMeta {
    pub id: DekIdWrapper,
    pub label: String,
    pub scope: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy)]
pub struct DekIdWrapper(pub u64);

pub struct EncryptionKey;
impl EncryptionKey {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &[0u8; 32]
    }
}
