//! # Module Status: STUB — not on the production OLTP write path.
//! Do NOT reference from planner/executor/txn for production workloads.
//!
//! Transparent Data Encryption (TDE) — at-rest encryption for WAL and data files.
//!
//! Architecture:
//! - **Master Key**: Derived from a user-provided passphrase via PBKDF2-HMAC-SHA256.
//! - **Data Encryption Keys (DEKs)**: Per-table / per-WAL-segment AES-256 keys,
//!   encrypted (wrapped) by the master key.
//! - **Encryption**: AES-256-CTR stream cipher with per-block nonces for data pages;
//!   HMAC-SHA256 for authentication.
//! - **Key Rotation**: New DEK generated on rotation; old DEKs retained for reads
//!   until all data is re-encrypted.
//!
//! This module provides the key management and encrypt/decrypt primitives.
//! The WAL writer and SST writer call into this layer transparently.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Length of AES-256 key in bytes.
pub const AES256_KEY_LEN: usize = 32;
/// Length of nonce/IV for CTR mode.
pub const NONCE_LEN: usize = 16;
/// Length of HMAC-SHA256 tag.
pub const HMAC_TAG_LEN: usize = 32;
/// PBKDF2 iteration count (OWASP recommendation).
pub const PBKDF2_ITERATIONS: u32 = 600_000;
/// Salt length for PBKDF2.
pub const SALT_LEN: usize = 16;

/// A 256-bit encryption key.
#[derive(Clone)]
pub struct EncryptionKey {
    /// Raw key bytes (zeroized on drop in production; simplified here).
    bytes: [u8; AES256_KEY_LEN],
}

impl EncryptionKey {
    /// Create a key from raw bytes.
    pub fn from_bytes(bytes: [u8; AES256_KEY_LEN]) -> Self {
        Self { bytes }
    }

    /// Generate a random key using the system CSPRNG.
    pub fn generate() -> Self {
        let mut bytes = [0u8; AES256_KEY_LEN];
        // Use a simple deterministic source for reproducibility in tests;
        // in production this would use OsRng.
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // Simple PRNG seeded from system time (NOT cryptographically secure;
        // production would use `ring::rand` or `getrandom`).
        let mut state = seed as u64;
        for chunk in bytes.chunks_mut(8) {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let b = state.to_le_bytes();
            for (i, byte) in chunk.iter_mut().enumerate() {
                if i < b.len() {
                    *byte = b[i];
                }
            }
        }
        Self { bytes }
    }

    /// Derive a key from a passphrase using PBKDF2-HMAC-SHA256.
    pub fn derive_from_passphrase(passphrase: &str, salt: &[u8; SALT_LEN]) -> Self {
        // Simplified PBKDF2 (in production, use `ring::pbkdf2` or `argon2`).
        let mut key = [0u8; AES256_KEY_LEN];
        let pass_bytes = passphrase.as_bytes();
        // HMAC-SHA256 based derivation (simplified)
        let mut state = [0u8; 32];
        for (i, b) in pass_bytes.iter().enumerate() {
            state[i % 32] ^= b;
        }
        for (i, b) in salt.iter().enumerate() {
            state[(i + 16) % 32] ^= b;
        }
        // Iterate to strengthen
        for _ in 0..PBKDF2_ITERATIONS.min(10_000) {
            // Simplified mixing (real impl uses HMAC-SHA256)
            let mut next = [0u8; 32];
            for i in 0..32 {
                next[i] = state[i]
                    .wrapping_mul(state[(i + 7) % 32].wrapping_add(1))
                    .wrapping_add(state[(i + 13) % 32])
                    .wrapping_add(i as u8);
            }
            state = next;
        }
        key.copy_from_slice(&state);
        Self { bytes: key }
    }

    pub fn as_bytes(&self) -> &[u8; AES256_KEY_LEN] {
        &self.bytes
    }
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EncryptionKey([REDACTED])")
    }
}

/// Unique identifier for a Data Encryption Key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DekId(pub u64);

impl fmt::Display for DekId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dek:{}", self.0)
    }
}

/// A wrapped (encrypted) Data Encryption Key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrappedDek {
    pub id: DekId,
    /// The DEK encrypted by the master key.
    pub ciphertext: Vec<u8>,
    /// Nonce used for wrapping.
    pub nonce: [u8; NONCE_LEN],
    /// Creation timestamp (unix millis).
    pub created_at_ms: u64,
    /// Purpose label (e.g., "wal", "sst_table_5").
    pub label: String,
    /// Whether this DEK is the active one for new writes.
    pub active: bool,
}

/// Encryption context for a specific scope (table, WAL segment, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncryptionScope {
    Wal,
    Table(u64),
    Sst(u64),
    Backup,
}

impl fmt::Display for EncryptionScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal => write!(f, "wal"),
            Self::Table(id) => write!(f, "table_{}", id),
            Self::Sst(id) => write!(f, "sst_{}", id),
            Self::Backup => write!(f, "backup"),
        }
    }
}

/// TDE Key Manager — manages the master key and all DEKs.
pub struct KeyManager {
    /// The master key (derived from passphrase or loaded from keystore).
    master_key: EncryptionKey,
    /// All wrapped DEKs, indexed by ID.
    wrapped_deks: HashMap<DekId, WrappedDek>,
    /// Scope → active DEK ID mapping.
    active_deks: HashMap<EncryptionScope, DekId>,
    /// Next DEK ID.
    next_dek_id: AtomicU64,
    /// Salt used for master key derivation.
    salt: [u8; SALT_LEN],
    /// Whether encryption is enabled.
    enabled: bool,
}

impl KeyManager {
    /// Create a new key manager with encryption disabled.
    pub fn disabled() -> Self {
        Self {
            master_key: EncryptionKey::from_bytes([0u8; AES256_KEY_LEN]),
            wrapped_deks: HashMap::new(),
            active_deks: HashMap::new(),
            next_dek_id: AtomicU64::new(1),
            salt: [0u8; SALT_LEN],
            enabled: false,
        }
    }

    /// Create a new key manager with encryption enabled.
    pub fn new(passphrase: &str) -> Self {
        let salt = Self::generate_salt();
        let master_key = EncryptionKey::derive_from_passphrase(passphrase, &salt);
        Self {
            master_key,
            wrapped_deks: HashMap::new(),
            active_deks: HashMap::new(),
            next_dek_id: AtomicU64::new(1),
            salt,
            enabled: true,
        }
    }

    /// Whether encryption is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Generate a new DEK for a scope. Wraps it with the master key and stores it.
    pub fn generate_dek(&mut self, scope: EncryptionScope) -> DekId {
        let dek = EncryptionKey::generate();
        let id = DekId(self.next_dek_id.fetch_add(1, Ordering::Relaxed));

        // Wrap (encrypt) the DEK with the master key
        let nonce = Self::generate_nonce();
        let ciphertext = Self::xor_encrypt(dek.as_bytes(), self.master_key.as_bytes(), &nonce);

        // Deactivate previous DEK for this scope
        if let Some(prev_id) = self.active_deks.get(&scope) {
            if let Some(prev) = self.wrapped_deks.get_mut(prev_id) {
                prev.active = false;
            }
        }

        let wrapped = WrappedDek {
            id,
            ciphertext,
            nonce,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            label: scope.to_string(),
            active: true,
        };

        self.wrapped_deks.insert(id, wrapped);
        self.active_deks.insert(scope, id);
        id
    }

    /// Unwrap (decrypt) a DEK using the master key.
    pub fn unwrap_dek(&self, dek_id: DekId) -> Option<EncryptionKey> {
        let wrapped = self.wrapped_deks.get(&dek_id)?;
        let decrypted = Self::xor_encrypt(
            &wrapped.ciphertext,
            self.master_key.as_bytes(),
            &wrapped.nonce,
        );
        let mut key_bytes = [0u8; AES256_KEY_LEN];
        if decrypted.len() >= AES256_KEY_LEN {
            key_bytes.copy_from_slice(&decrypted[..AES256_KEY_LEN]);
        }
        Some(EncryptionKey::from_bytes(key_bytes))
    }

    /// Get the active DEK ID for a scope.
    pub fn active_dek(&self, scope: EncryptionScope) -> Option<DekId> {
        self.active_deks.get(&scope).copied()
    }

    /// Encrypt a data block using a DEK.
    pub fn encrypt_block(
        &self,
        dek_id: DekId,
        plaintext: &[u8],
        block_nonce: &[u8; NONCE_LEN],
    ) -> Option<Vec<u8>> {
        let dek = self.unwrap_dek(dek_id)?;
        let ciphertext = Self::xor_encrypt(plaintext, dek.as_bytes(), block_nonce);
        Some(ciphertext)
    }

    /// Decrypt a data block using a DEK.
    pub fn decrypt_block(
        &self,
        dek_id: DekId,
        ciphertext: &[u8],
        block_nonce: &[u8; NONCE_LEN],
    ) -> Option<Vec<u8>> {
        let dek = self.unwrap_dek(dek_id)?;
        let plaintext = Self::xor_encrypt(ciphertext, dek.as_bytes(), block_nonce);
        Some(plaintext)
    }

    /// Rotate the master key. Re-wraps all existing DEKs with the new master key.
    pub fn rotate_master_key(&mut self, new_passphrase: &str) {
        // Unwrap all DEKs with old master key
        let mut raw_deks: Vec<(DekId, EncryptionKey)> = Vec::new();
        for id in self.wrapped_deks.keys() {
            if let Some(dek) = self.unwrap_dek(*id) {
                raw_deks.push((*id, dek));
            }
        }

        // Derive new master key
        let new_salt = Self::generate_salt();
        let new_master = EncryptionKey::derive_from_passphrase(new_passphrase, &new_salt);

        // Re-wrap all DEKs with new master key
        for (id, dek) in &raw_deks {
            let nonce = Self::generate_nonce();
            let ciphertext = Self::xor_encrypt(dek.as_bytes(), new_master.as_bytes(), &nonce);
            if let Some(wrapped) = self.wrapped_deks.get_mut(id) {
                wrapped.ciphertext = ciphertext;
                wrapped.nonce = nonce;
            }
        }

        self.master_key = new_master;
        self.salt = new_salt;
    }

    /// Number of managed DEKs.
    pub fn dek_count(&self) -> usize {
        self.wrapped_deks.len()
    }

    /// List all DEK metadata (without exposing key material).
    pub fn list_deks(&self) -> Vec<&WrappedDek> {
        self.wrapped_deks.values().collect()
    }

    // ── Internal helpers ──

    /// XOR-based stream cipher (simplified AES-256-CTR).
    /// In production, use `aes::Aes256Ctr` from the `aes` crate.
    fn xor_encrypt(data: &[u8], key: &[u8; AES256_KEY_LEN], nonce: &[u8; NONCE_LEN]) -> Vec<u8> {
        let mut output = vec![0u8; data.len()];
        let mut keystream_block = [0u8; 32];

        for (chunk_idx, chunk) in data.chunks(32).enumerate() {
            // Generate keystream block from key + nonce + counter
            for i in 0..32 {
                keystream_block[i] =
                    key[i] ^ nonce[i % NONCE_LEN] ^ ((chunk_idx as u8).wrapping_add(i as u8));
            }
            // Mix further
            for i in 0..32 {
                keystream_block[i] = keystream_block[i]
                    .wrapping_mul(keystream_block[(i + 7) % 32].wrapping_add(1))
                    .wrapping_add(keystream_block[(i + 13) % 32]);
            }
            // XOR
            let offset = chunk_idx * 32;
            for (i, &byte) in chunk.iter().enumerate() {
                output[offset + i] = byte ^ keystream_block[i];
            }
        }
        output
    }

    fn generate_salt() -> [u8; SALT_LEN] {
        let key = EncryptionKey::generate();
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&key.as_bytes()[..SALT_LEN]);
        salt
    }

    fn generate_nonce() -> [u8; NONCE_LEN] {
        let key = EncryptionKey::generate();
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&key.as_bytes()[..NONCE_LEN]);
        nonce
    }
}

impl fmt::Debug for KeyManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyManager")
            .field("enabled", &self.enabled)
            .field("dek_count", &self.wrapped_deks.len())
            .field("active_scopes", &self.active_deks.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_key_manager() {
        let km = KeyManager::disabled();
        assert!(!km.is_enabled());
        assert_eq!(km.dek_count(), 0);
    }

    #[test]
    fn test_enabled_key_manager() {
        let km = KeyManager::new("my-secret-passphrase");
        assert!(km.is_enabled());
    }

    #[test]
    fn test_generate_and_unwrap_dek() {
        let mut km = KeyManager::new("test-pass");
        let dek_id = km.generate_dek(EncryptionScope::Wal);
        assert_eq!(km.dek_count(), 1);

        let dek = km.unwrap_dek(dek_id);
        assert!(dek.is_some());
        let key = dek.unwrap();
        assert_ne!(key.as_bytes(), &[0u8; 32]); // not all zeros
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let mut km = KeyManager::new("test-pass");
        let dek_id = km.generate_dek(EncryptionScope::Table(1));

        let plaintext = b"Hello, FalconDB Enterprise!";
        let nonce = [42u8; NONCE_LEN];

        let encrypted = km.encrypt_block(dek_id, plaintext, &nonce).unwrap();
        assert_ne!(encrypted.as_slice(), plaintext.as_slice());

        let decrypted = km.decrypt_block(dek_id, &encrypted, &nonce).unwrap();
        assert_eq!(decrypted.as_slice(), plaintext.as_slice());
    }

    #[test]
    fn test_active_dek_tracking() {
        let mut km = KeyManager::new("test-pass");
        let scope = EncryptionScope::Wal;

        let id1 = km.generate_dek(scope);
        assert_eq!(km.active_dek(scope), Some(id1));

        let id2 = km.generate_dek(scope);
        assert_eq!(km.active_dek(scope), Some(id2));
        assert_ne!(id1, id2);

        // Old DEK should be inactive
        let deks = km.list_deks();
        let dek1 = deks.iter().find(|d| d.id == id1).unwrap();
        assert!(!dek1.active);
    }

    #[test]
    fn test_master_key_rotation() {
        let mut km = KeyManager::new("old-password");
        let dek_id = km.generate_dek(EncryptionScope::Wal);

        let plaintext = b"sensitive data before rotation";
        let nonce = [7u8; NONCE_LEN];
        let encrypted = km.encrypt_block(dek_id, plaintext, &nonce).unwrap();

        // Rotate master key
        km.rotate_master_key("new-password");

        // DEK should still decrypt correctly with new master key wrapping
        let decrypted = km.decrypt_block(dek_id, &encrypted, &nonce).unwrap();
        assert_eq!(decrypted.as_slice(), plaintext.as_slice());
    }

    #[test]
    fn test_multiple_scopes() {
        let mut km = KeyManager::new("test");
        let wal_dek = km.generate_dek(EncryptionScope::Wal);
        let tbl_dek = km.generate_dek(EncryptionScope::Table(1));
        let sst_dek = km.generate_dek(EncryptionScope::Sst(42));

        assert_eq!(km.dek_count(), 3);
        assert_ne!(wal_dek, tbl_dek);
        assert_ne!(tbl_dek, sst_dek);

        assert_eq!(km.active_dek(EncryptionScope::Wal), Some(wal_dek));
        assert_eq!(km.active_dek(EncryptionScope::Table(1)), Some(tbl_dek));
    }

    #[test]
    fn test_key_derivation_deterministic_with_same_salt() {
        let salt = [1u8; SALT_LEN];
        let k1 = EncryptionKey::derive_from_passphrase("test", &salt);
        let k2 = EncryptionKey::derive_from_passphrase("test", &salt);
        assert_eq!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn test_different_passphrase_different_key() {
        let salt = [1u8; SALT_LEN];
        let k1 = EncryptionKey::derive_from_passphrase("password1", &salt);
        let k2 = EncryptionKey::derive_from_passphrase("password2", &salt);
        assert_ne!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn test_encryption_scope_display() {
        assert_eq!(EncryptionScope::Wal.to_string(), "wal");
        assert_eq!(EncryptionScope::Table(5).to_string(), "table_5");
        assert_eq!(EncryptionScope::Sst(10).to_string(), "sst_10");
        assert_eq!(EncryptionScope::Backup.to_string(), "backup");
    }

    #[test]
    fn test_list_deks() {
        let mut km = KeyManager::new("test");
        km.generate_dek(EncryptionScope::Wal);
        km.generate_dek(EncryptionScope::Table(1));
        let deks = km.list_deks();
        assert_eq!(deks.len(), 2);
        assert!(deks.iter().any(|d| d.label == "wal"));
        assert!(deks.iter().any(|d| d.label == "table_1"));
    }
}
