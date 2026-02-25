//! Transparent Data Encryption (TDE) — at-rest encryption for WAL and data files.
//!
//! Architecture:
//! - **Master Key**: Derived from a user-provided passphrase via PBKDF2-HMAC-SHA256
//!   (600 000 iterations, 16-byte random salt).
//! - **Data Encryption Keys (DEKs)**: Per-scope AES-256 keys, wrapped (encrypted)
//!   by the master key using AES-256-GCM.
//! - **Data encryption**: AES-256-GCM authenticated encryption with 12-byte random
//!   nonces per block. Every ciphertext carries a 16-byte authentication tag.
//! - **Key Rotation**: New DEK generated on rotation; old DEKs retained for reads
//!   until all data is re-encrypted.
//!
//! This module provides the key management and encrypt/decrypt primitives.
//! The WAL writer and page writer call into this layer transparently.

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Length of AES-256 key in bytes.
pub const AES256_KEY_LEN: usize = 32;
/// Length of AES-GCM nonce (96 bits per NIST recommendation).
pub const NONCE_LEN: usize = 12;
/// Length of AES-GCM authentication tag.
pub const GCM_TAG_LEN: usize = 16;
/// PBKDF2 iteration count (OWASP 2023 recommendation for HMAC-SHA256).
pub const PBKDF2_ITERATIONS: u32 = 600_000;
/// Salt length for PBKDF2.
pub const SALT_LEN: usize = 16;

/// A 256-bit encryption key.
///
/// In production builds, key bytes should be zeroized on drop (via the `zeroize`
/// crate). Simplified here to avoid an extra dependency.
#[derive(Clone)]
pub struct EncryptionKey {
    bytes: [u8; AES256_KEY_LEN],
}

impl EncryptionKey {
    /// Create a key from raw bytes.
    pub const fn from_bytes(bytes: [u8; AES256_KEY_LEN]) -> Self {
        Self { bytes }
    }

    /// Generate a random key using the OS CSPRNG.
    pub fn generate() -> Self {
        let mut bytes = [0u8; AES256_KEY_LEN];
        OsRng.fill_bytes(&mut bytes);
        Self { bytes }
    }

    /// Derive a key from a passphrase using PBKDF2-HMAC-SHA256.
    pub fn derive_from_passphrase(passphrase: &str, salt: &[u8; SALT_LEN]) -> Self {
        let mut key = [0u8; AES256_KEY_LEN];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(
            passphrase.as_bytes(),
            salt,
            PBKDF2_ITERATIONS,
            &mut key,
        );
        Self { bytes: key }
    }

    pub const fn as_bytes(&self) -> &[u8; AES256_KEY_LEN] {
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
    /// The DEK encrypted by the master key (AES-256-GCM ciphertext + tag).
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
            Self::Table(id) => write!(f, "table_{id}"),
            Self::Sst(id) => write!(f, "sst_{id}"),
            Self::Backup => write!(f, "backup"),
        }
    }
}

/// TDE error kinds.
#[derive(Debug)]
pub enum TdeError {
    /// DEK not found in the key manager.
    DekNotFound(DekId),
    /// AES-GCM decryption failed (tampered ciphertext or wrong key).
    DecryptionFailed,
    /// AES-GCM encryption failed (should not happen with valid keys).
    EncryptionFailed,
}

impl fmt::Display for TdeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DekNotFound(id) => write!(f, "DEK {id} not found"),
            Self::DecryptionFailed => write!(f, "AES-256-GCM decryption failed (auth tag mismatch)"),
            Self::EncryptionFailed => write!(f, "AES-256-GCM encryption failed"),
        }
    }
}

impl std::error::Error for TdeError {}

/// TDE Key Manager — manages the master key and all DEKs.
///
/// All cryptographic operations use:
/// - AES-256-GCM for encryption (AEAD — authenticated encryption with associated data)
/// - PBKDF2-HMAC-SHA256 for master key derivation
/// - OS CSPRNG (`getrandom`) for key and nonce generation
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
    /// Derives the master key from the passphrase using PBKDF2-HMAC-SHA256.
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

    /// Restore a key manager from a known salt (e.g., loaded from config/keyfile).
    pub fn from_salt(passphrase: &str, salt: [u8; SALT_LEN]) -> Self {
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
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the salt used for master key derivation.
    pub const fn salt(&self) -> &[u8; SALT_LEN] {
        &self.salt
    }

    /// Generate a new DEK for a scope. Wraps it with the master key and stores it.
    pub fn generate_dek(&mut self, scope: EncryptionScope) -> DekId {
        let dek = EncryptionKey::generate();
        let id = DekId(self.next_dek_id.fetch_add(1, Ordering::Relaxed));

        // Wrap (encrypt) the DEK with the master key using AES-256-GCM
        let nonce = Self::generate_nonce();
        let cipher = Aes256Gcm::new(self.master_key.as_bytes().into());
        let gcm_nonce = Nonce::from_slice(&nonce);
        let ciphertext = cipher
            .encrypt(gcm_nonce, dek.as_bytes().as_ref())
            .expect("AES-256-GCM encryption of DEK must not fail with valid key");

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
    pub fn unwrap_dek(&self, dek_id: DekId) -> Result<EncryptionKey, TdeError> {
        let wrapped = self
            .wrapped_deks
            .get(&dek_id)
            .ok_or(TdeError::DekNotFound(dek_id))?;
        let cipher = Aes256Gcm::new(self.master_key.as_bytes().into());
        let gcm_nonce = Nonce::from_slice(&wrapped.nonce);
        let decrypted = cipher
            .decrypt(gcm_nonce, wrapped.ciphertext.as_ref())
            .map_err(|_| TdeError::DecryptionFailed)?;
        let mut key_bytes = [0u8; AES256_KEY_LEN];
        if decrypted.len() >= AES256_KEY_LEN {
            key_bytes.copy_from_slice(&decrypted[..AES256_KEY_LEN]);
        }
        Ok(EncryptionKey::from_bytes(key_bytes))
    }

    /// Get the active DEK ID for a scope.
    pub fn active_dek(&self, scope: EncryptionScope) -> Option<DekId> {
        self.active_deks.get(&scope).copied()
    }

    /// Encrypt a data block using a DEK. Returns `nonce || ciphertext+tag`.
    ///
    /// The caller stores the returned bytes as-is; to decrypt, pass them to
    /// `decrypt_block` which splits the nonce prefix automatically.
    pub fn encrypt_block(&self, dek_id: DekId, plaintext: &[u8]) -> Result<Vec<u8>, TdeError> {
        let dek = self.unwrap_dek(dek_id)?;
        let cipher = Aes256Gcm::new(dek.as_bytes().into());
        let nonce_bytes = Self::generate_nonce();
        let gcm_nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(gcm_nonce, plaintext)
            .map_err(|_| TdeError::EncryptionFailed)?;
        // Prepend nonce so decrypt_block can recover it
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a data block previously produced by `encrypt_block`.
    /// Input format: `nonce (12 bytes) || ciphertext+tag`.
    pub fn decrypt_block(&self, dek_id: DekId, encrypted: &[u8]) -> Result<Vec<u8>, TdeError> {
        if encrypted.len() < NONCE_LEN + GCM_TAG_LEN {
            return Err(TdeError::DecryptionFailed);
        }
        let (nonce_bytes, ciphertext) = encrypted.split_at(NONCE_LEN);
        let dek = self.unwrap_dek(dek_id)?;
        let cipher = Aes256Gcm::new(dek.as_bytes().into());
        let gcm_nonce = Nonce::from_slice(nonce_bytes);
        cipher
            .decrypt(gcm_nonce, ciphertext)
            .map_err(|_| TdeError::DecryptionFailed)
    }

    /// Encrypt a data block with caller-supplied nonce (for WAL records where
    /// the nonce is derived from LSN to allow deterministic re-encryption).
    pub fn encrypt_block_with_nonce(
        &self,
        dek_id: DekId,
        plaintext: &[u8],
        nonce: &[u8; NONCE_LEN],
    ) -> Result<Vec<u8>, TdeError> {
        let dek = self.unwrap_dek(dek_id)?;
        let cipher = Aes256Gcm::new(dek.as_bytes().into());
        let gcm_nonce = Nonce::from_slice(nonce);
        cipher
            .encrypt(gcm_nonce, plaintext)
            .map_err(|_| TdeError::EncryptionFailed)
    }

    /// Decrypt a data block with caller-supplied nonce.
    pub fn decrypt_block_with_nonce(
        &self,
        dek_id: DekId,
        ciphertext: &[u8],
        nonce: &[u8; NONCE_LEN],
    ) -> Result<Vec<u8>, TdeError> {
        let dek = self.unwrap_dek(dek_id)?;
        let cipher = Aes256Gcm::new(dek.as_bytes().into());
        let gcm_nonce = Nonce::from_slice(nonce);
        cipher
            .decrypt(gcm_nonce, ciphertext)
            .map_err(|_| TdeError::DecryptionFailed)
    }

    /// Rotate the master key. Re-wraps all existing DEKs with the new master key.
    pub fn rotate_master_key(&mut self, new_passphrase: &str) {
        // Unwrap all DEKs with old master key
        let mut raw_deks: Vec<(DekId, EncryptionKey)> = Vec::new();
        for id in self.wrapped_deks.keys() {
            if let Ok(dek) = self.unwrap_dek(*id) {
                raw_deks.push((*id, dek));
            }
        }

        // Derive new master key
        let new_salt = Self::generate_salt();
        let new_master = EncryptionKey::derive_from_passphrase(new_passphrase, &new_salt);
        let cipher = Aes256Gcm::new(new_master.as_bytes().into());

        // Re-wrap all DEKs with new master key
        for (id, dek) in &raw_deks {
            let nonce = Self::generate_nonce();
            let gcm_nonce = Nonce::from_slice(&nonce);
            let ciphertext = cipher
                .encrypt(gcm_nonce, dek.as_bytes().as_ref())
                .expect("Re-wrap must not fail with valid key");
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

    fn generate_salt() -> [u8; SALT_LEN] {
        let mut salt = [0u8; SALT_LEN];
        OsRng.fill_bytes(&mut salt);
        salt
    }

    fn generate_nonce() -> [u8; NONCE_LEN] {
        let mut nonce = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce);
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

        let dek = km.unwrap_dek(dek_id).expect("unwrap should succeed");
        assert_ne!(dek.as_bytes(), &[0u8; 32]); // not all zeros
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let mut km = KeyManager::new("test-pass");
        let dek_id = km.generate_dek(EncryptionScope::Table(1));

        let plaintext = b"Hello, FalconDB Enterprise!";
        let encrypted = km.encrypt_block(dek_id, plaintext).unwrap();

        // Encrypted output should differ from plaintext
        assert_ne!(&encrypted[NONCE_LEN..], plaintext.as_slice());
        // Should be nonce + ciphertext + tag
        assert_eq!(encrypted.len(), NONCE_LEN + plaintext.len() + GCM_TAG_LEN);

        let decrypted = km.decrypt_block(dek_id, &encrypted).unwrap();
        assert_eq!(decrypted.as_slice(), plaintext.as_slice());
    }

    #[test]
    fn test_encrypt_decrypt_with_explicit_nonce() {
        let mut km = KeyManager::new("test-pass");
        let dek_id = km.generate_dek(EncryptionScope::Wal);

        let plaintext = b"WAL record payload";
        let nonce = [42u8; NONCE_LEN];

        let encrypted = km
            .encrypt_block_with_nonce(dek_id, plaintext, &nonce)
            .unwrap();
        let decrypted = km
            .decrypt_block_with_nonce(dek_id, &encrypted, &nonce)
            .unwrap();
        assert_eq!(decrypted.as_slice(), plaintext.as_slice());
    }

    #[test]
    fn test_tampered_ciphertext_detected() {
        let mut km = KeyManager::new("test-pass");
        let dek_id = km.generate_dek(EncryptionScope::Table(1));

        let plaintext = b"sensitive data";
        let mut encrypted = km.encrypt_block(dek_id, plaintext).unwrap();

        // Tamper with a ciphertext byte (after the nonce)
        let tamper_pos = NONCE_LEN + 2;
        if tamper_pos < encrypted.len() {
            encrypted[tamper_pos] ^= 0xFF;
        }

        let result = km.decrypt_block(dek_id, &encrypted);
        assert!(result.is_err(), "Tampered ciphertext must fail authentication");
    }

    #[test]
    fn test_wrong_dek_id_fails() {
        let mut km = KeyManager::new("test-pass");
        let dek_id = km.generate_dek(EncryptionScope::Wal);

        let encrypted = km.encrypt_block(dek_id, b"data").unwrap();

        // Try to decrypt with a non-existent DEK
        let result = km.decrypt_block(DekId(999), &encrypted);
        assert!(matches!(result, Err(TdeError::DekNotFound(_))));
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
        let encrypted = km.encrypt_block(dek_id, plaintext).unwrap();

        // Rotate master key
        km.rotate_master_key("new-password");

        // DEK should still decrypt correctly with new master key wrapping
        let decrypted = km.decrypt_block(dek_id, &encrypted).unwrap();
        assert_eq!(decrypted.as_slice(), plaintext.as_slice());
    }

    #[test]
    fn test_master_key_rotation_salt_changes() {
        let mut km = KeyManager::new("pass");
        let salt_before = *km.salt();
        km.rotate_master_key("new-pass");
        let salt_after = *km.salt();
        // Salt should change on rotation (overwhelmingly likely with 16 random bytes)
        assert_ne!(salt_before, salt_after);
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
    fn test_different_salt_different_key() {
        let s1 = [1u8; SALT_LEN];
        let s2 = [2u8; SALT_LEN];
        let k1 = EncryptionKey::derive_from_passphrase("same", &s1);
        let k2 = EncryptionKey::derive_from_passphrase("same", &s2);
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

    #[test]
    fn test_from_salt_reproduces_master_key() {
        let salt = [7u8; SALT_LEN];
        let km1 = KeyManager::from_salt("secret", salt);
        let km2 = KeyManager::from_salt("secret", salt);
        // Both should derive the same master key → unwrap the same DEKs
        assert_eq!(km1.master_key.as_bytes(), km2.master_key.as_bytes());
    }

    #[test]
    fn test_large_block_roundtrip() {
        let mut km = KeyManager::new("test");
        let dek_id = km.generate_dek(EncryptionScope::Sst(1));

        // 64 KB block (typical page size)
        let plaintext: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        let encrypted = km.encrypt_block(dek_id, &plaintext).unwrap();
        let decrypted = km.decrypt_block(dek_id, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_empty_block_roundtrip() {
        let mut km = KeyManager::new("test");
        let dek_id = km.generate_dek(EncryptionScope::Table(1));

        let encrypted = km.encrypt_block(dek_id, b"").unwrap();
        let decrypted = km.decrypt_block(dek_id, &encrypted).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn test_dek_isolation_across_scopes() {
        // Data encrypted with one scope's DEK cannot be decrypted with another's
        let mut km = KeyManager::new("test");
        let wal_dek = km.generate_dek(EncryptionScope::Wal);
        let tbl_dek = km.generate_dek(EncryptionScope::Table(1));

        let plaintext = b"scope-isolated data";
        let encrypted = km.encrypt_block(wal_dek, plaintext).unwrap();

        // Decrypt with the wrong DEK: should produce different plaintext or fail auth
        let wrong_result = km.decrypt_block(tbl_dek, &encrypted);
        assert!(
            wrong_result.is_err(),
            "Decrypting with wrong DEK must fail authentication"
        );
    }

    #[test]
    fn test_old_dek_still_decrypts_after_rotation() {
        let mut km = KeyManager::new("test");
        let scope = EncryptionScope::Wal;

        // Encrypt with first DEK
        let dek1 = km.generate_dek(scope);
        let encrypted1 = km.encrypt_block(dek1, b"old data").unwrap();

        // Rotate to new DEK
        let dek2 = km.generate_dek(scope);
        assert_ne!(dek1, dek2);

        // Old DEK should still decrypt old data
        let decrypted = km.decrypt_block(dek1, &encrypted1).unwrap();
        assert_eq!(decrypted.as_slice(), b"old data");
    }

    #[test]
    fn test_truncated_ciphertext_fails() {
        let mut km = KeyManager::new("test");
        let dek_id = km.generate_dek(EncryptionScope::Wal);

        let encrypted = km.encrypt_block(dek_id, b"data").unwrap();

        // Truncate to less than nonce + tag
        let truncated = &encrypted[..NONCE_LEN + 5];
        let result = km.decrypt_block(dek_id, truncated);
        assert!(result.is_err());
    }
}