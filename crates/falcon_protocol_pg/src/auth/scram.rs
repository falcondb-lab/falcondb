//! SCRAM-SHA-256 authentication engine (RFC 5802, RFC 7677).
//!
//! Implements the server side of PostgreSQL's SCRAM-SHA-256 SASL mechanism.
//! Supports stored verifiers in PostgreSQL format:
//!   `SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>`

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

// ── Constants ────────────────────────────────────────────────────────────

/// Default PBKDF2 iteration count (matches PostgreSQL 15+ default).
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// SCRAM mechanism name.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

// ── ScramVerifier ────────────────────────────────────────────────────────

/// A stored SCRAM-SHA-256 credential (never contains the plaintext password).
#[derive(Debug, Clone)]
pub struct ScramVerifier {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramVerifier {
    /// Generate a verifier from a plaintext password.
    /// The password is used only during this call and never stored.
    pub fn generate(password: &str, iterations: u32) -> Self {
        use rand::RngCore;
        let mut salt = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        Self::generate_with_salt(password, iterations, &salt)
    }

    /// Generate a verifier with a specific salt (for testing / import).
    pub fn generate_with_salt(password: &str, iterations: u32, salt: &[u8]) -> Self {
        let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        Self {
            iterations,
            salt: salt.to_vec(),
            stored_key,
            server_key,
        }
    }

    /// Parse a PostgreSQL SCRAM verifier string.
    /// Format: `SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>`
    pub fn parse(s: &str) -> Result<Self, ScramError> {
        let s = s.strip_prefix("SCRAM-SHA-256$").ok_or_else(|| {
            ScramError::InvalidVerifier("missing SCRAM-SHA-256$ prefix".into())
        })?;

        let (iter_salt, keys) = s
            .split_once('$')
            .ok_or_else(|| ScramError::InvalidVerifier("missing $ separator".into()))?;

        let (iter_str, salt_b64) = iter_salt
            .split_once(':')
            .ok_or_else(|| ScramError::InvalidVerifier("missing : in iter:salt".into()))?;

        let iterations: u32 = iter_str
            .parse()
            .map_err(|_| ScramError::InvalidVerifier("bad iteration count".into()))?;

        let salt = b64_decode(salt_b64)
            .map_err(|_| ScramError::InvalidVerifier("bad salt base64".into()))?;

        let (stored_b64, server_b64) = keys
            .split_once(':')
            .ok_or_else(|| ScramError::InvalidVerifier("missing : in keys".into()))?;

        let stored_key_vec = b64_decode(stored_b64)
            .map_err(|_| ScramError::InvalidVerifier("bad stored_key base64".into()))?;
        let server_key_vec = b64_decode(server_b64)
            .map_err(|_| ScramError::InvalidVerifier("bad server_key base64".into()))?;

        if stored_key_vec.len() != 32 || server_key_vec.len() != 32 {
            return Err(ScramError::InvalidVerifier("key length != 32".into()));
        }

        let mut stored_key = [0u8; 32];
        let mut server_key = [0u8; 32];
        stored_key.copy_from_slice(&stored_key_vec);
        server_key.copy_from_slice(&server_key_vec);

        Ok(Self {
            iterations,
            salt,
            stored_key,
            server_key,
        })
    }

    /// Serialize to PostgreSQL SCRAM verifier string.
    pub fn to_pg_verifier(&self) -> String {
        format!(
            "SCRAM-SHA-256${}:{}${}:{}",
            self.iterations,
            b64_encode(&self.salt),
            b64_encode(&self.stored_key),
            b64_encode(&self.server_key),
        )
    }
}

// ── ScramServerSession ───────────────────────────────────────────────────

/// Server-side SCRAM-SHA-256 handshake state machine.
pub struct ScramServerSession {
    verifier: ScramVerifier,
    state: ScramState,
    server_nonce: String,
    /// client-first-message-bare (without GS2 header)
    client_first_bare: String,
    /// The full server-first-message we sent
    server_first_msg: String,
}

#[derive(Debug, PartialEq, Eq)]
enum ScramState {
    /// Waiting for client-first-message.
    Initial,
    /// Sent server-first-message, waiting for client-final-message.
    ServerFirstSent,
    /// Authentication complete.
    Done,
}

/// Errors from the SCRAM engine.
#[derive(Debug)]
pub enum ScramError {
    /// The SCRAM verifier string is malformed.
    InvalidVerifier(String),
    /// The client message is malformed.
    InvalidMessage(String),
    /// The client proof does not match.
    AuthenticationFailed,
    /// Wrong state for the operation.
    InvalidState,
    /// Unsupported mechanism.
    UnsupportedMechanism(String),
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScramError::InvalidVerifier(s) => write!(f, "invalid SCRAM verifier: {s}"),
            ScramError::InvalidMessage(s) => write!(f, "invalid SCRAM message: {s}"),
            ScramError::AuthenticationFailed => write!(f, "SCRAM authentication failed"),
            ScramError::InvalidState => write!(f, "SCRAM state machine error"),
            ScramError::UnsupportedMechanism(m) => write!(f, "unsupported SASL mechanism: {m}"),
        }
    }
}

impl ScramServerSession {
    /// Create a new server session from a stored verifier.
    pub fn new(verifier: ScramVerifier) -> Self {
        Self {
            verifier,
            state: ScramState::Initial,
            server_nonce: generate_nonce(),
            client_first_bare: String::new(),
            server_first_msg: String::new(),
        }
    }

    /// Process the SASLInitialResponse.
    /// `mechanism` is the SASL mechanism name from the binary frame.
    /// `client_first_message` is the SASL data (the full client-first-message including GS2 header).
    ///
    /// Returns the server-first-message bytes to send in AuthenticationSASLContinue.
    pub fn handle_client_first(
        &mut self,
        mechanism: &str,
        client_first_message: &str,
    ) -> Result<Vec<u8>, ScramError> {
        if self.state != ScramState::Initial {
            return Err(ScramError::InvalidState);
        }
        if mechanism != SCRAM_SHA_256 {
            return Err(ScramError::UnsupportedMechanism(mechanism.to_owned()));
        }

        // Parse GS2 header: must be "n,," (no channel binding) or "y,," (client supports but not used)
        let client_first_bare = if let Some(rest) = client_first_message.strip_prefix("n,,") {
            rest
        } else if let Some(rest) = client_first_message.strip_prefix("y,,") {
            rest
        } else {
            return Err(ScramError::InvalidMessage(
                "unsupported GS2 header (channel binding not supported)".into(),
            ));
        };

        // Extract client nonce from "n=<user>,r=<nonce>"
        let client_nonce = extract_field(client_first_bare, "r")
            .ok_or_else(|| ScramError::InvalidMessage("missing r= field".into()))?;

        self.client_first_bare = client_first_bare.to_owned();

        // Build combined nonce
        let combined_nonce = format!("{client_nonce}{}", self.server_nonce);

        // Build server-first-message
        self.server_first_msg = format!(
            "r={},s={},i={}",
            combined_nonce,
            b64_encode(&self.verifier.salt),
            self.verifier.iterations,
        );

        self.state = ScramState::ServerFirstSent;
        Ok(self.server_first_msg.as_bytes().to_vec())
    }

    /// Process the SASLResponse (client-final-message).
    ///
    /// On success returns the server-final-message bytes (`v=<server_signature>`)
    /// to send in AuthenticationSASLFinal.
    pub fn handle_client_final(
        &mut self,
        client_final_message: &str,
    ) -> Result<Vec<u8>, ScramError> {
        if self.state != ScramState::ServerFirstSent {
            return Err(ScramError::InvalidState);
        }

        // Extract client proof
        let client_proof_b64 = extract_field(client_final_message, "p")
            .ok_or_else(|| ScramError::InvalidMessage("missing p= field".into()))?;

        let client_proof = b64_decode(client_proof_b64)
            .map_err(|_| ScramError::InvalidMessage("invalid base64 in proof".into()))?;

        if client_proof.len() != 32 {
            return Err(ScramError::InvalidMessage("proof length != 32".into()));
        }

        // Build client-final-message-without-proof
        let client_final_without_proof: String = client_final_message
            .split(',')
            .filter(|p| !p.starts_with("p="))
            .collect::<Vec<_>>()
            .join(",");

        // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first_msg, client_final_without_proof
        );

        // Verify: ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature =
            hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes());

        // Recover ClientKey = ClientProof XOR ClientSignature
        let mut recovered_client_key = [0u8; 32];
        for i in 0..32 {
            recovered_client_key[i] = client_proof[i] ^ client_signature[i];
        }

        // Verify: StoredKey == SHA256(recovered ClientKey)
        let recovered_stored_key: [u8; 32] = Sha256::digest(recovered_client_key).into();

        // Constant-time comparison
        if !constant_time_eq(&recovered_stored_key, &self.verifier.stored_key) {
            return Err(ScramError::AuthenticationFailed);
        }

        // Compute server signature for server-final-message
        let server_signature =
            hmac_sha256(&self.verifier.server_key, auth_message.as_bytes());

        self.state = ScramState::Done;

        let server_final = format!("v={}", b64_encode(&server_signature));
        Ok(server_final.as_bytes().to_vec())
    }

    /// Returns true if the handshake completed successfully.
    pub fn is_complete(&self) -> bool {
        self.state == ScramState::Done
    }
}

// ── Crypto Helpers ───────────────────────────────────────────────────────

/// PBKDF2-HMAC-SHA-256 (RFC 7677).
fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    // U1 = HMAC(password, salt || INT(1))
    let mut input = Vec::with_capacity(salt.len() + 4);
    input.extend_from_slice(salt);
    input.extend_from_slice(&1u32.to_be_bytes());

    let mut u_prev = hmac_sha256(password, &input);
    let mut result = u_prev;

    for _ in 1..iterations {
        let u_next = hmac_sha256(password, &u_prev);
        for (r, u) in result.iter_mut().zip(u_next.iter()) {
            *r ^= u;
        }
        u_prev = u_next;
    }
    result
}

/// HMAC-SHA-256 using the `hmac` crate.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac =
        HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// Constant-time byte array comparison.
fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff: u8 = 0;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Generate a cryptographically secure nonce (24 random bytes, base64-encoded).
fn generate_nonce() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut buf);
    b64_encode(&buf)
}

// ── Base64 Helpers ───────────────────────────────────────────────────────

/// Standard base64 encode (with padding).
pub fn b64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Standard base64 decode.
pub fn b64_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s)
}

// ── SCRAM Field Helpers ──────────────────────────────────────────────────

/// Extract a SCRAM attribute value by key (e.g., "r" from "n=user,r=nonce").
/// The key is matched as `<key>=` at the start of a comma-separated field.
fn extract_field<'a>(msg: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    msg.split(',')
        .find(|part| part.starts_with(&prefix))
        .map(|part| &part[prefix.len()..])
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verifier_generate_and_parse_roundtrip() {
        let v = ScramVerifier::generate("testpassword", 4096);
        let s = v.to_pg_verifier();
        assert!(s.starts_with("SCRAM-SHA-256$4096:"));

        let v2 = ScramVerifier::parse(&s).unwrap();
        assert_eq!(v2.iterations, 4096);
        assert_eq!(v2.salt, v.salt);
        assert_eq!(v2.stored_key, v.stored_key);
        assert_eq!(v2.server_key, v.server_key);
    }

    #[test]
    fn test_verifier_parse_rejects_bad_prefix() {
        assert!(ScramVerifier::parse("MD5$abc").is_err());
    }

    #[test]
    fn test_verifier_parse_rejects_bad_format() {
        assert!(ScramVerifier::parse("SCRAM-SHA-256$nocolon").is_err());
        assert!(ScramVerifier::parse("SCRAM-SHA-256$abc:salt$short").is_err());
    }

    #[test]
    fn test_verifier_deterministic_with_salt() {
        let salt = b"fixed_salt_16byt";
        let v1 = ScramVerifier::generate_with_salt("pw", 4096, salt);
        let v2 = ScramVerifier::generate_with_salt("pw", 4096, salt);
        assert_eq!(v1.stored_key, v2.stored_key);
        assert_eq!(v1.server_key, v2.server_key);
    }

    #[test]
    fn test_verifier_different_passwords_differ() {
        let salt = b"fixed_salt_16byt";
        let v1 = ScramVerifier::generate_with_salt("pw1", 100, salt);
        let v2 = ScramVerifier::generate_with_salt("pw2", 100, salt);
        assert_ne!(v1.stored_key, v2.stored_key);
    }

    #[test]
    fn test_full_scram_handshake() {
        // Simulate a complete SCRAM-SHA-256 handshake
        let password = "pencil";
        let salt = b"salty_salt_bytes!";
        let iterations = 4096u32;

        // Server has stored verifier
        let verifier = ScramVerifier::generate_with_salt(password, iterations, salt);
        let mut server = ScramServerSession::new(verifier.clone());

        // Client builds client-first-message
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first_bare = format!("n=user,r={client_nonce}");
        let client_first_msg = format!("n,,{client_first_bare}");

        // Server processes client-first
        let server_first_bytes = server
            .handle_client_first(SCRAM_SHA_256, &client_first_msg)
            .unwrap();
        let server_first_msg = String::from_utf8(server_first_bytes).unwrap();

        // Extract combined nonce from server-first
        let combined_nonce = extract_field(&server_first_msg, "r").unwrap();
        assert!(combined_nonce.starts_with(client_nonce));

        // Client derives keys
        let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();

        // Client builds auth message
        let client_final_without_proof = format!("c=biws,r={combined_nonce}");
        let auth_message =
            format!("{client_first_bare},{server_first_msg},{client_final_without_proof}");

        // Client computes proof
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let proof_b64 = b64_encode(&client_proof);

        let client_final_msg = format!("{client_final_without_proof},p={proof_b64}");

        // Server verifies client-final
        let server_final_bytes = server.handle_client_final(&client_final_msg).unwrap();
        let server_final_msg = String::from_utf8(server_final_bytes).unwrap();
        assert!(server_final_msg.starts_with("v="));
        assert!(server.is_complete());
    }

    #[test]
    fn test_wrong_password_fails() {
        let verifier = ScramVerifier::generate_with_salt("correct", 100, b"salt_16_bytes!!!");
        let mut server = ScramServerSession::new(verifier);

        let client_nonce = "abc123";
        let client_first = format!("n,,n=user,r={client_nonce}");
        let server_first_bytes = server.handle_client_first(SCRAM_SHA_256, &client_first).unwrap();
        let server_first_msg = String::from_utf8(server_first_bytes).unwrap();

        // Derive with WRONG password
        let combined_nonce = extract_field(&server_first_msg, "r").unwrap();
        let salted_password = pbkdf2_hmac_sha256(b"wrong", b"salt_16_bytes!!!", 100);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let client_first_bare = format!("n=user,r={client_nonce}");
        let cfwp = format!("c=biws,r={combined_nonce}");
        let auth_msg = format!("{client_first_bare},{server_first_msg},{cfwp}");
        let sig = hmac_sha256(&stored_key, auth_msg.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(sig.iter()).map(|(a, b)| a ^ b).collect();
        let client_final = format!("{cfwp},p={}", b64_encode(&proof));

        match server.handle_client_final(&client_final) {
            Err(ScramError::AuthenticationFailed) => {} // expected
            other => panic!("expected AuthenticationFailed, got: {other:?}"),
        }
    }

    #[test]
    fn test_unsupported_mechanism() {
        let verifier = ScramVerifier::generate("pw", 100);
        let mut server = ScramServerSession::new(verifier);
        match server.handle_client_first("SCRAM-SHA-1", "n,,n=user,r=nonce") {
            Err(ScramError::UnsupportedMechanism(_)) => {}
            other => panic!("expected UnsupportedMechanism, got: {other:?}"),
        }
    }

    #[test]
    fn test_bad_gs2_header() {
        let verifier = ScramVerifier::generate("pw", 100);
        let mut server = ScramServerSession::new(verifier);
        match server.handle_client_first(SCRAM_SHA_256, "p=tls-unique,,n=user,r=nonce") {
            Err(ScramError::InvalidMessage(_)) => {}
            other => panic!("expected InvalidMessage, got: {other:?}"),
        }
    }

    #[test]
    fn test_constant_time_eq_equal() {
        let a = [1u8; 32];
        assert!(constant_time_eq(&a, &a));
    }

    #[test]
    fn test_constant_time_eq_not_equal() {
        let a = [1u8; 32];
        let mut b = [1u8; 32];
        b[31] = 2;
        assert!(!constant_time_eq(&a, &b));
    }

    #[test]
    fn test_b64_roundtrip() {
        let data = b"hello SCRAM world";
        let encoded = b64_encode(data);
        let decoded = b64_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_extract_field() {
        assert_eq!(extract_field("n=user,r=abc123", "r"), Some("abc123"));
        assert_eq!(extract_field("n=user,r=abc123", "n"), Some("user"));
        assert_eq!(extract_field("n=user,r=abc123", "s"), None);
    }

    #[test]
    fn test_pbkdf2_known_vector() {
        // Verify determinism: same inputs → same output
        let r1 = pbkdf2_hmac_sha256(b"password", b"salt", 1);
        let r2 = pbkdf2_hmac_sha256(b"password", b"salt", 1);
        assert_eq!(r1, r2);
        // 1 iteration: result = HMAC(password, salt || INT(1))
        let expected = hmac_sha256(b"password", b"salt\x00\x00\x00\x01");
        assert_eq!(r1, expected);
    }
}
