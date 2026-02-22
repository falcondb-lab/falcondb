//! Native protocol TCP server.
//!
//! Accepts connections, performs handshake + auth, then processes queries.

use std::sync::Arc;

use falcon_protocol_native::codec::{decode_message, encode_message};
use falcon_protocol_native::types::*;

use crate::config::NativeServerConfig;
use crate::executor_bridge::ExecutorBridge;
use crate::nonce::NonceTracker;
use crate::session::{NativeSession, SessionRegistry};

/// The native protocol server state.
pub struct NativeServer {
    pub config: NativeServerConfig,
    pub bridge: Arc<ExecutorBridge>,
    pub sessions: Arc<SessionRegistry>,
    pub nonce_tracker: NonceTracker,
}

impl NativeServer {
    pub fn new(bridge: Arc<ExecutorBridge>, config: NativeServerConfig) -> Self {
        let max_conn = config.max_connections;
        Self {
            config,
            bridge,
            sessions: Arc::new(SessionRegistry::new(max_conn)),
            nonce_tracker: NonceTracker::default_tracker(),
        }
    }

    /// Process a single message from a connected client.
    /// Returns the response message(s) to send back.
    pub fn handle_message(&self, msg: &Message, session: &mut NativeSession) -> Vec<Message> {
        match msg {
            Message::ClientHello(hello) => self.handle_client_hello(hello, session),
            Message::AuthResponse(auth) => self.handle_auth_response(auth, session),
            Message::QueryRequest(req) => {
                if !session.is_ready() {
                    return vec![Message::ErrorResponse(ErrorResponse {
                        request_id: req.request_id,
                        error_code: ERR_AUTH_FAILED,
                        sqlstate: *b"28000",
                        retryable: false,
                        server_epoch: self.config.epoch,
                        message: "session not authenticated".into(),
                    })];
                }
                session.on_request();
                match self.bridge.execute_query(req) {
                    Ok(resp) => vec![Message::QueryResponse(resp)],
                    Err(err) => vec![Message::ErrorResponse(err)],
                }
            }
            Message::BatchRequest(req) => {
                if !session.is_ready() {
                    return vec![Message::ErrorResponse(ErrorResponse {
                        request_id: req.request_id,
                        error_code: ERR_AUTH_FAILED,
                        sqlstate: *b"28000",
                        retryable: false,
                        server_epoch: self.config.epoch,
                        message: "session not authenticated".into(),
                    })];
                }
                session.on_request();
                let resp = self.bridge.execute_batch(req);
                vec![Message::BatchResponse(resp)]
            }
            Message::Ping => {
                session.on_request();
                vec![Message::Pong]
            }
            Message::Disconnect => {
                session.disconnect();
                vec![Message::DisconnectAck]
            }
            Message::StartTls => {
                // TLS upgrade: acknowledge but actual TLS handshake is handled at transport layer
                vec![Message::StartTlsAck]
            }
            _ => {
                // Unknown or unexpected message in this state
                vec![Message::ErrorResponse(ErrorResponse {
                    request_id: 0,
                    error_code: ERR_INTERNAL_ERROR,
                    sqlstate: *b"XX000",
                    retryable: false,
                    server_epoch: self.config.epoch,
                    message: format!(
                        "unexpected message type 0x{:02x} in state {:?}",
                        msg.msg_type(),
                        session.state
                    ),
                })]
            }
        }
    }

    fn handle_client_hello(
        &self,
        hello: &ClientHello,
        session: &mut NativeSession,
    ) -> Vec<Message> {
        // Version check
        if hello.version_major != PROTOCOL_VERSION_MAJOR {
            return vec![Message::ErrorResponse(ErrorResponse {
                request_id: 0,
                error_code: ERR_INTERNAL_ERROR,
                sqlstate: *b"08004",
                retryable: false,
                server_epoch: self.config.epoch,
                message: format!(
                    "unsupported protocol version: {}.{} (server supports {}.{})",
                    hello.version_major,
                    hello.version_minor,
                    PROTOCOL_VERSION_MAJOR,
                    PROTOCOL_VERSION_MINOR,
                ),
            })];
        }

        // Anti-replay: check nonce uniqueness
        if !self.nonce_tracker.check_and_record(&hello.nonce) {
            return vec![Message::ErrorResponse(ErrorResponse {
                request_id: 0,
                error_code: ERR_AUTH_FAILED,
                sqlstate: *b"28000",
                retryable: false,
                server_epoch: self.config.epoch,
                message: "nonce replay detected".into(),
            })];
        }

        session.on_client_hello(hello);

        // Negotiate features: intersection of client and server capabilities
        let server_features =
            FEATURE_BATCH_INGEST | FEATURE_PIPELINE | FEATURE_EPOCH_FENCING | FEATURE_BINARY_PARAMS;
        let negotiated = hello.feature_flags & server_features;

        // Build server hello
        let server_hello = Message::ServerHello(ServerHello {
            version_major: PROTOCOL_VERSION_MAJOR,
            version_minor: PROTOCOL_VERSION_MINOR,
            feature_flags: negotiated,
            server_epoch: self.config.epoch,
            server_node_id: self.config.node_id,
            server_nonce: rand_nonce(),
            params: vec![
                Param {
                    key: "server_version".into(),
                    value: env!("CARGO_PKG_VERSION").to_string(),
                },
                Param {
                    key: "node_id".into(),
                    value: self.config.node_id.to_string(),
                },
            ],
        });

        // Request authentication
        let auth_req = Message::AuthRequest(AuthRequest {
            auth_method: AUTH_PASSWORD,
            challenge: vec![], // no challenge for password auth
        });

        vec![server_hello, auth_req]
    }

    fn handle_auth_response(
        &self,
        auth: &AuthResponse,
        session: &mut NativeSession,
    ) -> Vec<Message> {
        let credential = &auth.credential;
        if self.bridge.authenticate(&session.user, credential) {
            let server_features = FEATURE_BATCH_INGEST
                | FEATURE_PIPELINE
                | FEATURE_EPOCH_FENCING
                | FEATURE_BINARY_PARAMS;
            let negotiated = session.feature_flags & server_features;
            session.on_auth_ok(negotiated);
            session.set_ready();
            vec![Message::AuthOk]
        } else {
            vec![Message::AuthFail(format!(
                "authentication failed for user '{}'",
                session.user
            ))]
        }
    }
}

fn rand_nonce() -> [u8; 16] {
    // Simple nonce generation using std; in production use a CSPRNG
    let mut nonce = [0u8; 16];
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seed = now.as_nanos() as u64;
    nonce[..8].copy_from_slice(&seed.to_le_bytes());
    nonce[8..16]
        .copy_from_slice(&(seed.wrapping_mul(6364136223846793005).wrapping_add(1)).to_le_bytes());
    nonce
}

/// Process a raw byte buffer as a sequence of native protocol messages.
/// Returns (response_bytes, bytes_consumed).
pub fn process_bytes(
    server: &NativeServer,
    session: &mut NativeSession,
    input: &[u8],
) -> (Vec<u8>, usize) {
    let mut offset = 0;
    let mut output = Vec::new();

    while offset < input.len() {
        match decode_message(&input[offset..]) {
            Ok((msg, consumed)) => {
                let responses = server.handle_message(&msg, session);
                for resp in &responses {
                    let encoded = encode_message(resp);
                    output.extend_from_slice(&encoded);
                }
                offset += consumed;
            }
            Err(falcon_protocol_native::NativeProtocolError::Truncated { .. }) => {
                // Need more data
                break;
            }
            Err(e) => {
                tracing::warn!("native protocol decode error: {}", e);
                // Send error and stop processing
                let err_msg = Message::ErrorResponse(ErrorResponse {
                    request_id: 0,
                    error_code: ERR_INTERNAL_ERROR,
                    sqlstate: *b"08P01",
                    retryable: false,
                    server_epoch: server.config.epoch,
                    message: format!("protocol error: {}", e),
                });
                let encoded = encode_message(&err_msg);
                output.extend_from_slice(&encoded);
                break;
            }
        }
    }

    (output, offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_storage::engine::StorageEngine;
    use falcon_txn::manager::TxnManager;

    fn make_server() -> NativeServer {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let config = NativeServerConfig::default();
        let bridge = Arc::new(ExecutorBridge::new(storage, txn_mgr, config.clone()));
        NativeServer::new(bridge, config)
    }

    #[test]
    fn test_full_handshake_flow() {
        let server = make_server();
        let mut session = NativeSession::new();

        // Step 1: ClientHello
        let hello = Message::ClientHello(ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: FEATURE_BATCH_INGEST | FEATURE_PIPELINE,
            client_name: "test-driver".into(),
            database: "testdb".into(),
            user: "admin".into(),
            nonce: [0xAA; 16],
            params: vec![],
        });
        let responses = server.handle_message(&hello, &mut session);
        assert_eq!(responses.len(), 2);
        assert!(matches!(&responses[0], Message::ServerHello(_)));
        assert!(matches!(&responses[1], Message::AuthRequest(_)));

        // Step 2: AuthResponse
        let auth = Message::AuthResponse(AuthResponse {
            auth_method: AUTH_PASSWORD,
            credential: b"secret".to_vec(),
        });
        let responses = server.handle_message(&auth, &mut session);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Message::AuthOk));
        assert!(session.is_ready());
    }

    #[test]
    fn test_auth_failure() {
        let server = make_server();
        let mut session = NativeSession::new();

        let hello = Message::ClientHello(ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: 0,
            client_name: "test".into(),
            database: "db".into(),
            user: "admin".into(),
            nonce: [0; 16],
            params: vec![],
        });
        server.handle_message(&hello, &mut session);

        // Empty credential → fail
        let auth = Message::AuthResponse(AuthResponse {
            auth_method: AUTH_PASSWORD,
            credential: vec![],
        });
        let responses = server.handle_message(&auth, &mut session);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Message::AuthFail(_)));
    }

    #[test]
    fn test_version_mismatch() {
        let server = make_server();
        let mut session = NativeSession::new();

        let hello = Message::ClientHello(ClientHello {
            version_major: 99, // unsupported
            version_minor: 0,
            feature_flags: 0,
            client_name: "test".into(),
            database: "db".into(),
            user: "admin".into(),
            nonce: [0; 16],
            params: vec![],
        });
        let responses = server.handle_message(&hello, &mut session);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Message::ErrorResponse(_)));
    }

    #[test]
    fn test_query_before_auth() {
        let server = make_server();
        let mut session = NativeSession::new();

        let req = Message::QueryRequest(QueryRequest {
            request_id: 1,
            epoch: 0,
            sql: "SELECT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        });
        let responses = server.handle_message(&req, &mut session);
        assert_eq!(responses.len(), 1);
        if let Message::ErrorResponse(e) = &responses[0] {
            assert_eq!(e.error_code, ERR_AUTH_FAILED);
        } else {
            panic!("expected ErrorResponse");
        }
    }

    #[test]
    fn test_ping_pong() {
        let server = make_server();
        let mut session = NativeSession::new();
        let responses = server.handle_message(&Message::Ping, &mut session);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Message::Pong));
    }

    #[test]
    fn test_disconnect() {
        let server = make_server();
        let mut session = NativeSession::new();
        let responses = server.handle_message(&Message::Disconnect, &mut session);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Message::DisconnectAck));
        assert!(!session.is_ready());
    }

    #[test]
    fn test_full_query_flow() {
        let server = make_server();
        let mut session = NativeSession::new();

        // Handshake
        let hello = Message::ClientHello(ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: FEATURE_BATCH_INGEST,
            client_name: "test".into(),
            database: "db".into(),
            user: "admin".into(),
            nonce: [0; 16],
            params: vec![],
        });
        server.handle_message(&hello, &mut session);
        let auth = Message::AuthResponse(AuthResponse {
            auth_method: AUTH_PASSWORD,
            credential: b"pass".to_vec(),
        });
        server.handle_message(&auth, &mut session);
        assert!(session.is_ready());

        // Query
        let req = Message::QueryRequest(QueryRequest {
            request_id: 42,
            epoch: 0,
            sql: "SELECT 1 AS result".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        });
        let responses = server.handle_message(&req, &mut session);
        assert_eq!(responses.len(), 1);
        if let Message::QueryResponse(r) = &responses[0] {
            assert_eq!(r.request_id, 42);
            assert_eq!(r.columns.len(), 1);
            assert_eq!(r.columns[0].name, "result");
            assert_eq!(r.rows.len(), 1);
        } else {
            panic!("expected QueryResponse");
        }
    }

    #[test]
    fn test_process_bytes_roundtrip() {
        let server = make_server();
        let mut session = NativeSession::new();

        // Encode a Ping message
        let ping_bytes = encode_message(&Message::Ping);
        let (output, consumed) = process_bytes(&server, &mut session, &ping_bytes);
        assert_eq!(consumed, ping_bytes.len());

        // Decode the response — should be Pong
        let (resp, _) = decode_message(&output).unwrap();
        assert!(matches!(resp, Message::Pong));
    }

    #[test]
    fn test_process_bytes_multiple_messages() {
        let server = make_server();
        let mut session = NativeSession::new();

        // Encode two Ping messages back to back
        let mut input = Vec::new();
        let ping = encode_message(&Message::Ping);
        input.extend_from_slice(&ping);
        input.extend_from_slice(&ping);

        let (output, consumed) = process_bytes(&server, &mut session, &input);
        assert_eq!(consumed, input.len());

        // Should get two Pong responses
        let (resp1, c1) = decode_message(&output).unwrap();
        assert!(matches!(resp1, Message::Pong));
        let (resp2, _) = decode_message(&output[c1..]).unwrap();
        assert!(matches!(resp2, Message::Pong));
    }

    #[test]
    fn test_feature_negotiation() {
        let server = make_server();
        let mut session = NativeSession::new();

        let hello = Message::ClientHello(ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: FEATURE_BATCH_INGEST | FEATURE_COMPRESSION_LZ4 | FEATURE_TLS,
            client_name: "test".into(),
            database: "db".into(),
            user: "admin".into(),
            nonce: [0; 16],
            params: vec![],
        });
        let responses = server.handle_message(&hello, &mut session);
        if let Message::ServerHello(sh) = &responses[0] {
            // Server supports BATCH_INGEST but not COMPRESSION_LZ4 or TLS
            assert!(sh.feature_flags & FEATURE_BATCH_INGEST != 0);
            assert_eq!(sh.feature_flags & FEATURE_COMPRESSION_LZ4, 0);
            assert_eq!(sh.feature_flags & FEATURE_TLS, 0);
        } else {
            panic!("expected ServerHello");
        }
    }

    #[test]
    fn test_nonce_replay_rejected() {
        let server = make_server();

        let nonce = [0xDD; 16];

        // First handshake with this nonce succeeds
        let mut s1 = NativeSession::new();
        let hello = Message::ClientHello(ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: 0,
            client_name: "test".into(),
            database: "db".into(),
            user: "admin".into(),
            nonce,
            params: vec![],
        });
        let responses = server.handle_message(&hello, &mut s1);
        assert_eq!(responses.len(), 2);
        assert!(matches!(&responses[0], Message::ServerHello(_)));

        // Second handshake with same nonce is rejected
        let mut s2 = NativeSession::new();
        let hello2 = Message::ClientHello(ClientHello {
            version_major: 0,
            version_minor: 1,
            feature_flags: 0,
            client_name: "test2".into(),
            database: "db".into(),
            user: "admin".into(),
            nonce, // same nonce = replay
            params: vec![],
        });
        let responses = server.handle_message(&hello2, &mut s2);
        assert_eq!(responses.len(), 1);
        if let Message::ErrorResponse(e) = &responses[0] {
            assert_eq!(e.error_code, ERR_AUTH_FAILED);
            assert!(e.message.contains("nonce replay"));
        } else {
            panic!("expected ErrorResponse for nonce replay");
        }
    }

    #[test]
    fn test_zero_nonce_always_allowed() {
        let server = make_server();

        // Zero nonce is always allowed (test/dev mode)
        for _ in 0..3 {
            let mut session = NativeSession::new();
            let hello = Message::ClientHello(ClientHello {
                version_major: 0,
                version_minor: 1,
                feature_flags: 0,
                client_name: "test".into(),
                database: "db".into(),
                user: "admin".into(),
                nonce: [0; 16],
                params: vec![],
            });
            let responses = server.handle_message(&hello, &mut session);
            assert_eq!(responses.len(), 2);
            assert!(matches!(&responses[0], Message::ServerHello(_)));
        }
    }
}
