use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_executor::ExecutionResult;
use falcon_txn::TxnClassification;

use crate::codec::BackendMessage;
use crate::session::PgSession;

use super::handler::QueryHandler;

impl QueryHandler {
    /// Process COPY FROM STDIN data collected by the server connection loop.
    /// Returns backend messages to send (CommandComplete or ErrorResponse).
    ///
    /// Wrapped in crash-domain guard — panics are caught and converted to ErrorResponse.
    pub fn handle_copy_data(&self, data: &[u8], session: &mut PgSession) -> Vec<BackendMessage> {
        let rctx = falcon_common::request_context::RequestContext::new(session.id as u64);
        let ctx = format!("session_id={}", session.id);
        let result = falcon_common::crash_domain::catch_request("handle_copy_data", &ctx, || {
            self.handle_copy_data_inner(data, session)
        });
        match result {
            Ok(msgs) => msgs,
            Err(e) => vec![self.error_response(&e.with_request_context(&rctx))],
        }
    }

    pub(crate) fn handle_copy_data_inner(
        &self,
        data: &[u8],
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let copy_state = match session.copy_state.take() {
            Some(cs) => cs,
            None => {
                return vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "08P01".into(),
                    message: "No active COPY operation".into(),
                }];
            }
        };

        // Ensure a transaction
        let auto_txn = if session.txn.is_none() {
            let classification = TxnClassification::local(ShardId(0));
            let txn = match self
                .txn_mgr
                .try_begin_with_classification(session.default_isolation, classification)
            {
                Ok(t) => t,
                Err(e) => {
                    let ce: FalconError = e.into();
                    return vec![self.error_response(&ce)];
                }
            };
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let result = self.executor.exec_copy_from_data(
            copy_state.table_id,
            &copy_state.schema,
            &copy_state.columns,
            data,
            copy_state.format.csv,
            copy_state.format.delimiter,
            copy_state.format.header,
            &copy_state.format.null_string,
            copy_state.format.quote,
            copy_state.format.escape,
            match session.txn.as_ref() {
                Some(t) => t,
                None => {
                    return vec![BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "25P01".into(),
                        message: "no active transaction for COPY FROM".into(),
                    }]
                }
            },
        );

        let messages = match result {
            Ok(ExecutionResult::Dml { rows_affected, .. }) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.commit(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                vec![BackendMessage::CommandComplete {
                    tag: format!("COPY {}", rows_affected),
                }]
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.abort(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                vec![self.error_response(&e)]
            }
            _ => {
                vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "XX000".into(),
                    message: "Unexpected COPY FROM result".into(),
                }]
            }
        };

        messages
    }
}
