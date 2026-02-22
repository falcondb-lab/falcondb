//! PG-compatible logical replication protocol.
//!
//! Implements the server side of PostgreSQL's streaming replication protocol
//! (logical decoding) on top of FalconDB's CDC infrastructure.
//!
//! Supported replication commands (received as simple Query messages on a
//! connection started with `replication=database`):
//!
//! - `IDENTIFY_SYSTEM` — returns system identifier, timeline, current WAL position
//! - `CREATE_REPLICATION_SLOT <name> LOGICAL <plugin>` — creates a CDC slot
//! - `DROP_REPLICATION_SLOT <name>` — drops a CDC slot
//! - `START_REPLICATION SLOT <name> LOGICAL <lsn>` — begins streaming changes
//!
//! Wire format follows PG logical replication protocol:
//! - Server sends `CopyBothResponse` to enter streaming mode
//! - `XLogData` ('w') messages carry decoded change payloads
//! - `PrimaryKeepalive` ('k') messages are sent periodically
//! - Client sends `StandbyStatusUpdate` ('r') to acknowledge progress

use bytes::{BufMut, BytesMut};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use falcon_storage::cdc::{ChangeEvent, ChangeOp};
use falcon_storage::engine::StorageEngine;

use crate::codec::{BackendMessage, FieldDescription};

/// Microseconds from 2000-01-01 00:00:00 UTC (PG epoch).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Default output plugin name (test_decoding compatible).
pub const DEFAULT_PLUGIN: &str = "falcon_logical";

/// Result of handling a replication command.
pub enum ReplicationResult {
    /// Normal query result (RowDescription + DataRow + CommandComplete).
    Messages(Vec<BackendMessage>),
    /// Enter streaming mode: send CopyBothResponse, then stream XLogData.
    StartStreaming {
        slot_name: String,
        start_lsn: u64,
    },
    /// Error response.
    Error(String),
}

/// Format a CDC LSN as PG-style "X/XXXXXXXX" string.
fn format_lsn(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{:X}/{:08X}", hi, lo)
}

/// Parse a PG-style "X/XXXXXXXX" LSN string back to u64.
fn parse_lsn(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some((hi_str, lo_str)) = s.split_once('/') {
        let hi = u64::from_str_radix(hi_str.trim(), 16).ok()?;
        let lo = u64::from_str_radix(lo_str.trim(), 16).ok()?;
        Some((hi << 32) | lo)
    } else {
        // Plain decimal
        s.parse::<u64>().ok()
    }
}

/// Current PG-epoch timestamp in microseconds.
fn pg_timestamp_us() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_micros() as i64) - PG_EPOCH_OFFSET_US
}

/// Handle a replication-mode SQL command.
///
/// Called when the connection was started with `replication=database` in the
/// startup parameters. Replication commands arrive as plain Query messages.
pub fn handle_replication_command(
    sql: &str,
    storage: &Arc<StorageEngine>,
) -> ReplicationResult {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if upper == "IDENTIFY_SYSTEM" {
        return handle_identify_system(storage);
    }

    if upper.starts_with("CREATE_REPLICATION_SLOT") {
        return handle_create_replication_slot(trimmed, storage);
    }

    if upper.starts_with("DROP_REPLICATION_SLOT") {
        return handle_drop_replication_slot(trimmed, storage);
    }

    if upper.starts_with("START_REPLICATION") {
        return handle_start_replication(trimmed, storage);
    }

    // SHOW and SELECT are also allowed on replication connections
    if upper.starts_with("SHOW") || upper.starts_with("SELECT") {
        return ReplicationResult::Error(
            "regular queries on replication connections are forwarded to the normal handler"
                .to_string(),
        );
    }

    ReplicationResult::Error(format!("unrecognized replication command: {}", trimmed))
}

/// IDENTIFY_SYSTEM — returns system info.
fn handle_identify_system(storage: &Arc<StorageEngine>) -> ReplicationResult {
    let cdc = storage.cdc_manager.read();
    let current_lsn = cdc.buffer_len() as u64;
    drop(cdc);

    let fields = vec![
        FieldDescription {
            name: "systemid".into(),
            table_oid: 0,
            column_attr: 0,
            type_oid: 25, // text
            type_len: -1,
            type_modifier: -1,
            format_code: 0,
        },
        FieldDescription {
            name: "timeline".into(),
            table_oid: 0,
            column_attr: 0,
            type_oid: 23, // int4
            type_len: 4,
            type_modifier: -1,
            format_code: 0,
        },
        FieldDescription {
            name: "xlogpos".into(),
            table_oid: 0,
            column_attr: 0,
            type_oid: 25,
            type_len: -1,
            type_modifier: -1,
            format_code: 0,
        },
        FieldDescription {
            name: "dbname".into(),
            table_oid: 0,
            column_attr: 0,
            type_oid: 25,
            type_len: -1,
            type_modifier: -1,
            format_code: 0,
        },
    ];

    let row = vec![
        Some("falcondb".to_string()),       // systemid
        Some("1".to_string()),              // timeline
        Some(format_lsn(current_lsn)),      // xlogpos
        Some("falcon".to_string()),         // dbname
    ];

    ReplicationResult::Messages(vec![
        BackendMessage::RowDescription { fields },
        BackendMessage::DataRow { values: row },
        BackendMessage::CommandComplete {
            tag: "IDENTIFY_SYSTEM".into(),
        },
    ])
}

/// CREATE_REPLICATION_SLOT <name> LOGICAL <plugin> [NOEXPORT_SNAPSHOT]
fn handle_create_replication_slot(
    sql: &str,
    storage: &Arc<StorageEngine>,
) -> ReplicationResult {
    // Parse: CREATE_REPLICATION_SLOT slot_name LOGICAL plugin_name
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 4 {
        return ReplicationResult::Error(
            "syntax: CREATE_REPLICATION_SLOT <name> LOGICAL <plugin>".into(),
        );
    }

    let slot_name = tokens[1];
    // tokens[2] should be "LOGICAL"
    if !tokens[2].eq_ignore_ascii_case("LOGICAL") {
        return ReplicationResult::Error(
            "only LOGICAL replication slots are supported".into(),
        );
    }
    let _plugin = tokens[3]; // accepted but we use our own decoder

    let mut cdc = storage.cdc_manager.write();
    match cdc.create_slot(slot_name) {
        Ok(slot_id) => {
            let slot = cdc.list_slots().into_iter().find(|s| s.id == slot_id);
            let consistent_point = slot
                .map(|s| s.confirmed_flush_lsn.0)
                .unwrap_or(0);
            drop(cdc);

            let fields = vec![
                FieldDescription {
                    name: "slot_name".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25,
                    type_len: -1,
                    type_modifier: -1,
                    format_code: 0,
                },
                FieldDescription {
                    name: "consistent_point".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25,
                    type_len: -1,
                    type_modifier: -1,
                    format_code: 0,
                },
                FieldDescription {
                    name: "snapshot_name".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25,
                    type_len: -1,
                    type_modifier: -1,
                    format_code: 0,
                },
                FieldDescription {
                    name: "output_plugin".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25,
                    type_len: -1,
                    type_modifier: -1,
                    format_code: 0,
                },
            ];

            let row = vec![
                Some(slot_name.to_string()),
                Some(format_lsn(consistent_point)),
                None, // snapshot_name (not supported)
                Some(DEFAULT_PLUGIN.to_string()),
            ];

            ReplicationResult::Messages(vec![
                BackendMessage::RowDescription { fields },
                BackendMessage::DataRow { values: row },
                BackendMessage::CommandComplete {
                    tag: "CREATE_REPLICATION_SLOT".into(),
                },
            ])
        }
        Err(e) => ReplicationResult::Error(e),
    }
}

/// DROP_REPLICATION_SLOT <name> [WAIT]
fn handle_drop_replication_slot(
    sql: &str,
    storage: &Arc<StorageEngine>,
) -> ReplicationResult {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 2 {
        return ReplicationResult::Error(
            "syntax: DROP_REPLICATION_SLOT <name>".into(),
        );
    }

    let slot_name = tokens[1];
    let mut cdc = storage.cdc_manager.write();
    match cdc.drop_slot(slot_name) {
        Ok(_) => {
            drop(cdc);
            ReplicationResult::Messages(vec![BackendMessage::CommandComplete {
                tag: "DROP_REPLICATION_SLOT".into(),
            }])
        }
        Err(e) => ReplicationResult::Error(e),
    }
}

/// START_REPLICATION SLOT <name> LOGICAL <lsn> [(<option> '<value>', ...)]
fn handle_start_replication(
    sql: &str,
    storage: &Arc<StorageEngine>,
) -> ReplicationResult {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    // START_REPLICATION SLOT slot_name LOGICAL 0/0
    if tokens.len() < 5 {
        return ReplicationResult::Error(
            "syntax: START_REPLICATION SLOT <name> LOGICAL <lsn>".into(),
        );
    }

    if !tokens[1].eq_ignore_ascii_case("SLOT") {
        return ReplicationResult::Error(
            "syntax: START_REPLICATION SLOT <name> LOGICAL <lsn>".into(),
        );
    }

    let slot_name = tokens[2];
    // tokens[3] should be "LOGICAL"
    if !tokens[3].eq_ignore_ascii_case("LOGICAL") {
        return ReplicationResult::Error(
            "only LOGICAL replication is supported".into(),
        );
    }

    let start_lsn = parse_lsn(tokens[4]).unwrap_or(0);

    // Verify slot exists and activate it
    let mut cdc = storage.cdc_manager.write();
    let slot = match cdc.get_slot_by_name(slot_name) {
        Some(s) => s.id,
        None => {
            return ReplicationResult::Error(format!(
                "replication slot \"{}\" does not exist",
                slot_name
            ));
        }
    };

    if let Err(e) = cdc.activate_slot(slot) {
        return ReplicationResult::Error(e);
    }
    drop(cdc);

    ReplicationResult::StartStreaming {
        slot_name: slot_name.to_string(),
        start_lsn,
    }
}

/// Encode a CDC ChangeEvent as a PG logical decoding text message.
///
/// Format follows `test_decoding` output plugin style:
/// ```text
/// table public.users: INSERT: id[integer]:1 name[text]:'Alice'
/// table public.users: UPDATE: id[integer]:1 name[text]:'Bob'
/// table public.users: DELETE: id[integer]:1
/// BEGIN 42
/// COMMIT 42
/// ```
pub fn encode_change_event_text(event: &ChangeEvent) -> String {
    match event.op {
        ChangeOp::Begin => format!("BEGIN {}", event.txn_id.0),
        ChangeOp::Commit => format!("COMMIT {}", event.txn_id.0),
        ChangeOp::Rollback => format!("ROLLBACK {}", event.txn_id.0),
        ChangeOp::Insert => {
            let table = event
                .table_name
                .as_deref()
                .unwrap_or("unknown");
            let row_str = event
                .new_row
                .as_ref()
                .map(|r| format_row_values(&r.values))
                .unwrap_or_default();
            format!("table public.{}: INSERT: {}", table, row_str)
        }
        ChangeOp::Update => {
            let table = event
                .table_name
                .as_deref()
                .unwrap_or("unknown");
            let row_str = event
                .new_row
                .as_ref()
                .map(|r| format_row_values(&r.values))
                .unwrap_or_default();
            format!("table public.{}: UPDATE: {}", table, row_str)
        }
        ChangeOp::Delete => {
            let table = event
                .table_name
                .as_deref()
                .unwrap_or("unknown");
            let pk_str = event
                .pk_values
                .as_deref()
                .unwrap_or("(unknown pk)");
            format!("table public.{}: DELETE: {}", table, pk_str)
        }
        ChangeOp::Ddl => {
            let ddl = event
                .ddl_text
                .as_deref()
                .unwrap_or("(unknown DDL)");
            format!("DDL: {}", ddl)
        }
    }
}

/// Format row values as `col[type]:value col[type]:value ...`
fn format_row_values(row: &[falcon_common::datum::Datum]) -> String {
    use falcon_common::datum::Datum;
    row.iter()
        .enumerate()
        .map(|(i, d)| {
            let col = format!("col{}", i);
            match d {
                Datum::Null => format!("{}[null]:null", col),
                Datum::Boolean(b) => format!("{}[boolean]:{}", col, b),
                Datum::Int32(v) => format!("{}[integer]:{}", col, v),
                Datum::Int64(v) => format!("{}[bigint]:{}", col, v),
                Datum::Float64(v) => format!("{}[double precision]:{}", col, v),
                Datum::Text(s) => format!("{}[text]:'{}'", col, s),
                Datum::Timestamp(t) => format!("{}[timestamp]:{}", col, t),
                Datum::Date(d) => format!("{}[date]:{}", col, d),
                _ => format!("{}[unknown]:{}", col, d),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build an XLogData message ('w') for the CopyData stream.
///
/// Wire format:
/// ```text
/// 'd' (CopyData wrapper)
///   'w' (XLogData tag)
///   startLsn   (u64 BE)
///   currentLsn (u64 BE)
///   sendTime   (i64 BE, PG epoch microseconds)
///   payload    (bytes — the decoded change text)
/// ```
pub fn build_xlog_data_message(lsn: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(1 + 8 + 8 + 8 + payload.len());
    buf.put_u8(b'w'); // XLogData tag
    buf.put_u64(lsn); // WAL start
    buf.put_u64(lsn); // WAL end
    buf.put_i64(pg_timestamp_us()); // send time
    buf.extend_from_slice(payload);
    buf.to_vec()
}

/// Build a PrimaryKeepalive message ('k') for the CopyData stream.
///
/// Wire format:
/// ```text
/// 'd' (CopyData wrapper)
///   'k' (keepalive tag)
///   walEnd     (u64 BE)
///   sendTime   (i64 BE, PG epoch microseconds)
///   replyNow   (u8, 1 = server requests immediate reply)
/// ```
pub fn build_keepalive_message(wal_end: u64, reply_requested: bool) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(1 + 8 + 8 + 1);
    buf.put_u8(b'k'); // keepalive tag
    buf.put_u64(wal_end);
    buf.put_i64(pg_timestamp_us());
    buf.put_u8(if reply_requested { 1 } else { 0 });
    buf.to_vec()
}

/// Parse a StandbyStatusUpdate message ('r') from the client.
///
/// Wire format:
/// ```text
/// 'r' (status update tag)
/// writeLsn   (u64 BE) — last WAL position written to disk
/// flushLsn   (u64 BE) — last WAL position flushed to disk
/// applyLsn   (u64 BE) — last WAL position applied
/// sendTime   (i64 BE) — client timestamp
/// replyNow   (u8)     — 1 if server should reply immediately
/// ```
pub struct StandbyStatusUpdate {
    pub write_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    pub reply_requested: bool,
}

impl StandbyStatusUpdate {
    /// Parse from raw CopyData payload (after stripping the 'd' wrapper).
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 34 || data[0] != b'r' {
            return None;
        }
        let write_lsn = u64::from_be_bytes(data[1..9].try_into().ok()?);
        let flush_lsn = u64::from_be_bytes(data[9..17].try_into().ok()?);
        let apply_lsn = u64::from_be_bytes(data[17..25].try_into().ok()?);
        // bytes 25..33 = sendTime (i64), skip
        let reply_requested = data.get(33).copied().unwrap_or(0) != 0;
        Some(Self {
            write_lsn,
            flush_lsn,
            apply_lsn,
            reply_requested,
        })
    }
}

/// Check if a startup parameter set indicates a replication connection.
pub fn is_replication_connection(params: &std::collections::HashMap<String, String>) -> bool {
    params
        .get("replication")
        .map(|v| v.eq_ignore_ascii_case("database") || v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::TxnId;
    use falcon_storage::cdc::CdcLsn;

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0), "0/00000000");
        assert_eq!(format_lsn(1), "0/00000001");
        assert_eq!(format_lsn(0x1_00000000), "1/00000000");
        assert_eq!(format_lsn(0xAB_CDEF0123), "AB/CDEF0123");
    }

    #[test]
    fn test_parse_lsn() {
        assert_eq!(parse_lsn("0/00000000"), Some(0));
        assert_eq!(parse_lsn("0/00000001"), Some(1));
        assert_eq!(parse_lsn("1/00000000"), Some(0x1_00000000));
        assert_eq!(parse_lsn("AB/CDEF0123"), Some(0xAB_CDEF0123));
        assert_eq!(parse_lsn("42"), Some(42));
        assert_eq!(parse_lsn("invalid"), None);
    }

    #[test]
    fn test_parse_lsn_roundtrip() {
        for lsn in [0u64, 1, 255, 0x1_00000000, 0xFF_FFFFFFFF, 12345678] {
            let formatted = format_lsn(lsn);
            let parsed = parse_lsn(&formatted).unwrap();
            assert_eq!(parsed, lsn, "roundtrip failed for {}", lsn);
        }
    }

    #[test]
    fn test_is_replication_connection() {
        let mut params = std::collections::HashMap::new();
        assert!(!is_replication_connection(&params));

        params.insert("replication".into(), "database".into());
        assert!(is_replication_connection(&params));

        params.insert("replication".into(), "true".into());
        assert!(is_replication_connection(&params));

        params.insert("replication".into(), "1".into());
        assert!(is_replication_connection(&params));

        params.insert("replication".into(), "false".into());
        assert!(!is_replication_connection(&params));
    }

    #[test]
    fn test_identify_system() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let result = handle_replication_command("IDENTIFY_SYSTEM", &storage);
        match result {
            ReplicationResult::Messages(msgs) => {
                assert_eq!(msgs.len(), 3);
                assert!(matches!(msgs[0], BackendMessage::RowDescription { .. }));
                assert!(matches!(msgs[1], BackendMessage::DataRow { .. }));
                assert!(matches!(
                    msgs[2],
                    BackendMessage::CommandComplete { ref tag } if tag == "IDENTIFY_SYSTEM"
                ));
            }
            _ => panic!("expected Messages"),
        }
    }

    #[test]
    fn test_create_and_drop_replication_slot() {
        let storage = Arc::new(StorageEngine::new_in_memory());

        // Create
        let result = handle_replication_command(
            "CREATE_REPLICATION_SLOT test_slot LOGICAL test_decoding",
            &storage,
        );
        match result {
            ReplicationResult::Messages(msgs) => {
                assert_eq!(msgs.len(), 3);
                if let BackendMessage::DataRow { ref values } = msgs[1] {
                    assert_eq!(values[0], Some("test_slot".to_string()));
                    assert_eq!(values[3], Some(DEFAULT_PLUGIN.to_string()));
                }
                assert!(matches!(
                    msgs[2],
                    BackendMessage::CommandComplete { ref tag } if tag == "CREATE_REPLICATION_SLOT"
                ));
            }
            _ => panic!("expected Messages"),
        }

        // Duplicate should fail
        let result = handle_replication_command(
            "CREATE_REPLICATION_SLOT test_slot LOGICAL test_decoding",
            &storage,
        );
        assert!(matches!(result, ReplicationResult::Error(_)));

        // Drop
        let result = handle_replication_command(
            "DROP_REPLICATION_SLOT test_slot",
            &storage,
        );
        match result {
            ReplicationResult::Messages(msgs) => {
                assert_eq!(msgs.len(), 1);
                assert!(matches!(
                    msgs[0],
                    BackendMessage::CommandComplete { ref tag } if tag == "DROP_REPLICATION_SLOT"
                ));
            }
            _ => panic!("expected Messages"),
        }

        // Drop non-existent should fail
        let result = handle_replication_command(
            "DROP_REPLICATION_SLOT nonexistent",
            &storage,
        );
        assert!(matches!(result, ReplicationResult::Error(_)));
    }

    #[test]
    fn test_start_replication() {
        let storage = Arc::new(StorageEngine::new_in_memory());

        // Create slot first
        handle_replication_command(
            "CREATE_REPLICATION_SLOT my_slot LOGICAL falcon_logical",
            &storage,
        );

        // Start replication
        let result = handle_replication_command(
            "START_REPLICATION SLOT my_slot LOGICAL 0/0",
            &storage,
        );
        match result {
            ReplicationResult::StartStreaming {
                slot_name,
                start_lsn,
            } => {
                assert_eq!(slot_name, "my_slot");
                assert_eq!(start_lsn, 0);
            }
            _ => panic!("expected StartStreaming"),
        }

        // Deactivate for cleanup
        let mut cdc = storage.cdc_manager.write();
        let slot = cdc.get_slot_by_name("my_slot").unwrap();
        let slot_id = slot.id;
        cdc.deactivate_slot(slot_id);
    }

    #[test]
    fn test_start_replication_missing_slot() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let result = handle_replication_command(
            "START_REPLICATION SLOT nonexistent LOGICAL 0/0",
            &storage,
        );
        assert!(matches!(result, ReplicationResult::Error(_)));
    }

    #[test]
    fn test_encode_change_event_insert() {
        use falcon_common::datum::{Datum, OwnedRow};
        let event = ChangeEvent {
            seq: 1,
            lsn: CdcLsn(1),
            txn_id: TxnId(42),
            op: ChangeOp::Insert,
            table_id: None,
            table_name: Some("users".into()),
            timestamp_ms: 0,
            new_row: Some(OwnedRow::new(vec![Datum::Int32(1), Datum::Text("Alice".into())])),
            old_row: None,
            pk_values: None,
            ddl_text: None,
        };
        let text = encode_change_event_text(&event);
        assert!(text.starts_with("table public.users: INSERT:"));
        assert!(text.contains("integer"));
        assert!(text.contains("Alice"));
    }

    #[test]
    fn test_encode_change_event_begin_commit() {
        let begin = ChangeEvent {
            seq: 1,
            lsn: CdcLsn(1),
            txn_id: TxnId(99),
            op: ChangeOp::Begin,
            table_id: None,
            table_name: None,
            timestamp_ms: 0,
            new_row: None,
            old_row: None,
            pk_values: None,
            ddl_text: None,
        };
        assert_eq!(encode_change_event_text(&begin), "BEGIN 99");

        let commit = ChangeEvent {
            seq: 2,
            lsn: CdcLsn(2),
            txn_id: TxnId(99),
            op: ChangeOp::Commit,
            table_id: None,
            table_name: None,
            timestamp_ms: 0,
            new_row: None,
            old_row: None,
            pk_values: None,
            ddl_text: None,
        };
        assert_eq!(encode_change_event_text(&commit), "COMMIT 99");
    }

    #[test]
    fn test_build_xlog_data_message() {
        let payload = b"table public.t: INSERT: col0[integer]:1";
        let msg = build_xlog_data_message(42, payload);
        assert_eq!(msg[0], b'w');
        // LSN at bytes 1..9
        let lsn = u64::from_be_bytes(msg[1..9].try_into().unwrap());
        assert_eq!(lsn, 42);
        // Payload at byte 25 onward (1 + 8 + 8 + 8 = 25)
        assert_eq!(&msg[25..], payload);
    }

    #[test]
    fn test_build_keepalive_message() {
        let msg = build_keepalive_message(100, true);
        assert_eq!(msg[0], b'k');
        let wal_end = u64::from_be_bytes(msg[1..9].try_into().unwrap());
        assert_eq!(wal_end, 100);
        assert_eq!(msg[17], 1); // reply requested
    }

    #[test]
    fn test_standby_status_update_parse() {
        let mut data = vec![b'r'];
        data.extend_from_slice(&100u64.to_be_bytes()); // write_lsn
        data.extend_from_slice(&90u64.to_be_bytes());  // flush_lsn
        data.extend_from_slice(&80u64.to_be_bytes());  // apply_lsn
        data.extend_from_slice(&0i64.to_be_bytes());   // sendTime
        data.push(1); // reply requested

        let status = StandbyStatusUpdate::parse(&data).unwrap();
        assert_eq!(status.write_lsn, 100);
        assert_eq!(status.flush_lsn, 90);
        assert_eq!(status.apply_lsn, 80);
        assert!(status.reply_requested);
    }

    #[test]
    fn test_standby_status_update_parse_no_reply() {
        let mut data = vec![b'r'];
        data.extend_from_slice(&50u64.to_be_bytes());
        data.extend_from_slice(&50u64.to_be_bytes());
        data.extend_from_slice(&50u64.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes());
        data.push(0); // no reply

        let status = StandbyStatusUpdate::parse(&data).unwrap();
        assert!(!status.reply_requested);
    }

    #[test]
    fn test_standby_status_update_parse_invalid() {
        assert!(StandbyStatusUpdate::parse(&[]).is_none());
        assert!(StandbyStatusUpdate::parse(&[b'x'; 34]).is_none()); // wrong tag
        assert!(StandbyStatusUpdate::parse(&[b'r'; 10]).is_none()); // too short
    }

    #[test]
    fn test_unrecognized_command() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let result = handle_replication_command("FOOBAR", &storage);
        assert!(matches!(result, ReplicationResult::Error(_)));
    }

    #[test]
    fn test_create_slot_syntax_errors() {
        let storage = Arc::new(StorageEngine::new_in_memory());

        // Missing args
        let result = handle_replication_command("CREATE_REPLICATION_SLOT", &storage);
        assert!(matches!(result, ReplicationResult::Error(_)));

        // Wrong type (PHYSICAL instead of LOGICAL)
        let result = handle_replication_command(
            "CREATE_REPLICATION_SLOT s1 PHYSICAL",
            &storage,
        );
        assert!(matches!(result, ReplicationResult::Error(_)));
    }

    #[test]
    fn test_start_replication_syntax_errors() {
        let storage = Arc::new(StorageEngine::new_in_memory());

        // Missing SLOT keyword
        let result = handle_replication_command(
            "START_REPLICATION my_slot LOGICAL 0/0",
            &storage,
        );
        assert!(matches!(result, ReplicationResult::Error(_)));

        // Too few args
        let result = handle_replication_command("START_REPLICATION SLOT", &storage);
        assert!(matches!(result, ReplicationResult::Error(_)));
    }
}
