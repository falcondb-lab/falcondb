use std::collections::HashMap;
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

thread_local! {
    static NORM_CACHE: std::cell::RefCell<Option<(u64, u64)>> = const { std::cell::RefCell::new(None) };
}

const MAX_ENTRIES: usize = 5000;

#[derive(Debug)]
pub struct StmtEntry {
    pub query: String,
    pub calls: AtomicU64,
    pub total_time_us: AtomicU64,
    pub min_time_us: AtomicU64,
    pub max_time_us: AtomicU64,
    pub rows: AtomicU64,
}

impl StmtEntry {
    fn new(query: String, elapsed_us: u64, rows: u64) -> Self {
        Self {
            query,
            calls: AtomicU64::new(1),
            total_time_us: AtomicU64::new(elapsed_us),
            min_time_us: AtomicU64::new(elapsed_us),
            max_time_us: AtomicU64::new(elapsed_us),
            rows: AtomicU64::new(rows),
        }
    }

    fn record(&self, elapsed_us: u64, rows: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.total_time_us.fetch_add(elapsed_us, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        // min
        let mut cur = self.min_time_us.load(Ordering::Relaxed);
        while elapsed_us < cur {
            match self.min_time_us.compare_exchange_weak(
                cur,
                elapsed_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        // max
        cur = self.max_time_us.load(Ordering::Relaxed);
        while elapsed_us > cur {
            match self.max_time_us.compare_exchange_weak(
                cur,
                elapsed_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

pub struct StmtStatsStore {
    entries: RwLock<HashMap<u64, StmtEntry>>,
}

impl StmtStatsStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::with_capacity(256)),
        }
    }

    pub fn record(&self, sql: &str, elapsed_us: u64, rows: u64) {
        let raw_hash = hash_query(sql);
        // Thread-local cache: if same raw SQL as last call, reuse normalized hash
        let cached_norm = NORM_CACHE.with(|c| {
            let borrow = c.borrow();
            if let Some((rh, nh)) = *borrow {
                if rh == raw_hash {
                    return Some(nh);
                }
            }
            None
        });

        if let Some(norm_hash) = cached_norm {
            let map = self.entries.read();
            if let Some(entry) = map.get(&norm_hash) {
                entry.record(elapsed_us, rows);
                return;
            }
        }

        let normalized = normalize_query(sql);
        let hash = hash_query(&normalized);
        NORM_CACHE.with(|c| *c.borrow_mut() = Some((raw_hash, hash)));

        // Fast path: read lock
        {
            let map = self.entries.read();
            if let Some(entry) = map.get(&hash) {
                entry.record(elapsed_us, rows);
                return;
            }
        }

        // Slow path: write lock, insert
        let mut map = self.entries.write();
        if let Some(entry) = map.get(&hash) {
            entry.record(elapsed_us, rows);
            return;
        }
        if map.len() >= MAX_ENTRIES {
            if let Some(&evict_key) = map
                .iter()
                .min_by_key(|(_, e)| e.calls.load(Ordering::Relaxed))
                .map(|(k, _)| k)
            {
                map.remove(&evict_key);
            }
        }
        map.insert(hash, StmtEntry::new(normalized, elapsed_us, rows));
    }

    pub fn reset(&self) {
        self.entries.write().clear();
    }

    /// Snapshot all entries for query. Returns (queryid, query, calls, total_time_us, min_time_us, max_time_us, rows).
    pub fn snapshot(&self) -> Vec<(u64, String, u64, u64, u64, u64, u64)> {
        let map = self.entries.read();
        map.iter()
            .map(|(&qid, e)| {
                (
                    qid,
                    e.query.clone(),
                    e.calls.load(Ordering::Relaxed),
                    e.total_time_us.load(Ordering::Relaxed),
                    e.min_time_us.load(Ordering::Relaxed),
                    e.max_time_us.load(Ordering::Relaxed),
                    e.rows.load(Ordering::Relaxed),
                )
            })
            .collect()
    }
}

impl Default for StmtStatsStore {
    fn default() -> Self {
        Self::new()
    }
}

fn hash_query(normalized: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    normalized.hash(&mut h);
    h.finish()
}

/// Normalize SQL: replace numeric/string literals with $N placeholders.
pub fn normalize_query(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;
    let mut param = 1u32;

    while i < len {
        let b = bytes[i];
        match b {
            // String literal
            b'\'' => {
                out.push('$');
                let _ = write!(out, "{}", param);
                param += 1;
                i += 1;
                // Skip to closing quote (handle '' escapes)
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            i += 1; // escaped quote
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            // Numeric literal (not preceded by letter/underscore)
            b'0'..=b'9' if !is_ident_char(if i > 0 { bytes[i - 1] } else { b' ' }) => {
                out.push('$');
                let _ = write!(out, "{}", param);
                param += 1;
                // Skip digits, dots, hex, scientific notation
                while i < len
                    && (bytes[i].is_ascii_digit()
                        || bytes[i] == b'.'
                        || bytes[i] == b'x'
                        || bytes[i] == b'X'
                        || bytes[i] == b'e'
                        || bytes[i] == b'E'
                        || ((bytes[i] == b'+' || bytes[i] == b'-')
                            && i > 0
                            && (bytes[i - 1] == b'e' || bytes[i - 1] == b'E')))
                {
                    i += 1;
                }
            }
            // Collapse whitespace
            b' ' | b'\t' | b'\n' | b'\r' => {
                if !out.ends_with(' ') {
                    out.push(' ');
                }
                i += 1;
            }
            _ => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    let start = out.bytes().position(|b| b != b' ').unwrap_or(0);
    let end = out
        .bytes()
        .rposition(|b| b != b' ')
        .map_or(start, |p| p + 1);
    if start > 0 || end < out.len() {
        out.drain(end..);
        out.drain(..start);
    }
    out
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_simple() {
        assert_eq!(
            normalize_query("SELECT * FROM users WHERE id = 42"),
            "SELECT * FROM users WHERE id = $1"
        );
    }

    #[test]
    fn test_normalize_strings() {
        assert_eq!(
            normalize_query("INSERT INTO t VALUES (1, 'hello', 'world')"),
            "INSERT INTO t VALUES ($1, $2, $3)"
        );
    }

    #[test]
    fn test_normalize_escaped_quote() {
        assert_eq!(
            normalize_query("SELECT * FROM t WHERE name = 'it''s'"),
            "SELECT * FROM t WHERE name = $1"
        );
    }

    #[test]
    fn test_normalize_whitespace() {
        assert_eq!(normalize_query("SELECT  *\n  FROM\t t"), "SELECT * FROM t");
    }

    #[test]
    fn test_normalize_no_clobber_ident() {
        let n = normalize_query("SELECT col1 FROM table2");
        assert!(
            n.contains("col1"),
            "should not replace digits in identifiers: {n}"
        );
        assert!(
            n.contains("table2"),
            "should not replace digits in identifiers: {n}"
        );
    }

    #[test]
    fn test_store_record_and_snapshot() {
        let store = StmtStatsStore::new();
        store.record("SELECT 1", 100, 1);
        store.record("SELECT 1", 200, 1);
        store.record("SELECT 2", 50, 1);

        let snap = store.snapshot();
        // SELECT 1 and SELECT 2 both normalize to "SELECT $1"
        assert_eq!(snap.len(), 1);
        let (_, _, calls, total, min, max, rows) = &snap[0];
        assert_eq!(*calls, 3);
        assert_eq!(*total, 350);
        assert_eq!(*min, 50);
        assert_eq!(*max, 200);
        assert_eq!(*rows, 3);
    }

    #[test]
    fn test_store_reset() {
        let store = StmtStatsStore::new();
        store.record("SELECT 1", 100, 1);
        assert!(!store.snapshot().is_empty());
        store.reset();
        assert!(store.snapshot().is_empty());
    }
}
