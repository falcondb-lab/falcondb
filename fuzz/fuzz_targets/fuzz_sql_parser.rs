#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 — the parser expects &str.
    if let Ok(sql) = std::str::from_utf8(data) {
        // We don't care about the result; we only want to ensure no panics,
        // infinite loops, or memory-safety violations.
        let _ = falcon_sql_frontend::parser::parse_sql(sql);
    }
});
