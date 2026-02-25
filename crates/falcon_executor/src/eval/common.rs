use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;

// ── Typed argument extractors ───────────────────────────────────

pub fn expect_text_arg(
    args: &[Datum],
    idx: usize,
    func_name: &str,
) -> Result<Option<String>, ExecutionError> {
    match args.get(idx) {
        Some(Datum::Text(s)) => Ok(Some(s.clone())),
        Some(Datum::Null) => Ok(None),
        _ => Err(ExecutionError::TypeError(format!(
            "{func_name} requires text"
        ))),
    }
}
