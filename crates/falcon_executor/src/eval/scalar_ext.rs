use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;

pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    if let Some(r) = super::scalar_array_ext::dispatch(func, args) { return r; }
    if let Some(r) = super::scalar_math_ext::dispatch(func, args) { return r; }
    if let Some(r) = super::scalar_time_ext::dispatch(func, args) { return r; }
    Err(ExecutionError::Internal(format!("Unimplemented scalar function: {:?}", func)))
}

