//! Parameter environment for type inference in parameterized queries.
//!
//! During binding, `ParamEnv` tracks parameter placeholders ($1, $2, ...) and
//! infers their types from surrounding context (column types, operator operands,
//! function signatures). After binding completes, `finalize()` freezes the
//! inferred types or returns an error if any parameter remains unresolved.

use falcon_common::error::SqlError;
use falcon_common::types::DataType;

/// Tracks parameter types during binding.
///
/// Parameters are 1-indexed ($1..$n). Internally stored as 0-indexed vec.
#[derive(Debug, Clone, Default)]
pub struct ParamEnv {
    /// Inferred types for each parameter. `None` means not yet determined.
    param_types: Vec<Option<DataType>>,
}

impl ParamEnv {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a ParamEnv pre-seeded with client-declared types.
    /// OID 0 means "unspecified" → None.
    pub fn with_type_hints(hints: &[DataType]) -> Self {
        let param_types = hints.iter().map(|dt| Some(dt.clone())).collect();
        Self { param_types }
    }

    /// Ensure the param vec is large enough for a 1-indexed parameter.
    /// Returns a mutable reference to the type slot.
    pub fn ensure_param(&mut self, index: usize) -> &mut Option<DataType> {
        debug_assert!(index >= 1, "Parameter index must be >= 1");
        let idx = index - 1;
        if idx >= self.param_types.len() {
            self.param_types.resize(idx + 1, None);
        }
        &mut self.param_types[idx]
    }

    /// Unify a parameter with an expected type from context.
    ///
    /// - If the parameter has no type yet, assign the expected type.
    /// - If the parameter already has a type, check compatibility.
    /// - Compatible means identical or safely promotable.
    pub fn unify_param(
        &mut self,
        index: usize,
        expected: &DataType,
    ) -> Result<(), SqlError> {
        let slot = self.ensure_param(index);
        match slot {
            None => {
                *slot = Some(expected.clone());
                Ok(())
            }
            Some(existing) => {
                if existing == expected {
                    Ok(())
                } else if let Some(promoted) = promote_types(existing, expected) {
                    *slot = Some(promoted);
                    Ok(())
                } else {
                    Err(SqlError::ParamTypeConflict {
                        index,
                        expected: format!("{:?}", expected),
                        got: format!("{:?}", existing),
                    })
                }
            }
        }
    }

    /// Register a parameter without a known type (just track its existence).
    pub fn touch_param(&mut self, index: usize) {
        self.ensure_param(index);
    }

    /// Finalize: all parameters must have resolved types.
    /// Returns the ordered type list or an error for the first unresolved param.
    pub fn finalize(&self) -> Result<Vec<DataType>, SqlError> {
        self.param_types
            .iter()
            .enumerate()
            .map(|(i, opt)| {
                opt.clone().ok_or(SqlError::ParamTypeRequired(i + 1))
            })
            .collect()
    }

    /// Number of parameters seen so far.
    pub fn param_count(&self) -> usize {
        self.param_types.len()
    }

    /// Get the inferred type for a parameter (1-indexed), if any.
    pub fn get_type(&self, index: usize) -> Option<&DataType> {
        if index == 0 || index > self.param_types.len() {
            None
        } else {
            self.param_types[index - 1].as_ref()
        }
    }

    /// Get all inferred types (some may be None if not yet resolved).
    pub fn types(&self) -> &[Option<DataType>] {
        &self.param_types
    }
}

/// Minimal type promotion lattice for parameter unification.
/// Returns the promoted type if two types are compatible, None if conflict.
fn promote_types(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    match (a, b) {
        // Integer promotions
        (Int32, Int64) | (Int64, Int32) => Some(Int64),
        (Int32, Float64) | (Float64, Int32) => Some(Float64),
        (Int64, Float64) | (Float64, Int64) => Some(Float64),
        // Text is flexible — any type can be text-ified
        (Text, _) | (_, Text) => Some(Text),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::DataType;

    #[test]
    fn test_unify_assigns_type() {
        let mut env = ParamEnv::new();
        env.unify_param(1, &DataType::Int32).unwrap();
        assert_eq!(env.get_type(1), Some(&DataType::Int32));
    }

    #[test]
    fn test_unify_same_type_ok() {
        let mut env = ParamEnv::new();
        env.unify_param(1, &DataType::Int32).unwrap();
        env.unify_param(1, &DataType::Int32).unwrap();
        assert_eq!(env.get_type(1), Some(&DataType::Int32));
    }

    #[test]
    fn test_unify_promotion() {
        let mut env = ParamEnv::new();
        env.unify_param(1, &DataType::Int32).unwrap();
        env.unify_param(1, &DataType::Int64).unwrap();
        assert_eq!(env.get_type(1), Some(&DataType::Int64));
    }

    #[test]
    fn test_unify_conflict() {
        let mut env = ParamEnv::new();
        env.unify_param(1, &DataType::Int32).unwrap();
        let err = env.unify_param(1, &DataType::Boolean).unwrap_err();
        match err {
            SqlError::ParamTypeConflict { index, .. } => assert_eq!(index, 1),
            _ => panic!("expected ParamTypeConflict"),
        }
    }

    #[test]
    fn test_finalize_all_resolved() {
        let mut env = ParamEnv::new();
        env.unify_param(1, &DataType::Int32).unwrap();
        env.unify_param(2, &DataType::Text).unwrap();
        let types = env.finalize().unwrap();
        assert_eq!(types, vec![DataType::Int32, DataType::Text]);
    }

    #[test]
    fn test_finalize_gap_unresolved() {
        let mut env = ParamEnv::new();
        env.touch_param(2); // $2 exists but no type
        env.unify_param(1, &DataType::Int32).unwrap();
        let err = env.finalize().unwrap_err();
        match err {
            SqlError::ParamTypeRequired(idx) => assert_eq!(idx, 2),
            _ => panic!("expected ParamTypeRequired"),
        }
    }

    #[test]
    fn test_with_type_hints() {
        let env = ParamEnv::with_type_hints(&[DataType::Int32, DataType::Text]);
        assert_eq!(env.param_count(), 2);
        assert_eq!(env.get_type(1), Some(&DataType::Int32));
        assert_eq!(env.get_type(2), Some(&DataType::Text));
    }

    #[test]
    fn test_text_promotion() {
        let mut env = ParamEnv::new();
        env.unify_param(1, &DataType::Int32).unwrap();
        env.unify_param(1, &DataType::Text).unwrap();
        assert_eq!(env.get_type(1), Some(&DataType::Text));
    }
}
