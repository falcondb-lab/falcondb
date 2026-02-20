#[path = "eval/mod.rs"]
pub mod eval;
pub mod expr_engine;
pub mod executor;
mod executor_aggregate;
mod executor_copy;
mod executor_dml;
mod executor_join;
mod executor_query;
mod executor_setops;
mod executor_subquery;
mod executor_window;
pub mod external_sort;
pub mod parallel;
pub mod param_subst;
pub mod vectorized;
#[cfg(test)]
mod tests;

pub use executor::{ExecutionResult, Executor};
