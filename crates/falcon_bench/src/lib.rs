// crates/falcon_bench/src/lib.rs
//
// 公共 bench 基础设施。由 benches/ 下各 Criterion bench 文件 use 本 crate。

pub mod dataset;
pub mod metrics;
pub mod regression;
pub mod runner;
pub mod workload;
