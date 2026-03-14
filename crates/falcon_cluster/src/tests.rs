// falcon_cluster unit tests — split into per-domain modules.
// Each submodule lives in src/tests/<name>.rs

#[cfg(test)]
#[path = "tests/scatter_gather_tests.rs"]
mod scatter_gather_tests;

#[cfg(test)]
#[path = "tests/replication_tests.rs"]
mod replication_tests;

#[cfg(test)]
#[path = "tests/execute_subplan_tests.rs"]
mod execute_subplan_tests;

#[cfg(test)]
#[path = "tests/dist_query_engine_tests.rs"]
mod dist_query_engine_tests;

#[cfg(test)]
#[path = "tests/two_phase_tests.rs"]
mod two_phase_tests;

#[cfg(test)]
#[path = "tests/end_to_end_tests.rs"]
mod end_to_end_tests;

#[cfg(test)]
#[path = "tests/wal_chunk_transport_tests.rs"]
mod wal_chunk_transport_tests;

#[cfg(test)]
#[path = "tests/async_transport_tests.rs"]
mod async_transport_tests;

#[cfg(test)]
#[path = "tests/grpc_transport_tests.rs"]
mod grpc_transport_tests;

#[cfg(test)]
#[path = "tests/promote_fencing_tests.rs"]
mod promote_fencing_tests;

#[cfg(test)]
#[path = "tests/txn_context_tests.rs"]
mod txn_context_tests;

#[cfg(test)]
#[path = "tests/replication_gc_tests.rs"]
mod replication_gc_tests;

#[cfg(test)]
#[path = "tests/replica_runner_tests.rs"]
mod replica_runner_tests;
