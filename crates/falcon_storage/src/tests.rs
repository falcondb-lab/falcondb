// falcon_storage unit tests — split into per-domain modules.
// Each submodule lives in src/tests/<name>.rs

#[cfg(test)]
#[path = "tests/mvcc_tests.rs"]
mod mvcc_tests;

#[cfg(test)]
#[path = "tests/memtable_tests.rs"]
mod memtable_tests;

#[cfg(test)]
#[path = "tests/wal_tests.rs"]
mod wal_tests;

#[cfg(test)]
#[path = "tests/engine_tests.rs"]
mod engine_tests;

#[cfg(test)]
#[path = "tests/occ_tests.rs"]
mod occ_tests;

#[cfg(test)]
#[path = "tests/engine_occ_tests.rs"]
mod engine_occ_tests;

#[cfg(test)]
#[path = "tests/secondary_index_tests.rs"]
mod secondary_index_tests;

#[cfg(test)]
#[path = "tests/recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
#[path = "tests/write_set_tests.rs"]
mod write_set_tests;

#[cfg(test)]
#[path = "tests/checkpoint_tests.rs"]
mod checkpoint_tests;

#[cfg(test)]
#[path = "tests/bench_tests.rs"]
mod bench_tests;

#[cfg(test)]
#[path = "tests/gc_tests.rs"]
mod gc_tests;

#[cfg(test)]
#[path = "tests/wal_observer_tests.rs"]
mod wal_observer_tests;

#[cfg(all(test, feature = "columnstore"))]
#[path = "tests/columnstore_integration_tests.rs"]
mod columnstore_integration_tests;

#[cfg(all(test, feature = "disk_rowstore"))]
#[path = "tests/disk_rowstore_integration_tests.rs"]
mod disk_rowstore_integration_tests;

#[cfg(test)]
#[path = "tests/si_litmus_tests.rs"]
mod si_litmus_tests;

#[cfg(test)]
#[path = "tests/ddl_concurrency_tests.rs"]
mod ddl_concurrency_tests;
