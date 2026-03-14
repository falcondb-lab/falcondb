//! P3-8: Ecosystem compatibility — PG driver matrix, migration tools, benchmark framework.
//!
//! Provides type infrastructure for:
//! - PostgreSQL driver/ORM compatibility tracking
//! - Data migration tool types (PG import, consistency verification)
//! - Performance benchmark report framework

use serde::{Deserialize, Serialize};
use std::fmt;

// ── PG Driver / ORM Compatibility Matrix ──

/// Compatibility status for a driver or ORM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatStatus {
    /// Fully compatible — all features work.
    Full,
    /// Partially compatible — basic features work, some advanced features unsupported.
    Partial,
    /// Incompatible — does not work.
    Incompatible,
    /// Not yet tested.
    Untested,
}

impl fmt::Display for CompatStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Partial => write!(f, "partial"),
            Self::Incompatible => write!(f, "incompatible"),
            Self::Untested => write!(f, "untested"),
        }
    }
}

/// A single entry in the compatibility matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatEntry {
    /// Driver / ORM name (e.g. "psycopg2", "JDBC", "sqlx").
    pub name: String,
    /// Language ecosystem (e.g. "Python", "Java", "Rust").
    pub language: String,
    /// Tested version.
    pub version: String,
    /// Compatibility status.
    pub status: CompatStatus,
    /// Known limitations or notes.
    pub notes: String,
}

/// The full PG compatibility matrix.
pub fn pg_compat_matrix() -> Vec<CompatEntry> {
    vec![
        CompatEntry {
            name: "psycopg2".into(),
            language: "Python".into(),
            version: "2.9+".into(),
            status: CompatStatus::Full,
            notes: "Standard PG wire protocol, full support.".into(),
        },
        CompatEntry {
            name: "psycopg3".into(),
            language: "Python".into(),
            version: "3.1+".into(),
            status: CompatStatus::Full,
            notes: "Extended query protocol supported.".into(),
        },
        CompatEntry {
            name: "asyncpg".into(),
            language: "Python".into(),
            version: "0.28+".into(),
            status: CompatStatus::Partial,
            notes: "Binary protocol partially supported.".into(),
        },
        CompatEntry {
            name: "JDBC (pgjdbc)".into(),
            language: "Java".into(),
            version: "42.6+".into(),
            status: CompatStatus::Full,
            notes: "Standard PG JDBC driver, full support.".into(),
        },
        CompatEntry {
            name: "node-postgres".into(),
            language: "JavaScript".into(),
            version: "8.11+".into(),
            status: CompatStatus::Full,
            notes: "Simple and extended query protocol.".into(),
        },
        CompatEntry {
            name: "go-pq".into(),
            language: "Go".into(),
            version: "1.10+".into(),
            status: CompatStatus::Full,
            notes: "Standard PG Go driver.".into(),
        },
        CompatEntry {
            name: "pgx".into(),
            language: "Go".into(),
            version: "5.4+".into(),
            status: CompatStatus::Partial,
            notes: "Extended query protocol, some type coercions pending.".into(),
        },
        CompatEntry {
            name: "sqlx".into(),
            language: "Rust".into(),
            version: "0.7+".into(),
            status: CompatStatus::Partial,
            notes: "Compile-time query checking requires pg_catalog support.".into(),
        },
        CompatEntry {
            name: "tokio-postgres".into(),
            language: "Rust".into(),
            version: "0.7+".into(),
            status: CompatStatus::Full,
            notes: "Async PG driver, full wire protocol support.".into(),
        },
        CompatEntry {
            name: "libpq (C)".into(),
            language: "C".into(),
            version: "15+".into(),
            status: CompatStatus::Full,
            notes: "Native PG client library.".into(),
        },
        // ORMs
        CompatEntry {
            name: "SQLAlchemy".into(),
            language: "Python".into(),
            version: "2.0+".into(),
            status: CompatStatus::Partial,
            notes: "Basic CRUD works. Advanced DDL introspection limited.".into(),
        },
        CompatEntry {
            name: "Django ORM".into(),
            language: "Python".into(),
            version: "4.2+".into(),
            status: CompatStatus::Partial,
            notes: "Migrations may require manual adaptation.".into(),
        },
        CompatEntry {
            name: "Hibernate".into(),
            language: "Java".into(),
            version: "6.2+".into(),
            status: CompatStatus::Partial,
            notes: "Use PostgreSQL dialect. Sequence generation may differ.".into(),
        },
        CompatEntry {
            name: "GORM".into(),
            language: "Go".into(),
            version: "2.0+".into(),
            status: CompatStatus::Partial,
            notes: "Basic operations supported. AutoMigrate has limitations.".into(),
        },
    ]
}

// ── Migration Tool Types ──

/// Migration source type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationSource {
    PostgreSQL,
    MySQL,
    CsvFile,
    JsonFile,
}

impl fmt::Display for MigrationSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PostgreSQL => write!(f, "postgresql"),
            Self::MySQL => write!(f, "mysql"),
            Self::CsvFile => write!(f, "csv"),
            Self::JsonFile => write!(f, "json"),
        }
    }
}

/// Status of a data migration job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationJobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for MigrationJobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Metadata for a data migration job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationJob {
    /// Unique job ID.
    pub job_id: String,
    /// Source system.
    pub source: MigrationSource,
    /// Source connection string or file path.
    pub source_uri: String,
    /// Target tenant ID.
    pub target_tenant_id: u64,
    /// Tables to migrate (empty = all).
    pub tables: Vec<String>,
    /// Current status.
    pub status: MigrationJobStatus,
    /// Rows migrated so far.
    pub rows_migrated: u64,
    /// Rows failed.
    pub rows_failed: u64,
    /// Error message if status == Failed.
    pub error: Option<String>,
    /// Start time (unix seconds).
    pub started_at: u64,
    /// Duration so far (seconds).
    pub duration_secs: u64,
}

/// Result of a data consistency check between source and target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyCheckResult {
    /// Table being verified.
    pub table_name: String,
    /// Number of rows in source.
    pub source_row_count: u64,
    /// Number of rows in target.
    pub target_row_count: u64,
    /// Whether row counts match.
    pub count_match: bool,
    /// Number of rows with content mismatches (sampled).
    pub content_mismatches: u64,
    /// Whether the check passed.
    pub passed: bool,
}

// ── Benchmark Framework ──

/// Benchmark workload type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BenchmarkWorkload {
    /// TPC-C-like OLTP workload.
    TpcC,
    /// YCSB workload (key-value operations).
    Ycsb,
    /// Point read/write microbenchmark.
    PointOps,
    /// Mixed read-write workload.
    MixedReadWrite,
    /// Custom user-defined workload.
    Custom,
}

impl fmt::Display for BenchmarkWorkload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TpcC => write!(f, "tpc-c"),
            Self::Ycsb => write!(f, "ycsb"),
            Self::PointOps => write!(f, "point-ops"),
            Self::MixedReadWrite => write!(f, "mixed-rw"),
            Self::Custom => write!(f, "custom"),
        }
    }
}

/// Results of a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// Workload type.
    pub workload: BenchmarkWorkload,
    /// Duration of the benchmark (seconds).
    pub duration_secs: u64,
    /// Number of threads / clients.
    pub threads: u32,
    /// Total operations completed.
    pub total_ops: u64,
    /// Throughput (operations per second).
    pub ops_per_sec: f64,
    /// Average latency (microseconds).
    pub avg_latency_us: u64,
    /// p50 latency (microseconds).
    pub p50_latency_us: u64,
    /// p95 latency (microseconds).
    pub p95_latency_us: u64,
    /// p99 latency (microseconds).
    pub p99_latency_us: u64,
    /// Maximum latency (microseconds).
    pub max_latency_us: u64,
    /// Error count.
    pub errors: u64,
    /// FalconDB version tested.
    pub version: String,
    /// Number of nodes in the cluster.
    pub node_count: u32,
    /// Number of shards.
    pub shard_count: u32,
}

impl BenchmarkReport {
    /// Error rate as a percentage.
    pub fn error_rate(&self) -> f64 {
        if self.total_ops == 0 {
            return 0.0;
        }
        (self.errors as f64 / self.total_ops as f64) * 100.0
    }

    /// Format as a compact summary string.
    pub fn summary(&self) -> String {
        format!(
            "{} | {}s | {} threads | {:.0} ops/s | p50={}us p99={}us | errors={}",
            self.workload,
            self.duration_secs,
            self.threads,
            self.ops_per_sec,
            self.p50_latency_us,
            self.p99_latency_us,
            self.errors,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compat_matrix_populated() {
        let matrix = pg_compat_matrix();
        assert!(matrix.len() >= 10);
        let psycopg2 = matrix.iter().find(|e| e.name == "psycopg2").unwrap();
        assert_eq!(psycopg2.status, CompatStatus::Full);
        assert_eq!(psycopg2.language, "Python");
    }

    #[test]
    fn test_compat_status_display() {
        assert_eq!(CompatStatus::Full.to_string(), "full");
        assert_eq!(CompatStatus::Partial.to_string(), "partial");
    }

    #[test]
    fn test_migration_source_display() {
        assert_eq!(MigrationSource::PostgreSQL.to_string(), "postgresql");
        assert_eq!(MigrationSource::CsvFile.to_string(), "csv");
    }

    #[test]
    fn test_benchmark_report_summary() {
        let report = BenchmarkReport {
            workload: BenchmarkWorkload::TpcC,
            duration_secs: 60,
            threads: 16,
            total_ops: 120000,
            ops_per_sec: 2000.0,
            avg_latency_us: 500,
            p50_latency_us: 400,
            p95_latency_us: 1200,
            p99_latency_us: 3000,
            max_latency_us: 15000,
            errors: 5,
            version: "0.2.0".into(),
            node_count: 3,
            shard_count: 6,
        };
        let summary = report.summary();
        assert!(summary.contains("tpc-c"));
        assert!(summary.contains("2000 ops/s"));
        assert!((report.error_rate() - 0.004166).abs() < 0.001);
    }

    #[test]
    fn test_benchmark_workload_display() {
        assert_eq!(BenchmarkWorkload::Ycsb.to_string(), "ycsb");
        assert_eq!(BenchmarkWorkload::PointOps.to_string(), "point-ops");
    }

    #[test]
    fn test_consistency_check_result() {
        let result = ConsistencyCheckResult {
            table_name: "users".into(),
            source_row_count: 1000,
            target_row_count: 1000,
            count_match: true,
            content_mismatches: 0,
            passed: true,
        };
        assert!(result.passed);
        assert!(result.count_match);
    }

    #[test]
    fn test_migration_job() {
        let job = MigrationJob {
            job_id: "mig-001".into(),
            source: MigrationSource::PostgreSQL,
            source_uri: "postgres://localhost:5432/mydb".into(),
            target_tenant_id: 1,
            tables: vec!["users".into(), "orders".into()],
            status: MigrationJobStatus::Running,
            rows_migrated: 5000,
            rows_failed: 2,
            error: None,
            started_at: 1700000000,
            duration_secs: 30,
        };
        assert_eq!(job.status, MigrationJobStatus::Running);
        assert_eq!(job.tables.len(), 2);
    }
}
