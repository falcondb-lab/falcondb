# FalconDB — Evidence Pack Index (P0 + P1)

> **Purpose**: This directory contains all evidence artifacts for FalconDB's
> commercial-grade readiness claims. Every conclusion can be traced back to
> a script + raw data.

## Version Binding

All evidence in this pack is bound to:

| Field | Value |
|-------|-------|
| **Version** | Read from `Cargo.toml` `[workspace.package] version` |
| **Git hash** | `git rev-parse --short=8 HEAD` |
| **Collection script** | `scripts/collect_evidence.sh` |

## Evidence Categories

### E1: Versioning (`evidence/versioning/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `version_check_*.txt` | CI version consistency gate output | `scripts/ci_version_check.sh` |
| `binary_version_*.txt` | `falcon --version` output | Binary |
| `cargo_version_*.txt` | Workspace Cargo.toml version | `Cargo.toml` |
| `dist_VERSION_*.txt` | Distribution VERSION file | `dist/VERSION` |

**Claim**: Version is defined in exactly one place. All artifacts derive from it.

**Verification**: Run `scripts/ci_version_check.sh` — must exit 0.

### E2: Failover Determinism (`evidence/failover/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `matrix_results_*.txt` | Full 3×3 matrix test output | `cargo test --test failover_determinism` |
| `summary_*.json` | Machine-readable pass/fail summary | `scripts/run_failover_matrix.sh` |

**Claim**: Under all tested (fault × load) combinations, transaction outcomes are deterministic. Zero phantom commits.

**Verification**: Run `scripts/run_failover_matrix.sh` — must exit 0.

**Report**: `docs/failover_determinism_report.md`

### E3: Benchmarks (`evidence/benchmarks/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `README_*.md` | Benchmark methodology snapshot | `benchmarks/README.md` |
| `RESULTS_*.md` | Results template/baseline | `benchmarks/RESULTS.md` |
| `*_w1_t*.txt` | Raw pgbench output per workload/thread | `benchmarks/scripts/run_workload.sh` |
| `environment_*.txt` | Hardware/OS/version capture | `benchmarks/scripts/run_all.sh` |

**Claim**: FalconDB OLTP performance is reproducible by third parties on identical hardware.

**Verification**: Run `benchmarks/scripts/run_all.sh` — produces comparison report.

### E4: CI Reports (`evidence/ci_reports/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `test_results_*.txt` | Full `cargo test --workspace` output | `scripts/collect_evidence.sh` |
| `summary_*.json` | Test count summary | `scripts/collect_evidence.sh` |

**Claim**: All tests pass on every release.

**Verification**: `cargo test --workspace` — 0 failures.

## How to Use This Pack

### For Investors / Customers

1. Open `evidence/INDEX.md` (this file)
2. For any claim, follow the **Verification** command
3. For raw data, look in the corresponding subdirectory
4. Every file is timestamped and version-tagged

### For CI

```bash
# Verify all evidence artifacts exist (no collection, just check):
./scripts/collect_evidence.sh --verify

# Full collection (runs tests, captures output):
./scripts/collect_evidence.sh
```

### For Releases

```bash
# Before tagging a release:
./scripts/collect_evidence.sh
git add evidence/
git commit -m "evidence: v$(grep -m1 '^version' Cargo.toml | sed 's/.*\"\(.*\)\".*/\1/') evidence pack"
```

---

## P1 Evidence Categories

### E5: Memory Backpressure (`evidence/memory/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `backpressure_test_*.txt` | Memory backpressure integration test output | `cargo test --test memory_backpressure` |
| `governor_stats_*.json` | Governor tier transition counters | `scripts/collect_evidence_p1.sh` |

**Claim**: Under artificial memory pressure, FalconDB throttles/rejects/sheds — never OOMs or silently stalls.

**Verification**: `cargo test -p falcon_storage --test memory_backpressure` — 16 tests, 0 failures.

**Report**: `docs/memory_backpressure.md`

### E6: Long-Run Stability (`evidence/stability/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `72h_report_*.md` | 72-hour stability report | `scripts/run_stability_test.sh --duration 72h` |
| `72h_metrics_*.csv` | RSS, TPS, latency, errors sampled every 60s | Same |
| `7d_report_*.md` | 7-day stability report | `scripts/run_stability_test.sh --duration 7d` |
| `7d_metrics_*.csv` | Same metrics, 7-day window | Same |

**Claim**: FalconDB runs for 72h+ with no memory leaks, no performance degradation, and zero unplanned restarts.

**Verification**: `./scripts/run_stability_test.sh --duration 72h`

### E7: Replication Integrity (`evidence/replication/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `integrity_test_*.txt` | Replication integrity test output | `cargo test --test replication_integrity` |
| `drift_detection_*.json` | Drift detection results | `scripts/collect_evidence_p1.sh` |

**Claim**: Primary–replica data is identical after replication. Drift is detected and actionable.

**Verification**: `cargo test -p falcon_cluster --test replication_integrity` — 5 tests, 0 failures.

**Report**: `docs/replication_integrity.md`

### E8: Operability (`evidence/operability/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `metrics_catalog_*.txt` | Full Prometheus metrics listing | `scripts/collect_evidence_p1.sh` |
| `health_check_*.json` | `/health`, `/ready`, `/status` responses | Same |

**Claim**: An ops engineer can diagnose any common issue using only metrics and logs, without reading source code.

**Report**: `docs/operability_baseline.md`

---

## P2 Evidence Categories

### E9: Industry Focus (`evidence/industry/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `industry_sla_*.md` | SLA document snapshot | `docs/industry_sla.md` |
| `traceability_*.json` | SLA claim → evidence mapping | `scripts/collect_evidence_p2.sh` |

**Claim**: FalconDB targets Financial Trading OLTP exclusively. Every SLA is evidence-backed.

**Report**: `docs/industry_focus.md` + `docs/industry_edition_overview.md`

### E10: Industry SLA & Comparisons (`evidence/industry/sla/` + `evidence/industry/comparisons/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `traceability_*.json` | SLA → evidence path mapping | `scripts/collect_evidence_p2.sh` |
| `comparison_summary_*.json` | FalconDB vs PG/Oracle structured comparison | `scripts/collect_evidence_p2.sh` |

**Claim**: FalconDB delivers 5–20× lower P99 write latency than PostgreSQL, zero phantom commits, and Apache 2.0 vs $47.5K/core Oracle licensing.

**Verification**: `./benchmarks/scripts/run_all.sh` + `cargo test -p falcon_cluster --test failover_determinism`

**Report**: `docs/industry_sla.md`

### E11: Commercial Model (`docs/commercial_model.md`)

**Claim**: Clear Core (free, Apache 2.0) vs Financial Edition (paid) boundary. Customer value equation quantified.

### E12: PoC Playbook (`evidence/industry/poc/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `poc_playbook_*.md` | PoC process snapshot | `docs/industry_poc_playbook.md` |

**Claim**: Any engineer can execute a 2–4 week customer PoC using only pre-packaged materials.

**Report**: `docs/industry_poc_playbook.md`

---

## v1.2 Evidence Categories

### E13: v1.2 Performance Baseline (`evidence/benchmarks/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `v1.2_baseline_*.md` | v1.2 performance baseline snapshot | `docs/benchmarks/v1.2_baseline.md` |
| `v1.2_w1_*.txt` | W1 single-shard OLTP raw pgbench output | `benchmarks/scripts/run_workload.sh` |
| `v1.2_w2_*.txt` | W2 cross-shard raw pgbench output | `benchmarks/scripts/run_workload.sh` |
| `v1.2_w4_groupby_*.txt` | W4 GROUP BY benchmark (new in v1.2) | `benchmarks/scripts/run_workload.sh` |
| `v1.2_w5_vectorized_*.txt` | W5 vectorized comparison benchmark (new in v1.2) | `benchmarks/scripts/run_workload.sh` |

**Claim**: v1.2 executor optimizations (HashMap GROUP BY, byte-encoded keys, direct Datum comparison) produce measurable improvement over v1.0 baseline.

**Verification**: `benchmarks/scripts/run_all.sh` + compare against `docs/benchmarks/v1.0_baseline.md`

**Report**: `docs/benchmarks/v1.2_baseline.md`

### E14: OS Platform Tuning (`docs/os/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `linux_ubuntu_24_04.md` | Ubuntu 24.04 LTS tuning guide | `docs/os/linux_ubuntu_24_04.md` |
| `rhel_9.md` | RHEL 9 / Rocky Linux 9 tuning guide | `docs/os/rhel_9.md` |
| `windows_server_2022.md` | Windows Server 2022 tuning guide | `docs/os/windows_server_2022.md` |

**Claim**: FalconDB provides platform-specific tuning for all three supported OS families, covering I/O, memory, scheduling, networking, and security.

### E15: Cluster Observability (`docs/observability/`)

| Artifact | Description | Source |
|----------|-------------|--------|
| `segment_streaming_metrics` | 7 Prometheus metrics for replication | `falcon_observability::record_segment_streaming_metrics` |
| `rebalancer_metrics` | 9 Prometheus metrics for shard rebalancing | `falcon_observability::record_rebalancer_metrics` |
| `grafana_dashboard.json` | Grafana dashboard for cluster | `docs/observability/grafana_dashboard.json` |
| `alerting_rules.yml` | 25 Prometheus alerting rules | `docs/observability/alerting_rules.yml` |

**Claim**: All cluster components (replication, rebalancing, gateway, discovery) emit Prometheus metrics. Grafana dashboard and alerting rules are provided.

---

## Completeness Checklist

| # | Requirement | Status | Artifact |
|---|------------|--------|----------|
| P0-1 | Version is single-source | ✅ | `evidence/versioning/` |
| P0-2 | Failover txn fate provable | ✅ | `evidence/failover/` |
| P0-3 | Benchmarks reproducible | ✅ | `evidence/benchmarks/` |
| P0-4 | Evidence pack complete | ✅ | This file |
| P1-1 | Memory controllable (backpressure) | ✅ | `evidence/memory/` |
| P1-2 | 72h / 7d stability verified | ✅ | `evidence/stability/` |
| P1-3 | Primary–replica long-term consistent | ✅ | `evidence/replication/` |
| P1-4 | Ops-observable without source code | ✅ | `evidence/operability/` |
| P1-5 | P1 evidence pack complete | ✅ | This file |
| P2-1 | Industry positioning unique and clear | ✅ | `docs/industry_focus.md` |
| P2-2 | Industry edition product defined | ✅ | `docs/industry_edition_overview.md` |
| P2-3 | SLA with replacement evidence | ✅ | `docs/industry_sla.md` |
| P2-4 | Commercial model with license boundary | ✅ | `docs/commercial_model.md` |
| P2-5 | Repeatable PoC playbook | ✅ | `docs/industry_poc_playbook.md` |
| P2-6 | P2 evidence pack complete | ✅ | `evidence/industry/` |
