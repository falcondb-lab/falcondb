# FalconDB Financial Edition — PoC Playbook

> A step-by-step, repeatable process for running a customer Proof of Concept.
> Any engineer can execute this without special training.

---

## 1. PoC Parameters

| Parameter | Value |
|-----------|-------|
| **Duration** | 2–4 weeks |
| **Effort** | 1 FalconDB engineer (part-time) + 1 customer engineer |
| **Hardware** | Customer-provided or cloud VM (8+ cores, 16+ GB RAM, NVMe SSD) |
| **Success criterion** | Measurable, agreed upon in Week 0 |
| **Deliverable** | Written PoC report with pass/fail per criterion |

## 2. Timeline

### Week 0: Qualification & Setup (2 days)

**Goal**: Confirm fit, agree on success criteria, provision environment.

| Step | Owner | Deliverable |
|------|-------|-------------|
| 1. Discovery call: understand customer's current system | Both | Meeting notes |
| 2. Confirm use case matches UC-1/UC-2/UC-3 | FalconDB | Go/No-Go decision |
| 3. Define success criteria (see §3) | Both | Signed criteria document |
| 4. Provision hardware (2 nodes: primary + standby) | Customer | SSH access |
| 5. Deploy FalconDB with financial defaults | FalconDB | Running cluster |
| 6. Verify PG wire connectivity (psql, JDBC) | Customer | Connection confirmed |

**No-Go triggers** (abort PoC immediately):
- Customer needs JOINs across 10+ tables → not a fit for v1.x
- Customer needs OLAP/analytics → wrong product category
- Customer cannot provide hardware within 1 week → not serious

### Week 1: Schema & Workload (3 days)

**Goal**: Customer's actual schema and representative workload running.

| Step | Owner | Deliverable |
|------|-------|-------------|
| 1. Customer provides schema DDL | Customer | `.sql` file |
| 2. Adapt schema for FalconDB (minor adjustments) | FalconDB | Modified DDL |
| 3. Customer provides sample data (10K–100K rows) | Customer | Data file |
| 4. Load data via `psql` / JDBC | Customer | Data loaded |
| 5. Customer runs representative queries | Customer | Query log |
| 6. Verify correctness (SELECT results match expected) | Both | Correctness confirmed |

### Week 2: Performance & Resilience (5 days)

**Goal**: Measure latency, throughput, failover behavior.

| Day | Test | Tool | Metric |
|-----|------|------|--------|
| Day 1 | Baseline: single-thread write latency | pgbench | P50/P99/P99.9 |
| Day 2 | Throughput: 4/8/16 thread write | pgbench | TPS at each concurrency |
| Day 3 | Mixed workload: customer's actual txn pattern | Custom script | TPS + latency |
| Day 4 | Failover: kill primary, measure recovery | Manual | Failover time, data loss |
| Day 5 | Stress: 2× peak load for 1 hour | pgbench | Backpressure behavior |

### Week 3 (Optional): Extended Stability

| Test | Duration | Metric |
|------|----------|--------|
| Continuous write + monitor | 72 hours | RSS, TPS drift, error rate |
| Replication integrity check | After 72h | Row count + checksum match |

### Week 4: Report & Decision

| Step | Owner | Deliverable |
|------|-------|-------------|
| 1. Compile results into PoC report | FalconDB | `poc_report_<customer>.md` |
| 2. Review report with customer | Both | Meeting |
| 3. Customer decision: proceed / no-go | Customer | Written decision |

## 3. Success Criteria Template

Agree on these **before** starting Week 1. All criteria are measurable.

| # | Criterion | Target | Measurement |
|---|-----------|--------|-------------|
| C1 | Write latency P99 | < X ms | pgbench W1 output |
| C2 | Write throughput at N threads | > Y TPS | pgbench W1 output |
| C3 | Mixed txn latency P99 | < X ms | Custom workload script |
| C4 | Failover data loss | 0 rows | Count before/after failover |
| C5 | Failover recovery time | < X seconds | Measured from kill to first successful query |
| C6 | Memory under stress | No OOM, no crash | 1-hour stress test at 2× peak |
| C7 | PG client compatibility | Customer's driver works | Customer runs their application |

**Pass**: All C1–C7 met.
**Conditional pass**: C1–C5 met, C6/C7 with known workaround.
**Fail**: Any of C1–C5 not met.

## 4. PoC Report Template

```markdown
# FalconDB PoC Report — [Customer Name]

| Field | Value |
|-------|-------|
| Customer | [Name] |
| Use Case | [UC-1/UC-2/UC-3] |
| Duration | [Start] — [End] |
| FalconDB Version | [Version] |
| Hardware | [CPU, RAM, Disk] |

## Results

| Criterion | Target | Actual | Status |
|-----------|--------|--------|--------|
| C1: Write P99 | < 1ms | X ms | ✅/❌ |
| C2: Throughput | > 10K TPS | X TPS | ✅/❌ |
| C3: Mixed P99 | < 2ms | X ms | ✅/❌ |
| C4: Failover loss | 0 | X rows | ✅/❌ |
| C5: Recovery time | < 3s | X s | ✅/❌ |
| C6: Stress test | No crash | [Result] | ✅/❌ |
| C7: PG compat | Works | [Result] | ✅/❌ |

## Conclusion

[PASS / CONDITIONAL PASS / FAIL]

## Recommendations

[Next steps if pass: licensing, deployment timeline, support tier]
```

## 5. Required Materials (Pre-packaged)

| Material | Path | Purpose |
|----------|------|---------|
| Financial defaults config | `benchmarks/falcondb/config.toml` | Zero-tuning start |
| Benchmark suite | `benchmarks/scripts/run_all.sh` | Reproducible perf test |
| Stability test | `scripts/run_stability_test.sh` | 72h endurance |
| Failover test | `scripts/run_failover_matrix.sh` | Determinism proof |
| Evidence pack | `evidence/` | Pre-existing proof points |
| Architecture docs | `docs/` | Self-service understanding |

## 6. Cost Control

| Item | Budget |
|------|--------|
| FalconDB engineer time | ≤ 40 hours total |
| Hardware | Customer-provided (or 2× cloud VMs ≈ $200/month) |
| License | $0 (PoC uses Core, Apache 2.0) |
| Custom development | $0 (PoC uses existing features only) |

**Total PoC cost**: < $10K (engineer time) + customer hardware.

## 7. Anti-Patterns (DO NOT)

| Anti-pattern | Why |
|-------------|-----|
| ❌ Custom features for one customer | Creates tech debt; delays product |
| ❌ PoC longer than 4 weeks | Customer is not serious |
| ❌ PoC without written success criteria | Goalpost will move |
| ❌ PoC on hardware below minimum spec | Results will be misleading |
| ❌ Comparing against PG with default config | PG must be tuned for fair comparison |
| ❌ Promising features not yet built | Only demo what exists today |

## 8. Post-PoC Conversion Path

| PoC Result | Next Step | Timeline |
|-----------|-----------|----------|
| **Pass** | Commercial proposal (Financial Edition) | 2 weeks |
| **Conditional pass** | Fix gaps, re-test specific criteria | 2–4 weeks |
| **Fail** | Post-mortem, feed back to product | Immediate |
