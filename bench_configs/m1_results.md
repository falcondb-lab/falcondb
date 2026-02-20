# Falcon M1 Benchmark Results (Release Build)

Machine: Windows, single-process, in-memory (no WAL)
Seed: 42, Isolation: ReadCommitted
Date: 2026-02-17

## Config 1: High Local (90:10)

| Metric | Value |
|--------|-------|
| Ops | 20,000 |
| TPS | 5,961 |
| Fast-path commits | 19,045 |
| Slow-path commits | 1,955 |
| Fast p50/p95/p99 (µs) | 154 / 192 / 274 |
| Slow p50/p95/p99 (µs) | 154 / 190 / 272 |
| OCC conflicts | 0 |

## Config 2: Balanced (50:50)

| Metric | Value |
|--------|-------|
| Ops | 20,000 |
| TPS | 5,833 |
| Fast-path commits | 10,872 |
| Slow-path commits | 10,128 |
| All p50/p95/p99 (µs) | 158 / 195 / 237 |
| OCC conflicts | 0 |

## Chart 1: TPS vs Shard Count (scatter/gather)

| Shards | TPS | Scatter total (µs) | Max subplan (µs) | Gather merge (µs) |
|--------|-----|---------------------|-------------------|--------------------|
| 1 | 3,643 | 2,592,240 | 706 | 145,607 |
| 2 | 4,100 | 2,286,933 | 422 | 204,850 |
| 4 | 4,403 | 2,104,446 | 487 | 175,061 |
| 8 | 3,771 | 2,458,215 | 437 | 211,045 |

Peak at 4 shards (4,403 TPS). Diminishing returns at 8 due to gather-merge overhead.

## Chart 2: Fast-Path ON vs OFF (p99 latency)

| Mode | TPS | Fast p99 (µs) | Slow p99 (µs) | All p99 (µs) |
|------|-----|---------------|---------------|--------------|
| Fast-Path ON | 5,834 | 245 | 249 | 247 |
| Fast-Path OFF | 5,908 | — | 226 | 226 |

Note: In single-process benchmarks, fast-path overhead is minimal since
there's no network 2PC. The fast-path benefit is most visible in multi-node
deployments (M2+) where 2PC adds 2 extra RTTs.

## Chart 3: Failover Before/After

| Phase | Ops | TPS | p50 (ns) | p95 (ns) | p99 (ns) | Data intact |
|-------|-----|-----|----------|----------|----------|-------------|
| Before | 5,000 | 5,855 | 168,500 | 200,100 | 238,000 | true |
| After | 5,000 | 5,675 | 173,200 | 212,500 | 258,700 | true |

Post-failover TPS drop: ~3%. Data integrity verified (zero data loss).
