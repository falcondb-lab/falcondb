-- FalconDB Benchmark — W3: Analytic Scan
-- Full-table and range scans with aggregation.
--
-- Tests scan throughput and aggregation performance.
-- Every engine should be able to run these queries without indexes
-- beyond the primary key.
--
-- Usage: Called by scripts/run_workload.sh, not run directly.

-- ── Range scan with aggregation ──
-- Scan ~10% of accounts and compute stats
\set lo random(1, 90000)
\set hi :lo + 10000
SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance)
  FROM accounts
 WHERE id BETWEEN :lo AND :hi;
