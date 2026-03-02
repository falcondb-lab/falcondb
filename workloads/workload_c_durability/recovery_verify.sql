-- Workload C: Post-crash recovery verification
-- Run against the restarted server AFTER crash + recovery.
-- Every check must pass for the workload to be considered correct.

-- 1. Total surviving rows
SELECT 'surviving_rows' AS check, COUNT(*)::TEXT AS value FROM commit_log;

-- 2. PK uniqueness (no double commit)
SELECT 'duplicate_pks' AS check, COUNT(*)::TEXT AS value
FROM (SELECT seq_id FROM commit_log GROUP BY seq_id HAVING COUNT(*) > 1) d;

-- 3. Monotonic ID continuity — find gaps (lost commits)
--    Compare against the client-side committed log.
--    Any seq_id present in client log but absent here = DATA LOSS.
SELECT 'max_seq_id' AS check, COALESCE(MAX(seq_id), 0)::TEXT AS value FROM commit_log;

-- 4. No rows with NULL PK
SELECT 'null_pks' AS check, COUNT(*)::TEXT AS value
FROM commit_log WHERE seq_id IS NULL;

-- 5. No dirty reads — all rows should have valid payload format
SELECT 'corrupt_rows' AS check, COUNT(*)::TEXT AS value
FROM commit_log WHERE payload IS NULL OR payload = '';
