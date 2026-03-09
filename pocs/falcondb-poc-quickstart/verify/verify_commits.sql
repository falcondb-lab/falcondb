-- FalconDB Failover-Under-Load: Post-failover consistency verification
-- Run against any surviving node after failover.

-- 1. Total committed rows
SELECT 'total_rows' AS check, COUNT(*)::TEXT AS value FROM tx_commits;

-- 2. Min / Max commit_id (should be 1..N with no gaps)
SELECT 'id_range' AS check,
       MIN(commit_id)::TEXT || '..' || MAX(commit_id)::TEXT AS value
FROM tx_commits;

-- 3. Gap detection — any missing commit_ids in the sequence?
SELECT 'missing_ids' AS check,
       COALESCE(string_agg(gap::TEXT, ','), 'none') AS value
FROM (
    SELECT generate_series AS gap
    FROM generate_series(
        (SELECT MIN(commit_id) FROM tx_commits),
        (SELECT MAX(commit_id) FROM tx_commits)
    )
    WHERE generate_series NOT IN (SELECT commit_id FROM tx_commits)
    LIMIT 20
) gaps;

-- 4. Duplicate detection
SELECT 'duplicates' AS check,
       COUNT(*)::TEXT AS value
FROM (
    SELECT commit_id FROM tx_commits
    GROUP BY commit_id HAVING COUNT(*) > 1
) dups;
