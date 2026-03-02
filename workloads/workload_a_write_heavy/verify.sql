-- Workload A: Write-Heavy — Verification
-- Run after workload completes. All checks must pass.

-- 1. Row count
SELECT 'row_count' AS check, COUNT(*)::TEXT AS value FROM orders;

-- 2. PK uniqueness (must be 0)
SELECT 'duplicate_pks' AS check, COUNT(*)::TEXT AS value
FROM (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1) d;

-- 3. No NULL PKs
SELECT 'null_pks' AS check, COUNT(*)::TEXT AS value
FROM orders WHERE order_id IS NULL;

-- 4. No NULL required fields
SELECT 'null_customer_ids' AS check, COUNT(*)::TEXT AS value
FROM orders WHERE customer_id IS NULL;

-- 5. Amount range sanity
SELECT 'amount_range' AS check,
       MIN(amount)::TEXT || '..' || MAX(amount)::TEXT AS value
FROM orders;
