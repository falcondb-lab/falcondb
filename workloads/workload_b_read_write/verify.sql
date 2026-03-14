-- Workload B: Read-Write OLTP — Verification

-- 1. User count
SELECT 'user_count' AS check, COUNT(*) AS value FROM users;

-- 2. Order count
SELECT 'order_count' AS check, COUNT(*) AS value FROM orders;

-- 3. PK uniqueness — users
SELECT 'dup_users' AS check, COUNT(*) AS value
FROM (SELECT user_id FROM users GROUP BY user_id HAVING COUNT(*) > 1) d;

-- 4. PK uniqueness — orders
SELECT 'dup_orders' AS check, COUNT(*) AS value
FROM (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1) d;

-- 5. No negative balances (sanity — updates only add)
SELECT 'negative_balances' AS check, COUNT(*) AS value
FROM users WHERE balance < 0;

-- 6. All orders reference valid users
SELECT 'orphan_orders' AS check, COUNT(*) AS value
FROM orders o WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.user_id = o.user_id);
