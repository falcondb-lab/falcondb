-- Workload B: Read-Write OLTP — Schema
-- Two tables: users (account) + orders (transaction log)

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id   BIGINT PRIMARY KEY,
    username  TEXT NOT NULL,
    balance   BIGINT NOT NULL DEFAULT 0,
    status    TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE orders (
    order_id    BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    amount      BIGINT NOT NULL,
    order_type  TEXT NOT NULL DEFAULT 'purchase',
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed 50K users
INSERT INTO users (user_id, username, balance, status)
SELECT
    g,
    'u_' || g,
    10000 + (g % 90000),
    CASE WHEN g % 20 = 0 THEN 'inactive' ELSE 'active' END
FROM generate_series(1, 50000) AS g;

-- Seed 100K orders
INSERT INTO orders (order_id, user_id, amount, order_type)
SELECT
    g,
    (g % 50000) + 1,
    50 + (g % 5000),
    CASE WHEN g % 3 = 0 THEN 'refund' ELSE 'purchase' END
FROM generate_series(1, 100000) AS g;
