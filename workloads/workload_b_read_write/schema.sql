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

-- Seed data is loaded by run.ps1 (batch INSERT VALUES)
-- FalconDB does not support INSERT...SELECT FROM generate_series
