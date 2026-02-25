-- FalconDB Benchmark — Schema Setup
-- Used by both FalconDB and PostgreSQL targets

DROP TABLE IF EXISTS transfers;
DROP TABLE IF EXISTS accounts;

-- W1 + W2: accounts table
CREATE TABLE accounts (
    id       INT PRIMARY KEY,
    balance  BIGINT NOT NULL DEFAULT 0,
    name     TEXT NOT NULL
);

-- W2: transfers table
CREATE TABLE transfers (
    id       SERIAL PRIMARY KEY,
    from_id  INT NOT NULL,
    to_id    INT NOT NULL,
    amount   BIGINT NOT NULL,
    ts       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Pre-load 100K accounts
INSERT INTO accounts (id, balance, name)
SELECT
    g,
    1000 + (g % 9000),
    'user_' || g
FROM generate_series(1, 100000) AS g;
