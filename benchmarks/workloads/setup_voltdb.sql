-- FalconDB Benchmark — VoltDB-compatible schema setup
-- VoltDB uses a subset of SQL; this adapts the shared schema.

-- VoltDB does not support DROP TABLE IF EXISTS in all versions;
-- ignore errors on drop.
-- DROP TABLE transfers IF EXISTS;
-- DROP TABLE accounts IF EXISTS;

CREATE TABLE accounts (
    id       INTEGER NOT NULL,
    balance  BIGINT NOT NULL,
    name     VARCHAR(64) NOT NULL,
    PRIMARY KEY (id)
);
PARTITION TABLE accounts ON COLUMN id;

CREATE TABLE transfers (
    id       INTEGER NOT NULL,
    from_id  INTEGER NOT NULL,
    to_id    INTEGER NOT NULL,
    amount   BIGINT NOT NULL,
    ts       TIMESTAMP DEFAULT NOW,
    PRIMARY KEY (id)
);
PARTITION TABLE transfers ON COLUMN id;

CREATE TABLE bench_events (
    id          INTEGER NOT NULL,
    account_id  INTEGER NOT NULL,
    event_type  VARCHAR(32) NOT NULL,
    amount      BIGINT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW,
    PRIMARY KEY (id)
);
PARTITION TABLE bench_events ON COLUMN id;
