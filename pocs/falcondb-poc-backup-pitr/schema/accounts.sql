-- ============================================================================
-- FalconDB PoC #7 — Backup & PITR: Account Schema
-- ============================================================================
-- Simple, deterministic schema for verifiable recovery.
-- Every balance change is an explicit committed transaction.
-- ============================================================================

CREATE SEQUENCE accounts_id_seq START 1;

CREATE TABLE accounts (
    account_id   BIGINT PRIMARY KEY DEFAULT nextval('accounts_id_seq'),
    balance      BIGINT        NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP     DEFAULT NOW()
);

-- Seed 100 accounts with known starting balances
INSERT INTO accounts (account_id, balance, updated_at)
SELECT id, 10000, NOW()
FROM generate_series(1, 100) AS id;

-- Reset sequence past seeded IDs
SELECT setval('accounts_id_seq', 100);
