-- FalconDB Benchmark — SingleStore-compatible schema setup
-- SingleStore uses MySQL-compatible SQL with rowstore/columnstore extensions.
-- We use ROWSTORE tables for fair OLTP comparison.

DROP TABLE IF EXISTS transfers;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS bench_events;

-- W1 + W2: accounts table (rowstore for OLTP)
CREATE ROWSTORE TABLE accounts (
    id       INT NOT NULL,
    balance  BIGINT NOT NULL DEFAULT 0,
    name     TEXT NOT NULL,
    PRIMARY KEY (id)
);

-- W2: transfers table (rowstore)
CREATE ROWSTORE TABLE transfers (
    id       BIGINT AUTO_INCREMENT,
    from_id  INT NOT NULL,
    to_id    INT NOT NULL,
    amount   BIGINT NOT NULL,
    ts       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- W5: batch insert target (rowstore)
CREATE ROWSTORE TABLE bench_events (
    id          INT NOT NULL,
    account_id  INT NOT NULL,
    event_type  VARCHAR(32) NOT NULL,
    amount      BIGINT NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Pre-load 100K accounts
-- SingleStore supports INSERT ... SELECT with sequence generation
DELIMITER //
CREATE OR REPLACE PROCEDURE load_accounts()
AS
BEGIN
    DECLARE i INT = 1;
    WHILE i <= 100000 DO
        INSERT INTO accounts (id, balance, name)
        VALUES (i, 1000 + (i % 9000), CONCAT('user_', i));
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;

CALL load_accounts();
DROP PROCEDURE IF EXISTS load_accounts;
