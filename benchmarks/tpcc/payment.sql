-- ============================================================================
-- TPC-C Transaction Profile: Payment (43% of mix)
-- TPC-C Standard §2.5
-- ============================================================================
-- Updates warehouse/district YTD, customer balance/payment count,
-- inserts history row.
-- ============================================================================

\set w_id 1
\set d_id random(1, 10)
\set c_id random(1, 3000)
\set h_amount random(100, 500000)

BEGIN;

-- Step 1: Update warehouse YTD
UPDATE warehouse SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id;

-- Step 2: Read warehouse address
SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip
FROM warehouse WHERE w_id = :w_id;

-- Step 3: Update district YTD
UPDATE district SET d_ytd = d_ytd + :h_amount WHERE d_w_id = :w_id AND d_id = :d_id;

-- Step 4: Read district address
SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip
FROM district WHERE d_w_id = :w_id AND d_id = :d_id;

-- Step 5: Update customer balance + payment count
UPDATE customer
SET c_balance = c_balance - :h_amount,
    c_ytd_payment = c_ytd_payment + :h_amount,
    c_payment_cnt = c_payment_cnt + 1
WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

-- Step 6: Read customer info
SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,
       c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance
FROM customer WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

-- Step 7: Insert history
INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_data)
VALUES (:c_id, :d_id, :w_id, :d_id, :w_id, :h_amount, 'payment_' || :c_id);

COMMIT;
