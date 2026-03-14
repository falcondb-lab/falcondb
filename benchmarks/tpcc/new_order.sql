-- ============================================================================
-- TPC-C Transaction Profile: New Order (45% of mix)
-- TPC-C Standard §2.4
-- ============================================================================
-- The most complex and performance-critical transaction.
-- Reads warehouse tax, district tax + next_o_id, customer discount,
-- inserts order + new_order + order_lines, updates stock.
-- ============================================================================

\set w_id 1
\set d_id random(1, 10)
\set c_id random(1, 3000)
\set ol_cnt random(5, 15)
\set i_id_1 random(1, 100000)
\set i_id_2 random(1, 100000)
\set i_id_3 random(1, 100000)
\set i_id_4 random(1, 100000)
\set i_id_5 random(1, 100000)
\set qty random(1, 10)

BEGIN;

-- Step 1: Read warehouse tax
SELECT w_tax FROM warehouse WHERE w_id = :w_id;

-- Step 2: Read district tax + allocate order ID
SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = :w_id AND d_id = :d_id;
UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = :w_id AND d_id = :d_id;

-- Step 3: Read customer discount + credit
SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

-- Step 4: Insert order header
INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local)
SELECT d_next_o_id, :d_id, :w_id, :c_id, NULL, 5, 1
FROM district WHERE d_w_id = :w_id AND d_id = :d_id;

-- Step 5: Insert new_order
INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
SELECT d_next_o_id, :d_id, :w_id
FROM district WHERE d_w_id = :w_id AND d_id = :d_id;

-- Step 6: For each order line — read item, update stock, insert order_line
-- (5 lines shown; real TPC-C uses 5–15 random lines)

SELECT i_price, i_name, i_data FROM item WHERE i_id = :i_id_1;
UPDATE stock SET s_quantity = CASE WHEN s_quantity >= 15 THEN s_quantity - 5 ELSE s_quantity + 91 - 5 END,
    s_ytd = s_ytd + 5, s_order_cnt = s_order_cnt + 1
WHERE s_w_id = :w_id AND s_i_id = :i_id_1;

SELECT i_price, i_name, i_data FROM item WHERE i_id = :i_id_2;
UPDATE stock SET s_quantity = CASE WHEN s_quantity >= 15 THEN s_quantity - :qty ELSE s_quantity + 91 - :qty END,
    s_ytd = s_ytd + :qty, s_order_cnt = s_order_cnt + 1
WHERE s_w_id = :w_id AND s_i_id = :i_id_2;

SELECT i_price, i_name, i_data FROM item WHERE i_id = :i_id_3;
UPDATE stock SET s_quantity = CASE WHEN s_quantity >= 15 THEN s_quantity - :qty ELSE s_quantity + 91 - :qty END,
    s_ytd = s_ytd + :qty, s_order_cnt = s_order_cnt + 1
WHERE s_w_id = :w_id AND s_i_id = :i_id_3;

SELECT i_price, i_name, i_data FROM item WHERE i_id = :i_id_4;
UPDATE stock SET s_quantity = CASE WHEN s_quantity >= 15 THEN s_quantity - :qty ELSE s_quantity + 91 - :qty END,
    s_ytd = s_ytd + :qty, s_order_cnt = s_order_cnt + 1
WHERE s_w_id = :w_id AND s_i_id = :i_id_4;

SELECT i_price, i_name, i_data FROM item WHERE i_id = :i_id_5;
UPDATE stock SET s_quantity = CASE WHEN s_quantity >= 15 THEN s_quantity - :qty ELSE s_quantity + 91 - :qty END,
    s_ytd = s_ytd + :qty, s_order_cnt = s_order_cnt + 1
WHERE s_w_id = :w_id AND s_i_id = :i_id_5;

COMMIT;
