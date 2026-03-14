-- ============================================================================
-- TPC-C Transaction Profile: Stock-Level (4% of mix)
-- TPC-C Standard §2.8
-- ============================================================================
-- Read-only: counts the number of recently sold items with stock below
-- a given threshold. Exercises range scan + join + aggregation.
-- ============================================================================

\set w_id 1
\set d_id random(1, 10)
\set threshold random(10, 20)

-- Step 1: Get next order ID for this district
SELECT d_next_o_id FROM district WHERE d_w_id = :w_id AND d_id = :d_id;

-- Step 2: Count distinct items from last 20 orders with stock below threshold
SELECT COUNT(DISTINCT ol_i_id) AS low_stock_count
FROM order_line
JOIN stock ON s_i_id = ol_i_id AND s_w_id = ol_w_id
WHERE ol_w_id = :w_id
  AND ol_d_id = :d_id
  AND ol_o_id >= (SELECT d_next_o_id - 20 FROM district WHERE d_w_id = :w_id AND d_id = :d_id)
  AND ol_o_id < (SELECT d_next_o_id FROM district WHERE d_w_id = :w_id AND d_id = :d_id)
  AND s_quantity < :threshold;
