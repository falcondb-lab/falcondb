-- ============================================================================
-- FalconDB PoC #5 — Migration: Sample Seed Data
-- ============================================================================
-- Realistic e-commerce data for the demo application.
-- Small enough to migrate quickly, large enough to be meaningful.
-- ============================================================================

-- Customers
INSERT INTO customers (id, email, name, phone, created_at) VALUES
(1, 'alice@example.com', 'Alice Chen', '+1-555-0101', '2026-01-15 09:30:00'),
(2, 'bob@example.com', 'Bob Martinez', '+1-555-0102', '2026-01-16 10:15:00'),
(3, 'carol@example.com', 'Carol Williams', '+1-555-0103', '2026-01-17 14:00:00'),
(4, 'dave@example.com', 'Dave Johnson', '+1-555-0104', '2026-01-18 11:45:00'),
(5, 'eve@example.com', 'Eve Park', '+1-555-0105', '2026-01-19 16:20:00');

SELECT setval('customers_id_seq', 5);

-- Orders
INSERT INTO orders (id, customer_id, status, total_amount, notes, created_at, updated_at) VALUES
(1, 1, 'completed', 15000, 'Express shipping requested', '2026-02-01 10:00:00', '2026-02-01 14:30:00'),
(2, 1, 'completed', 8500, NULL, '2026-02-03 09:00:00', '2026-02-03 12:00:00'),
(3, 2, 'shipped', 22000, 'Gift wrap', '2026-02-05 11:30:00', '2026-02-06 08:00:00'),
(4, 3, 'pending', 5000, NULL, '2026-02-10 15:00:00', '2026-02-10 15:00:00'),
(5, 4, 'completed', 35000, 'Business account', '2026-02-12 08:00:00', '2026-02-13 10:00:00'),
(6, 5, 'cancelled', 12000, 'Customer changed mind', '2026-02-14 13:00:00', '2026-02-14 18:00:00'),
(7, 2, 'pending', 9500, NULL, '2026-02-15 16:00:00', '2026-02-15 16:00:00'),
(8, 1, 'shipped', 18000, 'Second address', '2026-02-16 10:30:00', '2026-02-17 07:00:00');

SELECT setval('orders_id_seq', 8);

-- Order Items
INSERT INTO order_items (id, order_id, product, quantity, unit_price, created_at) VALUES
(1,  1, 'Wireless Keyboard', 1, 8000, '2026-02-01 10:00:00'),
(2,  1, 'USB-C Cable', 2, 1500, '2026-02-01 10:00:00'),
(3,  1, 'Mouse Pad', 1, 2500, '2026-02-01 10:00:00'),
(4,  2, 'Notebook Stand', 1, 8500, '2026-02-03 09:00:00'),
(5,  3, 'Monitor Arm', 1, 15000, '2026-02-05 11:30:00'),
(6,  3, 'HDMI Cable', 2, 2000, '2026-02-05 11:30:00'),
(7,  3, 'Screen Cleaner', 1, 1000, '2026-02-05 11:30:00'),
(8,  4, 'Webcam Cover', 5, 1000, '2026-02-10 15:00:00'),
(9,  5, 'Standing Desk Mat', 1, 12000, '2026-02-12 08:00:00'),
(10, 5, 'Desk Lamp', 1, 15000, '2026-02-12 08:00:00'),
(11, 5, 'Cable Organizer', 2, 4000, '2026-02-12 08:00:00'),
(12, 6, 'Bluetooth Speaker', 1, 12000, '2026-02-14 13:00:00'),
(13, 7, 'USB Hub', 1, 6000, '2026-02-15 16:00:00'),
(14, 7, 'Phone Charger', 1, 3500, '2026-02-15 16:00:00'),
(15, 8, 'Laptop Sleeve', 1, 8000, '2026-02-16 10:30:00'),
(16, 8, 'Portable SSD', 1, 10000, '2026-02-16 10:30:00');

SELECT setval('order_items_id_seq', 16);

-- Payments
INSERT INTO payments (id, order_id, amount, method, status, paid_at, created_at) VALUES
(1, 1, 15000, 'card', 'completed', '2026-02-01 10:05:00', '2026-02-01 10:05:00'),
(2, 2, 8500, 'card', 'completed', '2026-02-03 09:02:00', '2026-02-03 09:02:00'),
(3, 3, 22000, 'paypal', 'completed', '2026-02-05 11:35:00', '2026-02-05 11:35:00'),
(4, 5, 35000, 'bank_transfer', 'completed', '2026-02-12 08:30:00', '2026-02-12 08:30:00'),
(5, 6, 12000, 'card', 'refunded', '2026-02-14 13:05:00', '2026-02-14 13:05:00'),
(6, 8, 18000, 'card', 'completed', '2026-02-16 10:35:00', '2026-02-16 10:35:00');

SELECT setval('payments_id_seq', 6);
