-- ============================================================================
-- FalconDB PoC #5 — Migration: Sample OLTP Schema (PostgreSQL-native)
-- ============================================================================
-- This schema represents a realistic e-commerce OLTP application.
-- It uses only standard PostgreSQL features that a typical app would use.
-- ============================================================================

-- Sequences for ID generation
CREATE SEQUENCE customers_id_seq START WITH 1;
CREATE SEQUENCE orders_id_seq START WITH 1;
CREATE SEQUENCE payments_id_seq START WITH 1;
CREATE SEQUENCE order_items_id_seq START WITH 1;

-- Customers
CREATE TABLE customers (
    id          BIGINT PRIMARY KEY DEFAULT nextval('customers_id_seq'),
    email       TEXT NOT NULL,
    name        TEXT NOT NULL,
    phone       TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_customers_email ON customers (email);

-- Orders
CREATE TABLE orders (
    id            BIGINT PRIMARY KEY DEFAULT nextval('orders_id_seq'),
    customer_id   BIGINT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    total_amount  BIGINT NOT NULL DEFAULT 0,
    notes         TEXT,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_status ON orders (status);
CREATE INDEX idx_orders_created_at ON orders (created_at);

-- Order Items
CREATE TABLE order_items (
    id          BIGINT PRIMARY KEY DEFAULT nextval('order_items_id_seq'),
    order_id    BIGINT NOT NULL,
    product     TEXT NOT NULL,
    quantity    BIGINT NOT NULL DEFAULT 1,
    unit_price  BIGINT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_order_items_order_id ON order_items (order_id);

-- Payments
CREATE TABLE payments (
    id          BIGINT PRIMARY KEY DEFAULT nextval('payments_id_seq'),
    order_id    BIGINT NOT NULL,
    amount      BIGINT NOT NULL,
    method      TEXT NOT NULL DEFAULT 'card',
    status      TEXT NOT NULL DEFAULT 'pending',
    paid_at     TIMESTAMP,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_payments_order_id ON payments (order_id);
CREATE INDEX idx_payments_status ON payments (status);

-- Summary view
CREATE VIEW order_summary AS
SELECT
    o.id AS order_id,
    c.name AS customer_name,
    c.email AS customer_email,
    o.status,
    o.total_amount,
    o.created_at
FROM orders o
JOIN customers c ON c.id = o.customer_id;
