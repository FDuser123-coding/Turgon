-- Demo systems for the shop-orders-to-erp recipe. Turgon's own state
-- (idempotency, cursors, cross-references) is created by `turgon run`.
CREATE SCHEMA IF NOT EXISTS shop;
CREATE SCHEMA IF NOT EXISTS erp;

CREATE TABLE IF NOT EXISTS shop.outbox (
	id      bigserial PRIMARY KEY,
	event   text NOT NULL,
	payload jsonb NOT NULL
);

-- The shop's orders, read by change capture for shop-order-rows-to-erp
-- (needs wal_level = logical). Its columns match the outbox payload.
CREATE TABLE IF NOT EXISTS shop.orders (
	order_number bigint PRIMARY KEY,
	created_at   timestamptz NOT NULL DEFAULT now(),
	total        numeric(12,2) NOT NULL,
	currency     text NOT NULL,
	customer     jsonb NOT NULL,
	items        jsonb NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS erp.customers (
	id    text PRIMARY KEY,
	name  text NOT NULL
);

CREATE TABLE IF NOT EXISTS erp.sales_orders (
	id          bigserial PRIMARY KEY,
	external_id text UNIQUE NOT NULL,
	customer_id text NOT NULL REFERENCES erp.customers (id),
	order_date  date NOT NULL,
	net_value   numeric(12,2) NOT NULL CHECK (net_value >= 0),
	currency    char(3) NOT NULL,
	lines       jsonb NOT NULL DEFAULT '[]',
	status      text NOT NULL DEFAULT 'open'
);

INSERT INTO erp.customers (id, name) VALUES ('C-100', 'Ada Lovelace GmbH') ON CONFLICT DO NOTHING;

-- For the credit-check plugin (examples/plugins/credit-check.yaml): each
-- customer's credit limit, and the status the plugin proposes for an order.
ALTER TABLE erp.customers ADD COLUMN IF NOT EXISTS credit_limit numeric(12,2) NOT NULL DEFAULT 5000;
ALTER TABLE erp.sales_orders ADD COLUMN IF NOT EXISTS credit_status text;

-- Payments received, for the stripe-payments-to-erp recipe.
CREATE TABLE IF NOT EXISTS erp.payments (
	id          bigserial PRIMARY KEY,
	external_id text UNIQUE NOT NULL,
	invoice_ref text NOT NULL,
	customer_id text NOT NULL REFERENCES erp.customers (id),
	amount      numeric(12,2) NOT NULL CHECK (amount >= 0),
	currency    char(3) NOT NULL,
	paid_on     date NOT NULL,
	status      text NOT NULL DEFAULT 'received'
);
