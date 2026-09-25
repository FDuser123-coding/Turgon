-- Demo systems for the shop-orders-to-erp recipe. Turgon's own state
-- (idempotency, cursors, cross-references) is created by `turgon run`.
CREATE SCHEMA IF NOT EXISTS shop;
CREATE SCHEMA IF NOT EXISTS erp;

CREATE TABLE IF NOT EXISTS shop.outbox (
	id      bigserial PRIMARY KEY,
	event   text NOT NULL,
	payload jsonb NOT NULL
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
