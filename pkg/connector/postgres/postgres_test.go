package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

const ddl = `
CREATE TABLE sales_orders (
	id           bigserial PRIMARY KEY,
	external_id  text UNIQUE NOT NULL,
	customer_id  text NOT NULL,
	order_date   date NOT NULL,
	net_value    numeric(12,2) NOT NULL CHECK (net_value >= 0),
	currency     char(3) NOT NULL,
	lines        jsonb NOT NULL DEFAULT '[]',
	status       text NOT NULL DEFAULT 'open'
);
CREATE TABLE outbox (id bigserial PRIMARY KEY, event text NOT NULL, payload jsonb NOT NULL);
`

func setup(t *testing.T) (*Conn, func(string) int) {
	t.Helper()
	pool, _ := pgtest.Pool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	c := New(pool, Config{
		Outbox: &Outbox{Table: "outbox"},
		Operations: map[string]Operation{
			"create-sales-order": {Table: "sales_orders", Action: "insert", Key: "external_id",
				Columns: []string{"external_id", "customer_id", "order_date", "net_value", "currency", "lines"}},
			"cancel-sales-order": {Table: "sales_orders", Action: "update", Key: "external_id", Set: map[string]any{"status": "cancelled"}},
		},
	})
	count := func(where string) int {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM sales_orders WHERE "+where).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return c, count
}

var order = json.RawMessage(`{"externalId":"1001","customerId":"C-100","orderDate":"2026-09-24","netValue":1200.5,
	"currency":"EUR","lines":[{"material":"M-1","quantity":2}],"customerRef":"ignored@example.com"}`)

func TestSimulateRollsBack(t *testing.T) {
	c, count := setup(t)
	preview, err := c.Simulate(context.Background(), "create-sales-order", order)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(preview), `"mode":"rollback"`) || !strings.Contains(string(preview), `"net_value":1200.50`) {
		t.Fatalf("preview = %s", preview)
	}
	if n := count("true"); n != 0 {
		t.Fatalf("simulation left %d rows", n)
	}
	bad := json.RawMessage(`{"externalId":"x","customerId":"C","orderDate":"2026-09-24","netValue":-1,"currency":"EUR"}`)
	if _, err := c.Simulate(context.Background(), "create-sales-order", bad); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "check constraint") {
		t.Fatalf("constraint violation not surfaced by simulation: %v", err)
	}
}

func TestCommitIsIdempotentAndCompensable(t *testing.T) {
	c, count := setup(t)
	ctx := context.Background()
	first, err := c.Commit(ctx, "create-sales-order", "k", order)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.Commit(ctx, "create-sales-order", "k", order)
	if err != nil || string(again) != string(first) || count("true") != 1 {
		t.Fatalf("repeat insert: %s %v (rows %d)", again, err, count("true"))
	}
	if err := c.Confirm(ctx, "create-sales-order", first); err != nil {
		t.Fatal(err)
	}
	// Compensation receives the committed row as its payload.
	if _, err := c.Commit(ctx, "cancel-sales-order", "k#compensate", first); err != nil {
		t.Fatal(err)
	}
	if count("status = 'cancelled'") != 1 {
		t.Fatal("order not cancelled")
	}
}

func TestPollOutbox(t *testing.T) {
	c, _ := setup(t)
	ctx := context.Background()
	_, err := c.pool.Exec(ctx, `INSERT INTO outbox (event, payload) VALUES
		('Order.Created', '{"n":1}'), ('Order.Paid', '{"n":2}'), ('Order.Created', '{"n":3}'), ('Order.Created', '{"n":4}')`)
	if err != nil {
		t.Fatal(err)
	}
	evs, err := c.Poll(ctx, "Order.Created", 0, 2)
	if err != nil || len(evs) != 2 || evs[0].Position != 1 || evs[1].Position != 3 {
		t.Fatalf("poll: %+v %v", evs, err)
	}
	evs, _ = c.Poll(ctx, "Order.Created", evs[1].Position, 10)
	if len(evs) != 1 || string(evs[0].Payload) != `{"n": 4}` {
		t.Fatalf("second poll: %+v", evs)
	}
}

func TestSnake(t *testing.T) {
	for in, want := range map[string]string{"netValue": "net_value", "external_id": "external_id", "ID": "id", "customerID": "customer_id", "line2Qty": "line2_qty", "a": "a"} {
		if got := Snake(in); got != want {
			t.Errorf("Snake(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInsertKeepsDefaultsForAbsentFields(t *testing.T) {
	c, count := setup(t)
	noLines := json.RawMessage(`{"externalId":"1002","customerId":"C-100","orderDate":"2026-09-24","netValue":5,"currency":"EUR"}`)
	if _, err := c.Commit(context.Background(), "create-sales-order", "k2", noLines); err != nil {
		t.Fatal(err)
	}
	if count(`external_id = '1002' AND lines = '[]'`) != 1 {
		t.Fatal("absent field did not take the column default")
	}
}
