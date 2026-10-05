package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

func TestDiscover(t *testing.T) {
	pool, _ := pgtest.Pool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE customers (id text PRIMARY KEY, name varchar(80) NOT NULL);
		COMMENT ON COLUMN customers.name IS 'Customer name';
		CREATE TABLE sales_orders (
			id bigserial PRIMARY KEY,
			external_id text UNIQUE NOT NULL,
			customer_id text NOT NULL REFERENCES customers (id),
			net_value numeric(12,2) NOT NULL,
			status text NOT NULL DEFAULT 'open',
			total_cents bigint GENERATED ALWAYS AS ((net_value * 100)::bigint) STORED);
		CREATE TABLE outbox (id bigserial PRIMARY KEY, event text, payload jsonb);
		INSERT INTO outbox (event, payload) VALUES
			('Order.Created', '{"order_number": 1, "total": "9.50", "customer": {"email": "a@b"}}'),
			('Order.Created', '{"order_number": 2, "total": "12.00", "coupon": null}'),
			('Order.Cancelled', '{"order_number": 1}');
		CREATE TABLE unrelated (x int)`); err != nil {
		t.Fatal(err)
	}
	c := New(pool, Config{
		Outbox: &Outbox{Table: "outbox"},
		Operations: map[string]Operation{
			"create-sales-order": {Table: "sales_orders", Action: "insert", Key: "external_id", Columns: []string{"external_id", "customer_id", "net_value"}},
			"cancel-sales-order": {Table: "sales_orders", Action: "update", Key: "external_id", Set: map[string]any{"status": "cancelled"}},
		},
		Changes: map[string]Change{"Customer.Created": {Table: "customers"}},
	})
	cat, err := c.Discover(ctx, []string{"customers"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	var tables []meta.Object
	for _, o := range cat.Objects {
		if !o.Sampled {
			names = append(names, o.Name)
			tables = append(tables, o)
		}
	}
	if len(names) != 3 || !strings.HasSuffix(names[0], ".customers") || !strings.HasSuffix(names[2], ".sales_orders") {
		t.Fatalf("objects %v", names)
	}
	orders := tables[2]
	f := func(name string) meta.Field {
		t.Helper()
		v, ok := orders.Field(name)
		if !ok {
			t.Fatalf("no field %s in %+v", name, orders.Fields)
		}
		return v
	}
	if id := f("id"); !id.Key || id.Required || id.Type != "bigint" {
		t.Fatalf("id %+v", id)
	}
	if v := f("net_value"); !v.Required || v.Type != "numeric" {
		t.Fatalf("net_value %+v", v)
	}
	if v := f("status"); v.Required {
		t.Fatalf("status has a default: %+v", v)
	}
	if v := f("total_cents"); !v.ReadOnly || v.Required {
		t.Fatalf("total_cents %+v", v)
	}
	if len(orders.Links) != 1 || !strings.HasSuffix(orders.Links[0].To, ".customers") || orders.Links[0].ToField != "id" {
		t.Fatalf("links %+v", orders.Links)
	}
	customers := tables[0]
	if n, _ := customers.Field("name"); n.Length != 80 || n.Label != "Customer name" {
		t.Fatalf("name %+v", n)
	}
	usage := meta.Usage(cat, nil)
	if got := strings.Join(usage[orders.Name+".status"], ","); got != "operation cancel-sales-order" {
		t.Fatalf("status used by %q (%v)", got, usage)
	}
	if got := strings.Join(usage[tables[1].Name+".payload"], ","); got != "outbox" {
		t.Fatalf("payload used by %q", got)
	}
	if got := strings.Join(meta.Creators(cat)[orders.Name], ","); got != "operation create-sales-order" {
		t.Fatalf("orders created by %q", got)
	}
	payload, ok := cat.Object(cat.Events["Order.Created"])
	if !ok || !payload.Sampled || !strings.HasSuffix(payload.Name, ".outbox#Order.Created") || len(payload.Fields) != 4 {
		t.Fatalf("outbox payload %+v (events %v)", payload, cat.Events)
	}
	if total, _ := payload.Field("total"); total.Type != "string" {
		t.Fatalf("total %+v", total)
	}
	if ev := cat.Events["Customer.Created"]; ev != tables[0].Name {
		t.Fatalf("events %v", cat.Events)
	}

	// The table changes: a column dropped, one narrowed.
	if _, err := pool.Exec(ctx, `ALTER TABLE customers ALTER COLUMN name TYPE varchar(40); ALTER TABLE sales_orders DROP COLUMN total_cents`); err != nil {
		t.Fatal(err)
	}
	after, err := c.Discover(ctx, []string{"customers"})
	if err != nil {
		t.Fatal(err)
	}
	var changes []string
	for _, ch := range meta.Diff(cat, after) {
		changes = append(changes, ch.Kind+" "+ch.Field)
	}
	if strings.Join(changes, ",") != "length-shrunk name,field-removed total_cents" {
		t.Fatalf("changes %v", changes)
	}
}
