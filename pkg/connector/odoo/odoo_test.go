package odoo_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/odoo"
	"github.com/fduser123-coding/turgon/pkg/connector/odoo/odootest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// exampleConfig reads the example connection's config, so tests and the
// example cannot drift apart, and points it at the fake.
func exampleConfig(t *testing.T, base string) odoo.Config {
	t.Helper()
	data, err := os.ReadFile("../../../examples/connections/odoo-erp.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var conn struct {
		Spec struct {
			Config json.RawMessage `json:"config"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(data, &conn); err != nil {
		t.Fatal(err)
	}
	var cfg odoo.Config
	dec := json.NewDecoder(strings.NewReader(strings.ReplaceAll(string(conn.Spec.Config), "https://acme.odoo.com", base)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newOdoo(t *testing.T) (*odootest.Odoo, *odoo.Conn) {
	t.Helper()
	fake := odootest.New("key-123")
	t.Cleanup(fake.Close)
	c, err := odoo.New(exampleConfig(t, fake.URL()), "key-123", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return fake, c
}

// IDs arrive as strings from a mapping; the field types turn them into integers.
const order = `{"customerId": "7", "orderDate": "2026-11-05", "origin": "Shopify SHOPIFY-1",
	"odooLines": [{"product": 11, "quantity": 2, "price": "210.00"}, {"product": "12", "quantity": 1}]}`

func TestOdooDryRunCreateRetryAndCancel(t *testing.T) {
	fake, c := newOdoo(t)
	ctx := context.Background()
	preview, err := c.Simulate(ctx, "create-sales-order", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Mode   string
		Record struct {
			AmountUntaxed float64          `json:"amount_untaxed"`
			OrderLine     []map[string]any `json:"order_line"`
		}
	}
	_ = json.Unmarshal(preview, &p)
	// 2 x 210.00 at the shop's price, and the chair at Odoo's list price 235.00.
	if p.Mode != "onchange" || p.Record.AmountUntaxed != 655 || len(p.Record.OrderLine) != 2 || p.Record.OrderLine[1]["price_subtotal"] != 235.0 {
		t.Fatalf("preview = %s", preview)
	}
	if fake.Orders() != 0 {
		t.Fatal("the dry run saved an order")
	}
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	var created odoo.CreateResult
	_ = json.Unmarshal(res, &created)
	o := fake.Order(created.ID)
	if created.Number != "S00001" || created.Reference != "SHOPIFY-1" || created.Existing || o == nil ||
		o["client_order_ref"] != "SHOPIFY-1" || o["amount_untaxed"] != 655.0 || o["date_order"] != "2026-11-05 00:00:00" {
		t.Fatalf("result %s, order %v", res, o)
	}
	if err := c.Confirm(ctx, "create-sales-order", res); err != nil {
		t.Fatal(err)
	}
	// A retry after a lost answer finds the quotation instead of making a second.
	again, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(order))
	if err != nil || fake.Orders() != 1 || !strings.Contains(string(again), `"existing":true`) || !strings.Contains(string(again), `"number":"S00001"`) {
		t.Fatalf("retry: %s, %v, orders %d", again, err, fake.Orders())
	}
	if fake.Calls["sale.order/create"] != 1 {
		t.Fatalf("the retry created again: %v", fake.Calls)
	}
	undo, err := c.Commit(ctx, "cancel-sales-order", "SHOPIFY-1#compensate", res)
	if err != nil || fake.Order(created.ID)["state"] != "cancel" {
		t.Fatalf("cancel: %s, %v", undo, err)
	}
	if err := c.Confirm(ctx, "cancel-sales-order", res); err != nil {
		t.Fatal(err)
	}
	// Odoo's refusals are the write's, and permanent.
	unknown := strings.ReplaceAll(order, `"customerId": "7"`, `"customerId": "999"`)
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-2", json.RawMessage(unknown)); !errors.Is(err, writeguard.ErrInvalid) ||
		!strings.Contains(err.Error(), "res.partner(999,)") {
		t.Fatalf("unknown customer: %v", err)
	}
	if _, err := c.Simulate(ctx, "create-sales-order", json.RawMessage(strings.ReplaceAll(order, `"quantity": 2`, `"quantity": "two"`))); !errors.Is(err, writeguard.ErrInvalid) ||
		!strings.Contains(err.Error(), "product_uom_qty is a float field") {
		t.Fatalf("a quantity that is not a number: %v", err)
	}
}

func TestOdooUnlinkAndAlreadyGone(t *testing.T) {
	fake := odootest.New("key-123")
	t.Cleanup(fake.Close)
	cfg := exampleConfig(t, fake.URL())
	op := cfg.Operations["cancel-sales-order"]
	op.Method = "unlink"
	cfg.Operations["cancel-sales-order"] = op
	c, err := odoo.New(cfg, "key-123", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(ctx, "cancel-sales-order", "k", res); err != nil || fake.Orders() != 0 {
		t.Fatalf("unlink: %v, orders %d", err, fake.Orders())
	}
	if err := c.Confirm(ctx, "cancel-sales-order", res); err != nil {
		t.Fatal(err)
	}
	again, err := c.Commit(ctx, "cancel-sales-order", "k", res)
	if err != nil || !strings.Contains(string(again), `"alreadyGone":true`) {
		t.Fatalf("a second undo: %s, %v", again, err)
	}
}

func TestOdooPollReadCheckAndDiscover(t *testing.T) {
	fake, c := newOdoo(t)
	ctx := context.Background()
	var mu sync.Mutex
	clock := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	fake.StartAt(1, func() time.Time { mu.Lock(); defer mu.Unlock(); return clock })
	// The first two share a second.
	for i, key := range []string{"SHOPIFY-1", "SHOPIFY-2", "SHOPIFY-3"} {
		mu.Lock()
		clock = clock.Add(time.Duration(i/2) * time.Minute)
		mu.Unlock()
		if _, err := c.Commit(ctx, "create-sales-order", key, json.RawMessage(order)); err != nil {
			t.Fatal(err)
		}
	}
	// The cursor is (write_date, id): a poll may end inside a second, and
	// the next one reads the rest of it.
	evs, err := c.Poll(ctx, "SalesOrder.Changed", 0, 2)
	if err != nil || len(evs) != 2 {
		t.Fatalf("first poll = %d events, %v", len(evs), err)
	}
	var first map[string]any
	_ = json.Unmarshal(evs[0].Payload, &first)
	if first["client_order_ref"] != "SHOPIFY-1" || evs[1].Position <= evs[0].Position {
		t.Fatalf("oldest first: %s, %d %d", evs[0].Payload, evs[0].Position, evs[1].Position)
	}
	// An order written in the second the cursor stopped in, after the poll,
	// is not skipped.
	mu.Lock()
	clock = clock.Add(-time.Minute)
	mu.Unlock()
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-4", json.RawMessage(order)); err != nil {
		t.Fatal(err)
	}
	later, err := c.Poll(ctx, "SalesOrder.Changed", evs[1].Position, 10)
	if err != nil || len(later) != 2 || !strings.Contains(string(later[0].Payload), "SHOPIFY-4") || !strings.Contains(string(later[1].Payload), "SHOPIFY-3") {
		t.Fatalf("after the first two: %d events, %v", len(later), err)
	}
	if none, err := c.Poll(ctx, "SalesOrder.Changed", later[1].Position, 10); err != nil || len(none) != 0 {
		t.Fatalf("after the last: %+v, %v", none, err)
	}
	// A record younger than the settle time (60 s) waits: Odoo stamps
	// write_date when its transaction starts, not when it commits.
	c.SetClock(func() time.Time { mu.Lock(); defer mu.Unlock(); return clock.Add(30 * time.Second) })
	mu.Lock()
	clock = clock.Add(2 * time.Minute)
	mu.Unlock()
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-5", json.RawMessage(order)); err != nil {
		t.Fatal(err)
	}
	if fresh, err := c.Poll(ctx, "SalesOrder.Changed", later[1].Position, 10); err != nil || len(fresh) != 0 {
		t.Fatalf("an unsettled record: %d events, %v", len(fresh), err)
	}
	c.SetClock(func() time.Time { mu.Lock(); defer mu.Unlock(); return clock.Add(61 * time.Second) })
	if settled, err := c.Poll(ctx, "SalesOrder.Changed", later[1].Position, 10); err != nil || len(settled) != 1 {
		t.Fatalf("a settled record: %d events, %v", len(settled), err)
	}
	got, err := c.Read(ctx, "get-customer", "8")
	if err != nil || !strings.Contains(string(got), `"name":"Hopper Industries"`) || !strings.Contains(string(got), `"customerNumber":"C-200"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	if _, err := c.Read(ctx, "get-customer", "4711"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("a missing partner: %v", err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s: %s", r.Name, r.Detail)
		}
	}
	cat, err := c.Discover(ctx, []string{"product.product"})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, o := range cat.Objects {
		names[o.Name] = true
	}
	if !names["sale.order"] || !names["sale.order.line"] || !names["res.partner"] || !names["product.product"] {
		t.Fatalf("objects = %v", names)
	}
	creates := false
	for _, u := range cat.Uses {
		creates = creates || (u.Object == "sale.order.line" && u.Field == "price_unit" && u.Creates && u.By == "operation create-sales-order")
	}
	if !creates || cat.Events["SalesOrder.Changed"] != "sale.order" {
		t.Fatalf("uses = %+v, events %v", cat.Uses, cat.Events)
	}
	// A field a module makes required is seen by the next discovery, though
	// the worker read the fields before.
	fake.SetRequired("sale.order", "note")
	cat, _ = c.Discover(ctx, nil)
	for _, o := range cat.Objects {
		for _, f := range o.Fields {
			if o.Name == "sale.order" && f.Name == "note" && !f.Required {
				t.Fatal("note is required now, and discovery did not see it")
			}
		}
	}
}

func TestOdooRefusesBadKeysAndConfig(t *testing.T) {
	fake := odootest.New("key-123")
	t.Cleanup(fake.Close)
	c, err := odoo.New(exampleConfig(t, fake.URL()), "wrong", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	checks := c.Check(context.Background())
	if len(checks) != 1 || checks[0].OK || !strings.Contains(checks[0].Fix, "API key") {
		t.Fatalf("checks with a wrong key = %+v", checks)
	}
	fake.Database = "acme"
	c, _ = odoo.New(exampleConfig(t, fake.URL()), "key-123", http.DefaultClient)
	if checks := c.Check(context.Background()); checks[0].OK || !strings.Contains(checks[0].Fix, "database") {
		t.Fatalf("checks without the database = %+v", checks)
	}
	cfg := exampleConfig(t, fake.URL())
	cfg.Database = "acme"
	c, _ = odoo.New(cfg, "key-123", http.DefaultClient)
	if checks := c.Check(context.Background()); !checks[0].OK {
		t.Fatalf("checks with the database = %+v", checks)
	}
	for name, bad := range map[string]func(*odoo.Config){
		"a path in baseURL": func(c *odoo.Config) { c.BaseURL += "/odoo" },
		"unknown cancel": func(c *odoo.Config) {
			op := c.Operations["cancel-sales-order"]
			op.Method = "write"
			c.Operations["cancel-sales-order"] = op
		},
		"bad domain": func(c *odoo.Config) {
			e := c.Events["SalesOrder.Changed"]
			e.Domain = []any{"x"}
			c.Events["SalesOrder.Changed"] = e
		},
		"no model": func(c *odoo.Config) {
			op := c.Operations["get-customer"]
			op.Model = ""
			c.Operations["get-customer"] = op
		},
	} {
		cfg := exampleConfig(t, fake.URL())
		bad(&cfg)
		if _, err := odoo.New(cfg, "k", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A quotation confirmed in the meantime is cancelled too: Odoo 16+ would
// answer action_cancel with its wizard, unless the warning is disabled.
func TestOdooCancelsAConfirmedOrderAndKeepsDefaults(t *testing.T) {
	fake, c := newOdoo(t)
	ctx := context.Background()
	// No orderDate in the payload: Odoo's default applies, rather than false.
	noDate := strings.ReplaceAll(order, `"orderDate": "2026-11-05", `, "")
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(noDate))
	if err != nil {
		t.Fatal(err)
	}
	var created odoo.CreateResult
	_ = json.Unmarshal(res, &created)
	if d, _ := fake.Order(created.ID)["date_order"].(string); d == "" {
		t.Fatalf("date_order = %v", fake.Order(created.ID)["date_order"])
	}
	fake.SetState(created.ID, "sale")
	if _, err := c.Commit(ctx, "cancel-sales-order", "k", res); err != nil {
		t.Fatal(err)
	}
	if got := fake.Order(created.ID)["state"]; got != "cancel" {
		t.Fatalf("state = %v", got)
	}
}

func TestOdooReferenceMustHoldTheKey(t *testing.T) {
	fake := odootest.New("key-123")
	t.Cleanup(fake.Close)
	cfg := exampleConfig(t, fake.URL())
	op := cfg.Operations["create-sales-order"]
	op.Reference = "partner_id"
	cfg.Operations["create-sales-order"] = op
	c, err := odoo.New(cfg, "key-123", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), "create-sales-order", "SHOPIFY-1", json.RawMessage(order)); !errors.Is(err, writeguard.ErrInvalid) ||
		!strings.Contains(err.Error(), "must be a char field") {
		t.Fatalf("a many2one reference: %v", err)
	}
}

func TestOdooDomainArity(t *testing.T) {
	fake := odootest.New("key-123")
	t.Cleanup(fake.Close)
	for domain, ok := range map[string]bool{
		`[["state", "=", "sale"], ["amount_total", ">", 0]]`:    true,
		`["|", ["state", "=", "sale"], ["state", "=", "done"]]`: true,
		`["!", ["state", "=", "cancel"]]`:                       true,
		`["|", ["state", "=", "sale"]]`:                         false,
		`["&"]`:                                                 false,
	} {
		cfg := exampleConfig(t, fake.URL())
		e := cfg.Events["SalesOrder.Changed"]
		_ = json.Unmarshal([]byte(domain), &e.Domain)
		cfg.Events["SalesOrder.Changed"] = e
		if _, err := odoo.New(cfg, "k", http.DefaultClient); (err == nil) != ok {
			t.Errorf("%s: accepted %v", domain, err == nil)
		}
	}
}

// Each change of a record is its own event: the engine starts one run per
// event ID, so an order changed after its first run starts another, while
// the same change read twice keeps its ID.
func TestOdooEventIDsNameTheChange(t *testing.T) {
	fake, c := newOdoo(t)
	ctx := context.Background()
	var mu sync.Mutex
	clock := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	fake.StartAt(1, now)
	c.SetClock(func() time.Time { return now().Add(time.Hour) })
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	var created struct{ ID int64 }
	_ = json.Unmarshal(res, &created)
	first, err := c.Poll(ctx, "SalesOrder.Changed", 0, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first poll: %+v, %v", first, err)
	}
	id := fmt.Sprint(created.ID)
	if first[0].ID != connector.VersionID(id, clock.Unix()) || connector.RecordID(first[0].ID) != id {
		t.Fatalf("event ID %q for order %s", first[0].ID, id)
	}
	again, _ := c.Poll(ctx, "SalesOrder.Changed", 0, 10)
	if len(again) != 1 || again[0].ID != first[0].ID {
		t.Fatalf("re-read: %+v", again)
	}
	mu.Lock()
	clock = clock.Add(5 * time.Minute)
	mu.Unlock()
	fake.SetState(created.ID, "sale")
	changed, err := c.Poll(ctx, "SalesOrder.Changed", first[0].Position, 10)
	if err != nil || len(changed) != 1 || changed[0].ID == first[0].ID || connector.RecordID(changed[0].ID) != id {
		t.Fatalf("after the change: %+v, %v", changed, err)
	}
}
