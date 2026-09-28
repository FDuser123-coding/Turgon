package sap_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/sap"
	"github.com/fduser123-coding/turgon/pkg/connector/sap/saptest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// clock is a settable time for the fake system.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) set(t time.Time) { c.mu.Lock(); c.t = t; c.mu.Unlock() }

func setup(t *testing.T) (*saptest.S4, *sap.Conn, *clock) {
	t.Helper()
	s4 := saptest.New("TURGON_COMM", "s3cret", "100")
	clk := &clock{t: time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)}
	s4.StartAt(1000, clk.now)
	srv := httptest.NewServer(s4)
	t.Cleanup(srv.Close)
	conn, err := sap.New(config(srv.URL), "TURGON_COMM:s3cret", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	return s4, conn, clk
}

func config(base string) sap.Config {
	return sap.Config{
		BaseURL: base, Client: "100", Auth: rest.Auth{Type: "basic"},
		Events: map[string]sap.Event{
			"SalesOrder.Changed": {Service: "API_SALES_ORDER_SRV", EntitySet: "A_SalesOrder", Key: "SalesOrder",
				Filter: "SalesOrderType eq 'OR'", Expand: []string{"to_Item"}},
		},
		Operations: map[string]sap.Operation{
			"create-sales-order": {
				Action: "create", Service: "API_SALES_ORDER_SRV", EntitySet: "A_SalesOrder", Key: "SalesOrder",
				Constants: map[string]any{"SalesOrderType": "OR", "SalesOrganization": "1710", "DistributionChannel": "10", "OrganizationDivision": "00"},
				Fields:    map[string]string{"SoldToParty": "customerId", "SalesOrderDate": "orderDate"},
				Types:     map[string]string{"SalesOrderDate": "date"},
				Items: &sap.Items{Navigation: "to_Item", From: "lines",
					Fields: map[string]string{"Material": "material", "RequestedQuantity": "quantity"},
					Types:  map[string]string{"RequestedQuantity": "decimal"}},
				Reference: "PurchaseOrderByCustomer",
				Simulate:  &sap.Simulation{Service: "API_SALES_ORDER_SIMULATION_SRV", EntitySet: "A_SalesOrderSimulation"},
			},
			"delete-sales-order": {Action: "delete", Service: "API_SALES_ORDER_SRV", EntitySet: "A_SalesOrder", Key: "SalesOrder"},
			"get-customer": {Action: "get", Service: "API_BUSINESS_PARTNER", EntitySet: "A_Customer", Key: "Customer",
				Fields: map[string]string{"CustomerName": "name", "DeletionIndicator": "blocked"}},
		},
	}
}

const order = `{"customerId":"C-100","orderDate":"2026-09-24","netValue":2350,
	"lines":[{"material":"M-2","quantity":10}]}`

func TestSimulateCreateConfirmDelete(t *testing.T) {
	s4, conn, _ := setup(t)
	ctx := context.Background()

	preview, err := conn.Simulate(ctx, "create-sales-order", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Mode   string         `json:"mode"`
		Record map[string]any `json:"record"`
	}
	if err := json.Unmarshal(preview, &p); err != nil || p.Mode != "testrun" || p.Record["TotalNetAmount"] != "2350.00" {
		t.Fatalf("preview = %s", preview)
	}
	if len(s4.Orders()) != 0 {
		t.Fatal("simulation saved an order")
	}

	res, err := conn.Commit(ctx, "create-sales-order", "hubspot-deal-42", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	var created sap.CreateResult
	_ = json.Unmarshal(res, &created)
	o := s4.Order(created.ID)
	if o == nil || o["PurchaseOrderByCustomer"] != "hubspot-deal-42" || o["SalesOrderDate"] != "/Date(1790208000000)/" || o["SoldToParty"] != "C-100" {
		t.Fatalf("created %s: %v", res, o)
	}
	if items := o["to_Item"].([]any); len(items) != 1 || items[0].(map[string]any)["RequestedQuantity"] != "10" {
		t.Fatalf("items = %v", o["to_Item"])
	}
	if created.Record["SalesOrderDate"] != "2026-09-24" {
		t.Fatalf("dates are not read back as ISO dates: %v", created.Record["SalesOrderDate"])
	}
	if err := conn.Confirm(ctx, "create-sales-order", res); err != nil {
		t.Fatal(err)
	}

	// The compensation deletes it, with its ETag, and is idempotent.
	del, err := conn.Commit(ctx, "delete-sales-order", "hubspot-deal-42#compensate", res)
	if err != nil {
		t.Fatal(err)
	}
	if s4.Order(created.ID) != nil {
		t.Fatal("order not deleted")
	}
	if err := conn.Confirm(ctx, "delete-sales-order", del); err != nil {
		t.Fatal(err)
	}
	if again, err := conn.Commit(ctx, "delete-sales-order", "hubspot-deal-42#compensate", res); err != nil || !strings.Contains(string(again), "alreadyGone") {
		t.Fatalf("second delete: %s, %v", again, err)
	}
}

func TestLostResponseDoesNotDuplicate(t *testing.T) {
	s4, conn, _ := setup(t)
	ctx := context.Background()
	s4.LoseNextCreate = true
	if _, err := conn.Commit(ctx, "create-sales-order", "hubspot-deal-7", json.RawMessage(order)); err == nil {
		t.Fatal("a lost response looked like success")
	} else if errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("a gateway timeout must be retryable: %v", err)
	}
	res, err := conn.Commit(ctx, "create-sales-order", "hubspot-deal-7", json.RawMessage(order))
	if err != nil {
		t.Fatal(err)
	}
	var created sap.CreateResult
	_ = json.Unmarshal(res, &created)
	if !created.Existing || len(s4.Orders()) != 1 {
		t.Fatalf("retry created a second order: %s, orders %v", res, s4.Orders())
	}
}

func TestRejectedOrdersAreFinal(t *testing.T) {
	_, conn, _ := setup(t)
	ctx := context.Background()
	for name, payload := range map[string]string{
		"unknown customer": `{"customerId":"C-999","orderDate":"2026-09-24","lines":[{"material":"M-2","quantity":1}]}`,
		"unknown material": `{"customerId":"C-100","orderDate":"2026-09-24","lines":[{"material":"NOPE","quantity":1}]}`,
		"no items":         `{"customerId":"C-100","orderDate":"2026-09-24","lines":[]}`,
		"bad date":         `{"customerId":"C-100","orderDate":"tomorrow","lines":[{"material":"M-2","quantity":1}]}`,
	} {
		_, err := conn.Commit(ctx, "create-sales-order", "k-"+strings.ReplaceAll(name, " ", "-"), json.RawMessage(payload))
		if !errors.Is(err, writeguard.ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", name, err)
		}
	}
	// SAP's message reaches the error, for the person who fixes the data.
	_, err := conn.Commit(ctx, "create-sales-order", "k-msg", json.RawMessage(`{"customerId":"C-999","orderDate":"2026-09-24","lines":[{"material":"M-2","quantity":1}]}`))
	if err == nil || !strings.Contains(err.Error(), "Sold-to party C-999 not maintained") {
		t.Fatalf("error = %v", err)
	}
	if _, err := conn.Commit(ctx, "create-sales-order", strings.Repeat("x", 36), json.RawMessage(order)); !errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("an idempotency key too long for the reference: %v", err)
	}
}

func TestPollInChangeOrder(t *testing.T) {
	s4, conn, clk := setup(t)
	ctx := context.Background()
	base := clk.now()
	var ids []string
	for i := 0; i < 5; i++ {
		// Two orders share each timestamp.
		clk.set(base.Add(time.Duration(i/2) * time.Minute))
		res, err := conn.Commit(ctx, "create-sales-order", "k"+string(rune('a'+i)), json.RawMessage(order))
		if err != nil {
			t.Fatal(err)
		}
		var c sap.CreateResult
		_ = json.Unmarshal(res, &c)
		ids = append(ids, c.ID)
	}
	conn.PageSize = 3
	evs, err := conn.Poll(ctx, "SalesOrder.Changed", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	// A page of 3 ends inside the second timestamp's pair: the batch stops
	// before it.
	if len(evs) != 2 || evs[0].ID != ids[0] || evs[1].ID != ids[1] {
		t.Fatalf("first batch = %v", evs)
	}
	var payload map[string]any
	_ = json.Unmarshal(evs[0].Payload, &payload)
	if _, ok := payload["__metadata"]; ok || payload["SalesOrderDate"] != "2026-09-24" || len(payload["to_Item"].([]any)) != 1 {
		t.Fatalf("payload = %s", evs[0].Payload)
	}
	rest, err := conn.Poll(ctx, "SalesOrder.Changed", evs[1].Position, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Again a full page: the order alone at the last timestamp waits, as
	// more might share it.
	if len(rest) != 2 || rest[0].ID != ids[2] || rest[1].ID != ids[3] {
		t.Fatalf("second batch = %v", rest)
	}
	last, err := conn.Poll(ctx, "SalesOrder.Changed", rest[1].Position, 3)
	if err != nil || len(last) != 1 || last[0].ID != ids[4] {
		t.Fatalf("third batch = %v, %v", last, err)
	}
	rest = append(rest, last...)
	// A change in SAP brings the order back, at its new position.
	clk.set(base.Add(time.Hour))
	s4.Change(ids[0], "SalesOrderType", "OR")
	again, err := conn.Poll(ctx, "SalesOrder.Changed", rest[2].Position, 3)
	if err != nil || len(again) != 1 || again[0].ID != ids[0] {
		t.Fatalf("after a change: %v, %v", again, err)
	}
}

func TestReadAndCheck(t *testing.T) {
	_, conn, _ := setup(t)
	ctx := context.Background()
	got, err := conn.Read(ctx, "get-customer", "C-100")
	if err != nil || !strings.Contains(string(got), `"name":"Lovelace GmbH"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	if _, err := conn.Read(ctx, "get-customer", "C-404"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("unknown customer: %v", err)
	}
	if _, err := conn.Read(ctx, "get-customer", "x') or ('1"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("a key that is not an SAP key: %v", err)
	}
	for _, r := range conn.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s failed: %s", r.Name, r.Detail)
		}
	}
}

func TestCheckExplains(t *testing.T) {
	s4 := saptest.New("TURGON_COMM", "s3cret", "100")
	srv := httptest.NewServer(s4)
	defer srv.Close()
	cfg := config(srv.URL)
	op := cfg.Operations["create-sales-order"]
	op.Fields = map[string]string{"SoldToParty": "customerId", "YY1_Typo_SDH": "x"}
	cfg.Operations["create-sales-order"] = op
	cfg.Operations["get-customer"] = sap.Operation{Action: "get", Service: "API_NOT_ACTIVE_SRV", EntitySet: "A_X", Key: "X", Fields: map[string]string{"A": "a"}}
	conn, err := sap.New(cfg, "TURGON_COMM:wrong", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	res := conn.Check(context.Background())
	if len(res) == 0 || res[0].OK || !strings.Contains(res[0].Fix, "credentials") {
		t.Fatalf("wrong password: %+v", res)
	}
	conn, _ = sap.New(cfg, "TURGON_COMM:s3cret", srv.Client())
	byName := map[string]string{}
	for _, r := range conn.Check(context.Background()) {
		if !r.OK {
			byName[r.Name] = r.Detail + " | " + r.Fix
		}
	}
	if !strings.Contains(byName["service API_SALES_ORDER_SRV"], "no property YY1_Typo_SDH") {
		t.Errorf("typo not found: %v", byName)
	}
	if !strings.Contains(byName["service API_NOT_ACTIVE_SRV"], "Activate it") {
		t.Errorf("inactive service not explained: %v", byName)
	}
}

func TestCSRFTokenRenewed(t *testing.T) {
	s4 := saptest.New("TURGON_COMM", "s3cret", "100")
	srv := httptest.NewServer(s4)
	defer srv.Close()
	conn, err := sap.New(config(srv.URL), "TURGON_COMM:s3cret", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := conn.Commit(ctx, "create-sales-order", "a", json.RawMessage(order)); err != nil {
		t.Fatal(err)
	}
	// The session expires on the server: the next write gets 403 with
	// "X-CSRF-Token: Required", fetches a new token and succeeds.
	s4.ExpireSessions()
	if _, err := conn.Commit(ctx, "create-sales-order", "b", json.RawMessage(order)); err != nil {
		t.Fatal(err)
	}
	if n := s4.Requests["GET API_SALES_ORDER_SRV/"]; n != 2 {
		t.Fatalf("token fetched %d times, want 2", n)
	}
}

func TestConfigRejected(t *testing.T) {
	for name, mutate := range map[string]func(*sap.Config){
		"filter closes parentheses": func(c *sap.Config) {
			e := c.Events["SalesOrder.Changed"]
			e.Filter = "SalesOrderType eq 'OR') or (1 eq 1"
			c.Events["SalesOrder.Changed"] = e
		},
		"filter adds a parameter": func(c *sap.Config) {
			e := c.Events["SalesOrder.Changed"]
			e.Filter = "SalesOrderType eq 'OR'&$top=1"
			c.Events["SalesOrder.Changed"] = e
		},
		"unknown action": func(c *sap.Config) {
			c.Operations["x"] = sap.Operation{Action: "merge", Service: "S", EntitySet: "E", Key: "K"}
		},
		"bad client": func(c *sap.Config) { c.Client = "1; DROP" },
		"unknown type": func(c *sap.Config) {
			op := c.Operations["create-sales-order"]
			op.Types = map[string]string{"SalesOrderDate": "timestamp"}
			c.Operations["create-sales-order"] = op
		},
	} {
		cfg := config("https://s4.example.com")
		mutate(&cfg)
		if _, err := sap.New(cfg, "u:p", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
