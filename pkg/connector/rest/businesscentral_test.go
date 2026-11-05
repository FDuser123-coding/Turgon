package rest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/bctest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

func newBusinessCentral(t *testing.T) (*bctest.BC, *rest.Conn) {
	t.Helper()
	bc := bctest.New("turgon-app", "s3cret")
	t.Cleanup(bc.Close)
	cfg := exampleConfig(t, "business-central", func(s string) string {
		s = strings.ReplaceAll(s, "https://login.microsoftonline.com", bc.URL())
		return strings.ReplaceAll(s, "https://api.businesscentral.dynamics.com/v2.0", bc.URL()+"/v2.0")
	})
	c, err := rest.New(cfg, `{"clientId": "turgon-app", "clientSecret": "s3cret"}`, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return bc, c
}

const bcOrder = `{"customerNumber": "10000", "externalDocumentNumber": "SHOPIFY-1", "orderDate": "2026-11-05",
	"bcLines": [{"lineType": "Item", "lineObjectNumber": "1896-S", "quantity": 2, "unitPrice": 44},
	            {"lineType": "Item", "lineObjectNumber": "1900-S", "quantity": 1}]}`

// Business Central has no upsert by an external key, so a retried create
// finds its order by the external document number instead of making a second.
func TestBusinessCentralCreateFindsItsOrderOnRetryAndDelete(t *testing.T) {
	bc, c := newBusinessCentral(t)
	ctx := context.Background()
	preview, err := c.Simulate(ctx, "create-sales-order", json.RawMessage(bcOrder))
	if err != nil || !strings.Contains(string(preview), `"salesOrderLines":[`) {
		t.Fatalf("preview = %s, %v", preview, err)
	}
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(bcOrder))
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID, Number string
		Total      float64 `json:"totalAmountExcludingTax"`
		Existing   bool
	}
	_ = json.Unmarshal(res, &created)
	// 2 x 44.00 at the shop's price, and the chair at the item card's 192.80.
	if created.Number != "101001" || created.Total != 280.8 || created.Existing {
		t.Fatalf("result %s", res)
	}
	o := bc.Order(created.ID)
	if o == nil || o["externalDocumentNumber"] != "SHOPIFY-1" || len(o["salesOrderLines"].([]any)) != 2 {
		t.Fatalf("order %v", o)
	}
	again, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(bcOrder))
	if err != nil || bc.Orders() != 1 || !strings.Contains(string(again), `"existing":true`) || !strings.Contains(string(again), created.ID) {
		t.Fatalf("retry: %s, %v, orders %d", again, err, bc.Orders())
	}
	if bc.Requests["POST salesOrders"] != 1 {
		t.Fatalf("the retry posted again: %v", bc.Requests)
	}
	// A key with a quote cannot end the OData literal of the lookup.
	quoted := strings.ReplaceAll(bcOrder, "SHOPIFY-1", "o'brien")
	if _, err := c.Commit(ctx, "create-sales-order", "o'brien", json.RawMessage(quoted)); err != nil || bc.Orders() != 2 {
		t.Fatalf("quoted key: %v, orders %d", err, bc.Orders())
	}
	if _, err := c.Commit(ctx, "delete-sales-order", "k#compensate", res); err != nil || bc.Order(created.ID) != nil {
		t.Fatalf("delete: %v", err)
	}
	// Business Central's refusals are the write's.
	blocked := strings.ReplaceAll(bcOrder, `"10000"`, `"40000"`)
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-2", json.RawMessage(blocked)); err == nil ||
		!strings.Contains(err.Error(), "Customer 40000 is blocked") {
		t.Fatalf("blocked customer: %v", err)
	}
	unknown := strings.ReplaceAll(bcOrder, "1896-S", "9999-X")
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-3", json.RawMessage(unknown)); !errors.Is(err, writeguard.ErrInvalid) && !errors.Is(err, writeguard.ErrRejected) {
		t.Fatalf("unknown item: %v", err)
	}
}

func TestBusinessCentralChangedOrdersReadsAndChecks(t *testing.T) {
	bc, c := newBusinessCentral(t)
	ctx := context.Background()
	var mu sync.Mutex
	clock := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	bc.StartAt(101001, func() time.Time { mu.Lock(); defer mu.Unlock(); return clock })
	for i, key := range []string{"SHOPIFY-1", "SHOPIFY-2"} {
		mu.Lock()
		clock = clock.Add(time.Duration(i+1) * time.Minute)
		mu.Unlock()
		if _, err := c.Commit(ctx, "create-sales-order", key, json.RawMessage(strings.ReplaceAll(bcOrder, "SHOPIFY-1", key))); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := c.Poll(ctx, "SalesOrder.Changed", 0, 10)
	if err != nil || len(evs) != 2 {
		t.Fatalf("changed orders = %+v, %v", evs, err)
	}
	var first map[string]any
	_ = json.Unmarshal(evs[0].Payload, &first)
	if first["externalDocumentNumber"] != "SHOPIFY-1" {
		t.Fatalf("oldest first: %s", evs[0].Payload)
	}
	if later, err := c.Poll(ctx, "SalesOrder.Changed", evs[0].Position, 10); err != nil || len(later) != 1 {
		t.Fatalf("after the first: %+v, %v", later, err)
	}
	got, err := c.Read(ctx, "get-customer", bc.CustomerID(bctest.TreyResearch))
	if err != nil || !strings.Contains(string(got), `"name":"Trey Research"`) || !strings.Contains(string(got), `"customerNumber":"20000"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s: %s", r.Name, r.Detail)
		}
	}
}

func TestExistingLookupValidation(t *testing.T) {
	base := rest.Config{BaseURL: "https://x.example", Auth: rest.Auth{Type: "none"}}
	for name, op := range map[string]rest.Operation{
		"no items":      {Method: "POST", Path: "/x", Existing: &rest.Existing{Path: "/x"}},
		"relative path": {Method: "POST", Path: "/x", Existing: &rest.Existing{Path: "x", Items: "value"}},
		"on a delete":   {Method: "DELETE", Path: "/x/{{id}}", Existing: &rest.Existing{Path: "/x", Items: "value"}},
	} {
		cfg := base
		cfg.Operations = map[string]rest.Operation{"op": op}
		if _, err := rest.New(cfg, "", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
