package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/xerotest"
)

func newXeroConn(t *testing.T, x *xerotest.Xero, refresh string, tokens connector.TokenStore) *rest.Conn {
	t.Helper()
	cfg := exampleConfig(t, "xero", func(s string) string {
		s = strings.ReplaceAll(s, "https://identity.xero.com", x.URL())
		return strings.ReplaceAll(s, "https://api.xero.com", x.URL())
	})
	c, err := rest.New(cfg, `{"clientId": "xero-app", "clientSecret": "s3cret", "refreshToken": "`+refresh+`"}`, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	c.UseTokens(tokens)
	return c
}

const xeroInvoice = `{"contactId": "` + xerotest.Lovelace + `", "orderDate": "2026-11-05", "dueDate": "2026-11-05", "reference": "Shopify SHOPIFY-1",
	"xeroLines": [{"ItemCode": "M-1", "Quantity": 2, "UnitAmount": 210}, {"ItemCode": "M-2", "Quantity": 1}]}`

// Xero answers lists ({"Invoices": [...]}); the result is Invoices.0. A
// retried create carries the same Idempotency-Key and is answered with the
// first invoice; the undo voids it with the constant Status VOIDED.
func TestXeroInvoiceRetryAndVoid(t *testing.T) {
	x, seed := xerotest.New("xero-app", "s3cret")
	t.Cleanup(x.Close)
	c := newXeroConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	res, err := c.Commit(ctx, "create-invoice", "SHOPIFY-1", json.RawMessage(xeroInvoice))
	if err != nil {
		t.Fatal(err)
	}
	var inv struct {
		InvoiceID, InvoiceNumber, Status string
		Total                            float64
	}
	_ = json.Unmarshal(res, &inv)
	// 2 x 210.00 at the shop's price, and the chair at Xero's 235.00.
	if inv.InvoiceID == "" || inv.InvoiceNumber != "INV-0001" || inv.Status != "AUTHORISED" || inv.Total != 655 {
		t.Fatalf("result %s", res)
	}
	again, err := c.Commit(ctx, "create-invoice", "SHOPIFY-1", json.RawMessage(xeroInvoice))
	if err != nil || x.Invoices() != 1 || !strings.Contains(string(again), inv.InvoiceID) {
		t.Fatalf("retry: %s, %v, invoices %d", again, err, x.Invoices())
	}
	if _, err := c.Commit(ctx, "void-invoice", "SHOPIFY-1#compensate", res); err != nil {
		t.Fatal(err)
	}
	if got := x.Invoice(inv.InvoiceID); got["Status"] != "VOIDED" || got["AmountDue"] != 0.0 {
		t.Fatalf("voided invoice %v", got)
	}
	if _, err := c.Commit(ctx, "void-invoice", "k2", res); err == nil || !strings.Contains(err.Error(), "status of VOIDED") {
		t.Fatalf("a second void: %v", err)
	}
	archived := strings.ReplaceAll(xeroInvoice, xerotest.Lovelace, xerotest.Archived)
	if _, err := c.Commit(ctx, "create-invoice", "SHOPIFY-2", json.RawMessage(archived)); err == nil || !strings.Contains(err.Error(), "archived") {
		t.Fatalf("an archived contact: %v", err)
	}
	got, err := c.Read(ctx, "get-customer", xerotest.Hopper)
	if err != nil || !strings.Contains(string(got), `"name":"Hopper Industries"`) || !strings.Contains(string(got), `"status":"ACTIVE"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s: %s", r.Name, r.Detail)
		}
	}
	if x.Refreshes != 1 || x.Spent != 0 {
		t.Fatalf("refreshes %d, spent %d", x.Refreshes, x.Spent)
	}
}

// Changed invoices are read with If-Modified-Since, filled from the cursor;
// their /Date(ms)/ stamps are the positions.
func TestXeroChangedInvoicesSinceTheCursor(t *testing.T) {
	x, seed := xerotest.New("xero-app", "s3cret")
	t.Cleanup(x.Close)
	var mu sync.Mutex
	clock := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	x.StartAt(1, func() time.Time { mu.Lock(); defer mu.Unlock(); return clock })
	c := newXeroConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	for i, key := range []string{"SHOPIFY-1", "SHOPIFY-2"} {
		mu.Lock()
		clock = clock.Add(time.Duration(i+1) * time.Minute)
		mu.Unlock()
		if _, err := c.Commit(ctx, "create-invoice", key, json.RawMessage(strings.ReplaceAll(xeroInvoice, "SHOPIFY-1", key))); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := c.Poll(ctx, "Invoice.Changed", 0, 10)
	if err != nil || len(evs) != 2 || evs[0].Position != time.Date(2026, 11, 5, 8, 1, 0, 0, time.UTC).UnixMilli() {
		t.Fatalf("changed invoices = %+v, %v", evs, err)
	}
	later, err := c.Poll(ctx, "Invoice.Changed", evs[0].Position, 10)
	if err != nil || len(later) != 1 || !strings.Contains(string(later[0].Payload), "SHOPIFY-2") {
		t.Fatalf("after the first: %+v, %v", later, err)
	}
	if x.Requests["GET Invoices"] != 2 {
		t.Fatalf("requests %v", x.Requests)
	}
}

func TestConstantsAndListIndexValidation(t *testing.T) {
	base := rest.Config{BaseURL: "https://x.example", Auth: rest.Auth{Type: "none"}}
	for name, op := range map[string]rest.Operation{
		"bad constant":        {Method: "POST", Path: "/x", Constants: map[string]any{"a b": 1}},
		"mapped and constant": {Method: "POST", Path: "/x", Fields: map[string]string{"Status": "s"}, Constants: map[string]any{"Status": "VOIDED"}},
	} {
		cfg := base
		cfg.Operations = map[string]rest.Operation{"op": op}
		if _, err := rest.New(cfg, "", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	cfg := base
	cfg.Events = map[string]rest.Event{"e": {Path: "/x", Items: "x", PositionFormat: "ticks"}}
	if _, err := rest.New(cfg, "", http.DefaultClient); err == nil {
		t.Error("an unknown position format was accepted")
	}
}
