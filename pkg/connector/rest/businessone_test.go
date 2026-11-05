package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/b1test"
)

const b1Login = `{"CompanyDB": "SBODEMOUS", "UserName": "turgon", "Password": "s3cret"}`

func newBusinessOne(t *testing.T, secret string) (*b1test.B1, *rest.Conn) {
	t.Helper()
	b1 := b1test.New("SBODEMOUS", "turgon", "s3cret")
	t.Cleanup(b1.Close)
	cfg := exampleConfig(t, "business-one", func(s string) string {
		return strings.ReplaceAll(s, "https://b1.acme.internal:50000", b1.URL())
	})
	c, err := rest.New(cfg, secret, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return b1, c
}

const b1Order = `{"customerCode": "C20000", "orderDate": "2026-11-05", "dueDate": "2026-11-05", "reference": "SHOPIFY-1",
	"comments": "Shopify order SHOPIFY-1",
	"b1Lines": [{"ItemCode": "A00001", "Quantity": 2, "UnitPrice": 210}, {"ItemCode": "A00002", "Quantity": 1}]}`

// The Service Layer signs in with a session, finds a retried create's order
// by NumAtCard, signs in again when its session times out, and cancels an
// order with a POST that has no body.
func TestBusinessOneSessionRetryAndCancel(t *testing.T) {
	b1, c := newBusinessOne(t, b1Login)
	ctx := context.Background()
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(b1Order))
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		DocEntry, DocNum int
		DocTotal         float64
		Existing         bool
	}
	_ = json.Unmarshal(res, &created)
	// 2 x 210.00 at the shop's price, and the second item at the price list's 182.00.
	if created.DocNum != 1 || created.DocTotal != 602 || created.Existing || b1.Logins != 1 {
		t.Fatalf("result %s, logins %d", res, b1.Logins)
	}
	again, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(b1Order))
	if err != nil || b1.Orders() != 1 || !strings.Contains(string(again), `"existing":true`) || b1.Requests["POST Orders"] != 1 {
		t.Fatalf("retry: %s, %v, orders %d, requests %v", again, err, b1.Orders(), b1.Requests)
	}
	// The session times out; the next request signs in again.
	b1.ExpireSessions()
	if _, err := c.Commit(ctx, "cancel-sales-order", "SHOPIFY-1#compensate", res); err != nil {
		t.Fatal(err)
	}
	if o := b1.Order(created.DocEntry); o["Cancelled"] != "tYES" || b1.Logins != 2 {
		t.Fatalf("order %v, logins %d", o, b1.Logins)
	}
	// Business One's refusals are the write's.
	frozen := strings.ReplaceAll(b1Order, "C20000", "C99999")
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-2", json.RawMessage(frozen)); err == nil || !strings.Contains(err.Error(), "BP C99999 is inactive") {
		t.Fatalf("frozen customer: %v", err)
	}
	if _, err := c.Commit(ctx, "cancel-sales-order", "k", res); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("a second cancel: %v", err)
	}
}

func TestBusinessOneNewOrdersReadsAndChecks(t *testing.T) {
	_, c := newBusinessOne(t, b1Login)
	ctx := context.Background()
	for _, key := range []string{"SHOPIFY-1", "SHOPIFY-2"} {
		if _, err := c.Commit(ctx, "create-sales-order", key, json.RawMessage(strings.ReplaceAll(b1Order, "SHOPIFY-1", key))); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := c.Poll(ctx, "SalesOrder.Created", 0, 10)
	if err != nil || len(evs) != 2 || evs[0].Position != 1 || evs[1].Position != 2 {
		t.Fatalf("new orders = %+v, %v", evs, err)
	}
	if later, err := c.Poll(ctx, "SalesOrder.Created", 1, 10); err != nil || len(later) != 1 {
		t.Fatalf("after the first: %+v, %v", later, err)
	}
	got, err := c.Read(ctx, "get-customer", b1test.Norm)
	if err != nil || !strings.Contains(string(got), `"name":"Norm Thompson"`) || !strings.Contains(string(got), `"frozen":"tNO"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s: %s", r.Name, r.Detail)
		}
	}
}

func TestSessionAuthRefusesBadLogins(t *testing.T) {
	_, c := newBusinessOne(t, `{"CompanyDB": "SBODEMOUS", "UserName": "turgon", "Password": "wrong"}`)
	for _, r := range c.Check(context.Background()) {
		if r.OK && strings.Contains(r.Name, "reach") {
			t.Errorf("check %s passed with a wrong password", r.Name)
		}
	}
	if _, err := c.Poll(context.Background(), "SalesOrder.Created", 0, 1); err == nil || !strings.Contains(err.Error(), "Login failed") {
		t.Fatalf("poll with a wrong password: %v", err)
	}
	base := rest.Config{BaseURL: "https://x.example", Auth: rest.Auth{Type: "session", LoginURL: "https://x.example/Login"}}
	if _, err := rest.New(base, "user:password", http.DefaultClient); err == nil {
		t.Error("a session secret that is not the login's JSON body was accepted")
	}
	base.Auth.LoginURL = "/Login"
	if _, err := rest.New(base, b1Login, http.DefaultClient); err == nil {
		t.Error("a relative loginURL was accepted")
	}
	body := rest.Config{BaseURL: "https://x.example", Auth: rest.Auth{Type: "none"},
		Operations: map[string]rest.Operation{"op": {Method: "POST", Path: "/x", BodyFormat: "none", Fields: map[string]string{"a": "b"}}}}
	if _, err := rest.New(body, "", http.DefaultClient); err == nil {
		t.Error("a request without a body that maps fields was accepted")
	}
}
