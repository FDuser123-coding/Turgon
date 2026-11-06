package rest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/qbotest"
)

func qboSecret(refresh string) string {
	return `{"clientId": "qb-app", "clientSecret": "s3cret", "refreshToken": "` + refresh + `"}`
}

func newQuickBooksConn(t *testing.T, q *qbotest.QBO, secret string, tokens connector.TokenStore) *rest.Conn {
	t.Helper()
	cfg := exampleConfig(t, "quickbooks", func(s string) string {
		s = strings.ReplaceAll(s, "https://oauth.platform.intuit.com", q.URL())
		return strings.ReplaceAll(s, "https://quickbooks.api.intuit.com", q.URL())
	})
	c, err := rest.New(cfg, secret, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if tokens != nil {
		c.UseTokens(tokens)
	}
	return c
}

const qboReceipt = `{"customerId": "1", "orderDate": "2026-11-05", "docNumber": "SHOP1001", "note": "Shopify order SHOPIFY-1",
	"qboLines": [{"DetailType": "SalesItemLineDetail", "Amount": 420, "SalesItemLineDetail": {"ItemRef": {"value": "11"}, "Qty": 2, "UnitPrice": 210}}]}`

// Intuit rotates the refresh token on every use. Two workers sharing the
// token store refresh once and share the access token; when it expires, the
// rotated refresh token (not the seed, which Intuit no longer takes) gets
// the next one.
func TestQuickBooksRotatingRefreshTokens(t *testing.T) {
	q, seed := qbotest.New("qb-app", "s3cret")
	t.Cleanup(q.Close)
	store := &connector.MemoryTokens{}
	a := newQuickBooksConn(t, q, qboSecret(seed), store)
	b := newQuickBooksConn(t, q, qboSecret(seed), store)
	ctx := context.Background()
	if _, err := a.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
		t.Fatal(err)
	}
	if q.Refreshes != 1 || q.Spent != 0 {
		t.Fatalf("refreshes %d, spent %d: the second worker should reuse the first one's access token", q.Refreshes, q.Spent)
	}
	q.ExpireAccessTokens()
	for _, c := range []*rest.Conn{a, b} {
		if _, err := c.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
			t.Fatal(err)
		}
	}
	if q.Refreshes != 2 || q.Spent != 0 {
		t.Fatalf("after expiry: refreshes %d, spent %d", q.Refreshes, q.Spent)
	}
	// The seed was spent long ago: a worker without the store would spend
	// it again and be refused, so none runs without one.
	alone := newQuickBooksConn(t, q, qboSecret(seed), nil)
	if _, err := alone.Poll(ctx, "SalesReceipt.Changed", 0, 10); !errors.Is(err, rest.ErrNoTokenStore) {
		t.Fatalf("without a token store: %v", err)
	}
	// A refused (spent) token says what to do.
	fresh := newQuickBooksConn(t, q, qboSecret(seed), &connector.MemoryTokens{})
	if _, err := fresh.Poll(ctx, "SalesReceipt.Changed", 0, 10); err == nil || !strings.Contains(err.Error(), "authorize the app again") {
		t.Fatalf("a spent seed: %v", err)
	}
}

// A new authorization (a new refresh token in the secret) replaces the
// rotated token the store holds, even one that still works.
func TestQuickBooksNewSeedReplacesStoredToken(t *testing.T) {
	q, seed := qbotest.New("qb-app", "s3cret")
	t.Cleanup(q.Close)
	store := &connector.MemoryTokens{}
	ctx := context.Background()
	if _, err := newQuickBooksConn(t, q, qboSecret(seed), store).Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
		t.Fatal(err)
	}
	q.ExpireAccessTokens()
	seed2 := q.Authorize()
	if _, err := newQuickBooksConn(t, q, qboSecret(seed2), store).Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
		t.Fatalf("a new seed: %v", err)
	}
	// The new seed was spent (rotated), not the token the old one rotated to.
	if q.Refreshes != 2 || q.Spent != 0 {
		t.Fatalf("refreshes %d, spent %d", q.Refreshes, q.Spent)
	}
	if _, err := newQuickBooksConn(t, q, qboSecret(seed2), &connector.MemoryTokens{}).Poll(ctx, "SalesReceipt.Changed", 0, 10); err == nil {
		t.Fatal("the new seed was not spent")
	}
}

// requestid makes a retried create answer the first receipt; a void needs
// the create's SyncToken; changed receipts are queried oldest first, and a
// query that finds nothing (QuickBooks leaves the list out) is empty.
func TestQuickBooksReceiptRetryVoidAndQuery(t *testing.T) {
	q, seed := qbotest.New("qb-app", "s3cret")
	t.Cleanup(q.Close)
	c := newQuickBooksConn(t, q, qboSecret(seed), &connector.MemoryTokens{})
	ctx := context.Background()
	if evs, err := c.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil || len(evs) != 0 {
		t.Fatalf("no receipts yet: %v, %v", evs, err)
	}
	res, err := c.Commit(ctx, "create-sales-receipt", "SHOPIFY-1", json.RawMessage(qboReceipt))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Id, SyncToken, DocNumber string
		TotalAmt                 float64
	}
	_ = json.Unmarshal(res, &r)
	if r.Id == "" || r.DocNumber != "SHOP1001" || r.TotalAmt != 420 {
		t.Fatalf("result %s", res)
	}
	again, err := c.Commit(ctx, "create-sales-receipt", "SHOPIFY-1", json.RawMessage(qboReceipt))
	if err != nil || q.Receipts() != 1 || !strings.Contains(string(again), `"Id":"`+r.Id+`"`) {
		t.Fatalf("retry: %s, %v, receipts %d", again, err, q.Receipts())
	}
	evs, err := c.Poll(ctx, "SalesReceipt.Changed", 0, 10)
	if err != nil || len(evs) != 1 || connector.RecordID(evs[0].ID) != r.Id {
		t.Fatalf("changed receipts = %+v, %v", evs, err)
	}
	if _, err := c.Commit(ctx, "void-sales-receipt", "SHOPIFY-1#compensate", res); err != nil {
		t.Fatal(err)
	}
	if got := q.Receipt(r.Id); got["TotalAmt"] != 0.0 || got["PrivateNote"] != "Voided" {
		t.Fatalf("voided receipt %v", got)
	}
	// The result's SyncToken is stale now; QuickBooks refuses a second void.
	if _, err := c.Commit(ctx, "void-sales-receipt", "k", res); err == nil || !strings.Contains(err.Error(), "Stale Object") {
		t.Fatalf("a stale void: %v", err)
	}
	wrong := strings.ReplaceAll(qboReceipt, `"Amount": 420`, `"Amount": 400`)
	if _, err := c.Commit(ctx, "create-sales-receipt", "SHOPIFY-2", json.RawMessage(wrong)); err == nil || !strings.Contains(err.Error(), "UnitPrice * Qty") {
		t.Fatalf("a wrong amount: %v", err)
	}
	got, err := c.Read(ctx, "get-customer", qbotest.Bill)
	if err != nil || !strings.Contains(string(got), `"name":"Bill's Windsurf Shop"`) || !strings.Contains(string(got), `"email":"grace@example.com"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s: %s", r.Name, r.Detail)
		}
	}
}
