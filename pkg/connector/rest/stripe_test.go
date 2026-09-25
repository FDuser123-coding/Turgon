package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/stripetest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// stripeConfig is the example stripe-billing connection's config.
func stripeConfig(base string) Config {
	return Config{
		BaseURL:   base,
		Auth:      Auth{Type: "bearer"},
		CheckPath: "/v1/balance",
		Events: map[string]Event{"Invoice.Paid": {
			Path:   "/v1/events",
			Params: map[string]string{"type": "invoice.paid", "created[gt]": "{{position}}", "limit": "{{limit}}"},
			Items:  "data", Position: "created", PositionFormat: "unix", Order: "desc",
			Pagination: &Pagination{Param: "starting_after", More: "has_more"},
		}},
		Operations: map[string]Operation{
			"get-invoice": {Method: "GET", Path: "/v1/invoices/{{id}}", Fields: map[string]string{"number": "number", "amount_paid": "amountPaid", "status": "status"}},
			"link-invoice": {
				Method: "POST", Path: "/v1/invoices/{{invoiceId}}", BodyFormat: "form", IdempotencyHeader: "Idempotency-Key",
				Fields:  map[string]string{"metadata.erp_payment_id": "erpPaymentId"},
				Capture: &Capture{Path: "/v1/invoices/{{invoiceId}}"},
			},
			"unlink-invoice": {Method: "POST", Path: "/v1/invoices/{{invoiceId}}", BodyFormat: "form", Restore: true},
		},
	}
}

func newStripe(t *testing.T) (*stripetest.Stripe, *Conn) {
	t.Helper()
	s := stripetest.New("sk_test_turgon")
	t.Cleanup(s.Close)
	c, err := New(stripeConfig(s.URL()), "sk_test_turgon", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

func ids(t *testing.T, evs []json.RawMessage) []string {
	t.Helper()
	var out []string
	for _, p := range evs {
		var e struct {
			Data struct {
				Object struct {
					ID string `json:"id"`
				} `json:"object"`
			} `json:"data"`
		}
		_ = json.Unmarshal(p, &e)
		out = append(out, e.Data.Object.ID)
	}
	return out
}

// Stripe lists newest first. A burst of payments larger than one poll is
// read oldest first across polls, walking pages back to the cursor, so
// nothing is skipped.
func TestNewestFirstListsAreReadOldestFirst(t *testing.T) {
	s, c := newStripe(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s.PayInvoice("cus_ada", 1000+int64(i), "eur")
	}
	var seen []string
	after := int64(0)
	for poll := 0; poll < 4 && len(seen) < 5; poll++ {
		evs, err := c.Poll(ctx, "Invoice.Paid", after, 2) // pages of 2, so each poll pages back
		if err != nil {
			t.Fatal(err)
		}
		var payloads []json.RawMessage
		for _, e := range evs {
			payloads = append(payloads, e.Payload)
			if e.Position <= after {
				t.Fatalf("position went back: %d <= %d", e.Position, after)
			}
			after = e.Position
		}
		seen = append(seen, ids(t, payloads)...)
	}
	if strings.Join(seen, ",") != "in_0001,in_0002,in_0003,in_0004,in_0005" {
		t.Fatalf("read %v", seen)
	}
	if s.Requests["GET /v1/events"] < 4 {
		t.Errorf("expected paging: %v", s.Requests)
	}
}

func TestPaymentsInTheSameSecondAreNeverSplit(t *testing.T) {
	s, c := newStripe(t)
	s.PayInvoice("cus_ada", 100, "eur")
	s.PayInvoice("cus_ada", 200, "eur")
	s.PayInvoice("cus_ada", 300, "eur", true) // same second as the second
	evs, err := c.Poll(context.Background(), "Invoice.Paid", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	var payloads []json.RawMessage
	for _, e := range evs {
		payloads = append(payloads, e.Payload)
	}
	// Two fit, but the second shares its second with the third: only the first is taken.
	if got := ids(t, payloads); len(got) != 1 || got[0] != "in_0001" {
		t.Fatalf("got %v", got)
	}
	rest, _ := c.Poll(context.Background(), "Invoice.Paid", evs[0].Position, 10)
	if len(rest) != 2 {
		t.Fatalf("second poll got %d", len(rest))
	}
}

func TestTooManyNewPagesFailsInsteadOfSkipping(t *testing.T) {
	s, c := newStripe(t)
	cfg := stripeConfig(s.URL())
	ev := cfg.Events["Invoice.Paid"]
	ev.Pagination.MaxPages = 2
	cfg.Events["Invoice.Paid"] = ev
	c, _ = New(cfg, "sk_test_turgon", http.DefaultClient)
	for i := 0; i < 7; i++ {
		s.PayInvoice("cus_ada", 100, "eur")
	}
	if _, err := c.Poll(context.Background(), "Invoice.Paid", 0, 2); err == nil || !strings.Contains(err.Error(), "maxPages") {
		t.Fatalf("err = %v", err)
	}
}

func TestFormEncodedMetadataIsCapturedConfirmedAndUnset(t *testing.T) {
	s, c := newStripe(t)
	ctx := context.Background()
	id := s.PayInvoice("cus_ada", 123450, "eur")
	payload := json.RawMessage(`{"invoiceId": "` + id + `", "erpPaymentId": 42}`)

	preview, err := c.Simulate(ctx, "link-invoice", payload)
	if err != nil || !strings.Contains(string(preview), `"current":{"metadata.erp_payment_id":null}`) || !strings.Contains(string(preview), `"proposed":{"metadata.erp_payment_id":42}`) {
		t.Fatalf("preview %s %v", preview, err)
	}
	result, err := c.Commit(ctx, "link-invoice", "evt_0001", payload)
	if err != nil {
		t.Fatal(err)
	}
	if md := s.Invoice(id)["metadata"].(map[string]any); md["erp_payment_id"] != "42" {
		t.Fatalf("metadata %v", md)
	}
	// Stripe stores metadata as strings; the read-back still confirms 42.
	if err := c.Confirm(ctx, "link-invoice", result); err != nil {
		t.Fatal(err)
	}
	// A retried request with the same key is replayed by Stripe, not re-applied.
	if _, err := c.Commit(ctx, "link-invoice", "evt_0001", payload); err != nil {
		t.Fatal(err)
	}
	// Compensation unsets the key: it had no previous value.
	if _, err := c.Commit(ctx, "unlink-invoice", "evt_0001#compensate", result); err != nil {
		t.Fatal(err)
	}
	if md := s.Invoice(id)["metadata"].(map[string]any); len(md) != 0 {
		t.Fatalf("metadata after restore %v", md)
	}

	got, err := c.Read(ctx, "get-invoice", id)
	if err != nil || !strings.Contains(string(got), `"amountPaid":123450`) {
		t.Fatalf("read %s %v", got, err)
	}
	if _, err := c.Read(ctx, "get-invoice", "in_missing"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("missing invoice: %v", err)
	}
	s.FailUpdates = true
	if _, err := c.Commit(ctx, "link-invoice", "evt_x", payload); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("400 is not permanent: %v", err)
	}
}

func TestEncodeForm(t *testing.T) {
	got := encodeForm(map[string]any{"metadata": map[string]any{"a": "1", "b": nil}, "items": []any{map[string]any{"price": json.Number("5")}}, "paid": true}).Encode()
	if got != "items%5B0%5D%5Bprice%5D=5&metadata%5Ba%5D=1&metadata%5Bb%5D=&paid=true" {
		t.Fatal(got)
	}
}

func TestDescendingListsNeedPagination(t *testing.T) {
	cfg := stripeConfig("https://api.stripe.com")
	ev := cfg.Events["Invoice.Paid"]
	ev.Pagination = nil
	cfg.Events["Invoice.Paid"] = ev
	if _, err := New(cfg, "k", http.DefaultClient); err == nil {
		t.Fatal("accepted a descending list without pagination")
	}
}
