package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/postgres"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/shoptest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/stripetest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// hooked is a fixture whose worker receives webhooks, with a dispatcher
// on a clock the test moves.
type hooked struct {
	*fixture
	srv   *httptest.Server
	d     *Dispatcher
	rec   *recorder
	clock time.Time
}

func newHooked(t *testing.T, f *fixture) *hooked {
	t.Helper()
	h := &hooked{fixture: f, rec: &recorder{}, clock: time.Now()}
	h.srv = httptest.NewServer(WebhookHandler(f.rt, f.store, nil))
	t.Cleanup(h.srv.Close)
	h.d = &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: h.rec, Inbox: f.store, Now: func() time.Time { return h.clock }}
	return h
}

// poll runs one dispatcher pass and returns the runs it started.
func (h *hooked) poll() []RunInput {
	h.t.Helper()
	before := len(h.rec.ids)
	if _, err := h.d.Poll(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	var out []RunInput
	for _, id := range h.rec.ids[before:] {
		out = append(out, h.rec.runs[id])
	}
	return out
}

func newStripeHooked(t *testing.T) (*hooked, *stripetest.Stripe) {
	t.Helper()
	st := stripetest.New("rk_test_turgon")
	t.Cleanup(st.Close)
	f := newFixtureWith(t, "stripe-payments-to-erp", connector.StaticSecrets{
		"openbao://stripe-billing/restricted-key": "rk_test_turgon",
		"openbao://stripe-billing/webhook-secret": "whsec_turgon",
	}, true, func(cfg string) string { return strings.ReplaceAll(cfg, "https://api.stripe.com", st.URL()) })
	if err := f.store.PutXref(context.Background(), "Customer", "stripe-billing", "cus_ada", "C-100"); err != nil {
		t.Fatal(err)
	}
	h := newHooked(t, f)
	st.SendWebhooks(h.srv.URL+"/webhooks/stripe-billing/Invoice.Paid", "whsec_turgon")
	return h, st
}

// A payment delivered by webhook starts its run without polling Stripe;
// one whose delivery never came is found by the reconciling poll, and an
// event read both ways starts one run.
func TestWebhooksStartRunsAndPollingReconciles(t *testing.T) {
	h, st := newStripeHooked(t)
	if runs := h.poll(); len(runs) != 0 { // the first pass reconciles: nothing yet
		t.Fatalf("runs = %d", len(runs))
	}
	polls := st.Requests["GET /v1/events"]

	a := st.PayInvoice("cus_ada", 4990, "eur")
	runs := h.poll()
	if len(st.Deliveries) != 1 || st.Deliveries[0] != http.StatusOK || len(runs) != 1 || st.Requests["GET /v1/events"] != polls {
		t.Fatalf("deliveries %v, runs %d, polls %d -> %d", st.Deliveries, len(runs), polls, st.Requests["GET /v1/events"])
	}
	res, err, _ := h.run(runs[0])
	if err != nil || res.Writes[1].Status != writeguard.StatusCommitted || h.payments(`external_id = '`+a+`' AND amount = 49.90`) != 1 {
		t.Fatalf("run: %v %+v", err, res.Writes)
	}

	// Stripe gives up on a delivery; until the next reconcile, nothing.
	st.DropWebhooks = true
	b := st.PayInvoice("cus_ada", 1500, "eur")
	if runs := h.poll(); len(runs) != 0 {
		t.Fatalf("started %d before reconciling", len(runs))
	}
	h.clock = h.clock.Add(DefaultReconcile + time.Second)
	runs = h.poll()
	var ev struct {
		Data struct {
			Object struct {
				ID string `json:"id"`
			} `json:"object"`
		} `json:"data"`
	}
	// The reconciling poll read both payments; the delivered one was
	// already in the inbox, so only the missed one starts a run.
	if len(runs) != 1 || json.Unmarshal(runs[0].Event.Payload, &ev) != nil || ev.Data.Object.ID != b {
		t.Fatalf("reconciled runs %+v", runs)
	}
	if _, err, _ := h.run(runs[0]); err != nil || h.payments("true") != 2 {
		t.Fatalf("reconciled run: %v", err)
	}
	h.clock = h.clock.Add(DefaultReconcile + time.Second)
	if runs := h.poll(); len(runs) != 0 {
		t.Fatalf("restarted %d", len(runs))
	}
	// Not even attempted: a failed run would otherwise start again.
	if h.rec.calls != 2 {
		t.Fatalf("%d starts for 2 events", h.rec.calls)
	}
}

func post(t *testing.T, url string, header http.Header, body []byte) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.String()
}

func TestWebhookDeliveriesAreVerified(t *testing.T) {
	h, _ := newStripeHooked(t)
	url := h.srv.URL + "/webhooks/stripe-billing/Invoice.Paid"
	body := []byte(`{"id":"evt_forged","object":"event","type":"invoice.paid","created":1790409600,"data":{"object":{"id":"in_forged","customer":"cus_ada","amount_paid":100000000,"currency":"eur","number":"X","status_transitions":{"paid_at":1790409600}}}}`)
	sign := func(secret string, b []byte) http.Header {
		return http.Header{"Stripe-Signature": {stripetest.Sign(secret, time.Now(), b)}}
	}

	if code, _ := post(t, url, sign("whsec_guess", body), body); code != http.StatusUnauthorized {
		t.Fatalf("forged: %d", code)
	}
	if code, _ := post(t, url, http.Header{}, body); code != http.StatusUnauthorized {
		t.Fatalf("unsigned: %d", code)
	}
	if code, _ := post(t, h.srv.URL+"/webhooks/erp-db/Invoice.Paid", sign("whsec_turgon", body), body); code != http.StatusNotFound {
		t.Fatalf("unknown event: %d", code)
	}
	if resp, err := http.Get(url); err != nil || resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %v %v", resp, err)
	}
	big := bytes.Repeat([]byte(" "), MaxWebhookBody+1)
	if code, _ := post(t, url, sign("whsec_turgon", big), big); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized: %d", code)
	}
	if got, _ := h.store.Inbox(context.Background(), "stripe-billing/Invoice.Paid", "Invoice.Paid", 0, 10); len(got) != 0 {
		t.Fatalf("refused deliveries were stored: %+v", got)
	}
	// A signed delivery is stored once, however often it is redelivered.
	if code, msg := post(t, url, sign("whsec_turgon", body), body); code != http.StatusOK || !strings.Contains(msg, `"new":1`) {
		t.Fatalf("signed: %d %s", code, msg)
	}
	if code, msg := post(t, url, sign("whsec_turgon", body), body); code != http.StatusOK || !strings.Contains(msg, `"new":0`) {
		t.Fatalf("redelivered: %d %s", code, msg)
	}
	if runs := h.poll(); len(runs) != 1 || runs[0].Event.ID != "evt_forged" {
		t.Fatalf("runs %+v", runs)
	}
}

// A worker that receives webhooks needs their signing secret; one that
// only polls does not.
func TestWebhooksNeedTheirSigningSecret(t *testing.T) {
	st := stripetest.New("rk")
	defer st.Close()
	f := newFixtureFor(t, "stripe-payments-to-erp", connector.StaticSecrets{"openbao://stripe-billing/restricted-key": "rk"},
		func(cfg string) string { return strings.ReplaceAll(cfg, "https://api.stripe.com", st.URL()) })
	if len(f.rt.Webhooks) != 0 {
		t.Fatalf("polling worker receives webhooks: %v", f.rt.Webhooks)
	}
	_, err := New(context.Background(), f.rt.Spec, Options{
		Registry: connector.Registry{postgres.Name: postgres.Factory, rest.Name: rest.Factory},
		Secrets:  connector.StaticSecrets{"openbao://stripe-billing/restricted-key": "rk", "openbao://erp-db/dsn": pgtest.URL(t, f.schema)},
		Store:    f.store, Resolver: f.store, Webhooks: true,
	})
	if err == nil || !strings.Contains(err.Error(), "openbao://stripe-billing/webhook-secret") {
		t.Fatalf("err = %v", err)
	}
}

func TestShopifyOrdersArriveByWebhook(t *testing.T) {
	shop := shoptest.New("shpat_test")
	t.Cleanup(shop.Close)
	f := newFixtureWith(t, "shopify-store-orders-to-erp", connector.StaticSecrets{
		"openbao://shopify-store/token": "shpat_test", "openbao://shopify-store/client-secret": "shpss_turgon",
	}, true, func(cfg string) string {
		return strings.ReplaceAll(cfg, "https://turgon-demo.myshopify.com/admin/api/"+shoptest.Version, shop.URL())
	})
	if err := f.store.PutXref(context.Background(), "Customer", "shopify-store", "ada@example.com", "C-100"); err != nil {
		t.Fatal(err)
	}
	h := newHooked(t, f)
	h.poll() // first reconcile
	shop.SendWebhooks(h.srv.URL+"/webhooks/shopify-store/Order.Created", "shpss_turgon")
	id := shop.AddOrder(map[string]any{
		"name": "#1001", "email": "ada@example.com", "created_at": "2026-09-25T09:30:00Z",
		"subtotal_price": "42.00", "currency": "eur", "line_items": []any{map[string]any{"sku": "M-1", "quantity": 1}},
	})
	runs := h.poll()
	if len(shop.Deliveries) != 1 || shop.Deliveries[0] != http.StatusOK || len(runs) != 1 || runs[0].Event.ID != fmt.Sprint(id) {
		t.Fatalf("deliveries %v, runs %+v", shop.Deliveries, runs)
	}
	if _, err, _ := h.run(runs[0], approve("03-write")); err != nil {
		t.Fatal(err)
	}
	if note, _ := shop.Order(id)["note"].(string); !strings.HasPrefix(note, "ERP order ") {
		t.Fatalf("note %q", note)
	}
}
