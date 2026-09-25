package rest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/shoptest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/stripetest"
)

// factory builds a connection the way the runtime does, resolving its
// webhook secrets.
func factory(t *testing.T, cfg Config, secrets connector.StaticSecrets) *Conn {
	t.Helper()
	raw, _ := json.Marshal(cfg)
	inst, err := Factory(context.Background(), compiler.ConnectorConfig{Endpoint: "stripe-billing", SecretRef: "openbao://stripe-billing/key", Config: raw}, secrets)
	if err != nil {
		t.Fatal(err)
	}
	return inst.(*Conn)
}

func stripeHooked(base string) Config {
	cfg := stripeConfig(base)
	e := cfg.Events["Invoice.Paid"]
	e.Webhook = &WebhookConfig{Signature: "stripe", SecretRef: "openbao://stripe-billing/webhook-secret", Filter: map[string]string{"type": "invoice.paid"}}
	cfg.Events["Invoice.Paid"] = e
	return cfg
}

// A Stripe delivery carries the same event object the events list
// returns, so a run sees one payload however the event arrived.
func TestStripeDeliveriesMatchPolledEvents(t *testing.T) {
	st := stripetest.New("sk_test")
	defer st.Close()
	c := factory(t, stripeHooked(st.URL()), connector.StaticSecrets{"openbao://stripe-billing/key": "sk_test", "openbao://stripe-billing/webhook-secret": "whsec_test"})
	hook, err := c.Webhook("Invoice.Paid")
	if err != nil {
		t.Fatal(err)
	}
	st.PayInvoice("cus_ada", 4990, "eur")
	polled, err := c.Poll(context.Background(), "Invoice.Paid", 0, 10)
	if err != nil || len(polled) != 1 {
		t.Fatalf("poll %v %v", polled, err)
	}
	body := polled[0].Payload
	now := time.Now()
	got, err := hook.Receive(http.Header{"Stripe-Signature": {stripetest.Sign("whsec_test", now, body)}}, body, now)
	if err != nil || len(got) != 1 || got[0].ID != polled[0].ID || string(got[0].Payload) != string(body) || got[0].Name != "Invoice.Paid" {
		t.Fatalf("received %+v %v", got, err)
	}
	// Another event type sent to the same endpoint is acknowledged, not kept.
	other := []byte(strings.Replace(string(body), `"type":"invoice.paid"`, `"type":"invoice.voided"`, 1))
	if got, err := hook.Receive(http.Header{"Stripe-Signature": {stripetest.Sign("whsec_test", now, other)}}, other, now); err != nil || len(got) != 0 {
		t.Fatalf("filtered %+v %v", got, err)
	}
}

func TestStripeSignatures(t *testing.T) {
	c := factory(t, stripeHooked("https://api.stripe.com"), connector.StaticSecrets{"openbao://stripe-billing/key": "sk", "openbao://stripe-billing/webhook-secret": "whsec_test"})
	hook, _ := c.Webhook("Invoice.Paid")
	body := []byte(`{"id":"evt_1","type":"invoice.paid","created":1}`)
	now := time.Now()
	valid := stripetest.Sign("whsec_test", now, body)
	for name, c := range map[string]struct {
		header string
		body   []byte
		ok     bool
	}{
		"valid":                  {valid, body, true},
		"rolled secret, old too": {valid + ",v1=" + strings.Repeat("0", 64), body, true},
		"tampered body":          {valid, []byte(`{"id":"evt_2","type":"invoice.paid","created":1}`), false},
		"wrong secret":           {stripetest.Sign("whsec_other", now, body), body, false},
		"replayed after 10 min":  {stripetest.Sign("whsec_test", now.Add(-10*time.Minute), body), body, false},
		"missing":                {"", body, false},
		"no v1":                  {"t=" + strings.Split(strings.TrimPrefix(valid, "t="), ",")[0], body, false},
	} {
		_, err := hook.Receive(http.Header{"Stripe-Signature": {c.header}}, c.body, now)
		if c.ok != (err == nil) || (!c.ok && !errors.Is(err, connector.ErrUnauthenticated)) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestShopifyAndGenericSignatures(t *testing.T) {
	shop := shoptest.New("shpat")
	defer shop.Close()
	cfg := Config{BaseURL: shop.URL(), Auth: Auth{Type: "header", Header: "X-Shopify-Access-Token"}, Events: map[string]Event{
		"Order.Created": {Path: "/orders.json", Items: "orders", Webhook: &WebhookConfig{Signature: "shopify", SecretRef: "openbao://shop/client-secret"}},
		"Thing.Changed": {Path: "/things", Webhook: &WebhookConfig{Signature: "hmac-sha256", SecretRef: "openbao://shop/generic", Header: "X-Hub-Signature-256", Prefix: "sha256=", Items: "things"}},
		"Thing.Base64":  {Path: "/things", Webhook: &WebhookConfig{Signature: "hmac-sha256", SecretRef: "openbao://shop/generic", Header: "X-Signature", Encoding: "base64"}},
	}}
	c := factory(t, cfg, connector.StaticSecrets{"openbao://stripe-billing/key": "shpat", "openbao://shop/client-secret": "shpss_test", "openbao://shop/generic": "s3cret"})

	order := []byte(`{"id":450789470,"email":"ada@example.com","total_price":"10.00"}`)
	hook, _ := c.Webhook("Order.Created")
	got, err := hook.Receive(http.Header{"X-Shopify-Hmac-Sha256": {shoptest.Sign("shpss_test", order)}}, order, time.Now())
	if err != nil || len(got) != 1 || got[0].ID != "450789470" {
		t.Fatalf("shopify %+v %v", got, err)
	}
	if _, err := hook.Receive(http.Header{"X-Shopify-Hmac-Sha256": {shoptest.Sign("other", order)}}, order, time.Now()); !errors.Is(err, connector.ErrUnauthenticated) {
		t.Fatalf("wrong secret: %v", err)
	}

	mac := hmac.New(sha256.New, []byte("s3cret"))
	batch := []byte(`{"things":[{"id":"a"},{"id":"b"}]}`)
	mac.Write(batch)
	sum := mac.Sum(nil)
	hook, _ = c.Webhook("Thing.Changed")
	got, err = hook.Receive(http.Header{"X-Hub-Signature-256": {"sha256=" + hex.EncodeToString(sum)}}, batch, time.Now())
	if err != nil || len(got) != 2 || got[1].ID != "b" {
		t.Fatalf("generic %+v %v", got, err)
	}
	if _, err := hook.Receive(http.Header{"X-Hub-Signature-256": {hex.EncodeToString(sum)}}, batch, time.Now()); !errors.Is(err, connector.ErrUnauthenticated) {
		t.Fatalf("missing prefix: %v", err)
	}
	hook, _ = c.Webhook("Thing.Base64")
	one := []byte(`{"id":"c"}`)
	mac.Reset()
	mac.Write(one)
	if got, err := hook.Receive(http.Header{"X-Signature": {base64.StdEncoding.EncodeToString(mac.Sum(nil))}}, one, time.Now()); err != nil || len(got) != 1 {
		t.Fatalf("base64 %+v %v", got, err)
	}
}

func TestWebhookConfiguration(t *testing.T) {
	// Not configured, or configured without its secret.
	st := stripetest.New("sk")
	defer st.Close()
	c := factory(t, stripeHooked(st.URL()), connector.StaticSecrets{"openbao://stripe-billing/key": "sk"})
	if _, err := c.Webhook("Other.Event"); !errors.Is(err, connector.ErrNoWebhook) {
		t.Fatalf("unconfigured: %v", err)
	}
	if _, err := c.Webhook("Invoice.Paid"); err == nil || !strings.Contains(err.Error(), "openbao://stripe-billing/webhook-secret") {
		t.Fatalf("missing secret: %v", err)
	}
	var reported bool
	for _, r := range c.Check(context.Background()) {
		if !r.OK {
			t.Fatalf("a polling deployment needs no webhook secret: %+v", r)
		}
		if r.Name == "webhook Invoice.Paid" && strings.Contains(r.Detail, "poll only") && strings.Contains(r.Detail, "--webhook-listen") {
			reported = true
		}
	}
	if !reported {
		t.Fatal("check does not report the missing webhook secret")
	}
	c = factory(t, stripeHooked(st.URL()), connector.StaticSecrets{"openbao://stripe-billing/key": "sk", "openbao://stripe-billing/webhook-secret": "whsec"})
	for _, r := range c.Check(context.Background()) {
		if !r.OK || (r.Name == "webhook Invoice.Paid" && !strings.Contains(r.Detail, "/webhooks/stripe-billing/Invoice.Paid")) {
			t.Fatalf("%+v", r)
		}
	}

	for name, w := range map[string]*WebhookConfig{
		"unsigned":        {Signature: "none", SecretRef: "x"},
		"no secret":       {Signature: "stripe"},
		"hmac, no header": {Signature: "hmac-sha256", SecretRef: "x"},
		"bad encoding":    {Signature: "hmac-sha256", SecretRef: "x", Header: "X-Sig", Encoding: "base32"},
		"bad filter":      {Signature: "stripe", SecretRef: "x", Filter: map[string]string{"a b": "c"}},
	} {
		cfg := stripeHooked("https://api.stripe.com")
		e := cfg.Events["Invoice.Paid"]
		e.Webhook = w
		cfg.Events["Invoice.Paid"] = e
		if _, err := New(cfg, "k", http.DefaultClient); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
