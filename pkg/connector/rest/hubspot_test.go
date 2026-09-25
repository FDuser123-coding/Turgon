package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/hubspottest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// hubspotConfig is the example hubspot-crm connection's config, read from
// the example so the two cannot drift apart.
func hubspotConfig(t *testing.T, base string) Config {
	t.Helper()
	data, err := os.ReadFile("../../../examples/connections/hubspot-crm.yaml")
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
	var cfg Config
	dec := json.NewDecoder(strings.NewReader(string(conn.Spec.Config)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	cfg.BaseURL = base
	return cfg
}

func newHubSpot(t *testing.T) (*hubspottest.HubSpot, *Conn) {
	t.Helper()
	h := hubspottest.New("pat-eu1-turgon")
	t.Cleanup(h.Close)
	c, err := New(hubspotConfig(t, h.URL()), "pat-eu1-turgon", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return h, c
}

func dealIDs(evs []connector.Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.ID)
	}
	return out
}

func won(name, email, amount string) map[string]string {
	return map[string]string{"dealname": name, "customer_email": email, "amount": amount, "deal_currency_code": "EUR",
		"dealstage": "closedwon", "closedate": "2026-09-25T08:00:00.000Z"}
}

// Won deals are found with the search API, oldest change first, across
// polls; open deals and deals already carrying an ERP order are not.
func TestWonDealsAreSearchedInOrderOfChange(t *testing.T) {
	h, c := newHubSpot(t)
	ctx := context.Background()
	a := h.AddDeal(won("Lovelace rollout", "ada@lovelace-gmbh.example", "1500"))
	open := h.AddDeal(map[string]string{"dealname": "Turing pilot", "dealstage": "presentationscheduled"})
	b := h.AddDeal(won("Hopper renewal", "grace@lovelace-gmbh.example", "900"))
	done := won("Old deal", "ada@lovelace-gmbh.example", "10")
	done["erp_order_number"] = "7"
	h.AddDeal(done)

	evs, err := c.Poll(ctx, "Deal.Won", 0, 1)
	if err != nil || strings.Join(dealIDs(evs), ",") != a {
		t.Fatalf("first poll %v %v", dealIDs(evs), err)
	}
	var deal struct {
		ID         string            `json:"id"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(evs[0].Payload, &deal); err != nil || deal.Properties["customer_email"] != "ada@lovelace-gmbh.example" || deal.Properties["amount"] != "1500" {
		t.Fatalf("payload %s %v", evs[0].Payload, err)
	}
	evs, err = c.Poll(ctx, "Deal.Won", evs[0].Position, 10)
	if err != nil || strings.Join(dealIDs(evs), ",") != b {
		t.Fatalf("second poll %v %v", dealIDs(evs), err)
	}
	after := evs[0].Position
	// A deal won later is found by its change, not its creation.
	h.SetStage(open, "closedwon")
	evs, err = c.Poll(ctx, "Deal.Won", after, 10)
	if err != nil || strings.Join(dealIDs(evs), ",") != open {
		t.Fatalf("third poll %v %v", dealIDs(evs), err)
	}
	if h.Requests["POST /crm/v3/objects/deals/search"] != 3 {
		t.Errorf("requests %v", h.Requests)
	}
}

// Deals imported in one millisecond are never split between polls: the
// search cursor is strictly after the last change time read.
func TestDealsChangedInTheSameMillisecondAreNeverSplit(t *testing.T) {
	h, c := newHubSpot(t)
	first := h.AddDeal(won("A", "a@x.example", "1"))
	h.AddDealsAtOnce(won("B", "b@x.example", "2"), won("C", "c@x.example", "3"))
	evs, err := c.Poll(context.Background(), "Deal.Won", 0, 2)
	if err != nil || strings.Join(dealIDs(evs), ",") != first {
		t.Fatalf("got %v %v", dealIDs(evs), err)
	}
	rest, err := c.Poll(context.Background(), "Deal.Won", evs[0].Position, 10)
	if err != nil || len(rest) != 2 {
		t.Fatalf("second poll %v %v", dealIDs(rest), err)
	}
}

// The ERP order number is written to the deal, previewed, confirmed, and
// cleared again by compensation with "", which is how HubSpot clears a
// property. Once set, the deal leaves the search.
func TestDealLinkIsCapturedConfirmedAndCleared(t *testing.T) {
	h, c := newHubSpot(t)
	ctx := context.Background()
	id := h.AddDeal(won("Lovelace rollout", "ada@lovelace-gmbh.example", "1500"))
	payload := json.RawMessage(`{"dealId": "` + id + `", "erpOrderNumber": "4711"}`)

	preview, err := c.Simulate(ctx, "link-deal", payload)
	if err != nil || !strings.Contains(string(preview), `"current":{"properties.erp_order_number":null}`) ||
		!strings.Contains(string(preview), `"request":"PATCH /crm/v3/objects/deals/`+id+`"`) {
		t.Fatalf("preview %s %v", preview, err)
	}
	result, err := c.Commit(ctx, "link-deal", "deal-"+id, payload)
	if err != nil {
		t.Fatal(err)
	}
	if h.Deal(id)["erp_order_number"] != "4711" {
		t.Fatalf("deal %v", h.Deal(id))
	}
	if err := c.Confirm(ctx, "link-deal", result); err != nil {
		t.Fatal(err)
	}
	if evs, _ := c.Poll(ctx, "Deal.Won", 0, 10); len(evs) != 0 {
		t.Fatalf("linked deal still searched: %v", dealIDs(evs))
	}
	got, err := c.Read(ctx, "get-deal", id)
	if err != nil || !strings.Contains(string(got), `"erpOrderNumber":"4711"`) || !strings.Contains(string(got), `"stage":"closedwon"`) {
		t.Fatalf("read %s %v", got, err)
	}

	// Without nullValue the restore sends null, which HubSpot rejects.
	cfg := hubspotConfig(t, h.URL())
	op := cfg.Operations["unlink-deal"]
	op.NullValue = nil
	cfg.Operations["unlink-deal"] = op
	plain, _ := New(cfg, "pat-eu1-turgon", http.DefaultClient)
	if _, err := plain.Commit(ctx, "unlink-deal", "deal-"+id+"#compensate", result); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "INVALID_STRING") {
		t.Fatalf("null restore: %v", err)
	}
	if _, err := c.Commit(ctx, "unlink-deal", "deal-"+id+"#compensate", result); err != nil {
		t.Fatal(err)
	}
	if v, ok := h.Deal(id)["erp_order_number"]; ok {
		t.Fatalf("erp_order_number = %q after restore", v)
	}

	if _, err := c.Read(ctx, "get-deal", "1"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("missing deal: %v", err)
	}
	h.FailUpdates = true
	if _, err := c.Commit(ctx, "link-deal", "deal-x", payload); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("400 is not permanent: %v", err)
	}
}

func TestHubSpotCheck(t *testing.T) {
	_, c := newHubSpot(t)
	for _, r := range c.Check(context.Background()) {
		if !r.OK {
			t.Fatalf("%+v", r)
		}
		if r.Name == "event Deal.Won" && !strings.Contains(r.Detail, "POST /crm/v3/objects/deals/search") {
			t.Errorf("detail %q", r.Detail)
		}
	}
	h := hubspottest.New("other-token")
	defer h.Close()
	c, _ = New(hubspotConfig(t, h.URL()), "pat-eu1-turgon", http.DefaultClient)
	if r := c.Check(context.Background()); len(r) != 1 || r[0].OK || !strings.Contains(r[0].Fix, "rejected the credential") {
		t.Fatalf("%+v", r)
	}
}

func TestSearchBodiesAndNullValues(t *testing.T) {
	body := fillBody(map[string]any{"limit": "{{limit}}", "filters": []any{map[string]any{"value": "{{positionMillis}}"}, "t={{position}}"}, "n": 3.0},
		func(s string) string {
			return strings.NewReplacer("{{positionMillis}}", "1700", "{{position}}", "P").Replace(s)
		}, 25)
	if b, _ := json.Marshal(body); string(b) != `{"filters":[{"value":"1700"},"t=P"],"limit":25,"n":3}` {
		t.Fatal(string(b))
	}
	empty := ""
	if b, _ := json.Marshal(Operation{NullValue: &empty, Restore: true}.request(map[string]any{"properties.a": nil, "properties.b": "x"})); string(b) != `{"properties":{"a":"","b":"x"}}` {
		t.Fatal(string(b))
	}
	for name, mutate := range map[string]func(*Config){
		"GET with a body": func(c *Config) { e := c.Events["Deal.Won"]; e.Method = ""; c.Events["Deal.Won"] = e },
		"PUT list":        func(c *Config) { e := c.Events["Deal.Won"]; e.Method = "PUT"; c.Events["Deal.Won"] = e },
		"form nullValue": func(c *Config) {
			o := c.Operations["unlink-deal"]
			o.BodyFormat = "form"
			c.Operations["unlink-deal"] = o
		},
	} {
		cfg := hubspotConfig(t, "https://api.hubapi.com")
		mutate(&cfg)
		if _, err := New(cfg, "k", http.DefaultClient); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
