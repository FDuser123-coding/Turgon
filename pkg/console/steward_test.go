package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/engine"
)

type fakeSteward struct {
	runs    []UnresolvedRun
	retried []string
	failID  string
}

func (f *fakeSteward) Unresolved(context.Context, int) ([]UnresolvedRun, error) {
	var out []UnresolvedRun
	for _, r := range f.runs {
		keep := true
		for _, id := range f.retried {
			keep = keep && id != r.ID
		}
		if keep {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeSteward) Retry(_ context.Context, id string) error {
	if id == f.failID {
		return errors.New("run is running or completed")
	}
	f.retried = append(f.retried, id)
	return nil
}

type fakeXref map[string]string

func (x fakeXref) PutXref(_ context.Context, entity, system, source, master string) error {
	x[entity+"/"+system+"/"+source] = master
	return nil
}

func unresolvedRuns() *fakeSteward {
	now := time.Now()
	nobody := engine.Unresolved{Entity: "Customer", System: "shopify-store", Ref: "nobody@example.com"}
	return &fakeSteward{runs: []UnresolvedRun{
		{ID: "shopify-store-orders-to-erp/2", Workflow: "shopify-store-orders-to-erp", Failed: now, Link: nobody},
		{ID: "shopify-store-orders-to-erp/1", Workflow: "shopify-store-orders-to-erp", Failed: now.Add(-time.Hour), Link: nobody},
		{ID: "stripe-payments-to-erp/evt_9", Workflow: "stripe-payments-to-erp", Failed: now.Add(-time.Minute),
			Link: engine.Unresolved{Entity: "Customer", System: "stripe-billing", Ref: "cus_new"}},
	}}
}

func stewardAuth() ProxyAuth {
	a := proxyAuth()
	a.StewardGroup = "turgon-stewards"
	return a
}

func TestStewardQueueGroupsRunsByMissingLink(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: stewardAuth(), Steward: unresolvedRuns()})
	rec := do(t, s, "GET", "/api/steward", "", as("vera@example.com", "sales"))
	var items []StewardItem
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	// Oldest wait first; the two Shopify runs wait on one link.
	if len(items) != 2 || items[0].Ref != "nobody@example.com" || len(items[0].Runs) != 2 || items[1].Ref != "cus_new" {
		t.Fatalf("items %+v", items)
	}
}

func TestStewardLinksAndRetries(t *testing.T) {
	steward, xref := unresolvedRuns(), fakeXref{}
	var log bytes.Buffer
	s := New(Config{Runs: newRuns(), Auth: stewardAuth(), Steward: steward, Xref: xref, Recorder: audit.New(&log)})
	link := `{"entity":"Customer","system":"shopify-store","ref":"nobody@example.com","master":"C-100","note":"same company, new buyer"}`

	for name, c := range map[string]struct {
		body   string
		groups string
		code   int
	}{
		"approvers are not stewards": {link, "turgon-approvers", http.StatusForbidden},
		"master ID required":         {strings.Replace(link, "C-100", " ", 1), "turgon-stewards", http.StatusBadRequest},
		"odd master ID":              {strings.Replace(link, "C-100", "C-100; DROP", 1), "turgon-stewards", http.StatusBadRequest},
		"nothing waits on it":        {strings.Replace(link, "nobody@", "somebody@", 1), "turgon-stewards", http.StatusConflict},
		"unknown fields":             {strings.Replace(link, `"note"`, `"by":"ceo","note"`, 1), "turgon-stewards", http.StatusBadRequest},
	} {
		if rec := do(t, s, "POST", "/api/steward/links", c.body, as("sam@example.com", c.groups)); rec.Code != c.code {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
	if len(xref) != 0 || len(steward.retried) != 0 {
		t.Fatalf("refused requests changed state: %v %v", xref, steward.retried)
	}

	rec := do(t, s, "POST", "/api/steward/links", link, as("sam@example.com", "turgon-stewards"))
	var res LinkResult
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if rec.Code != 200 || len(res.Retried) != 2 || xref["Customer/shopify-store/nobody@example.com"] != "C-100" {
		t.Fatalf("link: %d %s, xref %v", rec.Code, rec.Body, xref)
	}
	entries := log.String()
	if !strings.Contains(entries, `"actor":"sam@example.com","action":"xref.linked"`) || !strings.Contains(entries, "same company, new buyer") ||
		!strings.Contains(entries, `"action":"run.retried"`) {
		t.Fatalf("audit log:\n%s", entries)
	}
	if _, err := audit.Verify(strings.NewReader(entries)); err != nil {
		t.Fatal(err)
	}
	// The queue no longer lists them.
	rec = do(t, s, "GET", "/api/steward", "", as("sam@example.com", "turgon-stewards"))
	if strings.Contains(rec.Body.String(), "nobody@example.com") {
		t.Fatalf("still queued: %s", rec.Body)
	}
}

func TestStewardReportsRunsThatCouldNotBeRetried(t *testing.T) {
	steward := unresolvedRuns()
	steward.failID = "shopify-store-orders-to-erp/1"
	var log bytes.Buffer
	s := New(Config{Runs: newRuns(), Auth: stewardAuth(), Steward: steward, Xref: fakeXref{}, Recorder: audit.New(&log)})
	rec := do(t, s, "POST", "/api/steward/links", `{"entity":"Customer","system":"shopify-store","ref":"nobody@example.com","master":"C-100"}`, as("sam@example.com", "turgon-stewards"))
	var res LinkResult
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if rec.Code != 200 || len(res.Retried) != 1 || res.Failed["shopify-store-orders-to-erp/1"] == "" {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestStewardNeedsTheStateDatabase(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: stewardAuth(), Steward: unresolvedRuns()})
	rec := do(t, s, "POST", "/api/steward/links", `{"entity":"Customer","system":"stripe-billing","ref":"cus_new","master":"C-7"}`, as("sam@example.com", "turgon-stewards"))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "--database-url") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}
