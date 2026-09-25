package engine

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/notify"
)

// told records notifications as a channel of a hub with a console URL.
type told struct {
	mu   sync.Mutex
	sent []notify.Notification
	fail bool
}

func (t *told) Name() string { return "test" }

func (t *told) Notify(_ context.Context, n notify.Notification) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, n)
	if t.fail {
		return context.DeadlineExceeded
	}
	return nil
}

func (t *told) kinds() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var k []string
	for _, n := range t.sent {
		k = append(k, n.Kind)
	}
	return strings.Join(k, ",")
}

func (f *fixture) listen() *told {
	rec := &told{}
	f.rt.Activities.Notifier = &notify.Hub{ConsoleURL: "https://turgon.example.com", Channels: []notify.Channel{rec}}
	return rec
}

func fact(n notify.Notification, name string) string {
	for _, f := range n.Facts {
		if f.Name == name {
			return f.Value
		}
	}
	return ""
}

func TestApproversAreToldWhatWaitsWithoutCustomerData(t *testing.T) {
	f, hs := newHubSpotFixture(t)
	rec := f.listen()
	hs.AddDeal(hubspotDeal("Ada@Lovelace-GmbH.example", "1500.50"))
	if _, err, _ := f.run(f.dispatch()[0], approve("03-write")); err != nil {
		t.Fatal(err)
	}
	if rec.kinds() != notify.ApprovalPending {
		t.Fatalf("sent %s", rec.kinds())
	}
	n := rec.sent[0]
	if n.Title != "Approval needed: create-sales-order on erp-db" || n.Workflow != "hubspot-won-deals-to-erp" || n.Step != "03-write" ||
		fact(n, "Amount") != "1500.50" || fact(n, "Risk") != "high" || !strings.HasPrefix(fact(n, "Reference"), "hubspot-deal-") ||
		n.Link != "https://turgon.example.com/runs/"+n.RunID || n.RunID == "" {
		t.Fatalf("%+v", n)
	}
	// The payload stays in the console.
	if b, _ := json.Marshal(n); strings.Contains(strings.ToLower(string(b)), "lovelace") {
		t.Fatalf("customer data in a notification: %s", b)
	}
}

func TestApproversAreToldWhenNobodyDecided(t *testing.T) {
	f, hs := newHubSpotFixture(t)
	rec := f.listen()
	hs.AddDeal(hubspotDeal("ada@lovelace-gmbh.example", "10"))
	if _, err, _ := f.run(f.dispatch()[0]); errType(err) != ErrTypeRejected {
		t.Fatalf("err = %v", err)
	}
	if rec.kinds() != notify.ApprovalPending+","+notify.ApprovalTimedOut {
		t.Fatalf("sent %s", rec.kinds())
	}
}

func TestStewardsAreToldAboutUnmatchedRecords(t *testing.T) {
	f, shop, _ := newShopifyFixture(t)
	rec := f.listen()
	ctx := context.Background()
	if err := f.store.Link(ctx, "Customer", "shop-db", "ada@lovelace-gmbh.example", "C-100", map[string]string{"email": "ada@lovelace-gmbh.example", "domain": "lovelace-gmbh.example"}); err != nil {
		t.Fatal(err)
	}
	if _, err, _ := f.runFor(shop, "edsger@lovelace-gmbh.example"); errType(err) != ErrTypeUnresolved {
		t.Fatalf("err = %v", err)
	}
	if rec.kinds() != notify.StewardNeeded {
		t.Fatalf("sent %s", rec.kinds())
	}
	n := rec.sent[0]
	if n.Title != "A Customer from shopify-store needs a data steward" || n.Link != "https://turgon.example.com/steward" ||
		!strings.HasPrefix(fact(n, "Best suggestion"), "C-100 (") || fact(n, "Suggestions") != "1" {
		t.Fatalf("%+v", n)
	}
	if b, _ := json.Marshal(n); strings.Contains(string(b), "edsger") {
		t.Fatalf("the record's address in a notification: %s", b)
	}
}

func TestOperatorsAreToldWhenWritesCannotBeUndone(t *testing.T) {
	f := newFixture(t)
	rec := f.listen()
	f.publish(1005, "ada@example.com", "500.00")
	in := f.dispatch()[0]
	first := *in.Workflow.Steps[2].Write
	first.Compensation = "not-configured"
	in.Workflow.Steps[2].Write = &first
	second := first
	second.Simulation, second.IdempotencyKey = "", "{{ doc.externalId }}"
	in.Workflow.Steps = append(in.Workflow.Steps,
		compiler.WorkflowStep{Name: "04-map", Map: &compiler.MapConfig{Mapping: "test-negative@1", Fields: map[string]string{
			"externalId": "externalId & '-B'", "customerId": "customerId", "orderDate": "orderDate", "netValue": "-1", "currency": "currency",
		}}},
		compiler.WorkflowStep{Name: "05-write", Write: &second},
	)
	if _, err, _ := f.run(in, approve("03-write"), approve("05-write")); errType(err) != ErrTypeCompensationFailed {
		t.Fatalf("err = %v", err)
	}
	k := rec.kinds()
	if !strings.HasSuffix(k, notify.CompensationFailed) {
		t.Fatalf("sent %s", k)
	}
	n := rec.sent[len(rec.sent)-1]
	if fact(n, "Failed step") != "05-write" || !strings.Contains(fact(n, "Step error"), "check constraint") ||
		!strings.HasPrefix(fact(n, "Undo errors"), "undo 03-write: ") || strings.Contains(fact(n, "Undo errors"), "scheduledEventID") {
		t.Fatalf("%+v", n)
	}
}

// A chat outage never holds up a run.
func TestNotificationFailuresDoNotStopRuns(t *testing.T) {
	f, hs := newHubSpotFixture(t)
	rec := f.listen()
	rec.fail = true
	id := hs.AddDeal(hubspotDeal("ada@lovelace-gmbh.example", "10"))
	if _, err, _ := f.run(f.dispatch()[0], approve("03-write")); err != nil {
		t.Fatal(err)
	}
	if len(rec.sent) != 5 || hs.Deal(id)["erp_order_number"] == "" {
		t.Fatalf("attempts %d, deal %v", len(rec.sent), hs.Deal(id))
	}
}

// Writes agents ask for wait for the same people, who are told which
// agent asked and for whom.
func TestApproversAreToldAboutAgentWrites(t *testing.T) {
	f := newFixture(t)
	rec := f.listen()
	if _, err, _ := f.runAgent(agentOrder("9", "integration-operator"), approve(AgentStep)); err != nil {
		t.Fatal(err)
	}
	if rec.kinds() != notify.ApprovalPending {
		t.Fatalf("sent %s", rec.kinds())
	}
	n := rec.sent[0]
	if n.Workflow != "agent/create_sales_order" || fact(n, "Requested by") != "agent claude for ada@example.com" || fact(n, "Amount") != "480.00" {
		t.Fatalf("%+v", n)
	}
}
