package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// newStreamFixture runs salesforce-won-deals-to-erp-cdc: won deals pushed
// over the (fake) Pub/Sub API.
func newStreamFixture(t *testing.T) (*fixture, *sftest.Server) {
	t.Helper()
	sf := sftest.New()
	t.Cleanup(sf.Close)
	t.Cleanup(sf.ClosePubSub)
	addr := sf.PubSub()
	f := newFixtureFor(t, "salesforce-won-deals-to-erp-cdc", connector.StaticSecrets{"openbao://salesforce/prod-jwt": sf.Credentials()},
		func(cfg string) string {
			return strings.Replace(cfg, `"subscriptions":`, `"pubsubEndpoint":"`+addr+`","subscriptions":`, 1)
		})
	if err := f.store.PutXref(context.Background(), "Customer", "salesforce-prod", account, "C-100"); err != nil {
		t.Fatal(err)
	}
	return f, sf
}

// subscribe runs the runtime's subscriptions until the returned stop is
// called; delivered receives a value per batch of new events.
func subscribe(t *testing.T, f *fixture) (delivered chan struct{}, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	delivered = make(chan struct{}, 100)
	s := &Streams{Runtime: f.rt, Inbox: f.store, Locker: f.store, Standby: 20 * time.Millisecond, MinBackoff: 10 * time.Millisecond,
		Delivered: func() { delivered <- struct{}{} }, Log: t.Logf}
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	stopped := false
	stop = func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return delivered, stop
}

func wait(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing delivered")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000 && !cond(); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("timed out waiting for " + what)
	}
}

func putDeal(sf *sftest.Server, id string, amount float64) {
	sf.Put("Opportunity", id, map[string]any{
		"StageName": "Closed Won", "AccountId": account, "Amount": amount, "CloseDate": "2026-09-24", "CurrencyIsoCode": "EUR",
		"OpportunityLineItems": map[string]any{"totalSize": 1, "done": true, "records": []any{
			map[string]any{"Quantity": 2, "Product2": map[string]any{"ProductCode": "M-1"}}}},
	}, time.Now())
}

// A deal won in Salesforce is pushed over the Pub/Sub API, stored in the
// inbox with the position after it, and becomes an ERP order.
func TestSubscribedEventsStartRuns(t *testing.T) {
	f, sf := newStreamFixture(t)
	if !f.rt.Streams["salesforce-prod"]["Opportunity.Won"] {
		t.Fatalf("streams = %v", f.rt.Streams)
	}
	delivered, stop := subscribe(t, f)
	waitFor(t, "the subscription", func() bool { return sf.Subscribers() == 1 })
	putDeal(sf, wonDeal, 1200.5)
	sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{wonDeal}, Fields: map[string]any{"Amount": 1200.5}}) // not won
	sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{wonDeal}, Fields: map[string]any{"StageName": "Closed Won"}})
	wait(t, delivered)

	d := &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: &recorder{}, Inbox: f.store}
	rec := d.Starter.(*recorder)
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.ids) != 1 || rec.runs[rec.ids[0]].Event.Name != "Opportunity.Won" {
		t.Fatalf("runs = %v", rec.ids)
	}
	res, err, _ := f.run(rec.runs[rec.ids[0]], approve("03-write"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Writes[0].Status != writeguard.StatusCommitted || f.orders(`external_id = '`+wonDeal+`' AND net_value = 1200.50
		AND lines = '[{"material":"M-1","quantity":2}]'`) != 1 {
		t.Fatalf("writes = %+v", res.Writes)
	}

	// The worker stops; a deal is won meanwhile; the new subscription
	// resumes after the stored position and delivers only that deal.
	stop()
	other := "006000000000002AAA"
	putDeal(sf, other, 99)
	sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{other}, Fields: map[string]any{"StageName": "Closed Won"}})
	delivered, _ = subscribe(t, f)
	wait(t, delivered)
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.ids) != 2 || rec.runs[rec.ids[1]].Event.ID == rec.runs[rec.ids[0]].Event.ID {
		t.Fatalf("after restart: %v", rec.ids)
	}
	if fetches := sf.Fetches(); len(fetches) != 2 || fetches[1].ReplayPreset != 2 {
		t.Fatalf("the restart did not resume: %+v", fetches)
	}
}

// Two workers: one subscribes, the other stands by and takes over.
func TestOneWorkerSubscribesPerEvent(t *testing.T) {
	f, sf := newStreamFixture(t)
	_, stopFirst := subscribe(t, f)
	waitFor(t, "the first subscription", func() bool { return sf.Subscribers() == 1 })
	delivered, _ := subscribe(t, f)
	time.Sleep(100 * time.Millisecond)
	if n := sf.Subscribers(); n != 1 {
		t.Fatalf("%d subscriptions", n)
	}
	stopFirst()
	waitFor(t, "the standby to take over", func() bool { return sf.Subscribers() == 1 })
	putDeal(sf, wonDeal, 10)
	sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{wonDeal}, Fields: map[string]any{"StageName": "Closed Won"}})
	wait(t, delivered)
}

// With a managed subscription, Salesforce keeps the position: a worker
// whose database lost it (a new deployment, a restore) resumes after the
// last event Turgon stored, not from the start.
func TestManagedSubscriptionResumesWhereSalesforceKeptIt(t *testing.T) {
	sf := sftest.New()
	t.Cleanup(sf.Close)
	t.Cleanup(sf.ClosePubSub)
	addr := sf.PubSub()
	sf.CreateManaged("Turgon_Won_Deals", "/data/OpportunityChangeEvent", "LATEST")
	f := newFixtureFor(t, "salesforce-won-deals-to-erp-cdc", connector.StaticSecrets{"openbao://salesforce/prod-jwt": sf.Credentials()},
		func(cfg string) string {
			cfg = strings.Replace(cfg, `"subscriptions":`, `"pubsubEndpoint":"`+addr+`","subscriptions":`, 1)
			return strings.Replace(cfg, `"topic":"/data/OpportunityChangeEvent"`, `"managed":"Turgon_Won_Deals","topic":"/data/OpportunityChangeEvent"`, 1)
		})
	delivered, stop := subscribe(t, f)
	waitFor(t, "the managed subscription", func() bool { return sf.ManagedSubscribers() == 1 })
	putDeal(sf, wonDeal, 1200.5)
	first := sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{wonDeal}, Fields: map[string]any{"StageName": "Closed Won"}})
	wait(t, delivered)
	waitFor(t, "the commit", func() bool { m, _ := sf.Managed("Turgon_Won_Deals"); return string(m.Committed) == string(first) })

	stop()
	if _, err := f.pool.Exec(context.Background(), `DELETE FROM turgon_stream_positions`); err != nil {
		t.Fatal(err)
	}
	other := "006000000000002AAA"
	putDeal(sf, other, 99)
	sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{other}, Fields: map[string]any{"StageName": "Closed Won"}})
	delivered, _ = subscribe(t, f)
	wait(t, delivered)

	d := &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: &recorder{}, Inbox: f.store}
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := d.Starter.(*recorder)
	if len(rec.ids) != 2 {
		t.Fatalf("runs = %v", rec.ids)
	}
	if fetches := sf.Fetches(); len(fetches) != 0 {
		t.Fatalf("an unmanaged subscription was opened: %+v", fetches)
	}
}
