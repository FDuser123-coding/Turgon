package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
)

const oppTopic = "/data/OpportunityChangeEvent"

func streamConn(t *testing.T, org *sftest.Server, sub Subscription) *Conn {
	t.Helper()
	c, err := New(org.Credentials(), Config{PubSubEndpoint: org.PubSub(), Subscriptions: map[string]Subscription{"Opportunity.Changed": sub}},
		&http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

type batch struct {
	events []connector.Event
	resume []byte
}

// collect runs a subscription in the background and returns what it
// delivers, and a stop function returning Stream's error.
func collect(t *testing.T, c *Conn, resume []byte) (<-chan batch, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan batch, 100)
	done := make(chan error, 1)
	go func() {
		done <- c.Stream(ctx, "Opportunity.Changed", resume, func(evs []connector.Event, r []byte) error {
			out <- batch{evs, r}
			return nil
		})
	}()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() { cancel(); err = <-done })
		return err
	}
	t.Cleanup(func() { stop() })
	return out, stop
}

// next waits for delivered events, skipping keepalives.
func next(t *testing.T, ch <-chan batch, n int) ([]connector.Event, []byte) {
	t.Helper()
	var evs []connector.Event
	var resume []byte
	deadline := time.After(5 * time.Second)
	for len(evs) < n {
		select {
		case b := <-ch:
			evs = append(evs, b.events...)
			resume = b.resume
		case <-deadline:
			t.Fatalf("got %d of %d events", len(evs), n)
		}
	}
	return evs, resume
}

func payload(t *testing.T, ev connector.Event) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func waitSubscribed(t *testing.T, org *sftest.Server, n int) {
	t.Helper()
	for i := 0; i < 500 && org.Subscribers() != n; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if org.Subscribers() != n {
		t.Fatalf("%d subscribers, want %d", org.Subscribers(), n)
	}
}

func TestSubscriptionDeliversChangeEvents(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic, ChangeTypes: []string{"CREATE", "UPDATE"}})
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Name": "before subscribing"}})
	ch, stop := collect(t, c, nil)
	waitSubscribed(t, org, 1)

	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006B"}, Fields: map[string]any{
		"Name": "Big deal", "AccountId": "001B", "Amount": 7800.5, "StageName": "Prospecting", "CloseDate": "2026-10-01",
		"LastModifiedDate": time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)}})
	org.PublishChange(sftest.Change{Type: "DELETE", RecordIDs: []string{"006B"}}) // not a wanted change type
	third := org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006B"}, Fields: map[string]any{"StageName": "Closed Won"},
		Nulled: []string{"Description"}})
	evs, resume := next(t, ch, 2)
	if err := stop(); err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("events: %+v", evs)
	}
	created, updated := payload(t, evs[0]), payload(t, evs[1])
	h := created["ChangeEventHeader"].(map[string]any)
	if created["Id"] != "006B" || created["Amount"] != 7800.5 || created["CloseDate"] != "2026-10-01" ||
		created["LastModifiedDate"] != "2026-09-29T12:00:00Z" || h["changeType"] != "CREATE" || h["entityName"] != "Opportunity" {
		t.Fatalf("create payload: %s", evs[0].Payload)
	}
	if changed := h["changedFields"].([]any); len(changed) != 6 {
		t.Fatalf("changedFields: %v", changed)
	}
	// An update carries what it changed and what it cleared, not the
	// fields it left alone.
	uh := updated["ChangeEventHeader"].(map[string]any)
	if updated["StageName"] != "Closed Won" || updated["Name"] != nil || !hasKey(updated, "Description") || hasKey(updated, "Name") ||
		strings.Join(toStrings(uh["changedFields"]), ",") != "StageName,Description" || strings.Join(toStrings(uh["nulledFields"]), ",") != "Description" {
		t.Fatalf("update payload: %s", evs[1].Payload)
	}
	if evs[0].ID == "" || evs[0].ID == evs[1].ID {
		t.Fatalf("IDs: %q %q", evs[0].ID, evs[1].ID)
	}
	if string(resume) != string(third) {
		t.Fatalf("resume %x, want the last event's replay ID %x", resume, third)
	}

	// Resuming from a stored position delivers only what came after it.
	fourth := org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006B"}, Fields: map[string]any{"Amount": 9000.0}})
	ch, stop = collect(t, c, resume)
	evs, resume = next(t, ch, 1)
	stop()
	if len(evs) != 1 || payload(t, evs[0])["Amount"] != 9000.0 || string(resume) != string(fourth) {
		t.Fatalf("resumed: %+v %x", evs, resume)
	}
	if f := org.Fetches(); f[1].ReplayPreset != 2 || string(f[1].ReplayID) != string(third) || f[0].ReplayPreset != 0 {
		t.Fatalf("fetch requests: %+v", f)
	}
}

func hasKey(m map[string]any, k string) bool { _, ok := m[k]; return ok }

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestSubscriptionStartsEarliestAndFallsBackWhenTheReplayExpired(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic, Start: "earliest"})
	first := org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Name": "a"}})
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006B"}, Fields: map[string]any{"Name": "b"}})
	ch, stop := collect(t, c, nil)
	evs, _ := next(t, ch, 2)
	stop()
	if payload(t, evs[0])["Name"] != "a" {
		t.Fatalf("earliest: %+v", evs)
	}
	// The worker was down past the retention: the stored position is gone.
	org.Expire(oppTopic, 1)
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006C"}, Fields: map[string]any{"Name": "c"}})
	ch, stop = collect(t, c, first)
	evs, _ = next(t, ch, 2)
	stop()
	if payload(t, evs[0])["Name"] != "b" || payload(t, evs[1])["Name"] != "c" {
		t.Fatalf("after expiry: %s %s", evs[0].Payload, evs[1].Payload)
	}
}

func TestSubscriptionMatchSplitAndKeepalive(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	defer func(d time.Duration) { sftest.KeepAlive = d }(sftest.KeepAlive)
	sftest.KeepAlive = 50 * time.Millisecond
	c := streamConn(t, org, Subscription{Topic: oppTopic, Match: map[string]any{"StageName": "Closed Won"}})
	ch, stop := collect(t, c, nil)
	defer stop()
	waitSubscribed(t, org, 1)
	org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"StageName": "Negotiation"}})
	org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Amount": 5.0}})
	won := org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006A", "006B"}, Fields: map[string]any{"StageName": "Closed Won"}})
	evs, _ := next(t, ch, 2)
	if len(evs) != 2 || payload(t, evs[0])["Id"] != "006A" || payload(t, evs[1])["Id"] != "006B" ||
		!strings.HasSuffix(evs[0].ID, ".0") || !strings.HasSuffix(evs[1].ID, ".1") {
		t.Fatalf("closed won: %+v", evs)
	}
	// Nothing more happens: keepalives still move the position forward.
	select {
	case b := <-ch:
		if len(b.events) != 0 || string(b.resume) != string(won) {
			t.Fatalf("keepalive: %+v", b)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no keepalive")
	}
}

func TestSubscriptionLogsInAgainAfterTheTokenExpired(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic})
	_, stop := collect(t, c, nil)
	waitSubscribed(t, org, 1)
	stop()
	org.ExpireTokens()
	err := c.Stream(context.Background(), "Opportunity.Changed", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "Unauthenticated") {
		t.Fatalf("err = %v", err)
	}
	logins := org.Logins
	ch, stop := collect(t, c, nil)
	defer stop()
	waitSubscribed(t, org, 1)
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006Z"}, Fields: map[string]any{"Name": "z"}})
	next(t, ch, 1)
	if org.Logins != logins+1 {
		t.Fatalf("logins %d -> %d", logins, org.Logins)
	}
}

func TestSubscriptionStopsWhenDeliveryFails(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic, Start: "earliest"})
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Name": "a"}})
	boom := errors.New("inbox unavailable")
	err := c.Stream(context.Background(), "Opportunity.Changed", nil, func([]connector.Event, []byte) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestSubscriptionConfig(t *testing.T) {
	for _, bad := range []Subscription{{Topic: "OpportunityChangeEvent"}, {Topic: "/data/X", Start: "now"}, {Topic: "/data/X", Batch: 5000},
		{Topic: "/data/X", ChangeTypes: []string{"CREATE;"}}} {
		if err := (Config{Subscriptions: map[string]Subscription{"E": bad}}).validate(); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
	org := sftest.New()
	defer org.Close()
	c := streamConn(t, org, Subscription{Topic: oppTopic})
	if !c.Streams("Opportunity.Changed") || c.Streams("Other") {
		t.Fatal("Streams")
	}
	if _, err := c.Poll(context.Background(), "Opportunity.Changed", 0, 1); err == nil || !strings.Contains(err.Error(), "Pub/Sub") {
		t.Fatalf("poll of a subscription: %v", err)
	}
}

func TestBitmapFields(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"AccountChangeEvent","fields":[
		{"name":"ChangeEventHeader","type":"string"},
		{"name":"Name","type":["null",{"type":"record","name":"Name","fields":[
			{"name":"Salutation","type":["null","string"]},{"name":"FirstName","type":["null","string"]},{"name":"LastName","type":["null","string"]}]}]},
		{"name":"Phone","type":["null","string"]},
		{"name":"F3","type":["null","string"]},{"name":"F4","type":["null","string"]},{"name":"F5","type":["null","string"]},
		{"name":"F6","type":["null","string"]},{"name":"F7","type":["null","string"]},{"name":"F8","type":["null","string"]}]}`).(*avro.RecordSchema)
	got := bitmapFields(s, []any{"0x104", "1-0x6"})
	if strings.Join(got, ",") != "Phone,F8,Name.FirstName,Name.LastName" {
		t.Fatalf("fields: %v", got)
	}
}

// Events keep coming past the first batch: the subscriber asks for more.
func TestSubscriptionRequestsMoreEvents(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic, Start: "earliest", Batch: 2})
	for i := 0; i < 7; i++ {
		org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Amount": float64(i)}})
	}
	ch, stop := collect(t, c, nil)
	defer stop()
	evs, _ := next(t, ch, 7)
	for i, ev := range evs {
		if payload(t, ev)["Amount"] != float64(i) {
			t.Fatalf("event %d: %s", i, ev.Payload)
		}
	}
}

// With fields, the payload is the record as it is now, read with SOQL.
func TestSubscriptionReadsTheRecord(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	org.Put("Opportunity", "006000000000001AAA", map[string]any{"AccountId": "001000000000001AAA", "Amount": 7800.0,
		"CloseDate": "2026-10-01", "OpportunityLineItems": map[string]any{"records": []any{map[string]any{"Quantity": 2.0}}}}, time.Now())
	c := streamConn(t, org, Subscription{Topic: oppTopic, Start: "earliest", Match: map[string]any{"StageName": "Closed Won"},
		Fields: []string{"AccountId", "Amount", "CloseDate", "(SELECT Quantity FROM OpportunityLineItems)"}})
	org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006000000000001AAA"}, Fields: map[string]any{"StageName": "Closed Won"}})
	org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006000000000009AAA"}, Fields: map[string]any{"StageName": "Closed Won"}}) // deleted since
	ch, stop := collect(t, c, nil)
	defer stop()
	evs, _ := next(t, ch, 2)
	got := payload(t, evs[0])
	h, _ := got["ChangeEventHeader"].(map[string]any)
	if got["Id"] != "006000000000001AAA" || got["AccountId"] != "001000000000001AAA" || got["Amount"] != 7800.0 || h["changeType"] != "UPDATE" ||
		got["OpportunityLineItems"] == nil || hasKey(got, "attributes") {
		t.Fatalf("payload: %s", evs[0].Payload)
	}
	if gone := payload(t, evs[1]); gone["StageName"] != "Closed Won" || gone["Id"] != "006000000000009AAA" {
		t.Fatalf("deleted record: %s", evs[1].Payload)
	}
}

func TestCheckSubscriptions(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	c, err := New(org.Credentials(), Config{PubSubEndpoint: org.PubSub(), Subscriptions: map[string]Subscription{
		"Opportunity.Changed": {Topic: oppTopic}, "Invoice.Posted": {Topic: "/event/Invoice_Posted__e"}}}, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	results := map[string]connector.CheckResult{}
	for _, r := range c.Check(context.Background()) {
		results[r.Name] = r
	}
	if r := results["subscription Opportunity.Changed"]; !r.OK {
		t.Errorf("opportunity: %+v", r)
	}
	if r := results["subscription Invoice.Posted"]; r.OK || !strings.Contains(r.Fix, "__e") {
		t.Errorf("unknown topic: %+v", r)
	}
	sftest.NoSubscribe[oppTopic] = true
	defer delete(sftest.NoSubscribe, oppTopic)
	for _, r := range c.Check(context.Background()) {
		if r.Name == "subscription Opportunity.Changed" && (r.OK || !strings.Contains(r.Fix, "permission set")) {
			t.Errorf("no permission: %+v", r)
		}
	}
}
