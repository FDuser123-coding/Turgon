package salesforce

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
)

func waitCommitted(t *testing.T, org *sftest.Server, name string, want []byte) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if m, _ := org.Managed(name); bytes.Equal(m.Committed, want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	m, _ := org.Managed(name)
	t.Fatalf("committed %x, want %x", m.Committed, want)
}

// Salesforce keeps the position: a worker that starts anywhere, without a
// stored position, resumes after the last batch Turgon delivered.
func TestManagedSubscriptionCommitsWhatWasDelivered(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	org.PubSub()
	org.CreateManaged("Turgon_Opportunities", oppTopic, "EARLIEST")
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Name": "a"}})
	second := org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006B"}, Fields: map[string]any{"StageName": "Closed Won"}})

	c := streamConn(t, org, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	ch, stop := collect(t, c, []byte("a position Turgon stored, ignored")) // Salesforce's position wins
	evs, resume := next(t, ch, 2)
	if evs[0].Name != "Opportunity.Changed" || payload(t, evs[1])["Id"] != "006B" || !bytes.Equal(resume, second) {
		t.Fatalf("events %+v resume %x", evs, resume)
	}
	waitCommitted(t, org, "Turgon_Opportunities", second)
	_ = stop()

	third := org.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{"006C"}, Fields: map[string]any{"Amount": 900.0}})
	again := streamConn(t, org, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	ch, stop = collect(t, again, nil)
	defer stop()
	evs, _ = next(t, ch, 1)
	if len(evs) != 1 || payload(t, evs[0])["Id"] != "006C" {
		t.Fatalf("after reopening: %+v", evs)
	}
	waitCommitted(t, org, "Turgon_Opportunities", third)
	if m, _ := org.Managed("Turgon_Opportunities"); m.Commits != 2 {
		t.Fatalf("commits %d", m.Commits)
	}
}

// A batch the inbox did not store is not committed: Salesforce sends it again.
func TestManagedSubscriptionDoesNotCommitWhatFailed(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	org.PubSub()
	org.CreateManaged("Turgon_Opportunities", oppTopic, "EARLIEST")
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Name": "a"}})
	c := streamConn(t, org, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	boom := errors.New("inbox unavailable")
	if err := c.Stream(context.Background(), "Opportunity.Changed", nil, func([]connector.Event, []byte) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if m, _ := org.Managed("Turgon_Opportunities"); m.Committed != nil {
		t.Fatalf("committed %x", m.Committed)
	}
	ch, stop := collect(t, c, nil)
	defer stop()
	if evs, _ := next(t, ch, 1); payload(t, evs[0])["Id"] != "006A" {
		t.Fatalf("redelivered %+v", evs)
	}
}

// A failed commit ends the stream; reopening resumes at the last commit.
func TestManagedSubscriptionFailedCommit(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	org.PubSub()
	org.CreateManaged("Turgon_Opportunities", oppTopic, "EARLIEST")
	org.PublishChange(sftest.Change{Type: "CREATE", RecordIDs: []string{"006A"}, Fields: map[string]any{"Name": "a"}})
	org.FailCommits(true)
	c := streamConn(t, org, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.Stream(ctx, "Opportunity.Changed", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "commit") || !strings.Contains(err.Error(), "Turgon_Opportunities") {
		t.Fatalf("err = %v", err)
	}
	if m, _ := org.Managed("Turgon_Opportunities"); m.Committed != nil {
		t.Fatalf("committed %x", m.Committed)
	}
}

func TestManagedSubscriptionMissingOrStopped(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	org.PubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	if err := c.Stream(context.Background(), "Opportunity.Changed", nil, nil); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing: %v", err)
	}
	org.CreateManaged("Turgon_Opportunities", oppTopic, "LATEST")
	org.SetManagedState("Turgon_Opportunities", "STOP")
	if err := c.Stream(context.Background(), "Opportunity.Changed", nil, nil); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Fatalf("stopped: %v", err)
	}
}

func TestManagedSubscriptionConfig(t *testing.T) {
	for _, bad := range []Subscription{{Topic: oppTopic, Managed: "1abc"}, {Topic: oppTopic, Managed: "a__b"}, {Topic: oppTopic, Managed: "a'b"},
		{Topic: oppTopic, Managed: "Turgon_", Start: ""}, {Topic: oppTopic, Managed: "Turgon", Start: "earliest"}} {
		if err := (Config{Subscriptions: map[string]Subscription{"E": bad}}).validate(); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
	if err := (Config{Subscriptions: map[string]Subscription{"E": {Topic: oppTopic, Managed: "Turgon_Opportunities"}}}).validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckManagedSubscription(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	defer org.ClosePubSub()
	org.PubSub()
	c := streamConn(t, org, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	check := func() connector.CheckResult {
		for _, r := range c.Check(context.Background()) {
			if r.Name == "subscription Opportunity.Changed" {
				return r
			}
		}
		t.Fatal("no subscription check")
		return connector.CheckResult{}
	}
	if r := check(); r.OK || !strings.Contains(r.Detail, "no managed subscription") || !strings.Contains(r.Fix, `"topicName":"/data/OpportunityChangeEvent"`) {
		t.Fatalf("missing: %+v", r)
	}
	org.CreateManaged("Turgon_Opportunities", "/data/AccountChangeEvent", "LATEST")
	if r := check(); r.OK || !strings.Contains(r.Detail, "follows /data/AccountChangeEvent") {
		t.Fatalf("other topic: %+v", r)
	}

	// A fresh org with the subscription as it should be.
	good := sftest.New()
	defer good.Close()
	defer good.ClosePubSub()
	good.PubSub()
	good.CreateManaged("Turgon_Opportunities", oppTopic, "EARLIEST")
	c = streamConn(t, good, Subscription{Topic: oppTopic, Managed: "Turgon_Opportunities"})
	if r := check(); !r.OK || !strings.Contains(r.Detail, "start at earliest") {
		t.Fatalf("ok: %+v", r)
	}
	good.SetManagedState("Turgon_Opportunities", "STOP")
	if r := check(); r.OK || !strings.Contains(r.Detail, "is STOP") || !strings.Contains(r.Fix, "state RUN") {
		t.Fatalf("stopped: %+v", r)
	}
}
