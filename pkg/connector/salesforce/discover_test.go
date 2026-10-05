package salesforce

import (
	"context"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

func TestDiscover(t *testing.T) {
	c, sf := newConn(t)
	sf.Put("Opportunity", oppID(1), map[string]any{"StageName": "Closed Won", "Amount": 100, "AccountId": "001A", "ERP_Order_Number__c": nil}, t0)
	sf.Put("Account", "001A", map[string]any{"Name": "Ada"}, t0)
	sf.Types = map[string]string{"Opportunity.Amount": "currency"}
	sf.ReadOnly = map[string]bool{"Opportunity.StageName": true}
	cat, err := c.Discover(context.Background(), []string{"Account"})
	if err != nil {
		t.Fatal(err)
	}
	opp, ok := cat.Object("Opportunity")
	if !ok {
		t.Fatalf("objects %+v", cat.Objects)
	}
	if _, ok := cat.Object("Account"); !ok {
		t.Fatal("Account was not described")
	}
	if a, _ := opp.Field("Amount"); a.Type != "currency" {
		t.Fatalf("Amount %+v", a)
	}
	if id, _ := opp.Field("Id"); !id.Key || id.ReadOnly {
		t.Fatalf("Id %+v", id)
	}
	if s, _ := opp.Field("StageName"); !s.ReadOnly {
		t.Fatalf("StageName %+v", s)
	}
	found := false
	for _, l := range opp.Links {
		found = found || (l.Name == "Account" && l.To == "Account" && l.ToField == "Id")
	}
	if !found {
		t.Fatalf("links %+v", opp.Links)
	}
	u := meta.Usage(cat, nil)
	if got := strings.Join(u["Opportunity.StageName"], ";"); got != "event Opportunity.ClosedWon (where)" {
		t.Fatalf("StageName used by %q", got)
	}
	if got := strings.Join(u["Opportunity.ERP_Order_Number__c"], ";"); got != "operation update-opportunity" {
		t.Fatalf("ERP_Order_Number__c used by %q", got)
	}
	if cat.Events["Opportunity.ClosedWon"] != "Opportunity" {
		t.Fatalf("events %v", cat.Events)
	}
}

func TestChangeEventObject(t *testing.T) {
	for topic, want := range map[string]string{
		"/data/OpportunityChangeEvent": "Opportunity", "/data/Invoice__ChangeEvent": "Invoice__c",
		"/data/ChangeEvents": "", "/event/Order_Placed__e": "", "/data/ChangeEvent": "",
	} {
		if got := changeEventObject(topic); got != want {
			t.Errorf("%s: %q, want %q", topic, got, want)
		}
	}
}

// A subscription's sObject is discovered, with what the subscription reads.
func TestDiscoverSubscription(t *testing.T) {
	org := sftest.New()
	defer org.Close()
	org.Fields = map[string][]string{"Opportunity": {"StageName", "AccountId", "Amount"}}
	c := streamConn(t, org, Subscription{Topic: oppTopic, Match: map[string]any{"StageName": "Closed Won"}, Fields: []string{"AccountId", "Amount"}})
	cat, err := c.Discover(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cat.Object("Opportunity"); !ok || cat.Events["Opportunity.Changed"] != "Opportunity" {
		t.Fatalf("objects %+v events %v", cat.Objects, cat.Events)
	}
	u := meta.Usage(cat)
	if strings.Join(u["Opportunity.StageName"], ";") != "subscription Opportunity.Changed (match)" || len(u["Opportunity.Amount"]) != 1 {
		t.Fatalf("usage %v", u)
	}
}
