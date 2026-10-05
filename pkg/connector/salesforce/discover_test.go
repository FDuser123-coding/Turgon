package salesforce

import (
	"context"
	"strings"
	"testing"

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
}
