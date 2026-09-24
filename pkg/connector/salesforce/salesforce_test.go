package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

var cfg = Config{
	Events: map[string]EventQuery{
		"Opportunity.ClosedWon": {SObject: "Opportunity", Where: "StageName = 'Closed Won'", Fields: []string{"Id", "AccountId", "Amount"}},
	},
	Operations: map[string]Operation{
		"update-opportunity":  {SObject: "Opportunity", Action: "update", IDField: "id", Fields: map[string]string{"ERP_Order_Number__c": "erpOrderNumber"}},
		"restore-opportunity": {SObject: "Opportunity", Action: "restore"},
	},
}

func newConn(t *testing.T) (*Conn, *sftest.Server) {
	t.Helper()
	sf := sftest.New()
	t.Cleanup(sf.Close)
	c, err := New(sf.Credentials(), cfg, sf.Client())
	if err != nil {
		t.Fatal(err)
	}
	return c, sf
}

var t0 = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func oppID(n int) string { return "006" + strings.Repeat("0", 11) + string(rune('A'+n)) }

func TestPollPagesAndNeverSplitsATimestamp(t *testing.T) {
	c, sf := newConn(t)
	sf.PageSize = 2
	// Six won deals: three share one timestamp. Lost deals never appear.
	stamps := []time.Duration{0, time.Second, time.Second, time.Second, 2 * time.Second, 3 * time.Second}
	for i, d := range stamps {
		sf.Put("Opportunity", oppID(i), map[string]any{"StageName": "Closed Won", "Amount": 100 * (i + 1)}, t0.Add(d))
	}
	sf.Put("Opportunity", oppID(9), map[string]any{"StageName": "Closed Lost"}, t0)

	ctx := context.Background()
	polls := [][]int{
		{0},       // full after [A B]; B starts a group that continues, so stop before it
		{1, 2, 3}, // [B C] is one group: read on; [D E] fills it, and E's group may continue
		{4, 5},    // the last page: nothing left to split
		{},
	}
	var after int64
	for i, want := range polls {
		evs, err := c.Poll(ctx, "Opportunity.ClosedWon", after, 2)
		if err != nil {
			t.Fatal(err)
		}
		var wantIDs []string
		for _, n := range want {
			wantIDs = append(wantIDs, oppID(n))
		}
		if strings.Join(ids(evs), ",") != strings.Join(wantIDs, ",") {
			t.Fatalf("poll %d = %v, want %v", i+1, ids(evs), wantIDs)
		}
		for _, ev := range evs {
			if strings.Contains(string(ev.Payload), "attributes") {
				t.Fatalf("API metadata leaked into the event: %s", ev.Payload)
			}
			after = ev.Position
		}
	}
}

func ids(evs []connector.Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.ID)
	}
	return out
}

func TestUpdateRestoreAndConfirm(t *testing.T) {
	c, sf := newConn(t)
	ctx := context.Background()
	id := oppID(1)
	sf.Put("Opportunity", id, map[string]any{"StageName": "Closed Won", "ERP_Order_Number__c": nil}, t0)
	payload := json.RawMessage(`{"id":"` + id + `","erpOrderNumber":"42"}`)

	preview, err := c.Simulate(ctx, "update-opportunity", payload)
	if err != nil || !strings.Contains(string(preview), `"proposed":{"ERP_Order_Number__c":"42"}`) || sf.Patches != 0 {
		t.Fatalf("preview = %s %v", preview, err)
	}
	result, err := c.Commit(ctx, "update-opportunity", "k", payload)
	if err != nil {
		t.Fatal(err)
	}
	if sf.Get("Opportunity", id)["ERP_Order_Number__c"] != "42" {
		t.Fatal("update not applied")
	}
	if err := c.Confirm(ctx, "update-opportunity", result); err != nil {
		t.Fatal(err)
	}
	// Compensation gets the update's result and puts the old value back.
	if _, err := c.Commit(ctx, "restore-opportunity", "k#compensate", result); err != nil {
		t.Fatal(err)
	}
	if v := sf.Get("Opportunity", id)["ERP_Order_Number__c"]; v != nil {
		t.Fatalf("restore left %v", v)
	}
}

func TestErrorsAreClassified(t *testing.T) {
	c, sf := newConn(t)
	ctx := context.Background()
	id := oppID(2)
	sf.Put("Opportunity", id, map[string]any{}, t0)
	payload := json.RawMessage(`{"id":"` + id + `","erpOrderNumber":"42"}`)

	sf.FailPatch = func(string, string, map[string]any) (int, string, string) {
		return 400, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "ERP number can only be set on won deals"
	}
	if _, err := c.Commit(ctx, "update-opportunity", "k", payload); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "won deals") {
		t.Fatalf("validation rule: %v", err)
	}
	sf.FailPatch = func(string, string, map[string]any) (int, string, string) {
		return 403, "REQUEST_LIMIT_EXCEEDED", "TotalRequests Limit exceeded"
	}
	var apiErr *APIError
	if _, err := c.Commit(ctx, "update-opportunity", "k", payload); errors.Is(err, writeguard.ErrInvalid) || !errors.As(err, &apiErr) {
		t.Fatalf("API limit should be retryable: %v", err)
	}
	bad := json.RawMessage(`{"id":"not-an-id","erpOrderNumber":"42"}`)
	if _, err := c.Commit(ctx, "update-opportunity", "k", bad); !errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("bad ID: %v", err)
	}
}

func TestExpiredSessionIsRefreshed(t *testing.T) {
	c, sf := newConn(t)
	ctx := context.Background()
	sf.Put("Opportunity", oppID(3), map[string]any{"StageName": "Closed Won"}, t0)
	if _, err := c.Poll(ctx, "Opportunity.ClosedWon", 0, 10); err != nil {
		t.Fatal(err)
	}
	sf.ExpireTokens()
	if evs, err := c.Poll(ctx, "Opportunity.ClosedWon", 0, 10); err != nil || len(evs) != 1 {
		t.Fatalf("after expiry: %v %v", evs, err)
	}
	if sf.Logins != 2 {
		t.Fatalf("logins = %d, want 2", sf.Logins)
	}
}

func TestLoginFailuresAreExplained(t *testing.T) {
	sf := sftest.New()
	defer sf.Close()
	other := sftest.New() // a key the org does not trust
	defer other.Close()
	creds := strings.Replace(sf.Credentials(), jsonEscape(sf.PrivateKeyPEM()), jsonEscape(other.PrivateKeyPEM()), 1)
	c, err := New(creds, cfg, sf.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Poll(context.Background(), "Opportunity.ClosedWon", 0, 1); err == nil || !strings.Contains(err.Error(), "invalid signature") {
		t.Fatalf("err = %v", err)
	}
	if _, err := New(`{"loginUrl":"x","clientId":"y"}`, cfg, nil); err == nil {
		t.Fatal("credentials without a flow accepted")
	}
	if _, err := New("not json with a secret in it", cfg, nil); err == nil || strings.Contains(err.Error(), "secret in it") {
		t.Fatalf("secret echoed or accepted: %v", err)
	}
}

func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return strings.Trim(string(b), `"`)
}

func TestCheck(t *testing.T) {
	c, sf := newConn(t)
	sf.Put("Opportunity", oppID(1), map[string]any{"StageName": "Closed Won", "AccountId": "001", "Amount": 1, "ERP_Order_Number__c": nil}, t0)
	results := map[string]string{}
	for _, r := range c.Check(context.Background()) {
		results[r.Name] = fmt.Sprint(r.OK, " ", r.Detail, " ", r.Fix)
	}
	if !strings.HasPrefix(results["login"], "true") || !strings.HasPrefix(results["sobject Opportunity"], "true") {
		t.Fatalf("healthy org: %v", results)
	}

	sf.ReadOnly = map[string]bool{"Opportunity.ERP_Order_Number__c": true}
	for _, r := range c.Check(context.Background()) {
		if r.Name == "sobject Opportunity" && (r.OK || !strings.Contains(r.Detail, "ERP_Order_Number__c is read-only") || !strings.Contains(r.Fix, "permission set")) {
			t.Fatalf("read-only field: %+v", r)
		}
	}

	other := sftest.New()
	defer other.Close()
	bad, _ := New(strings.Replace(sf.Credentials(), jsonEscape(sf.PrivateKeyPEM()), jsonEscape(other.PrivateKeyPEM()), 1), cfg, sf.Client())
	r := bad.Check(context.Background())[0]
	if r.OK || !strings.Contains(r.Fix, "certificate matching this private key") {
		t.Fatalf("wrong key: %+v", r)
	}
}
