package salesforce

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
)

func accountID(n int) string { return fmt.Sprintf("001%012dAAA", n) }

func newBulkConn(t *testing.T) (*Conn, *sftest.Server) {
	t.Helper()
	sf := sftest.New()
	t.Cleanup(sf.Close)
	bc := cfg
	bc.Exports = map[string]Export{
		"Account.ERPNumbers": {SObject: "Account", Fields: []string{"Id", "Name", "ERP_Customer_Number__c", "Owner.Email"},
			Where: "ERP_Customer_Number__c != null"},
		"Account.All": {SObject: "Account", Fields: []string{"Id"}, All: true},
	}
	c, err := New(sf.Credentials(), bc, sf.Client())
	if err != nil {
		t.Fatal(err)
	}
	c.BulkPoll = 1 // nanosecond waits: the fake completes jobs on the second check
	c.BulkPageSize = 3
	return c, sf
}

func TestBulkExportReadsEveryPage(t *testing.T) {
	c, sf := newBulkConn(t)
	for i := 1; i <= 8; i++ {
		fields := map[string]any{"Name": fmt.Sprintf("Account %d, \"Ltd\"", i), "Owner": map[string]any{"Email": "sam@example.com"}}
		if i != 4 {
			fields["ERP_Customer_Number__c"] = fmt.Sprintf("C-%d00", i)
		}
		if i == 8 {
			fields["IsDeleted"] = true
		}
		sf.Put("Account", accountID(i), fields, t0)
	}
	var got []map[string]string
	n, err := c.Export(context.Background(), "Account.ERPNumbers", func(r map[string]string) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Eight accounts: one without a number, one deleted. Six across two
	// pages of three, with commas and quotes in names intact.
	if n != 6 || len(got) != 6 || got[0]["Id"] != accountID(1) || got[0]["ERP_Customer_Number__c"] != "C-100" ||
		got[0]["Name"] != `Account 1, "Ltd"` || got[0]["Owner.Email"] != "sam@example.com" || got[5]["Id"] != accountID(7) {
		t.Fatalf("n = %d, records = %v", n, got)
	}
	if created, deleted := sf.BulkJobs(); created != 1 || deleted != 1 {
		t.Fatalf("jobs created %d, deleted %d", created, deleted)
	}
	// queryAll reads the deleted account too.
	if n, err := c.Export(context.Background(), "Account.All", func(map[string]string) error { return nil }); err != nil || n != 8 {
		t.Fatalf("all: n = %d, err = %v", n, err)
	}
}

func TestBulkExportFailuresAreReported(t *testing.T) {
	c, sf := newBulkConn(t)
	sf.Put("Account", accountID(1), map[string]any{"ERP_Customer_Number__c": "C-100"}, t0)

	// A failing callback stops the export; the job is deleted anyway.
	stop := errors.New("stop")
	if _, err := c.Export(context.Background(), "Account.ERPNumbers", func(map[string]string) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("err = %v", err)
	}
	if _, deleted := sf.BulkJobs(); deleted != 1 {
		t.Fatal("the job was not deleted")
	}
	// A failed job says why.
	sf.FailJobs = "InvalidBatch: Field name not found : ERP_Customer_Number__c"
	_, err := c.Export(context.Background(), "Account.ERPNumbers", func(map[string]string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "failed: InvalidBatch") {
		t.Fatalf("err = %v", err)
	}
	// A cancelled wait aborts the job.
	sf.FailJobs, sf.JobPolls = "", 1000
	ctx, cancel := context.WithCancel(context.Background())
	c.BulkPoll = 1 << 30
	go cancel()
	if _, err := c.Export(ctx, "Account.ERPNumbers", func(map[string]string) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if _, err := c.Export(context.Background(), "Missing", nil); err == nil {
		t.Fatal("an unknown export ran")
	}
}

func TestBulkExportConfigRejected(t *testing.T) {
	for name, e := range map[string]Export{
		"subquery":  {SObject: "Account", Fields: []string{"(SELECT Id FROM Contacts)"}},
		"no fields": {SObject: "Account"},
		"bad where": {SObject: "Account", Fields: []string{"Id"}, Where: "Id != null; DELETE"},
	} {
		if _, err := New(`{"loginUrl":"https://login.salesforce.com","clientId":"x","clientSecret":"y"}`, Config{Exports: map[string]Export{"x": e}}, nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCheckCoversExportFields(t *testing.T) {
	c, sf := newBulkConn(t)
	sf.Put("Account", accountID(1), map[string]any{"Name": "Lovelace GmbH"}, t0) // no ERP_Customer_Number__c yet
	for _, r := range c.Check(context.Background()) {
		if r.Name == "sobject Account" {
			if r.OK || !strings.Contains(r.Detail, "ERP_Customer_Number__c is not visible") {
				t.Fatalf("check = %+v", r)
			}
			return
		}
	}
	t.Fatal("Account was not checked")
}
