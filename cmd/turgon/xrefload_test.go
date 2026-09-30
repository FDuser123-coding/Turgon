package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func TestXrefLoadFromSalesforceBulkExport(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	url := pgtest.URL(t, schema)
	ctx := context.Background()
	if err := pgstore.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	st := pgstore.New(pool)
	sf := sftest.New()
	t.Cleanup(sf.Close)
	sf.JobPolls = 1 // complete at the first status check, a second after creation
	t.Setenv("TURGON_SECRET_SALESFORCE_PROD_JWT", sf.Credentials())
	account := func(n int) string { return fmt.Sprintf("001%012dAAA", n) }
	// Six accounts: five carry their ERP number, one does not.
	for i := 1; i <= 6; i++ {
		f := map[string]any{"Name": fmt.Sprintf("Kunde %d GmbH", i)}
		if i != 6 {
			f["ERP_Customer_Number__c"] = fmt.Sprintf("C-%d00", i)
		}
		sf.Put("Account", account(i), f, time.Now())
	}
	// A steward already linked account 2 elsewhere.
	if err := st.PutXref(ctx, "Customer", "salesforce-prod", account(2), "C-999"); err != nil {
		t.Fatal(err)
	}
	load := func(extra ...string) string {
		t.Helper()
		args := append([]string{"xref", "load", "salesforce-prod", "Account.ERPNumbers", "-c", "../../examples",
			"--database-url", url, "--entity", "Customer", "--master", "ERP_Customer_Number__c", "--match", "Name:name", "--by", "ada"}, extra...)
		out, err := run(t, args...)
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		return out
	}
	master := func(n int) string {
		m, _, _ := st.Xref(ctx, "Customer", "salesforce-prod", account(n))
		return m
	}

	out := load("--dry-run")
	if !strings.Contains(out, "read 5 Customer records") || !strings.Contains(out, "would link 4 new, 0 already linked") ||
		!strings.Contains(out, account(2)+": linked to C-999, the export says C-200") || master(1) != "" {
		t.Fatalf("dry run:\n%s", out)
	}
	out = load()
	if !strings.Contains(out, "linked 4 new") || master(1) != "C-100" || master(5) != "C-500" || master(2) != "C-999" {
		t.Fatalf("load:\n%s", out)
	}
	// The names are kept for matching later records.
	recs, _ := st.LinkedRecords(ctx, "Customer")
	if len(recs) != 4 || recs[0].Attributes["name"] == "" {
		t.Fatalf("linked records = %+v", recs)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM turgon_audit WHERE action = 'xref.loaded' AND actor = 'ada'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("audit entries = %d (%v)", n, err)
	}
	// Loading again changes nothing; --replace moves the conflicting link.
	if out = load(); !strings.Contains(out, "linked 0 new, 4 already linked") {
		t.Fatalf("again:\n%s", out)
	}
	if out = load("--replace"); !strings.Contains(out, "1 moved to a new master record") || master(2) != "C-200" {
		t.Fatalf("replace:\n%s", out)
	}
	if created, deleted := sf.BulkJobs(); created != 4 || deleted != 4 {
		t.Fatalf("bulk jobs created %d, deleted %d", created, deleted)
	}

	// A field the export does not read is named.
	if out, err := run(t, "xref", "load", "salesforce-prod", "Account.ERPNumbers", "-c", "../../examples",
		"--database-url", url, "--entity", "Customer", "--master", "SAP_Number__c"); err == nil || !strings.Contains(err.Error(), "has no field SAP_Number__c") {
		t.Fatalf("err = %v\n%s", err, out)
	}
}
