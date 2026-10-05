package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
)

// TestDiscoverDrift: discover the demo databases, find them unchanged, then
// see a column the ERP connector writes disappear.
func TestDiscoverDrift(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	url := pgtest.URL(t, schema)
	ctx := context.Background()
	local := func(s string) string {
		s = strings.ReplaceAll(s, "shop.", schema+"_shop.")
		s = strings.ReplaceAll(s, "erp.", schema+"_erp.")
		s = strings.ReplaceAll(s, "SCHEMA IF NOT EXISTS shop", "SCHEMA IF NOT EXISTS "+schema+"_shop")
		return strings.ReplaceAll(s, "SCHEMA IF NOT EXISTS erp", "SCHEMA IF NOT EXISTS "+schema+"_erp")
	}
	sql, err := os.ReadFile("../../examples/sql/demo.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, local(string(sql))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+"_shop CASCADE; DROP SCHEMA IF EXISTS "+schema+"_erp CASCADE")
	})
	dir := t.TempDir()
	_ = os.CopyFS(dir, os.DirFS("../../examples"))
	for _, c := range []string{"erp-db", "shop-db"} {
		p := filepath.Join(dir, "connections", c+".yaml")
		data, _ := os.ReadFile(p)
		_ = os.WriteFile(p, []byte(local(string(data))), 0o644)
	}
	spec := filepath.Join(dir, "spec.json")
	if out, err := run(t, "compile", "-c", dir, "shop-orders-to-erp", "-o", spec); err != nil {
		t.Fatalf("compile: %s %v", out, err)
	}
	t.Setenv("TURGON_SECRETS", "env")
	t.Setenv("TURGON_SECRET_ERP_DB_DSN", url)
	t.Setenv("TURGON_SECRET_SHOP_DB_DSN", url)
	t.Setenv("TURGON_CONNECTORS", "")

	out, err := run(t, "discover", "-s", spec, "--database-url", url)
	if err != nil || !strings.Contains(out, "erp-db (postgres") || !strings.Contains(out, "first snapshot") || strings.Contains(out, "MISSING") {
		t.Fatalf("first: %v\n%s", err, out)
	}
	out, err = run(t, "discover", "-s", spec, "--database-url", url, "--endpoint", "erp-db")
	if err != nil || !strings.Contains(out, "unchanged since snapshot") || strings.Contains(out, "shop-db") {
		t.Fatalf("again: %v\n%s", err, out)
	}

	// A DBA drops a column the ERP connector writes, shortens one, and adds one.
	if _, err := pool.Exec(ctx, local(`ALTER TABLE erp.sales_orders DROP COLUMN credit_status;
		ALTER TABLE erp.sales_orders ADD COLUMN region text;
		ALTER TABLE erp.payments ALTER COLUMN currency TYPE char(2)`)); err != nil {
		t.Fatal(err)
	}
	out, err = run(t, "discover", "-s", spec, "--database-url", url, "--endpoint", "erp-db", "--fail-on-breaking")
	if err == nil {
		t.Fatalf("breaking drift passed:\n%s", out)
	}
	lines := strings.Split(out, "\n")
	if !strings.Contains(out, "changed: snapshot") ||
		!strings.HasPrefix(lines[2], "  BREAKING ") || !strings.Contains(out, "field-removed "+schema+"_erp.sales_orders.credit_status (was text); used by operation get-sales-order, operation update-sales-order") ||
		!strings.Contains(out, "length-shrunk "+schema+"_erp.payments.currency (3 -> 2); used by operation get-payment, operation record-payment") ||
		!strings.Contains(out, "field-added "+schema+"_erp.sales_orders.region (text)") ||
		!strings.Contains(out, "MISSING   "+schema+"_erp.sales_orders.credit_status; used by operation get-sales-order, operation update-sales-order") {
		t.Fatalf("drift:\n%s", out)
	}

	out, err = run(t, "discover", "-s", spec, "--database-url", url, "--endpoint", "erp-db", "--object", "erp-db="+schema+"_shop.*", "--json")
	if err != nil {
		t.Fatalf("json: %v\n%s", err, out)
	}
	var res []discovery
	if err := json.Unmarshal([]byte(out), &res); err != nil || len(res) != 1 || !res[0].Created || res[0].Objects != 5 || len(res[0].Changes) != 2 {
		t.Fatalf("json %v %+v\n%s", err, res, out)
	}
	if _, err := run(t, "discover", "-s", spec, "--endpoint", "nope"); err == nil || !strings.Contains(err.Error(), "no such endpoint") {
		t.Fatalf("unknown endpoint: %v", err)
	}
	if _, err := run(t, "discover", "-s", spec, "--object", "erp=public.x"); err == nil || !strings.Contains(err.Error(), "--object erp=...: the spec has no such endpoint") {
		t.Fatalf("unknown endpoint in --object: %v", err)
	}
}
