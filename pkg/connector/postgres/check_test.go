package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/connector"
)

func TestCheckExplainsWhatIsWrong(t *testing.T) {
	pool, _ := pgtest.Pool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		CREATE TABLE orders_no_key (external_id text, net_value numeric);
		CREATE TABLE outbox (id bigserial PRIMARY KEY, event text, payload jsonb)`)
	if err != nil {
		t.Fatal(err)
	}
	c := New(pool, Config{
		Outbox: &Outbox{Table: "outbox"},
		Operations: map[string]Operation{
			"no-unique": {Table: "orders_no_key", Action: "insert", Key: "external_id", Columns: []string{"external_id", "net_value"}},
			"no-column": {Table: "orders_no_key", Action: "update", Key: "external_id", Columns: []string{"status"}},
			"no-table":  {Table: "erp.missing", Action: "insert", Key: "id", Columns: []string{"id"}},
		},
	})
	results := map[string]connector.CheckResult{}
	for _, r := range c.Check(ctx) {
		results[r.Name] = r
	}
	expect := func(name string, ok bool, fix string) {
		t.Helper()
		r := results[name]
		if r.OK != ok || !strings.Contains(r.Fix, fix) {
			t.Errorf("%s = %+v", name, r)
		}
	}
	expect("connect", true, "")
	expect("outbox outbox", true, "")
	expect("operation no-unique", false, "CREATE UNIQUE INDEX ON orders_no_key (external_id)")
	expect("operation no-column", false, "Add the columns")
	expect("operation no-table", false, "Create erp.missing")
}
