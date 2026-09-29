package postgres

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/connector"
)

// capture sets up a schema with a table of orders and a connection that
// reads its changes as Order.Changed. Slots and publications are
// database-wide, so their names carry the schema's.
func capture(t *testing.T, ops ...string) (*Conn, *pgxpool.Pool) {
	t.Helper()
	pool, schema := pgtest.Pool(t)
	ctx := context.Background()
	var level string
	if err := pool.QueryRow(ctx, `SHOW wal_level`).Scan(&level); err != nil {
		t.Fatal(err)
	}
	if level != "logical" {
		if os.Getenv("CI") != "" {
			t.Fatalf("wal_level is %s; change capture tests need logical", level)
		}
		t.Skip("wal_level is not logical; skipping change capture test")
	}
	_, err := pool.Exec(ctx, `
		CREATE TABLE orders (id bigserial PRIMARY KEY, ref text NOT NULL, net numeric(12,2), paid boolean,
			placed timestamptz, due date, lines jsonb, note text);
		CREATE TABLE other (id int);
		CREATE TABLE nokey (a int)`)
	if err != nil {
		t.Fatal(err)
	}
	name := "turgon_" + schema
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, name)
		_, _ = pool.Exec(ctx, `DROP PUBLICATION IF EXISTS `+name)
	})
	c := New(pool, Config{Changes: map[string]Change{
		"Order.Changed": {Table: schema + ".orders", Operations: ops, Slot: name, Publication: name},
	}})
	return c, pool
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

func poll(t *testing.T, c *Conn, after int64, limit int) []connector.Event {
	t.Helper()
	evs, err := c.Poll(context.Background(), "Order.Changed", after, limit)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

func refs(t *testing.T, evs []connector.Event) string {
	t.Helper()
	var out []string
	for _, ev := range evs {
		var row map[string]any
		if err := json.Unmarshal(ev.Payload, &row); err != nil {
			t.Fatal(err)
		}
		out = append(out, row["ref"].(string))
	}
	return strings.Join(out, ",")
}

func TestChangeCaptureReadsCommittedChangesInOrder(t *testing.T) {
	c, pool := capture(t, "insert", "update")
	if evs := poll(t, c, 0, 10); len(evs) != 0 {
		t.Fatalf("changes before the slot existed: %+v", evs)
	}
	exec(t, pool, `INSERT INTO orders (ref, net, paid, placed, due, lines) VALUES
		('a', 1200.50, true, '2026-09-24 10:00:00+02', '2026-10-01', '[{"sku":"M-1","qty":2}]'), ('b', NULL, false, NULL, NULL, NULL)`)
	exec(t, pool, `INSERT INTO other VALUES (1)`)
	exec(t, pool, `UPDATE orders SET ref = 'b2' WHERE ref = 'b'`)
	exec(t, pool, `DELETE FROM orders WHERE ref = 'a'`) // not an operation of the event

	evs := poll(t, c, 0, 2)
	if got := refs(t, evs); got != "a,b" {
		t.Fatalf("first poll: %s", got)
	}
	var a map[string]any
	_ = json.Unmarshal(evs[0].Payload, &a)
	want := map[string]any{"ref": "a", "net": 1200.5, "paid": true, "placed": "2026-09-24T08:00:00Z", "due": "2026-10-01",
		"lines": []any{map[string]any{"sku": "M-1", "qty": float64(2)}}, "note": nil}
	for k, v := range want {
		if b1, _ := json.Marshal(a[k]); string(b1) != string(mustJSON(v)) {
			t.Errorf("payload %s = %s, want %s (payload %s)", k, b1, mustJSON(v), evs[0].Payload)
		}
	}
	if evs[0].Position >= evs[1].Position || evs[0].ID == evs[1].ID || strings.Contains(evs[0].ID, "/") {
		t.Fatalf("positions and IDs: %+v", evs)
	}

	// A re-read from the same position gives the same events: a worker
	// that crashed before starting their runs loses nothing.
	again := poll(t, c, 0, 2)
	if again[0].ID != evs[0].ID || again[1].Position != evs[1].Position {
		t.Fatalf("re-read differs: %+v vs %+v", again, evs)
	}
	rest := poll(t, c, evs[1].Position, 10)
	if got := refs(t, rest); got != "b2" {
		t.Fatalf("second poll: %s", got)
	}
	if evs := poll(t, c, rest[0].Position, 10); len(evs) != 0 {
		t.Fatalf("third poll: %+v", evs)
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// Changes are ordered by commit, not by where they were written: a
// transaction that wrote first and committed last comes last, and is not
// skipped because a later-written change was delivered before it.
func TestChangeCaptureFollowsCommitOrder(t *testing.T) {
	c, pool := capture(t)
	poll(t, c, 0, 1) // create the slot
	ctx := context.Background()
	slow, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Rollback(ctx)
	if _, err := slow.Exec(ctx, `INSERT INTO orders (ref) VALUES ('early-write')`); err != nil {
		t.Fatal(err)
	}
	exec(t, pool, `INSERT INTO orders (ref) VALUES ('first-commit')`)
	evs := poll(t, c, 0, 10)
	if got := refs(t, evs); got != "first-commit" {
		t.Fatalf("before the slow commit: %s", got)
	}
	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	later := poll(t, c, evs[0].Position, 10)
	if got := refs(t, later); got != "early-write" {
		t.Fatalf("after the slow commit: %s", got)
	}
}

// A dispatcher that stopped partway through a transaction's changes gets
// the rest of them.
func TestChangeCaptureResumesInsideATransaction(t *testing.T) {
	c, pool := capture(t)
	poll(t, c, 0, 1)
	exec(t, pool, `INSERT INTO orders (ref) VALUES ('x1'), ('x2'), ('x3')`)
	first := poll(t, c, 0, 1)
	if got := refs(t, first); got != "x1" {
		t.Fatalf("first: %s", got)
	}
	rest := poll(t, c, first[0].Position, 10)
	if got := refs(t, rest); got != "x2,x3" {
		t.Fatalf("rest: %s", got)
	}
	exec(t, pool, `INSERT INTO orders (ref) VALUES ('y')`)
	if got := refs(t, poll(t, c, rest[1].Position, 10)); got != "y" {
		t.Fatalf("next transaction: %s", got)
	}
}

// Polling releases the log the slot holds: past delivered events, and
// past a busy database's changes when the table is quiet.
func TestChangeCaptureReleasesTheLog(t *testing.T) {
	c, pool := capture(t)
	ctx := context.Background()
	slot := c.cfg.Changes["Order.Changed"].Slot
	confirmed := func() uint64 {
		var s string
		if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&s); err != nil {
			t.Fatal(err)
		}
		l, err := parseLSN(s)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	poll(t, c, 0, 1)
	start := confirmed()
	for i := 0; i < 20; i++ {
		exec(t, pool, `INSERT INTO other VALUES (1)`)
	}
	if evs := poll(t, c, 0, 10); len(evs) != 0 {
		t.Fatalf("events from another table: %+v", evs)
	}
	quiet := confirmed()
	if quiet <= start {
		t.Fatalf("slot not released on a quiet table: %s -> %s", formatLSN(start), formatLSN(quiet))
	}
	exec(t, pool, `INSERT INTO orders (ref) VALUES ('z')`)
	exec(t, pool, `INSERT INTO orders (ref) VALUES ('z2')`)
	evs := poll(t, c, 0, 10)
	if len(evs) != 2 {
		t.Fatalf("events: %+v", evs)
	}
	poll(t, c, evs[1].Position, 10)
	if got := confirmed(); got < commitOf(evs[1].Position) {
		t.Fatalf("slot at %s, not past the delivered events (%s)", formatLSN(got), formatLSN(commitOf(evs[1].Position)))
	}
}

func TestChangeCaptureDeletesCarryTheKey(t *testing.T) {
	c, pool := capture(t, "delete")
	poll(t, c, 0, 1)
	exec(t, pool, `INSERT INTO orders (ref, note) VALUES ('d', 'gone')`)
	exec(t, pool, `DELETE FROM orders`)
	evs := poll(t, c, 0, 10)
	if len(evs) != 1 {
		t.Fatalf("events: %+v", evs)
	}
	var row map[string]any
	_ = json.Unmarshal(evs[0].Payload, &row)
	if row["id"] == nil || row["ref"] != nil {
		t.Fatalf("delete payload = %s, want only the key", evs[0].Payload)
	}
}

func TestChangeCaptureRefusesTablesWithoutReplicaIdentity(t *testing.T) {
	c, _ := capture(t)
	ch := c.cfg.Changes["Order.Changed"]
	ch.Table = strings.Replace(ch.Table, "orders", "nokey", 1)
	ch.Operations = []string{"insert", "update"}
	c.cfg.Changes["Order.Changed"] = ch
	_, err := c.Poll(context.Background(), "Order.Changed", 0, 10)
	if err == nil || !strings.Contains(err.Error(), "REPLICA IDENTITY FULL") {
		t.Fatalf("err = %v", err)
	}
	var n int
	_ = c.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_publication WHERE pubname = $1`, ch.Publication).Scan(&n)
	if n != 0 {
		t.Fatal("publication created anyway: the table's updates would now fail")
	}
}

func TestChangeConfig(t *testing.T) {
	ch := Change{Table: "orders"}.withDefaults("Order.Created")
	if ch.Slot != "turgon_order_created" || ch.Publication != ch.Slot || ch.Operations[0] != "insert" {
		t.Fatalf("defaults: %+v", ch)
	}
	for _, bad := range []Change{{}, {Table: "t", Operations: []string{"upsert"}}, {Table: "t", Slot: "Bad-Name"}} {
		if bad.withDefaults("E").validate() == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
	for _, s := range []string{"0/0", "16/B374D848", "FFFFFFFF/FFFFFFFF"} {
		l, err := parseLSN(s)
		if err != nil || formatLSN(l) != s {
			t.Errorf("LSN %s: %s %v", s, formatLSN(l), err)
		}
	}
	if p := position(0x16B374D848, 3); commitOf(p) != 0x16B374D848 || p&maxIndex != 3 {
		t.Fatalf("position %d", p)
	}
}

func TestCheckChangeCapture(t *testing.T) {
	c, _ := capture(t, "insert", "update")
	ch := c.cfg.Changes["Order.Changed"]
	c.cfg.Changes["Keyless.Changed"] = Change{Table: strings.Replace(ch.Table, "orders", "nokey", 1), Operations: []string{"delete"},
		Slot: ch.Slot + "_k", Publication: ch.Publication + "_k"}
	c.cfg.Changes["Missing.Changed"] = Change{Table: "nowhere", Slot: ch.Slot + "_m", Publication: ch.Publication + "_m"}.withDefaults("Missing.Changed")
	results := map[string]connector.CheckResult{}
	check := func() {
		for _, r := range c.Check(context.Background()) {
			results[r.Name] = r
		}
	}
	check()
	for name, ok := range map[string]bool{"change capture": true, "changes Order.Changed": true, "changes Keyless.Changed": false, "changes Missing.Changed": false} {
		if results[name].OK != ok {
			t.Errorf("%s = %+v", name, results[name])
		}
	}
	if !strings.Contains(results["changes Order.Changed"].Detail, "created on the first poll") ||
		!strings.Contains(results["changes Keyless.Changed"].Fix, "REPLICA IDENTITY FULL") {
		t.Errorf("details: %+v", results)
	}
	poll(t, c, 0, 1)
	check()
	if r := results["changes Order.Changed"]; !r.OK || !strings.Contains(r.Detail, "via slot "+ch.Slot) {
		t.Errorf("after the first poll: %+v", r)
	}
}
