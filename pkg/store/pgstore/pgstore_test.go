package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

func newPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool, schema := pgtest.Pool(t)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool, schema
}

func newStore(t *testing.T) *Store {
	t.Helper()
	pool, _ := pgtest.Pool(t)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migration is not idempotent: %v", err)
	}
	return New(pool)
}

func TestIdempotencyLifecycle(t *testing.T) {
	s := newStore(t)
	if out, err := s.Begin("k"); err != nil || out != nil {
		t.Fatalf("first claim: %v %v", out, err)
	}
	if _, err := s.Begin("k"); !errors.Is(err, writeguard.ErrInFlight) {
		t.Fatalf("second claim: %v", err)
	}
	if out, _ := s.Lookup("k"); out != nil {
		t.Fatal("in-flight key reported as done")
	}
	want := writeguard.Outcome{Status: writeguard.StatusCommitted, Result: json.RawMessage(`{"document":"4500000001"}`)}
	if err := s.Complete("k", want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Begin("k")
	if err != nil || got == nil || !sameJSON(got.Result, want.Result) {
		t.Fatalf("completed key: %+v %v", got, err)
	}

	if _, err := s.Begin("aborted"); err != nil {
		t.Fatal(err)
	}
	if err := s.Abort("aborted"); err != nil {
		t.Fatal(err)
	}
	if out, err := s.Begin("aborted"); err != nil || out != nil {
		t.Fatalf("reclaim after abort: %v %v", out, err)
	}
}

func TestExpiredLeaseIsTakenOver(t *testing.T) {
	s := newStore(t)
	s.Lease = 50 * time.Millisecond
	if _, err := s.Begin("crashed"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if out, err := s.Begin("crashed"); err != nil || out != nil {
		t.Fatalf("takeover: %v %v", out, err)
	}
}

func TestConcurrentClaimsHaveOneWinner(t *testing.T) {
	s := newStore(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if out, err := s.Begin("race"); err == nil && out == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("%d winners", winners)
	}
}

func TestCursorsAndXref(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if pos, _ := s.Cursor(ctx, "shop-db/Order.Created"); pos != 0 {
		t.Fatalf("new cursor = %d", pos)
	}
	_ = s.SetCursor(ctx, "shop-db/Order.Created", 42)
	_ = s.SetCursor(ctx, "shop-db/Order.Created", 7) // never moves back
	if pos, _ := s.Cursor(ctx, "shop-db/Order.Created"); pos != 42 {
		t.Fatalf("cursor = %d", pos)
	}
	if _, ok, _ := s.Xref(ctx, "Customer", "shop-db", "ada@example.com"); ok {
		t.Fatal("unexpected xref")
	}
	_ = s.PutXref(ctx, "Customer", "shop-db", "ada@example.com", "C-100")
	if id, ok, err := s.Xref(ctx, "Customer", "shop-db", "ada@example.com"); !ok || id != "C-100" || err != nil {
		t.Fatalf("xref = %q %v %v", id, ok, err)
	}
}

func sameJSON(a, b json.RawMessage) bool {
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}

// A state database created while Turgon was called Porter is renamed in
// place: its idempotency records and audit chain carry over.
func TestMigrateRenamesPorterTables(t *testing.T) {
	ctx := context.Background()
	pool, _ := pgtest.Pool(t)
	body := schema[strings.Index(schema, "CREATE TABLE"):]
	old := strings.ReplaceAll(body+auditSchema, "turgon_", "porter_")
	if _, err := pool.Exec(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO porter_writes (key, status, outcome) VALUES ('erp-db/create/SHOP-1', 'done', '{"status":"committed"}')`); err != nil {
		t.Fatal(err)
	}
	log := NewAuditLog(pool)
	// The old table has the same shape, so the current code can seed it.
	if _, err := pool.Exec(ctx, `ALTER TABLE porter_audit RENAME TO turgon_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Record("alice", "approve", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE turgon_audit RENAME TO porter_audit`); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE tablename LIKE 'porter\_%' AND schemaname = current_schema()`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d porter tables left (%v)", n, err)
	}
	out, err := New(pool).Lookup("erp-db/create/SHOP-1")
	if err != nil || out == nil || out.Status != writeguard.StatusCommitted {
		t.Fatalf("idempotency record lost: %+v %v", out, err)
	}
	e, err := log.Record("bob", "reject", nil)
	if err != nil || e.Seq != 2 {
		t.Fatalf("audit chain did not continue: %+v %v", e, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM turgon_audit`); err == nil {
		t.Fatal("renamed audit table is not append-only")
	}
}

func TestLinksKeepAttributesForMatching(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	ada := identity.Attributes{"email": "ada@lovelace-gmbh.example", "domain": "lovelace-gmbh.example", "name": "ada lovelace"}
	if err := s.Link(ctx, "Customer", "shopify-store", "ada@lovelace-gmbh.example", "C-100", ada); err != nil {
		t.Fatal(err)
	}
	if err := s.Link(ctx, "Customer", "stripe-billing", "cus_1", "C-100", identity.Attributes{"exact:vatId": "ATU123"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Link(ctx, "Customer", "shopify-store", "zed@other.example", "C-200", identity.Attributes{"email": "zed@other.example", "domain": "other.example", "name": "zed"}); err != nil {
		t.Fatal(err)
	}
	// A colleague shares the domain; a record with the same VAT ID shares it.
	got, err := s.Candidates(ctx, "Customer", identity.Attributes{"email": "grace@lovelace-gmbh.example", "domain": "lovelace-gmbh.example", "exact:vatId": "ATU123"})
	if err != nil || len(got) != 2 || got[0].Master != "C-100" || got[1].Master != "C-100" {
		t.Fatalf("candidates %+v %v", got, err)
	}
	if got, _ := s.Candidates(ctx, "Customer", identity.Attributes{"name": "adam"}); len(got) != 1 || got[0].Attributes["email"] != ada["email"] {
		t.Fatalf("name block %+v", got)
	}
	if got, _ := s.Candidates(ctx, "Order", identity.Attributes{"domain": "lovelace-gmbh.example"}); len(got) != 0 {
		t.Fatalf("other entity %+v", got)
	}
	// Re-linking without attributes keeps the ones already known.
	if err := s.PutXref(ctx, "Customer", "shopify-store", "ada@lovelace-gmbh.example", "C-101"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Candidates(ctx, "Customer", identity.Attributes{"email": "ada@lovelace-gmbh.example"}); len(got) != 1 || got[0].Master != "C-101" || got[0].Attributes["name"] != "ada lovelace" {
		t.Fatalf("after relink %+v", got)
	}
}
