package pgstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/audit"
)

func TestAuditChainInPostgres(t *testing.T) {
	pool, _ := newPool(t)
	log := NewAuditLog(pool)
	ctx := context.Background()

	// Concurrent writers (several workers) extend a single chain.
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if _, err := log.Record("worker", "writeback.committed", map[string]int{"worker": w, "i": i}); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	last, err := log.Verify(ctx)
	if err != nil || last == nil || last.Seq != 40 {
		t.Fatalf("verify: %+v %v", last, err)
	}
	tail, _ := log.Tail(ctx, 3)
	if len(tail) != 3 || tail[0].Seq != 40 || tail[2].Seq != 38 {
		t.Fatalf("tail = %+v", tail)
	}

	// The table refuses changes...
	for _, stmt := range []string{
		`UPDATE porter_audit SET actor = 'mallory' WHERE seq = 3`,
		`DELETE FROM porter_audit WHERE seq = 3`,
		`TRUNCATE porter_audit`,
	} {
		if _, err := pool.Exec(ctx, stmt); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%s: %v", stmt, err)
		}
	}
	// ...and if someone with enough rights disables that, the chain notices.
	if _, err := pool.Exec(ctx, `ALTER TABLE porter_audit DISABLE TRIGGER porter_audit_no_change;
		UPDATE porter_audit SET line = replace(line, '"i":1', '"i":7') WHERE seq = (SELECT min(seq) FROM porter_audit WHERE line LIKE '%"i":1%')`); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Verify(ctx); !errors.Is(err, audit.ErrTampered) {
		t.Fatalf("tampering not detected: %v", err)
	}
}
