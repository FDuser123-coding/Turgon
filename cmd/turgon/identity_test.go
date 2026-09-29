package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func TestIdentityTrain(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	url := pgtest.URL(t, schema)
	ctx := context.Background()
	if err := pgstore.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	st := pgstore.New(pool)
	if _, err := run(t, "identity", "train", "--database-url", url); err == nil {
		t.Fatal("trained without links")
	}
	// Links like a marketplace deployment's: direct buyers with names, and
	// relay addresses on one shared domain without.
	rng := rand.New(rand.NewSource(3))
	word := func() string {
		b := make([]byte, 7)
		for i := range b {
			b[i] = byte('a' + rng.Intn(26))
		}
		return string(b)
	}
	for i := 0; i < 120; i++ {
		master, w := fmt.Sprintf("C-%03d", i), word()
		name := w + " " + word() + " gmbh"
		for j, email := range []string{"buyer@" + w + ".example", "orders@" + w + ".example"} {
			_ = st.Link(ctx, "Customer", "shop", fmt.Sprintf("%s-%d", w, j), master, identity.Attributes{"email": email, "domain": w + ".example", "name": name})
		}
		for j := 0; j < 2; j++ {
			email := fmt.Sprintf("r%03d-%d@relay.marketplace.example", i, j)
			_ = st.Link(ctx, "Customer", "marketplace", email, master, identity.Attributes{"email": email, "domain": "relay.marketplace.example"})
		}
	}

	out, err := run(t, "identity", "train", "--database-url", url, "--threshold", "0.8", "--dry-run")
	if err != nil || !strings.Contains(out, "dry run: not stored") || !strings.Contains(out, "domain.same") {
		t.Fatalf("dry run: %s %v", out, err)
	}
	if _, found, _ := st.MatchModel(ctx, "Customer"); found {
		t.Fatal("a dry run stored the model")
	}
	out, err = run(t, "identity", "train", "--database-url", url, "--threshold", "0.8", "--by", "dana@example.com")
	if err != nil || !strings.Contains(out, "stored") {
		t.Fatalf("train: %s %v", out, err)
	}
	m, found, err := st.MatchModel(ctx, "Customer")
	if err != nil || !found {
		t.Fatalf("model not stored: %v", err)
	}
	// Two relay orders of different buyers: the default model calls them
	// likely the same (0.88); the trained one does not.
	a := identity.Attributes{"email": "x1@relay.marketplace.example", "domain": "relay.marketplace.example"}
	b := identity.Attributes{"email": "x2@relay.marketplace.example", "domain": "relay.marketplace.example"}
	def, _ := identity.Default.Compare(a, b)
	trained, _ := m.Compare(a, b)
	if def < 0.8 || trained > 0.5 {
		t.Fatalf("relay pair: default %.3f, trained %.3f", def, trained)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM turgon_audit WHERE line LIKE '%identity.model.trained%' AND line LIKE '%dana@example.com%'`).Scan(&n)
	if n != 1 {
		t.Fatalf("training audited %d times", n)
	}
}
