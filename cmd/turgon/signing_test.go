package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// A signing pipeline: keygen, compile --sign-key; workers with
// --trusted-keys load only what it signed.
func TestSignedSpecs(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "pipeline")
	if out, err := run(t, "keygen", "--out", prefix); err != nil || !strings.Contains(out, "wrote") {
		t.Fatalf("keygen: %s %v", out, err)
	}
	if _, err := run(t, "keygen", "--out", prefix); err == nil {
		t.Fatal("keygen overwrote a signing key")
	}
	signed := filepath.Join(dir, "signed.json")
	unsigned := filepath.Join(dir, "unsigned.json")
	if out, err := run(t, "compile", "-c", "../../examples", "shop-orders-to-erp", "-o", signed, "--sign-key", prefix+".key"); err != nil {
		t.Fatalf("compile --sign-key: %s %v", out, err)
	}
	if out, err := run(t, "compile", "-c", "../../examples", "shop-orders-to-erp", "-o", unsigned); err != nil {
		t.Fatalf("compile: %s %v", out, err)
	}

	trusted := []string{"--trusted-keys", prefix + ".pub"}
	if out, err := run(t, append(trusted, "secrets", signed)...); err != nil {
		t.Fatalf("signed spec refused: %s %v", out, err)
	}
	if _, err := run(t, append(trusted, "secrets", unsigned)...); err == nil || !strings.Contains(err.Error(), "not signed by a trusted key") {
		t.Fatalf("unsigned spec accepted: %v", err)
	}
	// Without trusted keys, as before, any spec whose digest holds runs.
	if _, err := run(t, "secrets", unsigned); err != nil {
		t.Fatal(err)
	}

	// Signing an existing spec afterwards works too.
	if out, err := run(t, "sign", unsigned, "--key", prefix+".key"); err != nil {
		t.Fatalf("sign: %s %v", out, err)
	}
	if _, err := run(t, append(trusted, "secrets", unsigned)...); err != nil {
		t.Fatalf("spec signed afterwards refused: %v", err)
	}

	// A policy loosened in a signed spec, digest recomputed or not, is refused.
	data, _ := os.ReadFile(signed)
	loosened := strings.Replace(string(data), "package turgon", "package turgon # edited", 1)
	if loosened == string(data) {
		t.Fatal("test spec has no policy to edit")
	}
	edited := filepath.Join(dir, "edited.json")
	_ = os.WriteFile(edited, []byte(loosened), 0o644)
	if _, err := run(t, append(trusted, "secrets", edited)...); err == nil {
		t.Fatal("edited spec accepted")
	}
}

// A recipe held in review compiles once a steward's approval, stored in
// Turgon's database, is applied with --reviews-db.
func TestCompileAppliesStoredReviews(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	url := pgtest.URL(t, schema)
	ctx := context.Background()
	if err := pgstore.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	_ = os.CopyFS(dir, os.DirFS("../../examples"))
	m := filepath.Join(dir, "mappings", "shop-order-to-sales-order.yaml")
	data, _ := os.ReadFile(m)
	_ = os.WriteFile(m, []byte(strings.Replace(string(data), "confidence: 0.97", "confidence: 0.80", 1)), 0o644)

	out := filepath.Join(dir, "shop.json")
	if _, err := run(t, "compile", "-c", dir, "shop-orders-to-erp", "-o", out); err == nil {
		t.Fatal("compiled with a field waiting for review")
	}
	if err := pgstore.New(pool).Reviews().Put(ctx, verifier.Review{Mapping: "shop-order-to-sales-order@1.0.0", Target: "lines",
		Expression: "items.{ 'material': sku, 'quantity': qty }[]", Decision: "approved", Reviewer: "sam@example.com"}); err != nil {
		t.Fatal(err)
	}
	if res, err := run(t, "compile", "-c", dir, "shop-orders-to-erp", "-o", out, "--reviews-db", url); err != nil {
		t.Fatalf("compile with the approval: %s %v", res, err)
	}
}
