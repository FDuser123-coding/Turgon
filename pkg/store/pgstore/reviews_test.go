package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/verifier"
)

func TestMappingReviews(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	r := st.Reviews()
	rv := verifier.Review{Mapping: "m@1.0.0", Target: "lines", Expression: "items.sku", Decision: "rejected", Reviewer: "sam", Note: "wrong unit", At: time.Now()}
	if err := r.Put(ctx, rv); err != nil {
		t.Fatal(err)
	}
	// Reconsidered: the later decision replaces the earlier one.
	rv.Decision, rv.Note = "approved", "checked"
	if err := r.Put(ctx, rv); err != nil {
		t.Fatal(err)
	}
	all, err := r.All(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("reviews = %+v, %v", all, err)
	}
	got, ok := all.Find("m@1.0.0", "lines", "items.sku")
	if !ok || got.Decision != "approved" || got.Reviewer != "sam" || got.Note != "checked" {
		t.Fatalf("review = %+v", got)
	}
	if _, ok := all.Find("m@1.0.0", "lines", "items.sku2"); ok {
		t.Fatal("a review applied to another expression")
	}
	if err := r.Put(ctx, verifier.Review{Mapping: "m@1.0.0", Target: "x", Expression: "e", Decision: "maybe", Reviewer: "sam"}); err == nil {
		t.Fatal("a decision other than approved or rejected was stored")
	}
}
