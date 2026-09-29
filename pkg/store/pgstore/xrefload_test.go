package pgstore

import (
	"context"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/identity"
)

func TestLinkManyKeepsTheLastLinkPerRecordAndStewardsLinks(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if err := s.PutXref(ctx, "Customer", "crm", "a-3", "C-9"); err != nil {
		t.Fatal(err)
	}
	links := []XrefLink{
		{Source: "a-1", Master: "C-1", Attributes: identity.Attributes{"name": "ada"}},
		{Source: "a-2", Master: "C-2"},
		{Source: "a-1", Master: "C-7"}, // the same record again: the last one wins
		{Source: "a-3", Master: "C-3"}, // linked elsewhere by a steward
	}
	c, err := s.LinkMany(ctx, "Customer", "crm", links, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if c.Added != 2 || len(c.Conflicts) != 1 || c.Conflicts[0].Linked != "C-9" {
		t.Fatalf("counts = %+v", c)
	}
	for src, want := range map[string]string{"a-1": "C-7", "a-2": "C-2", "a-3": "C-9"} {
		if got, _, _ := s.Xref(ctx, "Customer", "crm", src); got != want {
			t.Errorf("%s -> %s, want %s", src, got, want)
		}
	}
	// New attributes for a record already linked there are stored.
	c, err = s.LinkMany(ctx, "Customer", "crm", []XrefLink{{Source: "a-2", Master: "C-2", Attributes: identity.Attributes{"email": "b@x.example"}}}, false, false)
	recs, _ := s.LinkedRecords(ctx, "Customer")
	if err != nil || c.Unchanged != 1 || len(recs) != 1 || recs[0].Attributes["email"] != "b@x.example" {
		t.Fatalf("counts = %+v, records = %+v, err = %v", c, recs, err)
	}
}
