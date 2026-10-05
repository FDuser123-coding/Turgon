package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

func TestCatalogSnapshots(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	cat := meta.Catalog{Endpoint: "erp", Connector: "postgres", DiscoveredAt: t0,
		Objects: []meta.Object{{Name: "public.orders", Kind: "table", Fields: []meta.Field{{Name: "id", Type: "bigint", Key: true}, {Name: "name", Type: "character varying", Length: 80}}}},
		Uses:    []meta.Use{{Object: "public.orders", Field: "name", By: "operation upsert"}}}
	if _, ok, err := st.LatestCatalog(ctx, "erp"); err != nil || ok {
		t.Fatalf("latest before any: %v %v", ok, err)
	}
	first, created, err := st.SaveCatalog(ctx, cat)
	if err != nil || !created || first.Objects != 1 || first.Digest != cat.Digest() {
		t.Fatalf("first %+v %v %v", first, created, err)
	}

	// Discovered again unchanged: the snapshot stays, checked later, with the new uses.
	again := cat
	again.DiscoveredAt = t0.Add(time.Hour)
	again.Uses = append(again.Uses, meta.Use{Object: "public.orders", Field: "id", By: "operation upsert"})
	same, created, err := st.SaveCatalog(ctx, again)
	if err != nil || created || same.ID != first.ID || !same.DiscoveredAt.Equal(t0) || !same.CheckedAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("unchanged %+v %v %v", same, created, err)
	}
	got, ok, err := st.LatestCatalog(ctx, "erp")
	if err != nil || !ok || len(got.Uses) != 2 || !got.DiscoveredAt.Equal(t0) {
		t.Fatalf("latest %+v %v %v", got, ok, err)
	}

	// The column shrinks: a new snapshot, and the drift between the two.
	changed := again
	changed.DiscoveredAt = t0.Add(2 * time.Hour)
	changed.Objects = []meta.Object{{Name: "public.orders", Kind: "table", Fields: []meta.Field{{Name: "id", Type: "bigint", Key: true}, {Name: "name", Type: "character varying", Length: 40}}}}
	second, created, err := st.SaveCatalog(ctx, changed)
	if err != nil || !created || second.ID == first.ID {
		t.Fatalf("changed %+v %v %v", second, created, err)
	}
	old, _, _ := st.Catalog(ctx, first.ID)
	latest, _, _ := st.LatestCatalog(ctx, "erp")
	if d := meta.Diff(old, latest); len(d) != 1 || d[0].Kind != meta.LengthShrunk {
		t.Fatalf("drift %+v", d)
	}
	if _, _, err := st.SaveCatalog(ctx, meta.Catalog{Endpoint: "crm", Connector: "salesforce"}); err != nil {
		t.Fatal(err)
	}
	if all, err := st.Snapshots(ctx, ""); err != nil || len(all) != 3 || all[0].Endpoint != "crm" {
		t.Fatalf("all %+v %v", all, err)
	}
	if erp, err := st.Snapshots(ctx, "erp"); err != nil || len(erp) != 2 || erp[0].ID != second.ID {
		t.Fatalf("erp %+v %v", erp, err)
	}
}
