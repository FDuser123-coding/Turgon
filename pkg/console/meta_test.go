package console

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// fakeMeta keeps snapshots in memory, newest last.
type fakeMeta struct{ cats []meta.Catalog }

func (f *fakeMeta) Snapshots(_ context.Context, endpoint string) ([]meta.Snapshot, error) {
	var out []meta.Snapshot
	for i, c := range f.cats {
		if endpoint == "" || c.Endpoint == endpoint {
			out = append(out, meta.Snapshot{ID: int64(i + 1), Endpoint: c.Endpoint, Connector: c.Connector, Digest: c.Digest(),
				DiscoveredAt: c.DiscoveredAt, CheckedAt: c.DiscoveredAt, Objects: len(c.Objects)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out, nil
}

func (f *fakeMeta) Catalog(_ context.Context, id int64) (meta.Catalog, bool, error) {
	if id < 1 || int(id) > len(f.cats) {
		return meta.Catalog{}, false, nil
	}
	return f.cats[id-1], true, nil
}

func opportunity(amount string, extra ...meta.Field) meta.Catalog {
	fields := append([]meta.Field{{Name: "Id", Type: "id", Key: true}, {Name: "Amount", Type: amount}, {Name: "AccountId", Type: "reference"},
		{Name: "ERP_Order_Number__c", Type: "string", Length: 40}}, extra...)
	return meta.Catalog{Endpoint: "salesforce-prod", Connector: "salesforce", Version: "3.2.0", DiscoveredAt: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC),
		Objects: []meta.Object{{Name: "Opportunity", Kind: "sobject", Fields: fields}},
		Uses:    []meta.Use{{Object: "Opportunity", Field: "ERP_Order_Number__c", By: "operation update-opportunity"}, {Object: "Account", Field: "Name", By: "operation get-customer"}}}
}

// The console shows each system's latest snapshot, what changed since the
// one before, and what uses each field: the connector's operations and the
// catalog's mappings.
func TestMetaEndpoints(t *testing.T) {
	store := &fakeMeta{cats: []meta.Catalog{
		opportunity("currency"),
		{Endpoint: "erp-db", Connector: "postgres", Objects: []meta.Object{{Name: "erp.customers", Kind: "table", Fields: []meta.Field{{Name: "id", Type: "text"}}}}},
		opportunity("double", meta.Field{Name: "Region__c", Type: "picklist"}),
	}}
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}, Catalogs: []string{"../../examples"}, Meta: store})

	var ov MetaOverview
	if err := json.Unmarshal(do(t, s, "GET", "/api/meta", "", nil).Body.Bytes(), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Error != "" || len(ov.Systems) != 2 || ov.Systems[0].Endpoint != "erp-db" {
		t.Fatalf("overview %+v", ov)
	}
	sf := ov.Systems[1]
	if sf.Snapshots != 2 || sf.Latest.ID != 3 || sf.Changes != 2 || sf.Breaking != 1 || sf.Missing != 1 || sf.Fields != 5 || sf.Version != "3.2.0" {
		t.Fatalf("salesforce %+v", sf)
	}

	var d MetaDetail
	if err := json.Unmarshal(do(t, s, "GET", "/api/meta/salesforce-prod", "", nil).Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.To.ID != 3 || d.From == nil || d.From.ID != 1 || len(d.Snapshots) != 2 || len(d.Changes) != 2 {
		t.Fatalf("detail %+v", d)
	}
	amount := d.Changes[0]
	if amount.Kind != meta.TypeChanged || !amount.Breaks() || !strings.Contains(strings.Join(amount.UsedBy, ";"), "mapping sf-opportunity-to-order@3.0.0 (netValue)") {
		t.Fatalf("first change %+v", amount)
	}
	if d.Changes[1].Kind != meta.FieldAdded || d.Changes[1].Breaks() {
		t.Fatalf("second change %+v", d.Changes[1])
	}
	if len(d.Missing) != 1 || d.Missing[0].Object != "Account" {
		t.Fatalf("missing %+v", d.Missing)
	}
	fields := map[string]MetaField{}
	for _, f := range d.Objects[0].Fields {
		fields[f.Name] = f
	}
	if by := strings.Join(fields["ERP_Order_Number__c"].UsedBy, ";"); by != "operation update-opportunity" {
		t.Fatalf("ERP field used by %q", by)
	}
	if by := strings.Join(fields["AccountId"].UsedBy, ";"); !strings.Contains(by, "(customerRef)") {
		t.Fatalf("AccountId used by %q", by)
	}

	// Any two snapshots compare; the oldest has nothing before it.
	var oldest, back MetaDetail
	if err := json.Unmarshal(do(t, s, "GET", "/api/meta/salesforce-prod?to=1", "", nil).Body.Bytes(), &oldest); err != nil || oldest.From != nil || len(oldest.Changes) != 0 {
		t.Fatalf("oldest %+v %v", oldest, err)
	}
	if err := json.Unmarshal(do(t, s, "GET", "/api/meta/salesforce-prod?from=3&to=1", "", nil).Body.Bytes(), &back); err != nil || len(back.Changes) != 2 ||
		back.Changes[0].Kind != meta.TypeChanged {
		t.Fatalf("backwards %+v %v", back.Changes, err)
	}
	for path, code := range map[string]int{"/api/meta/salesforce-prod?to=2": 400, "/api/meta/salesforce-prod?from=x": 400, "/api/meta/nope": 404} {
		if rec := do(t, s, "GET", path, "", nil); rec.Code != code {
			t.Errorf("%s: %d %s", path, rec.Code, rec.Body)
		}
	}
}

func TestMetaWithoutDatabase(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}})
	var ov MetaOverview
	if err := json.Unmarshal(do(t, s, "GET", "/api/meta", "", nil).Body.Bytes(), &ov); err != nil || !strings.Contains(ov.Error, "--database-url") || ov.Systems == nil {
		t.Fatalf("%+v %v", ov, err)
	}
	if rec := do(t, s, "GET", "/api/meta/erp-db", "", nil); rec.Code != 404 {
		t.Fatalf("detail %d", rec.Code)
	}
}
