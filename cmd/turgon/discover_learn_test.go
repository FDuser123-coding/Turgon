package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// TestDiscoverLearnsRequiredFieldsFromRefusedWrites: an API without an
// OpenAPI description starts requiring a field. Its records cannot show
// that; the refused write can, and the next discovery reports the field as
// required for every operation creating those records, and keeps it so.
func TestDiscoverLearnsRequiredFieldsFromRefusedWrites(t *testing.T) {
	pool, _ := pgtest.Pool(t)
	ctx := context.Background()
	if err := pgstore.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := pgstore.New(pool)
	requireTax := false
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/customers":
			_, _ = w.Write([]byte(`[{"id": 1, "email": "ada@example.com", "name": "Ada"}]`))
		case r.Method == http.MethodPost && r.URL.Path == "/customers" && requireTax:
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"errors": {"taxId": ["can't be blank"]}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/customers":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id": 2}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	// An endpoint name of its own: the test database's audit log is shared.
	endpoint := "crm-" + strings.ReplaceAll(t.Name(), "/", "-")
	cfg, _ := json.Marshal(map[string]any{
		"baseURL": api.URL, "auth": map[string]any{"type": "none"},
		"operations": map[string]any{
			"create-customer": map[string]any{"method": "POST", "path": "/customers"},
			"import-customer": map[string]any{"method": "POST", "path": "/customers", "fields": map[string]string{"email": "email", "name": "name"}},
		},
	})
	spec := &compiler.RuntimeSpec{}
	spec.Spec.Connectors = []compiler.ConnectorConfig{{Endpoint: endpoint, Name: "rest", Version: "1.0.0", Config: cfg}}
	registry := connector.Registry{rest.Name: rest.Factory}
	discover := func() discovery {
		t.Helper()
		rs, err := discoverAll(ctx, spec, registry, connector.StaticSecrets{}, store, nil, nil)
		if err != nil || len(rs) != 1 || rs[0].Error != "" {
			t.Fatalf("discover: %v %+v", err, rs)
		}
		return rs[0]
	}
	discover() // first snapshot

	// The API starts requiring taxId; a write without it is refused and
	// the write guard's audit records the failure, as in a run.
	requireTax = true
	inst, err := registry[rest.Name](ctx, spec.Spec.Connectors[0], connector.StaticSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	_, cerr := inst.(writeguard.Target).Commit(ctx, "create-customer", "k1", json.RawMessage(`{"email": "grace@example.com"}`))
	inst.Close()
	if cerr == nil {
		t.Fatal("the refused write succeeded")
	}
	if _, err := store.Audit().Record("turgon/recipe/x", "writeback.failed", map[string]any{
		"target": endpoint, "operation": "create-customer", "key": "k1", "error": cerr.Error()}); err != nil {
		t.Fatal(err)
	}

	d := discover()
	var got []string
	for _, c := range d.Changes {
		if c.Breaks() {
			got = append(got, c.String()+"; used by "+strings.Join(c.UsedBy, ", "))
		}
	}
	if len(got) != 1 || !strings.HasPrefix(got[0], "field-added /customers.taxId (required: operation create-customer was refused without it on ") ||
		!strings.Contains(got[0], "operation create-customer (writes)") || !strings.Contains(got[0], "operation import-customer (writes)") {
		t.Fatalf("changes: %v", d.Changes)
	}

	// The sample still lacks taxId: it stays required, and nothing changed.
	d = discover()
	if d.Created || len(d.Changes) != 0 {
		t.Fatalf("again: created %v, %v", d.Created, d.Changes)
	}
	o, _ := d.Catalog.Object("/customers")
	if f, _ := o.Field("taxId"); !f.Required {
		t.Fatalf("taxId: %+v", o.Fields)
	}
}
