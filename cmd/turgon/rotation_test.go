package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
)

func TestRegressionsHoldBackOnlyWorseConnections(t *testing.T) {
	ok := func(n string) connector.CheckResult { return connector.Pass(n, "") }
	bad := func(n string) connector.CheckResult { return connector.Fail(n, "failed", "") }
	for name, tc := range map[string]struct {
		before, after []connector.CheckResult
		want          []string
	}{
		// A wrong password in the secret manager.
		"new password wrong": {[]connector.CheckResult{ok("connect"), bad("change capture")}, []connector.CheckResult{bad("connect")}, []string{"db: connect: failed"}},
		// The current password was revoked: its checks stop at connect, and
		// the new connections' permission problem was always there.
		"old password revoked": {[]connector.CheckResult{bad("connect")}, []connector.CheckResult{ok("connect"), bad("change capture")}, nil},
		// Failing either way.
		"missing permission": {[]connector.CheckResult{ok("connect"), bad("change capture")}, []connector.CheckResult{ok("connect"), bad("change capture")}, nil},
		"all good":           {[]connector.CheckResult{ok("connect")}, []connector.CheckResult{ok("connect")}, nil},
	} {
		got := regressionsOf(map[string][]connector.CheckResult{"db": tc.before}, map[string][]connector.CheckResult{"db": tc.after})
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}

func TestFingerprintsNoticeChangesWithoutKeepingValues(t *testing.T) {
	ctx := context.Background()
	refs := []string{"openbao://a/x", "openbao://b/y"}
	f1, _ := fingerprint(ctx, connector.StaticSecrets{"openbao://a/x": "one", "openbao://b/y": "two"}, refs)
	f2, _ := fingerprint(ctx, connector.StaticSecrets{"openbao://a/x": "one", "openbao://b/y": "three"}, refs)
	if got := f1.changed(f2); !reflect.DeepEqual(got, []string{"openbao://b/y"}) {
		t.Fatalf("changed = %v", got)
	}
	for _, h := range f1 {
		if h == "one" || h == "two" || len(h) != 64 {
			t.Fatalf("fingerprint %q", h)
		}
	}
	if _, err := fingerprint(ctx, connector.StaticSecrets{}, refs); err == nil {
		t.Fatal("an unreadable secret gave a fingerprint")
	}
}

func TestEndpointsUsing(t *testing.T) {
	cfg, _ := json.Marshal(map[string]any{"events": map[string]any{"Paid": map[string]any{"webhook": map[string]any{"secretRef": "openbao://stripe/hook"}}}})
	spec := &compiler.RuntimeSpec{}
	spec.Spec.Connectors = []compiler.ConnectorConfig{
		{Endpoint: "erp-db", SecretRef: "openbao://erp-db/dsn"},
		{Endpoint: "stripe", SecretRef: "openbao://stripe/key", Config: cfg},
	}
	if got := endpointsUsing(spec, []string{"openbao://stripe/hook"}); !reflect.DeepEqual(got, []string{"stripe"}) {
		t.Fatalf("got %v", got)
	}
	if got := endpointsUsing(spec, nil); got != nil {
		t.Fatalf("no references: %v", got)
	}
}

// fakeSecrets is a rotating secret backend over a map.
type fakeSecrets struct{ connector.StaticSecrets }

func (fakeSecrets) Rotates() bool             { return true }
func (fakeSecrets) Where(ref string) string   { return ref }
func (fakeSecrets) Missing(ref string) string { return ref }
func (f fakeSecrets) Fingerprints(ctx context.Context, refs []string) (fingerprints, error) {
	return fingerprint(ctx, f, refs)
}

func TestRotationSwitchesOnlyToWorkingConnections(t *testing.T) {
	ctx := context.Background()
	spec := &compiler.RuntimeSpec{}
	spec.Metadata.Name = "shop-orders-to-erp"
	spec.Spec.Connectors = []compiler.ConnectorConfig{{Endpoint: "erp-db", SecretRef: "openbao://erp-db/dsn"}}
	secrets := fakeSecrets{connector.StaticSecrets{"openbao://erp-db/dsn": "postgres://erp:one@db/erp"}}
	var out, auditLog strings.Builder
	rot, err := newRotation[string](ctx, &out, audit.New(&auditLog), spec, secrets)
	if err != nil {
		t.Fatal(err)
	}
	current, connects := "one", 0
	rot.connect = func() (string, error) {
		connects++
		v, _ := secrets.Resolve(ctx, "openbao://erp-db/dsn")
		return v, nil
	}
	rot.discard = func(string) {}
	rot.regressions = func(_ context.Context, next string, endpoints []string) []string {
		if !reflect.DeepEqual(endpoints, []string{"erp-db"}) {
			t.Errorf("checked %v", endpoints)
		}
		if strings.Contains(next, "wrong") {
			return []string{"erp-db: connect: password authentication failed"}
		}
		return nil
	}
	rot.use = func(next string) bool { current = next; return true }

	rot.refresh(ctx)
	if connects != 0 {
		t.Fatal("reconnected with no secret changed")
	}

	// A wrong password in the secret manager: refused, reported once, and
	// tried again at every refresh.
	secrets.StaticSecrets["openbao://erp-db/dsn"] = "postgres://erp:wrong@db/erp"
	rot.refresh(ctx)
	rot.refresh(ctx)
	if current != "one" || connects != 2 {
		t.Fatalf("current %q after %d connects", current, connects)
	}
	if n := strings.Count(out.String(), "keeping the current ones"); n != 1 {
		t.Fatalf("refusal reported %d times:\n%s", n, out.String())
	}

	// Fixed: the next refresh switches, and the one after has nothing to do.
	secrets.StaticSecrets["openbao://erp-db/dsn"] = "postgres://erp:two@db/erp"
	rot.refresh(ctx)
	rot.refresh(ctx)
	if current != "postgres://erp:two@db/erp" || connects != 3 {
		t.Fatalf("current %q after %d connects", current, connects)
	}
	for _, want := range []string{"connections.reload-refused", "connections.reloaded"} {
		if !strings.Contains(auditLog.String(), want) {
			t.Errorf("audit log lacks %s", want)
		}
	}
	if strings.Contains(out.String()+auditLog.String(), "wrong@") {
		t.Fatal("a secret value was logged")
	}
}
