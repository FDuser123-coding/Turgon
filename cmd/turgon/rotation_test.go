package main

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

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
