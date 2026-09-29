package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	root := newRoot(&out, &out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestValidateExamples(t *testing.T) {
	out, err := run(t, "validate", "../../examples")
	if err != nil || !strings.Contains(out, "0 invalid") {
		t.Fatalf("err=%v\n%s", err, out)
	}
}

func TestVerifyAndCompileBlueprint(t *testing.T) {
	out, err := run(t, "verify", "-c", "../../examples", "eu-distributor-core")
	if err != nil || !strings.Contains(out, "StackBlueprint/eu-distributor-core  level L1  deployable") {
		t.Fatalf("err=%v\n%s", err, out)
	}
	out, err = run(t, "compile", "-c", "../../examples", "eu-distributor-core")
	if err != nil {
		t.Fatalf("err=%v\n%s", err, out)
	}
	var spec struct {
		Kind     string
		Metadata struct{ Digest string }
	}
	if err := json.Unmarshal([]byte(out), &spec); err != nil || spec.Kind != "RuntimeSpec" || !strings.HasPrefix(spec.Metadata.Digest, "sha256:") {
		t.Fatalf("err=%v spec=%+v", err, spec)
	}
}

func TestVerifyBlockedExitsNonZero(t *testing.T) {
	out, err := run(t, "verify", "-c", "../../examples", "shopify-orders-to-sap")
	if err == nil || !strings.Contains(out, "BLOCKED") {
		t.Fatalf("err=%v\n%s", err, out)
	}
}

func TestWorkerPollers(t *testing.T) {
	for _, ok := range []string{"auto", "1", "32", "500"} {
		if o, err := workerOptions(ok); err != nil || o.WorkflowTaskPollerBehavior == nil || o.ActivityTaskPollerBehavior == nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "0", "501", "fast", "-3"} {
		if _, err := workerOptions(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// The console demo shows the example catalog's real map: regenerate it with
// go run ./cmd/turgon integrations -c examples --json > console/src/demo-integrations.json
func TestDemoIntegrationsAreUpToDate(t *testing.T) {
	out, err := run(t, "integrations", "-c", "../../examples", "--json")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("../../console/src/demo-integrations.json")
	if err != nil {
		t.Fatal(err)
	}
	if out != string(want) {
		t.Fatal("console/src/demo-integrations.json is out of date; regenerate it (see this test)")
	}
	text, err := run(t, "integrations", "-c", "../../examples")
	if err != nil || !strings.Contains(text, "when shop-db emits Order.Created (outbox)") ||
		!strings.Contains(text, "write create-sales-order to erp-db [high risk; dry-run first (rollback); approval: policy; undone by cancel-sales-order]") {
		t.Fatalf("text: %v\n%s", err, text)
	}
}
