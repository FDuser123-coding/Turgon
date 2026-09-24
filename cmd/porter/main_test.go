package main

import (
	"bytes"
	"encoding/json"
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
