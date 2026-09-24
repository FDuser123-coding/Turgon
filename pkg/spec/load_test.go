package spec

import (
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/api/v1alpha1"
)

func TestDecodeAllMultiDocument(t *testing.T) {
	data := `
# leading comment
apiVersion: porter.dev/v1alpha1
kind: PolicyPack
metadata: { name: a }
spec: { rego: "package a" }
---
# only a comment
---
apiVersion: porter.dev/v1alpha1
kind: PolicyPack
metadata: { name: b }
spec: { rego: "package b" }
`
	docs, err := DecodeAll([]byte(data), "test.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("got %d documents, want 2", len(docs))
	}
	if p, ok := docs[1].Object.(*v1alpha1.PolicyPack); !ok || p.Metadata.Name != "b" {
		t.Fatalf("second document = %#v", docs[1].Object)
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	_, err := Decode([]byte(`
apiVersion: porter.dev/v1alpha1
kind: PolicyPack
metadata: { name: a }
spec: { rego: "package a", regoo: "typo" }
`))
	if err == nil || !strings.Contains(err.Error(), "regoo") {
		t.Fatalf("err = %v, want unknown field error", err)
	}
}

func TestDecodeRejectsUnknownKindAndVersion(t *testing.T) {
	if _, err := Decode([]byte("apiVersion: porter.dev/v1alpha1\nkind: Widget\n")); err == nil {
		t.Error("unknown kind accepted")
	}
	if _, err := Decode([]byte("apiVersion: porter.dev/v2\nkind: Recipe\n")); err == nil {
		t.Error("unknown apiVersion accepted")
	}
}

func TestLoadExamples(t *testing.T) {
	docs, err := LoadPaths("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 {
		t.Fatal("no example documents")
	}
	for _, d := range docs {
		if errs := d.Object.Validate(); len(errs) > 0 {
			t.Errorf("%s: %v", d.Source, errs)
		}
	}
}
