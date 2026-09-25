package spec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
)

func TestDecodeAllMultiDocument(t *testing.T) {
	data := `
# leading comment
apiVersion: turgon.dev/v1alpha1
kind: PolicyPack
metadata: { name: a }
spec: { rego: "package a" }
---
# only a comment
---
apiVersion: turgon.dev/v1alpha1
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
apiVersion: turgon.dev/v1alpha1
kind: PolicyPack
metadata: { name: a }
spec: { rego: "package a", regoo: "typo" }
`))
	if err == nil || !strings.Contains(err.Error(), "regoo") {
		t.Fatalf("err = %v, want unknown field error", err)
	}
}

func TestDecodeRejectsUnknownKindAndVersion(t *testing.T) {
	if _, err := Decode([]byte("apiVersion: turgon.dev/v1alpha1\nkind: Widget\n")); err == nil {
		t.Error("unknown kind accepted")
	}
	if _, err := Decode([]byte("apiVersion: turgon.dev/v2\nkind: Recipe\n")); err == nil {
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

func TestLoadPathsSkipsConfigMapCopies(t *testing.T) {
	// A ConfigMap volume: visible symlinks into a hidden timestamped directory.
	dir := t.TempDir()
	hidden := filepath.Join(dir, "..2026_09_24_10_00_00.1")
	if err := os.Mkdir(hidden, 0o755); err != nil {
		t.Fatal(err)
	}
	doc := "apiVersion: turgon.dev/v1alpha1\nkind: PolicyPack\nmetadata: { name: a }\nspec: { rego: \"package a\" }\n"
	if err := os.WriteFile(filepath.Join(hidden, "a.yaml"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(hidden), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "a.yaml"), filepath.Join(dir, "a.yaml")); err != nil {
		t.Fatal(err)
	}
	docs, err := LoadPaths(dir)
	if err != nil || len(docs) != 1 {
		t.Fatalf("docs = %d, err = %v", len(docs), err)
	}
}
