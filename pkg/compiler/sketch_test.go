package compiler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// A recipe waiting for mapping review does not compile, but its sketch
// still says which fields its mappings read.
func TestSketchOfARecipeInReview(t *testing.T) {
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS("../../examples")); err != nil {
		t.Fatal(err)
	}
	m := filepath.Join(dir, "mappings", "shop-order-to-sales-order.yaml")
	data, _ := os.ReadFile(m)
	_ = os.WriteFile(m, []byte(strings.Replace(string(data), "confidence: 0.97", "confidence: 0.80", 1)), 0o644)
	cat, err := catalog.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := cat.Recipe("shop-orders-to-erp")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Compile(cat, obj, verifier.Options{}); err == nil {
		t.Fatal("a recipe with a field waiting for review compiled")
	}
	sk := Sketch(cat, obj)
	wf := sk.Spec.Workflows[0]
	if wf.Trigger.Endpoint != "shop-db" || wf.Trigger.Event != "Order.Created" || len(wf.Steps) != 1 {
		t.Fatalf("sketch %+v", wf)
	}
	mp := wf.Steps[0].Map
	if mp.Mapping != "shop-order-to-sales-order@1.0.0" || mp.From != "shop-db.Order" || mp.Fields["netValue"] != "$number(total)" {
		t.Fatalf("map %+v", mp)
	}
	// A mapping the catalog lacks is left out.
	broken := *obj
	broken.Spec.Steps = []v1alpha1.Step{{Map: &v1alpha1.MapStep{Mapping: "no-such-mapping@1"}}}
	if steps := Sketch(cat, &broken).Spec.Workflows[0].Steps; len(steps) != 0 {
		t.Fatalf("steps %+v", steps)
	}
}
