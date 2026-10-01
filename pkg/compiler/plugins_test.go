package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

func TestRecipeExtensionsAreEmbeddedAndWired(t *testing.T) {
	cat, err := catalog.Load("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := cat.Find("shop-orders-with-credit-check")
	rt, _, err := Compile(cat, obj, verifier.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.Spec.Plugins) != 1 {
		t.Fatalf("plugins %+v", rt.Spec.Plugins)
	}
	p := rt.Spec.Plugins[0]
	mod, _ := os.ReadFile("../../examples/plugins/credit-check/credit-check.wasm")
	sum := sha256.Sum256(mod)
	if string(p.Module) != string(mod) || p.ModuleSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("the module is not embedded with its digest")
	}
	if want := (EntityOperation{Endpoint: "erp-db", Operation: "get-customer", Risk: "read", IDField: "id"}); p.Reads["Customer"] != want {
		t.Errorf("reads %+v", p.Reads)
	}
	if want := (EntityOperation{Endpoint: "erp-db", Operation: "update-sales-order", Risk: "low", IDField: "externalId"}); p.Proposals["SalesOrder"] != want {
		t.Errorf("proposals %+v", p.Proposals)
	}
	w := rt.Spec.Workflows[0].Steps[2].Write
	if w.Event != "model.SalesOrder.created" || !reflect.DeepEqual(w.Plugins, []string{"credit-check"}) {
		t.Fatalf("write step %+v", w)
	}

	// The digest covers the plugin's code: a different module, a different spec.
	b := *rt
	b.Spec.Plugins = append([]PluginDeployment(nil), rt.Spec.Plugins...)
	b.Spec.Plugins[0].Module = append([]byte(nil), mod...)
	b.Spec.Plugins[0].Module[len(mod)-1] ^= 1
	if Digest(b.Spec) == rt.Metadata.Digest {
		t.Fatal("changing the module kept the digest")
	}

	// Without the extension, the recipe compiles as before: no plugins, no
	// events on its writes.
	plain, _ := cat.Find("shop-orders-to-erp")
	rt2, _, err := Compile(cat, plain, verifier.Options{})
	if err != nil || len(rt2.Spec.Plugins) != 0 || rt2.Spec.Workflows[0].Steps[2].Write.Event != "" {
		t.Fatalf("plain recipe: %v %+v", err, rt2.Spec.Plugins)
	}
}
