package verifier

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
)

// pluginCatalog is a catalog holding one logic plugin manifest, with the
// given module, world and type.
func pluginCatalog(t *testing.T, module, world, kind string) (*catalog.Catalog, *v1alpha1.Plugin) {
	t.Helper()
	dir := t.TempDir()
	if module != "" {
		b, err := os.ReadFile(module)
		if err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(filepath.Join(dir, "p.wasm"), b, 0o644)
	}
	manifest := `apiVersion: turgon.dev/v1alpha1
kind: Plugin
metadata: { name: p, publisher: acme, version: 1.0.0 }
spec:
  type: ` + kind + `
  runtime: wasm
  world: ` + world + `
  module: p.wasm
  subscribes: [model.SalesOrder.created]
  permissions:
    entities: { read: [Customer] }
    network: { allow: [api.example.com] }
  limits: { memoryMB: 16, timeoutMs: 500 }
`
	_ = os.WriteFile(filepath.Join(dir, "p.yaml"), []byte(manifest), 0o644)
	cat, err := catalog.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	p, err := cat.Plugin("p", "")
	if err != nil {
		t.Fatal(err)
	}
	return cat, p
}

func messages(r *Report) string {
	var b strings.Builder
	for _, f := range r.Findings {
		b.WriteString(string(f.Severity) + ": " + f.Message + "\n")
	}
	return b.String()
}

func TestLogicPluginModulesAreChecked(t *testing.T) {
	for name, tc := range map[string]struct {
		module, world, want string
		ok                  bool
	}{
		"a wit-bindgen module": {"../plugin/testdata/probe.wasm", "turgon:stack/logic-plugin@0.1.0", "imports entities.get, entities.propose-change, events.publish", true},
		"a module using WASI":  {"../plugin/testdata/probe-wasi.wasm", "turgon:stack/logic-plugin@0.1.0", "imports WASI", false},
		"world 0.2.0":          {"../plugin/testdata/probe2.wasm", "turgon:stack/logic-plugin@0.2.0", "imports entities.get, events.publish, http.send", true},
		"the wrong world":      {"../plugin/testdata/probe.wasm", "turgon:stack/logic-plugin@0.2.0", "p.wasm is built for turgon:stack/logic-plugin@0.1.0, but the manifest says turgon:stack/logic-plugin@0.2.0", false},
		"an unknown world":     {"../plugin/testdata/probe.wasm", "turgon:stack/logic-plugin@0.9.0", "runs logic plugins for turgon:stack/logic-plugin@0.1.0 or turgon:stack/logic-plugin@0.2.0", false},
		"no module":            {"", "turgon:stack/logic-plugin@0.1.0", "no such file", false},
	} {
		cat, p := pluginCatalog(t, tc.module, tc.world, "logic")
		r := New(cat, Options{}).Plugin(p)
		if got := messages(r); !strings.Contains(got, tc.want) || r.Deployable != tc.ok {
			t.Errorf("%s: deployable %v\n%s", name, r.Deployable, got)
		}
		// 0.1.0 has no network: the grant is unused. 0.2.0 uses it.
		if unused := strings.Contains(messages(r), "offers no network access"); tc.ok && unused != strings.HasSuffix(tc.world, "0.1.0") {
			t.Errorf("%s: unused network grant reported %v", name, unused)
		}
	}
}
