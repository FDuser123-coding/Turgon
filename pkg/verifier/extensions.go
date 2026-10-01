package verifier

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/plugin"
)

// logicPlugin checks a Wasm logic plugin: the world this Turgon runs, a
// module that loads within the plugin's limits, and grants the world can
// use.
func (v *Verifier) logicPlugin(r *Report, p *v1alpha1.Plugin) {
	if p.Spec.World != plugin.World {
		r.add(StageContract, SeverityError, "spec.world", "this Turgon runs logic plugins for %s, not %s", plugin.World, p.Spec.World)
		return
	}
	b, err := v.cat.PluginModule(p)
	if err != nil {
		r.add(StageResolve, SeverityError, "spec.module", "%v", err)
		return
	}
	ctx := context.Background()
	m, err := plugin.Load(ctx, b, plugin.Limits{MemoryMB: p.Spec.Limits.MemoryMB, Timeout: time.Duration(p.Spec.Limits.TimeoutMs) * time.Millisecond})
	if err != nil {
		r.add(StageContract, SeverityError, "spec.module", "%s: %v", p.Spec.Module, err)
		return
	}
	defer m.Close(ctx)
	imports := "nothing"
	if len(m.Imports) > 0 {
		imports = strings.Join(m.Imports, ", ")
	}
	r.add(StageContract, SeverityInfo, "spec.module", "%s: %d KiB, imports %s", p.Spec.Module, (len(b)+1023)/1024, imports)
	if n := p.Spec.Permissions.Network; n != nil && len(n.Allow) > 0 {
		r.add(StageContract, SeverityWarning, "spec.permissions.network", "%s offers no network access; this grant is unused", plugin.World)
	}
	if len(p.Spec.Permissions.Secrets) > 0 {
		r.add(StageContract, SeverityWarning, "spec.permissions.secrets", "%s offers no secrets; this grant is unused", plugin.World)
	}
}

// extensions resolves a recipe's logic plugins and checks what they ask
// for against the recipe's connections.
func (v *Verifier) extensions(r *Report, refs []string, conns map[string]*v1alpha1.ConnectorManifest) {
	entities, events := map[string]bool{}, map[string]bool{}
	for _, m := range conns {
		for _, e := range m.Spec.Entities {
			entities[e] = true
		}
		for _, e := range m.Spec.Events {
			events[e.Name] = true
		}
	}
	for i, ref := range refs {
		path := fmt.Sprintf("spec.extensions[%d]", i)
		name, con := v1alpha1.ParseRef(ref)
		p, err := v.cat.Plugin(name, con)
		if err != nil {
			r.add(StageResolve, SeverityError, path, "%v", err)
			r.raise(3)
			continue
		}
		r.Children = append(r.Children, v.Plugin(p))
		r.Resolution.Extensions = append(r.Resolution.Extensions, p)
		if p.Spec.Type != v1alpha1.PluginLogic {
			r.add(StageContract, SeverityError, path, "a recipe's extensions are logic plugins; %s is a %s plugin", p.Metadata.Name, p.Spec.Type)
			continue
		}
		checkExtension(r, path, p, entities, events)
		checkPluginOperations(r, path, p, conns)
	}
}

// checkPluginOperations warns when an entity a plugin may read or propose
// changes to has no operation to serve it, and when it subscribes to
// events the runtime does not deliver to plugins yet.
func checkPluginOperations(r *Report, path string, p *v1alpha1.Plugin, conns map[string]*v1alpha1.ConnectorManifest) {
	has := func(op, dir string) bool {
		for _, m := range conns {
			if o, ok := m.Operation(op); ok && o.Direction == dir {
				return true
			}
		}
		return false
	}
	entity := func(ref string) string { e, _, _ := strings.Cut(ref, "."); return e }
	if e := p.Spec.Permissions.Entities; e != nil {
		seen := map[string]bool{}
		for _, ref := range e.Read {
			if en := entity(ref); !seen["r"+en] && !has(plugin.ReadOperation(en), v1alpha1.DirectionRead) {
				seen["r"+en] = true
				r.add(StageContract, SeverityWarning, path, "%s may read %s, but no connection or slot offers a %s operation: get will answer invalid", p.Metadata.Name, en, plugin.ReadOperation(en))
			}
		}
		for _, ref := range e.Propose {
			if en := entity(ref); !seen["p"+en] && !has(plugin.ProposeOperation(en), v1alpha1.DirectionWrite) {
				seen["p"+en] = true
				r.add(StageContract, SeverityWarning, path, "%s may propose changes to %s, but no connection or slot offers a %s operation: propose-change will answer invalid", p.Metadata.Name, en, plugin.ProposeOperation(en))
			}
		}
	}
	var other []string
	for _, ev := range p.Spec.Subscribes {
		if !strings.HasPrefix(ev, "model.") {
			other = append(other, ev)
		}
	}
	if len(other) > 0 {
		sort.Strings(other)
		r.add(StageContract, SeverityWarning, path, "%s subscribes to %s: plugins receive model lifecycle events (model.<Entity>.created|updated|deleted) only, for now", p.Metadata.Name, strings.Join(other, ", "))
	}
}
