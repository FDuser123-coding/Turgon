package compiler

import (
	"fmt"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
)

// Sketch is a recipe's flow as far as the catalog resolves it: its
// trigger and its mapping steps, without verification, policies or
// connectors. It is for analysis (which fields a recipe's mappings read)
// of recipes that do not compile yet, such as one waiting for mapping
// review; it is never deployable. A mapping the catalog cannot resolve
// is left out.
func Sketch(cat *catalog.Catalog, r *v1alpha1.Recipe) *RuntimeSpec {
	spec := &RuntimeSpec{}
	spec.Metadata.Name = r.Metadata.Name
	wf := Workflow{Name: r.Metadata.Name, Trigger: WorkflowSource{Endpoint: r.Spec.Trigger.Source, Event: r.Spec.Trigger.Event}}
	for i, s := range r.Spec.Steps {
		if s.Map == nil {
			continue
		}
		m, err := cat.Mapping(s.Map.Mapping)
		if err != nil {
			continue
		}
		fields := map[string]string{}
		for _, f := range m.Spec.Fields {
			fields[f.Target] = f.Expression
		}
		wf.Steps = append(wf.Steps, WorkflowStep{Name: fmt.Sprintf("%02d-map", i+1),
			Map: &MapConfig{Mapping: ref(m.Metadata), From: s.Map.From, To: s.Map.To, Fields: fields}})
	}
	spec.Spec.Workflows = []Workflow{wf}
	return spec
}
