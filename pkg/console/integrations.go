package console

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// Integrations is the map of what is connected to what: the systems in the
// catalog and the flows (recipes) between them, step by step.
type Integrations struct {
	Error   string   `json:"error,omitempty"`
	Systems []System `json:"systems"`
	Flows   []Flow   `json:"flows"`
}

// System is a connected system (a Connection).
type System struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// Connector is the connector it is reached through; Product the system
	// the connector's manifest names ("PostgreSQL 13-17").
	Connector string `json:"connector"`
	Product   string `json:"product,omitempty"`
	// Role: source (flows start from its events), target (flows write to
	// it) or both.
	Role       string        `json:"role"`
	Events     []SystemEvent `json:"events"`
	Operations []string      `json:"operations"`
	Flows      []string      `json:"flows"`
}

// SystemEvent is an event a system emits and how Turgon receives it.
type SystemEvent struct {
	Name   string `json:"name"`
	Entity string `json:"entity,omitempty"`
	// Delivery: polling, webhook (with polling to reconcile), outbox,
	// change capture, or subscription.
	Delivery string `json:"delivery"`
}

// Flow is a recipe: an event in one system, the steps it takes, and the
// systems it writes to.
type Flow struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
	Level       string `json:"level,omitempty"`
	Deployable  bool   `json:"deployable"`
	Trigger     struct {
		System   string `json:"system"`
		Event    string `json:"event"`
		Delivery string `json:"delivery"`
	} `json:"trigger"`
	Steps   []FlowStep `json:"steps"`
	Targets []string   `json:"targets"`
	SLO     string     `json:"slo,omitempty"`
	// Problem is why the flow cannot deploy, if it cannot.
	Problem string `json:"problem,omitempty"`
}

// FlowStep is one step of a flow.
type FlowStep struct {
	// Kind: map, resolve or write.
	Kind string `json:"kind"`
	// Map: the mapping and how many fields it sets.
	Mapping string `json:"mapping,omitempty"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Fields  int    `json:"fields,omitempty"`
	// Resolve: the entity matched to a master record.
	Entity   string `json:"entity,omitempty"`
	Strategy string `json:"strategy,omitempty"`
	// Write: where, what, and the safeguards around it.
	System       string `json:"system,omitempty"`
	Operation    string `json:"operation,omitempty"`
	Risk         string `json:"risk,omitempty"`
	Approval     string `json:"approval,omitempty"`
	Simulation   string `json:"simulation,omitempty"`
	Compensation string `json:"compensation,omitempty"`
	// Plugins react to the write once it commits (logic plugins, sandboxed).
	Plugins []string `json:"plugins,omitempty"`
}

func (s *Server) integrations(w http.ResponseWriter, r *http.Request) {
	out := Integrations{Systems: []System{}, Flows: []Flow{}}
	if len(s.cfg.Catalogs) == 0 {
		writeJSON(w, http.StatusOK, out)
		return
	}
	cat, err := catalog.Load(s.cfg.Catalogs...)
	if err != nil {
		out.Error = err.Error()
		writeJSON(w, http.StatusOK, out)
		return
	}
	opts, err := s.verifierOptions(r.Context())
	if err != nil {
		out.Error = "mapping reviews: " + err.Error()
	}
	res := BuildIntegrations(cat, opts)
	res.Error = out.Error
	writeJSON(w, http.StatusOK, res)
}

// BuildIntegrations maps a catalog's systems and flows. Deployable recipes
// are compiled, so their steps are exactly what workers run.
func BuildIntegrations(cat *catalog.Catalog, opts verifier.Options) Integrations {
	out := Integrations{Systems: []System{}, Flows: []Flow{}}
	systems := map[string]*System{}
	system := func(name string) *System {
		if sys, ok := systems[name]; ok {
			return sys
		}
		sys := &System{Name: name, Events: []SystemEvent{}, Operations: []string{}, Flows: []string{}}
		if conn, err := cat.Connection(name); err == nil {
			sys.Description = conn.Metadata.Description
			ref, constraint, _ := strings.Cut(conn.Spec.Connector, "@")
			sys.Connector = ref
			if m, err := cat.Connector(ref, constraint); err == nil {
				sys.Product = m.Spec.System
			}
		}
		systems[name] = sys
		return sys
	}
	for _, obj := range cat.All() {
		if c, ok := obj.(*v1alpha1.Connection); ok {
			system(c.Metadata.Name)
		}
	}

	for _, obj := range cat.All() {
		rec, ok := obj.(*v1alpha1.Recipe)
		if !ok {
			continue
		}
		f := Flow{Name: rec.Metadata.Name, Version: rec.Metadata.Version, Description: rec.Metadata.Description,
			Steps: []FlowStep{}, Targets: []string{}}
		if rec.Spec.SLO != nil && rec.Spec.SLO.P95Latency != "" {
			f.SLO = "p95 " + rec.Spec.SLO.P95Latency
		}
		spec, rep, err := compiler.Compile(cat, rec, opts)
		if rep != nil {
			f.Level = string(rep.Level)
		}
		if err == nil && len(spec.Spec.Workflows) == 1 {
			f.Deployable = true
			wf := spec.Spec.Workflows[0]
			f.Trigger.System, f.Trigger.Event = wf.Trigger.Endpoint, wf.Trigger.Event
			f.Trigger.Delivery = delivery(spec, wf.Trigger)
			for _, st := range wf.Steps {
				switch {
				case st.Map != nil:
					f.Steps = append(f.Steps, FlowStep{Kind: "map", Mapping: st.Map.Mapping, From: st.Map.From, To: st.Map.To, Fields: len(st.Map.Fields)})
				case st.Resolve != nil:
					f.Steps = append(f.Steps, FlowStep{Kind: "resolve", Entity: st.Resolve.Entity, Strategy: st.Resolve.Strategy})
				case st.Write != nil:
					wr := st.Write
					f.Steps = append(f.Steps, FlowStep{Kind: "write", System: wr.Endpoint, Operation: wr.Operation, Risk: wr.Risk,
						Approval: wr.Approval, Simulation: wr.Simulation, Compensation: wr.Compensation, Entity: wr.Entity, Plugins: wr.Plugins})
				}
			}
		} else {
			// Not deployable: what the recipe itself says.
			f.Trigger.System, f.Trigger.Event = rec.Spec.Trigger.Source, rec.Spec.Trigger.Event
			for _, st := range rec.Spec.Steps {
				switch {
				case st.Map != nil:
					f.Steps = append(f.Steps, FlowStep{Kind: "map", Mapping: st.Map.Mapping, From: st.Map.From, To: st.Map.To})
				case st.Resolve != nil:
					f.Steps = append(f.Steps, FlowStep{Kind: "resolve", Entity: st.Resolve.Entity, Strategy: st.Resolve.Strategy})
				case st.Write != nil:
					f.Steps = append(f.Steps, FlowStep{Kind: "write", System: st.Write.Target, Operation: st.Write.Operation})
				}
			}
			if rep != nil {
				for _, e := range rep.Errors() {
					f.Problem = e.Message
					break
				}
			}
			if f.Problem == "" && err != nil {
				f.Problem = err.Error()
			}
		}
		out.Flows = append(out.Flows, f)
		if !f.Deployable {
			continue // its systems may be stack slots, not connected systems
		}
		src := system(f.Trigger.System)
		src.Flows = appendOnce(src.Flows, f.Name)
		addEvent(src, cat, f.Trigger.Event, f.Trigger.Delivery)
		for _, st := range f.Steps {
			if st.Kind != "write" || st.System == "" {
				continue
			}
			f.Targets = appendOnce(f.Targets, st.System)
			t := system(st.System)
			t.Flows = appendOnce(t.Flows, f.Name)
			t.Operations = appendOnce(t.Operations, st.Operation)
		}
		out.Flows[len(out.Flows)-1] = f
	}

	for _, sys := range systems {
		source, target := false, false
		for _, f := range out.Flows {
			if !f.Deployable {
				continue
			}
			if f.Trigger.System == sys.Name {
				source = true
			}
			for _, t := range f.Targets {
				if t == sys.Name {
					target = true
				}
			}
		}
		switch {
		case source && target:
			sys.Role = "both"
		case source:
			sys.Role = "source"
		case target:
			sys.Role = "target"
		default:
			sys.Role = "unused"
		}
		sort.Strings(sys.Operations)
		sort.Strings(sys.Flows)
		sort.Slice(sys.Events, func(i, j int) bool { return sys.Events[i].Name < sys.Events[j].Name })
		out.Systems = append(out.Systems, *sys)
	}
	sort.Slice(out.Systems, func(i, j int) bool { return out.Systems[i].Name < out.Systems[j].Name })
	sort.Slice(out.Flows, func(i, j int) bool { return out.Flows[i].Name < out.Flows[j].Name })
	return out
}

func addEvent(sys *System, cat *catalog.Catalog, event, how string) {
	for _, e := range sys.Events {
		if e.Name == event {
			return
		}
	}
	ev := SystemEvent{Name: event, Delivery: how}
	if conn, err := cat.Connection(sys.Name); err == nil {
		for _, ce := range conn.Spec.Events {
			if ce.Name == event {
				ev.Entity = ce.Entity
			}
		}
	}
	if ev.Delivery == "" {
		ev.Delivery = "polling"
	}
	sys.Events = append(sys.Events, ev)
}

// delivery says how the trigger event reaches the workers.
func delivery(spec *compiler.RuntimeSpec, trig compiler.WorkflowSource) string {
	var cfg struct {
		Events map[string]struct {
			Webhook json.RawMessage `json:"webhook"`
		} `json:"events"`
		Subscriptions map[string]json.RawMessage `json:"subscriptions"`
		Changes       map[string]json.RawMessage `json:"changes"`
	}
	for _, c := range spec.Spec.Connectors {
		if c.Endpoint == trig.Endpoint {
			if c.Name == "debezium" {
				return "change capture" // read from the database's log by Debezium
			}
			_ = json.Unmarshal(c.Config, &cfg)
		}
	}
	switch {
	case cfg.Subscriptions[trig.Event] != nil:
		return "subscription"
	case cfg.Changes[trig.Event] != nil:
		return "change capture"
	case len(cfg.Events[trig.Event].Webhook) > 0:
		return "webhook"
	case trig.Interface == "outbox":
		return "outbox"
	}
	return "polling"
}

func appendOnce(list []string, s string) []string {
	for _, x := range list {
		if x == s {
			return list
		}
	}
	return append(list, s)
}
