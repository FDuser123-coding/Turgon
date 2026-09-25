package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/policy/opa"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Options assemble a runtime.
type Options struct {
	Registry connector.Registry
	Secrets  connector.SecretResolver
	Store    writeguard.Store
	Resolver Resolver
	Audit    audit.Recorder
	// Policy defaults to policy.WritebackDefault until OPA bundles are wired in.
	Policy policy.Decider
}

// Runtime is a compiled spec bound to live connectors.
type Runtime struct {
	Spec       *compiler.RuntimeSpec
	Activities *Activities
	Sources    map[string]connector.Source
	instances  map[string]connector.Instance
}

// New connects every endpoint the spec's workflows use, and the endpoints
// of agent write tools this worker has a connector for. Other endpoints
// (for example slots reached only by read tools) are skipped.
func New(ctx context.Context, spec *compiler.RuntimeSpec, opts Options) (*Runtime, error) {
	if opts.Policy == nil {
		opts.Policy = policy.WritebackDefault{}
	}
	used := map[string]bool{}
	for _, wf := range spec.Spec.Workflows {
		used[wf.Trigger.Endpoint] = true
		for _, s := range wf.Steps {
			if s.Write != nil {
				used[s.Write.Endpoint] = true
			}
		}
	}
	agentOnly := map[string]bool{}
	for _, t := range spec.Spec.Tools {
		if t.Risk != v1alpha1.RiskRead && !used[t.Endpoint] {
			agentOnly[t.Endpoint] = true
		}
	}
	rt := &Runtime{Spec: spec, Sources: map[string]connector.Source{}, instances: map[string]connector.Instance{}}
	targets := map[string]writeguard.TargetConfig{}
	for _, c := range spec.Spec.Connectors {
		if !used[c.Endpoint] && !agentOnly[c.Endpoint] {
			continue
		}
		factory, ok := opts.Registry[c.Name]
		if !ok && agentOnly[c.Endpoint] {
			continue
		}
		if !ok {
			rt.Close()
			return nil, fmt.Errorf("endpoint %s: no runtime for connector %s (runtime %s) in this worker", c.Endpoint, c.Name, c.Runtime)
		}
		inst, err := factory(ctx, c, opts.Secrets)
		if err != nil {
			rt.Close()
			return nil, err
		}
		rt.instances[c.Endpoint] = inst
		targets[c.Endpoint] = writeguard.TargetConfig{Target: inst, Limits: c.Limits, Metered: c.Metered}
		if src, ok := inst.(connector.Source); ok {
			rt.Sources[c.Endpoint] = src
		}
	}
	for _, wf := range spec.Spec.Workflows {
		if _, ok := rt.Sources[wf.Trigger.Endpoint]; !ok {
			rt.Close()
			return nil, fmt.Errorf("workflow %s: endpoint %s cannot emit events", wf.Name, wf.Trigger.Endpoint)
		}
	}
	decider, err := recipeDeciders(ctx, spec, opts.Policy)
	if err != nil {
		rt.Close()
		return nil, err
	}
	guard, err := writeguard.New(writeguard.Config{
		Targets: targets, Policy: decider, Audit: opts.Audit, Store: opts.Store,
	})
	if err != nil {
		rt.Close()
		return nil, err
	}
	rt.Activities = &Activities{Guard: guard, Resolver: opts.Resolver}
	return rt, nil
}

// Close releases connector resources.
func (rt *Runtime) Close() {
	for _, inst := range rt.instances {
		inst.Close()
	}
}

// Starter starts a workflow run exactly once per ID; starting an ID that
// already exists must succeed without starting a second run.
type Starter interface {
	Start(ctx context.Context, id string, in RunInput) error
}

// CursorStore persists source positions.
type CursorStore interface {
	Cursor(ctx context.Context, name string) (int64, error)
	SetCursor(ctx context.Context, name string, pos int64) error
}

// Dispatcher reads events from sources and starts one run per event.
// Run IDs derive from the workflow and event, so re-reading an event after
// a crash never starts a second run.
type Dispatcher struct {
	Runtime *Runtime
	Cursors CursorStore
	Starter Starter
	// Batch is the maximum number of events read per source per poll. Default 100.
	Batch int
	// ApprovalTimeout is passed to each run; zero uses the default.
	ApprovalTimeout time.Duration
}

// RunID is the workflow ID for an event.
func RunID(workflow string, ev connector.Event) string {
	return workflow + "/" + ev.ID
}

// Poll performs one pass over every workflow's trigger and returns how
// many runs were started. The cursor advances only past started events.
func (d *Dispatcher) Poll(ctx context.Context) (int, error) {
	batch := d.Batch
	if batch <= 0 {
		batch = 100
	}
	wfs := append([]compiler.Workflow{}, d.Runtime.Spec.Spec.Workflows...)
	sort.Slice(wfs, func(i, j int) bool { return wfs[i].Name < wfs[j].Name })
	started := 0
	var errs []error
	for _, wf := range wfs {
		name := wf.Name + "@" + wf.Trigger.Endpoint + "/" + wf.Trigger.Event
		pos, err := d.Cursors.Cursor(ctx, name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		events, err := d.Runtime.Sources[wf.Trigger.Endpoint].Poll(ctx, wf.Trigger.Event, pos, batch)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		for _, ev := range events {
			in := RunInput{SpecDigest: d.Runtime.Spec.Metadata.Digest, Workflow: wf, Event: ev, ApprovalTimeout: d.ApprovalTimeout}
			if err := d.Starter.Start(ctx, RunID(wf.Name, ev), in); err != nil {
				errs = append(errs, fmt.Errorf("%s: start %s: %w", name, ev.ID, err))
				break // keep the cursor before the event that failed
			}
			started++
			if err := d.Cursors.SetCursor(ctx, name, ev.Position); err != nil {
				errs = append(errs, err)
				break
			}
		}
	}
	return started, errors.Join(errs...)
}

// recipeDeciders gives each workflow its own OPA decider built from the
// policy packs compiled into the spec, so one recipe's packs never loosen
// another's. Workflows whose packs do not define turgon.writeback, and
// requests from no recipe, use the fallback decider.
func recipeDeciders(ctx context.Context, spec *compiler.RuntimeSpec, fallback policy.Decider) (policy.Decider, error) {
	packs := map[string]opa.Module{}
	for _, p := range spec.Spec.Policies {
		packs[p.Name] = opa.Module{Name: p.Name, Source: p.Rego}
	}
	byRecipe := map[string]policy.Decider{}
	for _, wf := range spec.Spec.Workflows {
		var mods []opa.Module
		for _, name := range wf.Policies {
			m, ok := packs[name]
			if !ok {
				return nil, fmt.Errorf("workflow %s: policy pack %s is not in the spec", wf.Name, name)
			}
			mods = append(mods, m)
		}
		if !opa.DefinesWriteback(mods) {
			continue
		}
		d, err := opa.New(ctx, mods)
		if err != nil {
			return nil, fmt.Errorf("workflow %s: policies: %w", wf.Name, err)
		}
		byRecipe[wf.Name] = d
	}
	return policy.DeciderFunc(func(ctx context.Context, in policy.Input) (policy.Decision, error) {
		if d, ok := byRecipe[in.Recipe]; ok {
			return d.Decide(ctx, in)
		}
		return fallback.Decide(ctx, in)
	}), nil
}
