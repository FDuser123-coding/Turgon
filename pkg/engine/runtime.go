package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/policy"
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

// New connects every endpoint the spec's workflows use. Endpoints no
// workflow uses (for example slots reached only by agents) are skipped.
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
	rt := &Runtime{Spec: spec, Sources: map[string]connector.Source{}, instances: map[string]connector.Instance{}}
	targets := map[string]writeguard.TargetConfig{}
	for _, c := range spec.Spec.Connectors {
		if !used[c.Endpoint] {
			continue
		}
		factory, ok := opts.Registry[c.Name]
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
	guard, err := writeguard.New(writeguard.Config{
		Targets: targets, Policy: opts.Policy, Audit: opts.Audit, Store: opts.Store,
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
