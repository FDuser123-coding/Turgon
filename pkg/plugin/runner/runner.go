// Package runner runs a runtime spec's logic plugins for the engine: it
// loads their embedded modules, answers their host calls within their
// grants, and audits each invocation.
package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/plugin"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Proposer starts a governed write for a plugin's proposal, or finds the
// one already started under id, and reports its state without waiting
// for a person.
type Proposer interface {
	Propose(ctx context.Context, id string, req writeguard.Request) (engine.AgentWriteState, error)
}

// Reader performs governed reads: writeguard.Guard.
type Reader interface {
	Read(ctx context.Context, req writeguard.ReadRequest) (json.RawMessage, error)
}

type deployed struct {
	d      compiler.PluginDeployment
	module *plugin.Module
}

// Runner runs a spec's plugins. Modules are compiled once; Bind gives a
// runner the reads of the current connections, which change when secrets
// rotate.
type Runner struct {
	plugins map[string]*deployed
	read    Reader
	propose Proposer
	audit   audit.Recorder
}

var _ engine.PluginRunner = (*Runner)(nil)

// New loads every logic plugin in the spec, checking each module against
// the digest the spec records.
func New(ctx context.Context, spec *compiler.RuntimeSpec, propose Proposer, rec audit.Recorder) (*Runner, error) {
	r := &Runner{plugins: map[string]*deployed{}, propose: propose, audit: rec}
	for _, d := range spec.Spec.Plugins {
		if d.Type != v1alpha1.PluginLogic {
			continue
		}
		sum := sha256.Sum256(d.Module)
		if len(d.Module) == 0 || hex.EncodeToString(sum[:]) != d.ModuleSHA256 {
			r.Close(ctx)
			return nil, fmt.Errorf("plugin %s: the module does not match its digest", d.Name)
		}
		if d.Limits == nil {
			r.Close(ctx)
			return nil, fmt.Errorf("plugin %s: no limits", d.Name)
		}
		m, err := plugin.Load(ctx, d.Module, plugin.Limits{MemoryMB: d.Limits.MemoryMB, Timeout: time.Duration(d.Limits.TimeoutMs) * time.Millisecond})
		if err != nil {
			r.Close(ctx)
			return nil, fmt.Errorf("plugin %s: %w", d.Name, err)
		}
		r.plugins[d.Name] = &deployed{d: d, module: m}
	}
	return r, nil
}

// Names lists the plugins loaded.
func (r *Runner) Names() []string {
	var out []string
	for n, p := range r.plugins {
		out = append(out, n+"@"+p.d.Version)
	}
	slices.Sort(out)
	return out
}

// Bind returns a runner reading through read.
func (r *Runner) Bind(read Reader) *Runner {
	c := *r
	c.read = read
	return &c
}

// Close releases the modules.
func (r *Runner) Close(ctx context.Context) {
	for _, p := range r.plugins {
		p.module.Close(ctx)
	}
}

// event is what a plugin's handle function receives, as JSON.
type event struct {
	Type     string          `json:"type"`
	Entity   string          `json:"entity"`
	ID       string          `json:"id,omitempty"`
	Record   json.RawMessage `json:"record"`
	Workflow string          `json:"workflow"`
	Run      string          `json:"run"`
}

// Run runs one plugin on one event.
func (r *Runner) Run(ctx context.Context, in engine.PluginInput) error {
	p, ok := r.plugins[in.Plugin]
	if !ok {
		return fmt.Errorf("%w: %s is not deployed with this spec", plugin.ErrPlugin, in.Plugin)
	}
	if r.read == nil {
		return errors.New("plugin runner: no reader bound")
	}
	ev := event{Type: in.Event, Entity: in.Entity, ID: p.entityID(in.Entity, in.Record), Record: in.Record, Workflow: in.Workflow, Run: in.RunID}
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	h := &host{r: r, p: p, in: in}
	start := time.Now()
	err = p.module.Handle(ctx, b, h)
	data := map[string]any{"plugin": p.d.Name, "version": p.d.Version, "event": in.Event, "entity": in.Entity, "id": ev.ID,
		"workflow": in.Workflow, "run": in.RunID, "ms": time.Since(start).Milliseconds(), "proposals": h.proposals, "reads": h.reads}
	action := "plugin.handled"
	if err != nil {
		action, data["error"] = "plugin.failed", err.Error()
	}
	if _, aerr := r.audit.Record(p.actor(), action, data); aerr != nil && err == nil {
		err = aerr
	}
	return err
}

func (p *deployed) actor() string { return "plugin:" + p.d.Name + "@" + p.d.Version }

// entityID finds the written entity's identifier in the record: the field
// its proposal (or read) operation keys on, in either spelling.
func (p *deployed) entityID(entity string, record json.RawMessage) string {
	var rec map[string]any
	if json.Unmarshal(record, &rec) != nil {
		return ""
	}
	var fields []string
	for _, ops := range []map[string]compiler.EntityOperation{p.d.Proposals, p.d.Reads} {
		if eo, ok := ops[entity]; ok && eo.IDField != "" {
			fields = append(fields, eo.IDField, snake(eo.IDField))
		}
	}
	for _, f := range append(fields, "id") {
		switch v := rec[f].(type) {
		case string:
			return v
		case float64:
			return fmt.Sprint(v)
		}
	}
	return ""
}

func snake(s string) string {
	var b strings.Builder
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			c += 'a' - 'A'
		}
		b.WriteRune(c)
	}
	return b.String()
}

// host answers one invocation's calls.
type host struct {
	r                *Runner
	p                *deployed
	in               engine.PluginInput
	reads, proposals int
}

func (h *host) subject() policy.Subject {
	roles := []string{policy.RoleReader}
	if e := h.p.d.Grants.Entities; e != nil && len(e.Propose) > 0 {
		roles = append(roles, policy.RoleOperator)
	}
	return policy.Subject{ID: h.p.actor(), Agent: true, Roles: roles}
}

// granted reports whether refs (Entity or Entity.field) allow entity, and
// which fields, if not all of them.
func granted(refs []string, entity string) (ok bool, fields []string) {
	for _, ref := range refs {
		e, f, hasField := strings.Cut(ref, ".")
		if e != entity {
			continue
		}
		if !hasField {
			return true, nil
		}
		ok, fields = true, append(fields, f)
	}
	return ok, fields
}

func (h *host) Get(ctx context.Context, kind, id string) (string, error) {
	var reads []string
	if e := h.p.d.Grants.Entities; e != nil {
		reads = e.Read
	}
	if ok, _ := granted(reads, kind); !ok {
		return "", &plugin.ErrorCode{Kind: plugin.Denied}
	}
	eo, ok := h.p.d.Reads[kind]
	if !ok {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "no " + plugin.ReadOperation(kind) + " operation serves " + kind}
	}
	h.reads++
	data, err := h.r.read.Read(ctx, writeguard.ReadRequest{
		Target: eo.Endpoint, Operation: eo.Operation, Tool: h.p.actor(), Entity: kind, ID: id, Subject: h.subject(),
	})
	switch {
	case errors.Is(err, writeguard.ErrNotFound):
		return "", &plugin.ErrorCode{Kind: plugin.NotFound}
	case errors.Is(err, writeguard.ErrDenied):
		return "", &plugin.ErrorCode{Kind: plugin.Denied}
	case err != nil:
		return "", err // the target is failing: the invocation is retried
	}
	return string(data), nil
}

func (h *host) ProposeChange(ctx context.Context, kind, id, patch string) (string, error) {
	var refs []string
	if e := h.p.d.Grants.Entities; e != nil {
		refs = e.Propose
	}
	ok, fields := granted(refs, kind)
	if !ok {
		return "", &plugin.ErrorCode{Kind: plugin.Denied}
	}
	eo, found := h.p.d.Proposals[kind]
	if !found {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "no " + plugin.ProposeOperation(kind) + " operation applies changes to " + kind}
	}
	if strings.TrimSpace(id) == "" {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "the entity id is empty"}
	}
	var change map[string]any
	dec := json.NewDecoder(strings.NewReader(patch))
	dec.UseNumber()
	if err := dec.Decode(&change); err != nil || len(change) == 0 {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "the patch must be a JSON object of fields to change"}
	}
	for f := range change {
		if f == eo.IDField {
			return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "the patch cannot change " + f + ", the entity's identifier"}
		}
		if fields != nil && !slices.Contains(fields, f) {
			return "", &plugin.ErrorCode{Kind: plugin.Denied}
		}
	}
	change[eo.IDField] = id
	payload, _ := json.Marshal(change) // sorted keys: the same proposal, the same digest
	// One proposal per plugin, run, step, entity and change: retries and
	// redeliveries find the first.
	sum := sha256.Sum256(bytes.Join([][]byte{[]byte(h.in.RunID), []byte(h.in.Step), []byte(kind), []byte(id), payload}, []byte{0}))
	key := "plugin:" + h.p.d.Name + ":" + hex.EncodeToString(sum[:12])
	req := writeguard.Request{
		Target: eo.Endpoint, Operation: eo.Operation, Tool: h.p.actor(), Risk: eo.Risk, Subject: h.subject(),
		IdempotencyKey: key, Payload: payload, Entity: kind, Recipe: h.in.Workflow,
		Reason: fmt.Sprintf("proposed by plugin %s@%s on %s (run %s)", h.p.d.Name, h.p.d.Version, h.in.Event, h.in.RunID),
	}
	h.proposals++
	state, err := h.r.propose.Propose(ctx, "plugin/"+h.p.d.Name+":"+hex.EncodeToString(sum[:12]), req)
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(map[string]string{"proposal": key, "state": string(state)})
	return string(out), nil
}

// Publish: plugins cannot publish events yet; the world declares it so
// modules built for it load.
func (h *host) Publish(context.Context, string, []byte) error {
	return &plugin.ErrorCode{Kind: plugin.Invalid, Message: "publishing events is not available in this Turgon yet"}
}

// TemporalProposer starts proposals as agent writes on Temporal, so they
// get the same policy, approval and audit as an agent's.
type TemporalProposer struct {
	Writes          engine.AgentWrites
	SpecDigest      string
	ApprovalTimeout time.Duration
}

func (t TemporalProposer) Propose(ctx context.Context, id string, req writeguard.Request) (engine.AgentWriteState, error) {
	w := t.Writes
	if w.Wait <= 0 || w.Wait > 2*time.Second {
		w.Wait = 2 * time.Second
	}
	st, err := w.Submit(ctx, id, engine.AgentWriteInput{SpecDigest: t.SpecDigest, Request: req, ApprovalTimeout: t.ApprovalTimeout})
	return st.State, err
}
