// Package compiler turns a verified recipe or stack blueprint into a runtime
// spec: declarative configuration that generic workers interpret
// (architecture §7.5, AD-04). The output is reproducible: the same inputs
// always produce the same bytes and digest, so specs can be stored in Git,
// diffed, signed and rolled back.
package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// ErrNotDeployable is returned when verification blocks compilation.
var ErrNotDeployable = errors.New("not deployable")

// KindRuntimeSpec is the kind of compiler output.
const KindRuntimeSpec = "RuntimeSpec"

// RuntimeSpec is what the delivery agent applies and workers execute.
type RuntimeSpec struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   RuntimeMeta `json:"metadata"`
	Spec       RuntimeBody `json:"spec"`
}

type RuntimeMeta struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	Level  string `json:"level"`
	// Digest is the SHA-256 of the canonical JSON encoding of Spec.
	Digest string `json:"digest"`
}

type RuntimeBody struct {
	Connectors []ConnectorConfig  `json:"connectors"`
	Topics     []Topic            `json:"topics,omitempty"`
	Workflows  []Workflow         `json:"workflows,omitempty"`
	Plugins    []PluginDeployment `json:"plugins,omitempty"`
	Tools      []Tool             `json:"tools,omitempty"`
	Policies   []PolicyRef        `json:"policies,omitempty"`
	Monitors   []Monitor          `json:"monitors,omitempty"`
}

// ConnectorConfig configures one connector worker. Secrets are references.
type ConnectorConfig struct {
	Endpoint   string          `json:"endpoint"`
	Name       string          `json:"name"`
	Version    string          `json:"version"`
	Runtime    string          `json:"runtime"`
	SecretRef  string          `json:"secretRef"`
	Interfaces []string        `json:"interfaces,omitempty"`
	Prohibited []string        `json:"prohibited,omitempty"`
	Limits     v1alpha1.Limits `json:"limits"`
	Metered    bool            `json:"metered,omitempty"`
	// Connection names the Connection this endpoint was bound through.
	Connection string `json:"connection,omitempty"`
	// Config is the connection's connector-specific configuration.
	Config json.RawMessage `json:"config,omitempty"`
}

// Topic is an event-backbone topic (rendered as a Strimzi KafkaTopic).
type Topic struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Entity     string `json:"entity,omitempty"`
}

type Workflow struct {
	Name    string         `json:"name"`
	Trigger WorkflowSource `json:"trigger"`
	Steps   []WorkflowStep `json:"steps"`
	SLO     *v1alpha1.SLO  `json:"slo,omitempty"`
	// Policies names the packs that decide this workflow's writes.
	Policies []string `json:"policies,omitempty"`
}

type WorkflowSource struct {
	Endpoint  string `json:"endpoint"`
	Connector string `json:"connector"`
	Event     string `json:"event"`
	Interface string `json:"interface"`
	Topic     string `json:"topic"`
}

type WorkflowStep struct {
	Name    string         `json:"name"`
	Map     *MapConfig     `json:"map,omitempty"`
	Resolve *ResolveConfig `json:"resolve,omitempty"`
	Write   *WriteConfig   `json:"write,omitempty"`
}

type MapConfig struct {
	Mapping string `json:"mapping"`
	From    string `json:"from"`
	To      string `json:"to"`
	// Fields maps target field to JSONata expression.
	Fields map[string]string `json:"fields"`
}

type ResolveConfig struct {
	Entity         string  `json:"entity"`
	Strategy       string  `json:"strategy"`
	AutoMatchAbove float64 `json:"autoMatchAbove"`
}

type WriteConfig struct {
	Endpoint       string `json:"endpoint"`
	Connector      string `json:"connector"`
	Operation      string `json:"operation"`
	Interface      string `json:"interface"`
	Risk           string `json:"risk"`
	Entity         string `json:"entity,omitempty"`
	IdempotencyKey string `json:"idempotencyKey"`
	// Simulation is the dry-run mode to use first, or empty for none.
	Simulation   string `json:"simulation,omitempty"`
	Approval     string `json:"approval"`
	Compensation string `json:"compensation"`
	Metered      bool   `json:"metered,omitempty"`
	Output       string `json:"output,omitempty"`
}

// PluginDeployment is a plugin with the capabilities granted to it.
type PluginDeployment struct {
	Name       string                     `json:"name"`
	Version    string                     `json:"version"`
	Publisher  string                     `json:"publisher"`
	Type       string                     `json:"type"`
	Runtime    string                     `json:"runtime"`
	World      string                     `json:"world,omitempty"`
	Slot       string                     `json:"slot,omitempty"`
	Subscribes []string                   `json:"subscribes,omitempty"`
	Grants     v1alpha1.PluginPermissions `json:"grants"`
	Limits     *v1alpha1.PluginLimits     `json:"limits,omitempty"`
	UIPanel    string                     `json:"uiPanel,omitempty"`
}

// Tool is an MCP tool descriptor for the agent gateway, named in business
// terms rather than system terms (architecture §7.7).
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Risk        string `json:"risk"`
	Endpoint    string `json:"endpoint"`
	Operation   string `json:"operation"`
	Entity      string `json:"entity,omitempty"`
	// Fields are the business fields a write tool's record carries, known
	// from the mapping that feeds the write; empty when not known.
	Fields []string `json:"fields,omitempty"`
	// Simulate asks the target for a preview before a write is approved.
	Simulate bool `json:"simulate,omitempty"`
}

// PolicyRef is a policy pack compiled into the spec, with its Rego source,
// so the spec's digest covers exactly the policies workers evaluate.
type PolicyRef struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Rego    string `json:"rego"`
}

type Monitor struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Workflow string `json:"workflow"`
	Target   string `json:"target"`
}

// Compile verifies obj and, if deployable, compiles it. The report is always
// returned so callers can explain a refusal.
func Compile(cat *catalog.Catalog, obj v1alpha1.Object, opts verifier.Options) (*RuntimeSpec, *verifier.Report, error) {
	v := verifier.New(cat, opts)
	switch o := obj.(type) {
	case *v1alpha1.Recipe:
		rep := v.Recipe(o)
		if !rep.Deployable {
			return nil, rep, notDeployable(rep)
		}
		b := newBuilder()
		b.recipe(o, rep)
		return b.finish(o.Metadata.Name, "Recipe/"+ref(o.Metadata), rep.Level), rep, nil
	case *v1alpha1.StackBlueprint:
		rep := v.Blueprint(o)
		if !rep.Deployable {
			return nil, rep, notDeployable(rep)
		}
		b := newBuilder()
		b.blueprint(rep)
		return b.finish(o.Metadata.Name, "StackBlueprint/"+ref(o.Metadata), rep.Level), rep, nil
	default:
		return nil, nil, fmt.Errorf("cannot compile a %s; compile a Recipe or StackBlueprint", obj.GetTypeMeta().Kind)
	}
}

func notDeployable(rep *verifier.Report) error {
	errs := rep.Errors()
	if len(errs) == 0 {
		return fmt.Errorf("%s: %w: %d field(s) awaiting review", rep.Subject, ErrNotDeployable, countReviews(rep))
	}
	return fmt.Errorf("%s: %w: %d error(s), first: %s", rep.Subject, ErrNotDeployable, len(errs), errs[0].Message)
}

func countReviews(rep *verifier.Report) int {
	n := len(rep.ReviewQueue)
	for _, c := range rep.Children {
		n += countReviews(c)
	}
	return n
}

func ref(m v1alpha1.ObjectMeta) string {
	if m.Version == "" {
		return m.Name
	}
	return m.Name + "@" + m.Version
}

type builder struct {
	body       RuntimeBody
	connectors map[string]*ConnectorConfig // by endpoint
	topics     map[string]Topic
	tools      map[string]Tool
	policies   map[string]PolicyRef
	// alias maps a connector name to the slot it fills, so recipes that
	// name a tool directly share the slot's connector deployment.
	alias map[string]string
}

func newBuilder() *builder {
	return &builder{
		connectors: map[string]*ConnectorConfig{},
		topics:     map[string]Topic{},
		tools:      map[string]Tool{},
		policies:   map[string]PolicyRef{},
		alias:      map[string]string{},
	}
}

func (b *builder) endpoint(name string) string {
	if slot, ok := b.alias[name]; ok {
		return slot
	}
	return name
}

func (b *builder) connector(endpoint string, m *v1alpha1.ConnectorManifest, iface string) {
	endpoint = b.endpoint(endpoint)
	c, ok := b.connectors[endpoint]
	if !ok {
		c = &ConnectorConfig{
			Endpoint:  endpoint,
			Name:      m.Metadata.Name,
			Version:   m.Metadata.Version,
			Runtime:   m.Spec.Runtime,
			SecretRef: m.Spec.Auth.SecretRef,
			Limits:    m.Spec.Limits,
			Metered:   m.Spec.Metering.SAPDigitalAccess,
		}
		for _, p := range m.Spec.Interfaces.Prohibited {
			c.Prohibited = append(c.Prohibited, p.Kind)
		}
		sort.Strings(c.Prohibited)
		b.connectors[endpoint] = c
	}
	if iface != "" && !contains(c.Interfaces, iface) {
		c.Interfaces = append(c.Interfaces, iface)
		sort.Strings(c.Interfaces)
	}
}

func (b *builder) recipe(r *v1alpha1.Recipe, rep *verifier.Report) {
	conns := rep.Resolution.Connectors
	src := conns[r.Spec.Trigger.Source]
	ev, _ := src.Event(r.Spec.Trigger.Event)
	topic := topicName(b.endpoint(r.Spec.Trigger.Source), ev.Name)
	b.connector(r.Spec.Trigger.Source, src, ev.Interface)
	b.topics[topic] = Topic{Name: topic, Partitions: 3, Entity: ev.Entity}

	wf := Workflow{
		Name: r.Metadata.Name,
		Trigger: WorkflowSource{
			Endpoint: b.endpoint(r.Spec.Trigger.Source), Connector: src.Metadata.Name,
			Event: ev.Name, Interface: ev.Interface, Topic: topic,
		},
		SLO: r.Spec.SLO,
	}
	// doc tracks the fields of the document as the steps build it, so a
	// write tool can tell agents which fields its record takes. It is nil
	// until a mapping fixes the document's shape.
	var doc map[string]bool
	for i, s := range r.Spec.Steps {
		step := WorkflowStep{Name: fmt.Sprintf("%02d-%s", i+1, s.Kind())}
		switch {
		case s.Map != nil:
			m := rep.Resolution.Mappings[s.Map.Mapping]
			fields := map[string]string{}
			doc = map[string]bool{}
			for _, f := range m.Spec.Fields {
				fields[f.Target] = f.Expression
				doc[f.Target] = true
			}
			step.Map = &MapConfig{Mapping: ref(m.Metadata), From: s.Map.From, To: s.Map.To, Fields: fields}
		case s.Resolve != nil:
			step.Resolve = &ResolveConfig{Entity: s.Resolve.Entity, Strategy: s.Resolve.Strategy, AutoMatchAbove: s.Resolve.AutoMatchAbove}
			if doc != nil {
				// The resolve activity adds <entity>Id.
				doc[lowerFirst(strings.TrimPrefix(s.Resolve.Entity, "model."))+"Id"] = true
			}
		case s.Write != nil:
			w := s.Write
			m := conns[w.Target]
			op, _ := m.Operation(w.Operation)
			iface, _ := m.Permitted(v1alpha1.DirectionWrite, op.Interface)
			comp := w.Compensation
			if comp == "" {
				comp = op.Compensation
			}
			if cop, ok := m.Operation(comp); ok {
				b.connector(w.Target, m, cop.Interface)
			}
			approval := w.Approval
			if approval == "" {
				approval = v1alpha1.ApprovalPolicy
			}
			wc := &WriteConfig{
				Endpoint: b.endpoint(w.Target), Connector: m.Metadata.Name, Operation: op.Name,
				Interface: op.Interface, Risk: op.Risk, Entity: op.Entity, IdempotencyKey: w.IdempotencyKey,
				Approval: approval, Compensation: comp, Metered: m.Spec.Metering.SAPDigitalAccess, Output: w.Output,
			}
			if w.Simulate {
				wc.Simulation = iface.Simulation
			}
			step.Write = wc
			b.connector(w.Target, m, op.Interface)
			b.writeTool(b.endpoint(w.Target), op, fmt.Sprintf("%s via %s", humanize(op.Name), w.Target), doc, w.Simulate)
			if doc != nil && w.Output != "" {
				doc[w.Output] = true
			}
		}
		wf.Steps = append(wf.Steps, step)
	}
	// Every read operation of an endpoint the recipe uses becomes a
	// read-only agent tool, named in business terms (architecture §7.7).
	for ep, m := range conns {
		for _, op := range m.Spec.Operations {
			if op.Direction != v1alpha1.DirectionRead {
				continue
			}
			b.connector(ep, m, op.Interface)
			desc := fmt.Sprintf("%s from %s by its identifier (read-only)", humanize(op.Name), b.endpoint(ep))
			b.tool(b.endpoint(ep), op.Name, op.Entity, op.Risk, desc)
		}
	}
	for ep, conn := range rep.Resolution.Connections {
		if c := b.connectors[b.endpoint(ep)]; c != nil {
			c.Connection, c.Config = conn.Metadata.Name, conn.Spec.Config
		}
	}
	for _, p := range rep.Resolution.Policies {
		wf.Policies = append(wf.Policies, p.Metadata.Name)
	}
	sort.Strings(wf.Policies)
	b.body.Workflows = append(b.body.Workflows, wf)
	if r.Spec.SLO != nil && r.Spec.SLO.P95Latency != "" {
		b.body.Monitors = append(b.body.Monitors, Monitor{
			Name: r.Metadata.Name + "-p95", Kind: "latency-p95", Workflow: wf.Name, Target: r.Spec.SLO.P95Latency,
		})
	}
	for _, p := range rep.Resolution.Policies {
		b.policies[p.Metadata.Name] = PolicyRef{Name: p.Metadata.Name, Version: p.Metadata.Version, Rego: p.Spec.Rego}
	}
}

func (b *builder) blueprint(rep *verifier.Report) {
	slots := make([]string, 0, len(rep.Resolution.Slots))
	for s := range rep.Resolution.Slots {
		slots = append(slots, s)
	}
	sort.Strings(slots)
	for _, slot := range slots {
		m := rep.Resolution.Connectors[slot]
		b.alias[m.Metadata.Name] = slot
		// Every slot's connector is deployed, even if no recipe uses it yet:
		// agents and extensions reach it through the slot.
		b.connector(slot, m, "")
	}
	for _, slot := range slots {
		p := rep.Resolution.Slots[slot]
		b.body.Plugins = append(b.body.Plugins, deployment(p, slot))
		// Agent tools are generated from slot contracts, so they keep their
		// names and meaning when the tool behind a slot is swapped.
		for _, a := range rep.Resolution.Contracts[slot].Spec.Actions {
			b.tool(slot, a.Name, a.Entity, a.Risk, fmt.Sprintf("%s in the %s system", humanize(a.Name), strings.ToUpper(slot)))
		}
	}
	for _, p := range rep.Resolution.Extensions {
		b.body.Plugins = append(b.body.Plugins, deployment(p, ""))
	}
	for _, child := range rep.Children {
		if r := child.Resolution.Recipe; r != nil {
			b.recipe(r, child)
		}
	}
	for _, p := range rep.Resolution.Policies {
		b.policies[p.Metadata.Name] = PolicyRef{Name: p.Metadata.Name, Version: p.Metadata.Version, Rego: p.Spec.Rego}
	}
}

func deployment(p *v1alpha1.Plugin, slot string) PluginDeployment {
	d := PluginDeployment{
		Name: p.Metadata.Name, Version: p.Metadata.Version, Publisher: p.Metadata.Publisher,
		Type: p.Spec.Type, Runtime: p.Spec.Runtime, World: p.Spec.World, Slot: slot,
		Subscribes: p.Spec.Subscribes, Grants: p.Spec.Permissions, Limits: p.Spec.Limits,
	}
	if p.Spec.UI != nil {
		d.UIPanel = p.Spec.UI.Panel
	}
	return d
}

// tool registers an agent tool. Names are assigned in finish.
func (b *builder) tool(endpoint, op, entity, risk, desc string) {
	b.tools[endpoint+"/"+op] = Tool{Description: desc, Risk: risk, Endpoint: endpoint, Operation: op, Entity: entity}
}

// writeTool registers a write tool with the record fields a recipe passes
// it. When several recipes write through the same operation, the tool
// takes the union of their fields.
func (b *builder) writeTool(endpoint string, op v1alpha1.Operation, desc string, doc map[string]bool, simulate bool) {
	key := endpoint + "/" + op.Name
	prev, seen := b.tools[key]
	b.tool(endpoint, op.Name, op.Entity, op.Risk, desc)
	t := b.tools[key]
	fields := map[string]bool{}
	for f := range doc {
		fields[f] = true
	}
	if seen {
		for _, f := range prev.Fields {
			fields[f] = true
		}
		simulate = simulate || prev.Simulate
	}
	for f := range fields {
		t.Fields = append(t.Fields, f)
	}
	sort.Strings(t.Fields)
	t.Simulate = simulate
	b.tools[key] = t
}

func lowerFirst(s string) string {
	for i, r := range s {
		return string(unicode.ToLower(r)) + s[i+len(string(r)):]
	}
	return s
}

func (b *builder) finish(name, source, level string) *RuntimeSpec {
	for _, c := range b.connectors {
		b.body.Connectors = append(b.body.Connectors, *c)
	}
	sort.Slice(b.body.Connectors, func(i, j int) bool { return b.body.Connectors[i].Endpoint < b.body.Connectors[j].Endpoint })
	for _, t := range b.topics {
		b.body.Topics = append(b.body.Topics, t)
	}
	sort.Slice(b.body.Topics, func(i, j int) bool { return b.body.Topics[i].Name < b.body.Topics[j].Name })
	// Tools are named after the operation; when two endpoints offer the same
	// operation, every such tool is prefixed with its endpoint.
	byOp := map[string]int{}
	for _, t := range b.tools {
		byOp[t.Operation]++
	}
	for _, t := range b.tools {
		t.Name = strings.ReplaceAll(t.Operation, "-", "_")
		if byOp[t.Operation] > 1 {
			t.Name = strings.ReplaceAll(t.Endpoint, "-", "_") + "_" + t.Name
		}
		b.body.Tools = append(b.body.Tools, t)
	}
	sort.Slice(b.body.Tools, func(i, j int) bool { return b.body.Tools[i].Name < b.body.Tools[j].Name })
	for _, p := range b.policies {
		b.body.Policies = append(b.body.Policies, p)
	}
	sort.Slice(b.body.Policies, func(i, j int) bool { return b.body.Policies[i].Name < b.body.Policies[j].Name })
	sort.Slice(b.body.Workflows, func(i, j int) bool { return b.body.Workflows[i].Name < b.body.Workflows[j].Name })
	sort.Slice(b.body.Monitors, func(i, j int) bool { return b.body.Monitors[i].Name < b.body.Monitors[j].Name })

	return &RuntimeSpec{
		APIVersion: v1alpha1.APIVersion,
		Kind:       KindRuntimeSpec,
		Metadata:   RuntimeMeta{Name: name, Source: source, Level: level, Digest: Digest(b.body)},
		Spec:       b.body,
	}
}

// Digest returns "sha256:<hex>" over the canonical JSON of a runtime body.
// encoding/json emits struct fields in declaration order and map keys
// sorted, and the builder sorts every slice, so the encoding is canonical.
func Digest(body RuntimeBody) string {
	data, err := json.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("compiler: runtime body is not serializable: %v", err))
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func topicName(connector, event string) string {
	return "porter." + connector + "." + strings.ToLower(strings.ReplaceAll(event, ".", "-"))
}

func humanize(op string) string {
	s := strings.ReplaceAll(op, "-", " ")
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
