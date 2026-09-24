package verifier

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/mapping"
	"github.com/fduser123-coding/turgon/pkg/policy/opa"
)

// Options tune verification policy.
type Options struct {
	// ReviewThreshold is the confidence below which a mapped field goes to
	// the human review queue. Default 0.90.
	ReviewThreshold float64
	// WriteReviewThreshold applies instead when the mapping feeds a write
	// into a system of record; defaults are conservative there (§7.4).
	// Default 0.95.
	WriteReviewThreshold float64
	// CapacityHeadroom is the fraction of a target's declared rate above
	// which a recipe's peak load draws a warning. Default 0.8.
	CapacityHeadroom float64
}

func (o Options) withDefaults() Options {
	if o.ReviewThreshold == 0 {
		o.ReviewThreshold = 0.90
	}
	if o.WriteReviewThreshold == 0 {
		o.WriteReviewThreshold = 0.95
	}
	if o.CapacityHeadroom == 0 {
		o.CapacityHeadroom = 0.8
	}
	return o
}

// Verifier checks objects against a catalog.
type Verifier struct {
	cat  *catalog.Catalog
	opts Options
}

// New returns a verifier over the catalog.
func New(cat *catalog.Catalog, opts Options) *Verifier {
	return &Verifier{cat: cat, opts: opts.withDefaults()}
}

// Verify dispatches on the object's kind.
func (v *Verifier) Verify(obj v1alpha1.Object) *Report {
	switch o := obj.(type) {
	case *v1alpha1.Recipe:
		return v.Recipe(o)
	case *v1alpha1.StackBlueprint:
		return v.Blueprint(o)
	case *v1alpha1.Plugin:
		return v.Plugin(o)
	default:
		r := newReport(obj)
		schema(r, obj)
		r.finish()
		return r
	}
}

func schema(r *Report, obj v1alpha1.Object) bool {
	errs := obj.Validate()
	for _, e := range errs {
		r.add(StageSchema, SeverityError, e.Path, "%s", e.Message)
	}
	if len(errs) > 0 {
		r.raise(3)
	}
	return len(errs) == 0
}

// Recipe verifies a standalone recipe, resolving endpoints as connector names.
func (v *Verifier) Recipe(rec *v1alpha1.Recipe) *Report {
	return v.recipe(rec, nil)
}

// recipe verifies a recipe. bound maps endpoint names (slots, or connectors
// bound in a blueprint) to manifests; unbound endpoints resolve from the catalog.
func (v *Verifier) recipe(rec *v1alpha1.Recipe, bound map[string]*v1alpha1.ConnectorManifest) *Report {
	r := newReport(rec)
	r.Resolution.Recipe = rec
	defer r.finish()
	if !schema(r, rec) {
		return r
	}
	if rec.Metadata.Certification != v1alpha1.LevelL0 {
		r.raise(1) // uncertified recipes are at best assisted
	}

	// Stage: resolve connectors.
	for _, ep := range rec.Endpoints() {
		if m, ok := bound[ep]; ok {
			r.Resolution.Connectors[ep] = m
			continue
		}
		if conn, err := v.cat.Connection(ep); err == nil {
			if m := v.connection(r, conn, rec.Spec.Connectors[ep]); m != nil {
				r.Resolution.Connectors[ep] = m
				r.Resolution.Connections[ep] = conn
			}
			continue
		}
		m, err := v.cat.Connector(ep, rec.Spec.Connectors[ep])
		if err != nil {
			if _, cerr := v.cat.Contract(ep); cerr == nil {
				r.add(StageResolve, SeverityError, "", "%q is a stack slot, not a connector; verify this recipe within a StackBlueprint that fills the slot", ep)
				continue
			}
			r.add(StageResolve, SeverityError, "", "no connector for %q in the catalog (%v); an engineer must build one with the connector SDK", ep, err)
			r.raise(3)
			continue
		}
		r.Resolution.Connectors[ep] = m
	}
	for name := range rec.Spec.Connectors {
		if !contains(rec.Endpoints(), name) {
			r.add(StageResolve, SeverityWarning, "spec.connectors."+name, "pinned connector %q is not used by any step", name)
		}
	}

	hasWrites := false
	for _, s := range rec.Spec.Steps {
		if s.Write != nil {
			hasWrites = true
		}
	}

	// Stage: permitted interfaces.
	if src := r.Resolution.Connectors[rec.Spec.Trigger.Source]; src != nil {
		ev, ok := src.Event(rec.Spec.Trigger.Event)
		if !ok {
			r.add(StageInterfaces, SeverityError, "spec.trigger.event", "connector %s does not emit event %q", src.Metadata.Name, rec.Spec.Trigger.Event)
			r.raise(2)
		} else if _, err := src.Permitted("events", ev.Interface); err != nil {
			r.add(StageInterfaces, SeverityError, "spec.trigger.event", "%s: %v", src.Metadata.Name, err)
			r.raise(3)
		}
	}
	producing := "" // entity produced by the most recent map step
	for i, s := range rec.Spec.Steps {
		path := fmt.Sprintf("spec.steps[%d]", i)
		switch {
		case s.Map != nil:
			producing = strings.TrimPrefix(s.Map.To, "model.")
		case s.Write != nil:
			v.writeStep(r, path, s.Write, producing)
		}
	}

	// Stage: mappings and identity resolution.
	threshold := v.opts.ReviewThreshold
	if hasWrites {
		threshold = v.opts.WriteReviewThreshold
	}
	for i, s := range rec.Spec.Steps {
		path := fmt.Sprintf("spec.steps[%d]", i)
		if s.Map != nil {
			v.mapStep(r, path, s.Map, threshold)
		}
		if s.Resolve != nil {
			if s.Resolve.Strategy != "splink" && s.Resolve.Strategy != "exact" {
				r.add(StageMapping, SeverityError, path+".resolve.strategy", "unknown identity-resolution strategy %q (want splink or exact)", s.Resolve.Strategy)
			}
			if s.Resolve.AutoMatchAbove < 0.8 {
				r.add(StageMapping, SeverityWarning, path+".resolve.autoMatchAbove", "auto-matching above %.2f risks merging distinct records; ambiguous matches should go to a data steward", s.Resolve.AutoMatchAbove)
			}
		}
	}

	// Stage: policy.
	for _, ref := range rec.Spec.Policies {
		p, err := v.cat.Policy(ref)
		if err != nil {
			r.add(StagePolicy, SeverityError, "spec.policies", "policy pack %q: %v", ref, err)
			continue
		}
		r.Resolution.Policies = append(r.Resolution.Policies, p)
	}
	if hasWrites && len(rec.Spec.Policies) == 0 {
		r.add(StagePolicy, SeverityWarning, "spec.policies", "no policy packs referenced; writes fall back to the built-in write-back default")
	}
	checkPolicies(r, r.Resolution.Policies, hasWrites)

	// Stage: capacity.
	for i, s := range rec.Spec.Steps {
		if s.Write == nil {
			continue
		}
		m := r.Resolution.Connectors[s.Write.Target]
		if m == nil {
			continue
		}
		path := fmt.Sprintf("spec.steps[%d].write", i)
		limit := m.Spec.Limits.RequestsPerSecond
		switch {
		case rec.Spec.Capacity == nil:
			r.add(StageCapacity, SeverityInfo, path, "no peak load declared; the rate governor will hold %s to %g requests/s", m.Metadata.Name, limit)
		case rec.Spec.Capacity.PeakPerSecond > limit:
			r.add(StageCapacity, SeverityError, path, "peak load %g/s exceeds %s's declared limit of %g requests/s", rec.Spec.Capacity.PeakPerSecond, m.Metadata.Name, limit)
		case rec.Spec.Capacity.PeakPerSecond > limit*v.opts.CapacityHeadroom:
			r.add(StageCapacity, SeverityWarning, path, "peak load %g/s leaves little headroom under %s's limit of %g requests/s", rec.Spec.Capacity.PeakPerSecond, m.Metadata.Name, limit)
		}
	}

	// Stage: dry-run. Sample dry-runs need a sandbox or simulation endpoint,
	// which the design-time verifier does not reach; the runtime performs them.
	if hasWrites {
		r.add(StageDryRun, SeverityInfo, "", "sample dry-run deferred to deployment (simulation or shadow mode)")
	}
	return r
}

// connection resolves a connection's connector and returns the effective
// manifest, or nil after reporting why it cannot be used. pin, if set,
// further constrains the connector version.
func (v *Verifier) connection(r *Report, c *v1alpha1.Connection, pin string) *v1alpha1.ConnectorManifest {
	name, con := v1alpha1.ParseRef(c.Spec.Connector)
	if pin != "" {
		con = pin
	}
	m, err := v.cat.Connector(name, con)
	if err != nil {
		r.add(StageResolve, SeverityError, "", "connection %s: %v; an engineer must build the connector with the SDK", c.Metadata.Name, err)
		r.raise(3)
		return nil
	}
	errs := c.ValidateEffective(m)
	for _, e := range errs {
		r.add(StageInterfaces, SeverityError, e.Path, "%s", e.Message)
	}
	if len(errs) > 0 {
		r.raise(3) // the connection asks for an interface the connector does not sanction
		return nil
	}
	return c.Effective(m)
}

func (v *Verifier) writeStep(r *Report, path string, w *v1alpha1.WriteStep, producing string) {
	m := r.Resolution.Connectors[w.Target]
	if m == nil {
		return
	}
	op, ok := m.Operation(w.Operation)
	if !ok {
		r.add(StageInterfaces, SeverityError, path+".write.operation", "connector %s has no operation %q", m.Metadata.Name, w.Operation)
		r.raise(3)
		return
	}
	if op.Direction != v1alpha1.DirectionWrite {
		r.add(StageInterfaces, SeverityError, path+".write.operation", "%s.%s is a read operation", m.Metadata.Name, op.Name)
		return
	}
	iface, err := m.Permitted(v1alpha1.DirectionWrite, op.Interface)
	if err != nil {
		r.add(StageInterfaces, SeverityError, path+".write.operation", "%s.%s: %v", m.Metadata.Name, op.Name, err)
		r.raise(3)
		return
	}
	if w.Simulate && iface.Simulation == "" {
		r.add(StageInterfaces, SeverityWarning, path+".write.simulate", "%s interface %q cannot simulate writes; the dry-run needs a sandbox", m.Metadata.Name, iface.Kind)
	}
	if producing != "" && op.Entity != "" && op.Entity != producing {
		r.add(StageInterfaces, SeverityError, path+".write.operation", "%s.%s expects %s but the preceding map step produces %s", m.Metadata.Name, op.Name, op.Entity, producing)
		r.raise(2)
	}

	comp := w.Compensation
	if comp == "" {
		comp = op.Compensation
	}
	switch cop, ok := m.Operation(comp); {
	case comp == "":
		r.add(StageInterfaces, SeverityError, path+".write.compensation", "every write step must declare its undo action; %s.%s has none", m.Metadata.Name, op.Name)
	case !ok:
		r.add(StageInterfaces, SeverityError, path+".write.compensation", "connector %s has no compensation operation %q", m.Metadata.Name, comp)
	case cop.Direction != v1alpha1.DirectionWrite:
		r.add(StageInterfaces, SeverityError, path+".write.compensation", "compensation %q must be a write operation", comp)
	}

	approval := w.Approval
	if approval == "" {
		approval = v1alpha1.ApprovalPolicy
	}
	if op.Risk == v1alpha1.RiskHigh && approval == v1alpha1.ApprovalNone {
		r.add(StagePolicy, SeverityError, path+".write.approval", "%s.%s is a high-risk write and cannot disable approval", m.Metadata.Name, op.Name)
	}
}

func (v *Verifier) mapStep(r *Report, path string, s *v1alpha1.MapStep, threshold float64) {
	m, err := v.cat.Mapping(s.Mapping)
	if err != nil {
		r.add(StageMapping, SeverityError, path+".map.mapping", "mapping %q: %v; mappings must be confirmed in the console", s.Mapping, err)
		r.raise(2)
		return
	}
	r.Resolution.Mappings[s.Mapping] = m
	if m.Spec.From != s.From || m.Spec.To != s.To {
		r.add(StageMapping, SeverityError, path+".map", "mapping %s maps %s to %s, but the step needs %s to %s", s.Mapping, m.Spec.From, m.Spec.To, s.From, s.To)
		r.raise(2)
		return
	}
	ref := m.Metadata.Name + "@" + m.Metadata.Version
	for _, f := range m.Spec.Fields {
		if err := mapping.Check(f.Expression); err != nil {
			r.add(StageMapping, SeverityError, path+".map", "%s.%s: invalid JSONata expression: %v", ref, f.Target, err)
		}
	}
	for _, f := range m.Spec.Fields {
		if f.Approved || f.Origin == v1alpha1.OriginCertified || f.Confidence >= threshold {
			continue
		}
		r.ReviewQueue = append(r.ReviewQueue, ReviewItem{
			Mapping: ref, Target: f.Target, Expression: f.Expression,
			Origin: f.Origin, Confidence: f.Confidence, Rationale: f.Rationale,
		})
		r.add(StageMapping, SeverityReview, path+".map", "%s.%s has confidence %.2f, below the %.2f threshold; it needs review", ref, f.Target, f.Confidence, threshold)
	}
}

// Plugin verifies a plugin package on its own.
func (v *Verifier) Plugin(p *v1alpha1.Plugin) *Report {
	r := newReport(p)
	defer r.finish()
	if !schema(r, p) {
		return r
	}
	if p.Spec.Runtime == v1alpha1.PluginRuntimeWasm && !strings.HasPrefix(p.Spec.World, "turgon:stack/") {
		r.add(StageContract, SeverityError, "spec.world", "wasm plugins must target a turgon:stack world, got %q", p.Spec.World)
	}
	if p.Spec.Type == v1alpha1.PluginConnector {
		m, err := v.cat.Connector(v1alpha1.ParseRef(p.Spec.Connector))
		if err != nil {
			r.add(StageResolve, SeverityError, "spec.connector", "%v", err)
			r.raise(3)
			return r
		}
		for _, ref := range p.Spec.Implements {
			c, err := v.cat.Contract(ref)
			if err != nil {
				r.add(StageResolve, SeverityError, "spec.implements", "slot contract %q: %v", ref, err)
				continue
			}
			satisfies(r, "spec.implements", c, m)
		}
	}
	return r
}

// satisfies checks that a connector provides everything a slot contract requires.
func satisfies(r *Report, path string, c *v1alpha1.SlotContract, m *v1alpha1.ConnectorManifest) bool {
	ok := true
	fail := func(format string, args ...any) {
		ok = false
		r.add(StageContract, SeverityError, path, "%s does not satisfy contract %s@%s: "+format,
			append([]any{m.Metadata.Name, c.Metadata.Name, c.Metadata.Version}, args...)...)
	}
	for _, e := range c.Spec.Entities {
		if !m.HasEntity(e) {
			fail("missing entity %s", e)
		}
	}
	for _, e := range c.Spec.Events {
		if _, has := m.Event(e); !has {
			fail("missing event %s", e)
		}
	}
	for _, a := range c.Spec.Actions {
		op, has := m.Operation(a.Name)
		switch {
		case !has:
			fail("missing action %s", a.Name)
		case (a.Risk == v1alpha1.RiskRead) != (op.Direction == v1alpha1.DirectionRead):
			fail("action %s is a %s in the contract but a %s on the connector", a.Name, a.Risk, op.Direction)
		case riskRank(op.Risk) < riskRank(a.Risk):
			fail("action %s is %s risk in the contract but only %s on the connector", a.Name, a.Risk, op.Risk)
		}
	}
	return ok
}

func riskRank(r string) int {
	switch r {
	case v1alpha1.RiskRead:
		return 0
	case v1alpha1.RiskLow:
		return 1
	default:
		return 2
	}
}

// Blueprint verifies a whole stack: every slot binding against its
// contract, every extension's permissions against what the slots provide,
// and every recipe with its endpoints bound to the slots.
func (v *Verifier) Blueprint(b *v1alpha1.StackBlueprint) *Report {
	r := newReport(b)
	defer r.finish()
	if !schema(r, b) {
		return r
	}

	bound := map[string]*v1alpha1.ConnectorManifest{}
	byConnector := map[string]string{} // connector name -> slot
	entities := map[string]bool{}
	events := map[string]bool{}

	slots := make([]string, 0, len(b.Spec.Slots))
	for s := range b.Spec.Slots {
		slots = append(slots, s)
	}
	sort.Strings(slots)
	for _, slot := range slots {
		path := "spec.slots." + slot
		bind := b.Spec.Slots[slot]
		p, err := v.cat.Plugin(bind.Plugin, bind.Version)
		if err != nil {
			r.add(StageResolve, SeverityError, path, "%v; no certified plugin fills this slot", err)
			r.raise(3)
			continue
		}
		pr := v.Plugin(p)
		r.Children = append(r.Children, pr)
		if p.Spec.Type != v1alpha1.PluginConnector {
			r.add(StageContract, SeverityError, path, "plugin %s is a %s plugin; slots take connector plugins", p.Metadata.Name, p.Spec.Type)
			continue
		}
		var contractRef string
		for _, ref := range p.Spec.Implements {
			if name, _ := v1alpha1.ParseRef(ref); name == slot {
				contractRef = ref
			}
		}
		if contractRef == "" {
			r.add(StageContract, SeverityError, path, "plugin %s does not implement the %s slot contract", p.Metadata.Name, slot)
			r.raise(2)
			continue
		}
		c, err := v.cat.Contract(contractRef)
		if err != nil {
			r.add(StageResolve, SeverityError, path, "slot contract %q: %v", contractRef, err)
			continue
		}
		if b.Spec.Model != "" && c.Spec.Model != "" && c.Spec.Model != b.Spec.Model {
			r.add(StageContract, SeverityError, path, "contract %s is defined on model %s, but the stack uses %s", contractRef, c.Spec.Model, b.Spec.Model)
		}
		m, err := v.cat.Connector(v1alpha1.ParseRef(p.Spec.Connector))
		if err != nil {
			continue // reported by the plugin's own report
		}
		if !satisfies(r, path, c, m) {
			r.raise(2)
			continue
		}
		r.Resolution.Slots[slot] = p
		r.Resolution.Contracts[slot] = c
		r.Resolution.Connectors[slot] = m
		bound[slot] = m
		byConnector[m.Metadata.Name] = slot
		for _, e := range c.Spec.Entities {
			entities[e] = true
		}
		for _, e := range c.Spec.Events {
			events[e] = true
		}
	}

	for i, ref := range b.Spec.Extensions {
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
		if p.Spec.Type == v1alpha1.PluginConnector {
			r.add(StageContract, SeverityError, path, "connector plugins fill slots; %s cannot be an extension", p.Metadata.Name)
		}
		checkExtension(r, path, p, entities, events)
	}

	for i, ref := range b.Spec.Recipes {
		path := fmt.Sprintf("spec.recipes[%d]", i)
		rec, err := v.cat.Recipe(ref)
		if err != nil {
			r.add(StageResolve, SeverityError, path, "recipe %q: %v", ref, err)
			r.raise(2)
			continue
		}
		scope := map[string]*v1alpha1.ConnectorManifest{}
		for k, m := range bound {
			scope[k] = m
		}
		for _, ep := range rec.Endpoints() {
			if _, isSlot := b.Spec.Slots[ep]; isSlot {
				continue // an unfilled slot is already reported above
			}
			if slot, ok := byConnector[ep]; ok {
				scope[ep] = bound[slot]
				r.add(StageContract, SeverityInfo, path, "recipe %s names tool %q directly; naming slot %q would survive a swap", rec.Metadata.Name, ep, slot)
				continue
			}
			r.add(StageContract, SeverityError, path, "recipe %s uses %q, which is not a slot in this stack", rec.Metadata.Name, ep)
		}
		r.Children = append(r.Children, v.recipe(rec, scope))
	}

	for _, ref := range b.Spec.Policies {
		p, err := v.cat.Policy(ref)
		if err != nil {
			r.add(StagePolicy, SeverityError, "spec.policies", "policy pack %q: %v", ref, err)
			continue
		}
		r.Resolution.Policies = append(r.Resolution.Policies, p)
	}
	checkPolicies(r, r.Resolution.Policies, false)
	return r
}

// PolicyModules turns policy packs into OPA modules.
func PolicyModules(packs []*v1alpha1.PolicyPack) []opa.Module {
	mods := make([]opa.Module, 0, len(packs))
	for _, p := range packs {
		mods = append(mods, opa.Module{Name: p.Metadata.Name, Source: p.Spec.Rego})
	}
	return mods
}

// checkPolicies compiles the packs together and runs their test_ rules.
func checkPolicies(r *Report, packs []*v1alpha1.PolicyPack, hasWrites bool) {
	if len(packs) == 0 {
		return
	}
	mods := PolicyModules(packs)
	if err := opa.Compile(mods); err != nil {
		r.add(StagePolicy, SeverityError, "spec.policies", "policy packs do not compile: %v", err)
		return
	}
	fails, n, err := opa.Test(context.Background(), mods)
	if err != nil {
		r.add(StagePolicy, SeverityError, "spec.policies", "policy tests could not run: %v", err)
		return
	}
	for _, f := range fails {
		r.add(StagePolicy, SeverityError, "spec.policies", "policy test %s: %s", f.Name, f.Message)
	}
	if n > 0 && len(fails) == 0 {
		r.add(StagePolicy, SeverityInfo, "spec.policies", "%d policy test(s) passed", n)
	}
	if hasWrites && !opa.DefinesWriteback(mods) {
		r.add(StagePolicy, SeverityWarning, "spec.policies", "no policy pack defines package %s; writes use the built-in write-back default", opa.WritebackPackage)
	}
}

// checkExtension verifies that an extension only asks for entities and
// events some slot provides. Model lifecycle events have the form
// model.<Entity>.<created|updated|deleted>.
func checkExtension(r *Report, path string, p *v1alpha1.Plugin, entities, events map[string]bool) {
	entity := func(ref string) string {
		if i := strings.IndexByte(ref, '.'); i >= 0 {
			return ref[:i]
		}
		return ref
	}
	if e := p.Spec.Permissions.Entities; e != nil {
		for _, ref := range append(append([]string{}, e.Read...), e.Propose...) {
			if !entities[entity(ref)] {
				r.add(StageContract, SeverityError, path, "%s asks for entity %s, which no slot in this stack provides", p.Metadata.Name, entity(ref))
			}
		}
	}
	for _, ev := range p.Spec.Subscribes {
		if rest, ok := strings.CutPrefix(ev, "model."); ok {
			parts := strings.Split(rest, ".")
			if len(parts) == 2 && entities[parts[0]] && contains([]string{"created", "updated", "deleted"}, parts[1]) {
				continue
			}
		} else if events[ev] {
			continue
		}
		r.add(StageContract, SeverityError, path, "%s subscribes to %s, which no slot in this stack emits", p.Metadata.Name, ev)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
