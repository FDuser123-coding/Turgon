// Package verifier checks recipes, plugins and stack blueprints before
// anything is deployed (architecture §7.5): schema contracts, permitted
// interfaces, policy, capacity, mappings and slot contracts. It also decides
// which one-click level (L0–L3, §3) an integration reaches.
package verifier

import (
	"fmt"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
)

// Severity of a finding.
type Severity string

const (
	// SeverityError blocks deployment.
	SeverityError Severity = "error"
	// SeverityReview blocks deployment until a person approves the item.
	SeverityReview Severity = "review"
	// SeverityWarning is shown to the user but does not block.
	SeverityWarning Severity = "warning"
	// SeverityInfo is informational.
	SeverityInfo Severity = "info"
)

// Verifier stages, in the order they run.
const (
	StageSchema     = "schema"
	StageResolve    = "resolve"
	StageInterfaces = "interfaces"
	StageMapping    = "mapping"
	StagePolicy     = "policy"
	StageCapacity   = "capacity"
	StageContract   = "contract"
	StageDryRun     = "dry-run"
)

// Finding is one verifier result. Messages are written for the person who
// has to act on them.
type Finding struct {
	Stage    string   `json:"stage"`
	Severity Severity `json:"severity"`
	Path     string   `json:"path,omitempty"`
	Message  string   `json:"message"`
}

// ReviewItem is a mapped field waiting in the human review queue.
type ReviewItem struct {
	Mapping    string  `json:"mapping"`
	Target     string  `json:"target"`
	Expression string  `json:"expression"`
	Origin     string  `json:"origin"`
	Confidence float64 `json:"confidence"`
	Rationale  string  `json:"rationale,omitempty"`
}

// Resolution records what the verifier bound each reference to, so the
// compiler emits exactly what was verified.
type Resolution struct {
	// Recipe is the recipe a recipe report was produced for.
	Recipe *v1alpha1.Recipe `json:"-"`
	// Connectors maps a recipe endpoint (connector or slot name) to its
	// manifest; for blueprints, each bound slot to its connector.
	Connectors map[string]*v1alpha1.ConnectorManifest `json:"-"`
	// Connections maps a recipe endpoint to the connection it names, if any.
	Connections map[string]*v1alpha1.Connection `json:"-"`
	// Mappings maps a mapping reference to the resolved mapping.
	Mappings map[string]*v1alpha1.Mapping `json:"-"`
	// Slots maps a slot name to the plugin bound to it.
	Slots map[string]*v1alpha1.Plugin `json:"-"`
	// Contracts maps a slot name to its contract.
	Contracts map[string]*v1alpha1.SlotContract `json:"-"`
	// Extensions are the resolved extension plugins, in declaration order.
	Extensions []*v1alpha1.Plugin `json:"-"`
	// Policies are the resolved policy packs, in declaration order.
	Policies []*v1alpha1.PolicyPack `json:"-"`
}

// Report is the verification result for one object and, for blueprints, the
// recipes and plugins it contains.
type Report struct {
	Subject     string       `json:"subject"`
	Level       string       `json:"level"`
	Deployable  bool         `json:"deployable"`
	Findings    []Finding    `json:"findings,omitempty"`
	ReviewQueue []ReviewItem `json:"reviewQueue,omitempty"`
	Children    []*Report    `json:"children,omitempty"`
	Resolution  Resolution   `json:"-"`

	level int // 0..3; see raise
}

func newReport(obj v1alpha1.Object) *Report {
	m := obj.GetMeta()
	subject := obj.GetTypeMeta().Kind + "/" + m.Name
	if m.Version != "" {
		subject += "@" + m.Version
	}
	return &Report{
		Subject: subject,
		Resolution: Resolution{
			Connectors:  map[string]*v1alpha1.ConnectorManifest{},
			Mappings:    map[string]*v1alpha1.Mapping{},
			Connections: map[string]*v1alpha1.Connection{},
			Slots:       map[string]*v1alpha1.Plugin{},
			Contracts:   map[string]*v1alpha1.SlotContract{},
		},
	}
}

func (r *Report) add(stage string, sev Severity, path, format string, args ...any) {
	r.Findings = append(r.Findings, Finding{Stage: stage, Severity: sev, Path: path, Message: fmt.Sprintf(format, args...)})
}

// raise lowers the one-click level to at least lvl (0 = L0 ... 3 = L3).
func (r *Report) raise(lvl int) {
	if lvl > r.level {
		r.level = lvl
	}
}

// Errors returns the blocking error findings, including children's.
func (r *Report) Errors() []Finding {
	var out []Finding
	for _, f := range r.Findings {
		if f.Severity == SeverityError {
			out = append(out, f)
		}
	}
	for _, c := range r.Children {
		for _, f := range c.Errors() {
			f.Path = strings.TrimPrefix(c.Subject+" "+f.Path, " ")
			out = append(out, f)
		}
	}
	return out
}

// finish computes Level and Deployable after all stages have run.
func (r *Report) finish() {
	errs, reviews := 0, len(r.ReviewQueue)
	for _, f := range r.Findings {
		if f.Severity == SeverityError {
			errs++
		}
	}
	for _, c := range r.Children {
		r.raise(levelIndex(c.Level))
		if !c.Deployable {
			errs++
		}
	}
	if reviews > 0 {
		r.raise(1)
	}
	r.Level = fmt.Sprintf("L%d", r.level)
	r.Deployable = errs == 0 && reviews == 0
	sort.SliceStable(r.Findings, func(i, j int) bool {
		return stageOrder(r.Findings[i].Stage) < stageOrder(r.Findings[j].Stage)
	})
}

func levelIndex(l string) int {
	switch l {
	case v1alpha1.LevelL0:
		return 0
	case v1alpha1.LevelL1:
		return 1
	case v1alpha1.LevelL2:
		return 2
	default:
		return 3
	}
}

func stageOrder(s string) int {
	for i, st := range []string{StageSchema, StageResolve, StageInterfaces, StageContract, StageMapping, StagePolicy, StageCapacity, StageDryRun} {
		if st == s {
			return i
		}
	}
	return 99
}
