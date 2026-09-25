package v1alpha1

import (
	"fmt"
	"regexp"
	"strings"
)

// Recipe is a declarative, versioned description of one integration
// (architecture Appendix B). The compiler turns it into a runtime spec that
// generic workers interpret; no per-customer code is generated.
type Recipe struct {
	TypeMeta
	Metadata ObjectMeta `json:"metadata"`
	Spec     RecipeSpec `json:"spec"`
}

type RecipeSpec struct {
	Trigger Trigger `json:"trigger"`
	// Connectors pins connector versions by name, e.g. {"sap-ecc": "^0.4"}.
	// Endpoints may also name a stack slot ("erp") when deployed in a blueprint.
	Connectors map[string]string `json:"connectors,omitempty"`
	Steps      []Step            `json:"steps"`
	Policies   []string          `json:"policies,omitempty"`
	Capacity   *Capacity         `json:"capacity,omitempty"`
	SLO        *SLO              `json:"slo,omitempty"`
}

type Trigger struct {
	Source string `json:"source"`
	Event  string `json:"event"`
}

// Step is a tagged union: exactly one field is set.
type Step struct {
	Map     *MapStep     `json:"map,omitempty"`
	Resolve *ResolveStep `json:"resolve,omitempty"`
	Write   *WriteStep   `json:"write,omitempty"`
}

// Kind returns "map", "resolve", "write", or "" if the step is malformed.
func (s Step) Kind() string {
	n, kind := 0, ""
	if s.Map != nil {
		n, kind = n+1, "map"
	}
	if s.Resolve != nil {
		n, kind = n+1, "resolve"
	}
	if s.Write != nil {
		n, kind = n+1, "write"
	}
	if n != 1 {
		return ""
	}
	return kind
}

type MapStep struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Mapping string `json:"mapping"`
}

type ResolveStep struct {
	Entity string `json:"entity"`
	// Strategy is exact (a known cross-reference, else a data steward),
	// probabilistic (the built-in matcher: link automatically when the
	// match is certain enough, else a steward with suggestions), or splink
	// (reserved for an external Splink service).
	Strategy       string  `json:"strategy"`
	AutoMatchAbove float64 `json:"autoMatchAbove"`
	// Match names the document fields that identify a record and how to
	// compare them. Required for probabilistic; with exact they only feed
	// the suggestions a steward sees.
	Match []MatchField `json:"match,omitempty"`
}

// MatchField is one identifying field of a record.
type MatchField struct {
	Field string `json:"field"`
	// Kind is email (the same address), domain (the same company email
	// domain, read from an email field; free mail providers say nothing),
	// name (similar names), or exact (equal values, e.g. a VAT ID). Match
	// a company by domain, a person by email and name.
	Kind string `json:"kind"`
}

var fieldNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Identity-resolution strategies and match kinds.
const (
	StrategyExact         = "exact"
	StrategyProbabilistic = "probabilistic"
	StrategySplink        = "splink"
	MatchEmail            = "email"
	MatchDomain           = "domain"
	MatchName             = "name"
	MatchExact            = "exact"
)

// Approval modes for write steps.
const (
	ApprovalPolicy   = "policy"   // policy decides (default)
	ApprovalRequired = "required" // always ask a person
	ApprovalNone     = "none"     // never ask; only valid for low-risk writes
)

type WriteStep struct {
	Target         string `json:"target"`
	Operation      string `json:"operation"`
	IdempotencyKey string `json:"idempotencyKey"`
	Simulate       bool   `json:"simulate,omitempty"`
	Approval       string `json:"approval,omitempty"`
	Compensation   string `json:"compensation,omitempty"`
	// Output names a document field that receives the write's result, so
	// later steps can use it (for example, a document number to link back).
	Output string `json:"output,omitempty"`
}

type Capacity struct {
	PeakPerSecond float64 `json:"peakPerSecond"`
}

type SLO struct {
	P95Latency string `json:"p95Latency,omitempty"`
}

func (r *Recipe) GetTypeMeta() TypeMeta { return r.TypeMeta }
func (r *Recipe) GetMeta() ObjectMeta   { return r.Metadata }

// Endpoints returns every connector or slot name the recipe touches, trigger first.
func (r *Recipe) Endpoints() []string {
	seen := map[string]bool{}
	var out []string
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	add(r.Spec.Trigger.Source)
	for _, s := range r.Spec.Steps {
		if s.Write != nil {
			add(s.Write.Target)
		}
	}
	return out
}

func (r *Recipe) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(r.TypeMeta, KindRecipe, &es)
	validateMeta(r.Metadata, false, &es)
	if r.Spec.Trigger.Source == "" {
		es.add("spec.trigger.source", "is required")
	}
	if r.Spec.Trigger.Event == "" {
		es.add("spec.trigger.event", "is required")
	}
	if len(r.Spec.Steps) == 0 {
		es.add("spec.steps", "at least one step is required")
	}
	for i, s := range r.Spec.Steps {
		path := fmt.Sprintf("spec.steps[%d]", i)
		switch s.Kind() {
		case "map":
			if s.Map.From == "" || s.Map.To == "" {
				es.add(path+".map", "from and to are required")
			}
			if s.Map.Mapping == "" {
				es.add(path+".map.mapping", "is required")
			}
		case "resolve":
			if s.Resolve.Entity == "" {
				es.add(path+".resolve.entity", "is required")
			}
			if s.Resolve.AutoMatchAbove <= 0 || s.Resolve.AutoMatchAbove > 1 {
				es.add(path+".resolve.autoMatchAbove", "must be in (0, 1], got %v", s.Resolve.AutoMatchAbove)
			}
			for j, m := range s.Resolve.Match {
				if !fieldNameRE.MatchString(m.Field) {
					es.add(fmt.Sprintf("%s.resolve.match[%d].field", path, j), "must be a document field name, got %q", m.Field)
				}
				if m.Kind != MatchEmail && m.Kind != MatchDomain && m.Kind != MatchName && m.Kind != MatchExact {
					es.add(fmt.Sprintf("%s.resolve.match[%d].kind", path, j), "must be email, domain, name or exact, got %q", m.Kind)
				}
			}
			if s.Resolve.Strategy == StrategyProbabilistic && len(s.Resolve.Match) == 0 {
				es.add(path+".resolve.match", "the probabilistic strategy needs the fields to match on")
			}
		case "write":
			w := s.Write
			if w.Target == "" {
				es.add(path+".write.target", "is required")
			}
			if w.Operation == "" {
				es.add(path+".write.operation", "is required")
			}
			if strings.TrimSpace(w.IdempotencyKey) == "" {
				es.add(path+".write.idempotencyKey", "is required so retries never create duplicates")
			}
			if w.Output != "" && !fieldRE.MatchString(w.Output) {
				es.add(path+".write.output", "must be a field name, got %q", w.Output)
			}
			if w.Approval != "" && !oneOf(w.Approval, ApprovalPolicy, ApprovalRequired, ApprovalNone) {
				es.add(path+".write.approval", "must be policy, required or none, got %q", w.Approval)
			}
		default:
			es.add(path, "exactly one of map, resolve or write must be set")
		}
	}
	if r.Spec.Capacity != nil && r.Spec.Capacity.PeakPerSecond <= 0 {
		es.add("spec.capacity.peakPerSecond", "must be positive")
	}
	return es
}

// ParseRef splits "name@constraint" into its parts. The constraint may be empty.
func ParseRef(ref string) (name, constraint string) {
	if i := strings.LastIndex(ref, "@"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return ref, ""
}
