package v1alpha1

import "fmt"

// Mapping is a versioned set of JSONata field expressions from a system
// object to a semantic entity (architecture §7.4). Each field carries the
// confidence it was accepted with and where it came from, so the verifier can
// route low-confidence fields to human review.
type Mapping struct {
	TypeMeta
	Metadata ObjectMeta  `json:"metadata"`
	Spec     MappingSpec `json:"spec"`
}

type MappingSpec struct {
	From   string         `json:"from"`
	To     string         `json:"to"`
	Fields []FieldMapping `json:"fields"`
}

// Field mapping origins.
const (
	OriginCertified = "certified"
	OriginAI        = "ai"
	OriginHuman     = "human"
)

type FieldMapping struct {
	Target     string  `json:"target"`
	Expression string  `json:"expression"`
	Origin     string  `json:"origin"`
	Confidence float64 `json:"confidence"`
	// Approved is set when a person accepted the field in the review queue.
	Approved bool `json:"approved,omitempty"`
	// Rationale is the proposer's explanation, kept for reviewers.
	Rationale string `json:"rationale,omitempty"`
}

func (m *Mapping) GetTypeMeta() TypeMeta { return m.TypeMeta }
func (m *Mapping) GetMeta() ObjectMeta   { return m.Metadata }

func (m *Mapping) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(m.TypeMeta, KindMapping, &es)
	validateMeta(m.Metadata, true, &es)
	if m.Spec.From == "" {
		es.add("spec.from", "is required")
	}
	if m.Spec.To == "" {
		es.add("spec.to", "is required")
	}
	if len(m.Spec.Fields) == 0 {
		es.add("spec.fields", "at least one field is required")
	}
	seen := map[string]bool{}
	for i, f := range m.Spec.Fields {
		path := fmt.Sprintf("spec.fields[%d]", i)
		if f.Target == "" {
			es.add(path+".target", "is required")
		} else if seen[f.Target] {
			es.add(path+".target", "duplicate target %q", f.Target)
		}
		seen[f.Target] = true
		if f.Expression == "" {
			es.add(path+".expression", "is required")
		}
		if !oneOf(f.Origin, OriginCertified, OriginAI, OriginHuman) {
			es.add(path+".origin", "must be certified, ai or human, got %q", f.Origin)
		}
		if f.Confidence < 0 || f.Confidence > 1 {
			es.add(path+".confidence", "must be in [0, 1], got %v", f.Confidence)
		}
	}
	return es
}
