// Package v1alpha1 defines Turgon's declarative object model: connector
// manifests, recipes, mappings, slot contracts, plugins, stack blueprints and
// policy packs. Every object is data, versioned in Git and verified by the
// integration compiler before anything runs (architecture §2, "Declarative
// everything").
package v1alpha1

import (
	"fmt"
	"regexp"
	"strings"
)

// APIVersion is the only API version understood by this package.
const APIVersion = "turgon.dev/v1alpha1"

// Object kinds.
const (
	KindConnectorManifest = "ConnectorManifest"
	KindConnection        = "Connection"
	KindRecipe            = "Recipe"
	KindMapping           = "Mapping"
	KindSlotContract      = "SlotContract"
	KindPlugin            = "Plugin"
	KindStackBlueprint    = "StackBlueprint"
	KindPolicyPack        = "PolicyPack"
)

// Kinds lists every kind in load order: things other objects depend on first.
var Kinds = []string{
	KindConnectorManifest,
	KindConnection,
	KindSlotContract,
	KindPolicyPack,
	KindMapping,
	KindPlugin,
	KindRecipe,
	KindStackBlueprint,
}

// TypeMeta identifies the schema of an object.
type TypeMeta struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

// ObjectMeta is common metadata carried by every object.
type ObjectMeta struct {
	Name          string            `json:"name"`
	Version       string            `json:"version,omitempty"`
	Publisher     string            `json:"publisher,omitempty"`
	Certification string            `json:"certification,omitempty"`
	Description   string            `json:"description,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// Object is implemented by every kind in this package.
type Object interface {
	GetTypeMeta() TypeMeta
	GetMeta() ObjectMeta
	Validate() FieldErrors
}

// FieldError is a single schema violation, addressed by a JSON-path-like path.
type FieldError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

func (e FieldError) Error() string { return e.Path + ": " + e.Message }

// FieldErrors collects schema violations for one object.
type FieldErrors []FieldError

func (es *FieldErrors) add(path, format string, args ...any) {
	*es = append(*es, FieldError{Path: path, Message: fmt.Sprintf(format, args...)})
}

func (es FieldErrors) Error() string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "; ")
}

// Certification levels of the one-click contract (architecture §3).
const (
	LevelL0 = "L0" // certified
	LevelL1 = "L1" // assisted
	LevelL2 = "L2" // guided
	LevelL3 = "L3" // engineered
)

// Risk tiers carried by every operation and agent tool (architecture §7.7).
const (
	RiskRead = "read"
	RiskLow  = "low"
	RiskHigh = "high"
)

var (
	nameRE      = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	semverRE    = regexp.MustCompile(`^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?$`)
	secretRefRE = regexp.MustCompile(`^[a-z][a-z0-9+.-]*://\S+$`)
	handleRE    = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	fieldRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func validateTypeMeta(t TypeMeta, kind string, es *FieldErrors) {
	if t.APIVersion != APIVersion {
		es.add("apiVersion", "must be %q, got %q", APIVersion, t.APIVersion)
	}
	if t.Kind != kind {
		es.add("kind", "must be %q, got %q", kind, t.Kind)
	}
}

func validateMeta(m ObjectMeta, versionRequired bool, es *FieldErrors) {
	if !nameRE.MatchString(m.Name) {
		es.add("metadata.name", "must be lowercase alphanumerics and dashes, got %q", m.Name)
	}
	switch {
	case m.Version == "" && versionRequired:
		es.add("metadata.version", "is required")
	case m.Version != "" && !semverRE.MatchString(m.Version):
		es.add("metadata.version", "must be a semantic version, got %q", m.Version)
	}
	switch m.Certification {
	case "", LevelL0, LevelL1, LevelL2, LevelL3:
	default:
		es.add("metadata.certification", "must be one of L0, L1, L2, L3, got %q", m.Certification)
	}
}

func validRisk(r string) bool { return r == RiskRead || r == RiskLow || r == RiskHigh }

func oneOf(v string, allowed ...string) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}
