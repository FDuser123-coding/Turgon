package v1alpha1

import "fmt"

// ConnectorManifest is the machine-readable description of a connector:
// what it can do, how fast, and which interfaces it must never use
// (architecture §7.1 and Appendix A). The verifier and runtime enforce it.
type ConnectorManifest struct {
	TypeMeta
	Metadata ObjectMeta    `json:"metadata"`
	Spec     ConnectorSpec `json:"spec"`
}

type ConnectorSpec struct {
	// System is the supported system and version range, e.g. "SAP ECC 6.0, EHP 6-8".
	System string `json:"system"`
	// Runtime is the worker type that executes the connector.
	Runtime    string         `json:"runtime"`
	Auth       ConnectorAuth  `json:"auth"`
	Discovery  []InterfaceRef `json:"discovery,omitempty"`
	Interfaces Interfaces     `json:"interfaces"`
	// Entities are the semantic entities this connector can read or write.
	Entities []string `json:"entities,omitempty"`
	// Events are business events the connector can emit as triggers.
	Events []ConnectorEvent `json:"events,omitempty"`
	// Operations are the named actions recipes and agent tools may invoke.
	Operations       []Operation       `json:"operations,omitempty"`
	Limits           Limits            `json:"limits"`
	Metering         Metering          `json:"metering,omitempty"`
	SemanticBindings []SemanticBinding `json:"semanticBindings,omitempty"`
}

// Connector runtimes (architecture §7.1 worker types, plus Wasm plugins from §18).
const (
	RuntimeCamelJava = "camel-java"
	RuntimeDebezium  = "debezium"
	RuntimeFilesEDI  = "files-edi"
	RuntimeScreen    = "screen"
	RuntimeWasm      = "wasm"
	RuntimeContainer = "container"
)

type ConnectorAuth struct {
	Methods []string `json:"methods"`
	// SecretRef points into the customer's secret store. It is a reference,
	// never a value: secrets never appear in specs (architecture §9).
	SecretRef string `json:"secretRef"`
}

type InterfaceRef struct {
	Kind string `json:"kind"`
}

type Interfaces struct {
	Read       []Interface           `json:"read,omitempty"`
	Write      []Interface           `json:"write,omitempty"`
	Events     []Interface           `json:"events,omitempty"`
	Prohibited []ProhibitedInterface `json:"prohibited,omitempty"`
}

type Interface struct {
	Kind string `json:"kind"`
	// Simulation names the dry-run mode a write interface supports, such as
	// "testrun" or "validate-only". Empty means writes cannot be simulated.
	Simulation string `json:"simulation,omitempty"`
}

type ProhibitedInterface struct {
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

type ConnectorEvent struct {
	Name      string `json:"name"`
	Entity    string `json:"entity,omitempty"`
	Interface string `json:"interface"`
}

// Operation directions.
const (
	DirectionRead  = "read"
	DirectionWrite = "write"
)

type Operation struct {
	Name      string `json:"name"`
	Entity    string `json:"entity,omitempty"`
	Direction string `json:"direction"`
	Interface string `json:"interface"`
	Risk      string `json:"risk"`
	// Compensation names the operation that undoes this one in a saga.
	Compensation string `json:"compensation,omitempty"`
}

type Limits struct {
	MaxConcurrentCalls int     `json:"maxConcurrentCalls"`
	RequestsPerSecond  float64 `json:"requestsPerSecond"`
}

type Metering struct {
	SAPDigitalAccess bool `json:"sapDigitalAccess,omitempty"`
}

type SemanticBinding struct {
	Template string `json:"template"`
}

func (c *ConnectorManifest) GetTypeMeta() TypeMeta { return c.TypeMeta }
func (c *ConnectorManifest) GetMeta() ObjectMeta   { return c.Metadata }

// Operation returns the named operation, if declared.
func (c *ConnectorManifest) Operation(name string) (Operation, bool) {
	for _, op := range c.Spec.Operations {
		if op.Name == name {
			return op, true
		}
	}
	return Operation{}, false
}

// Event returns the named event, if declared.
func (c *ConnectorManifest) Event(name string) (ConnectorEvent, bool) {
	for _, ev := range c.Spec.Events {
		if ev.Name == name {
			return ev, true
		}
	}
	return ConnectorEvent{}, false
}

// Permitted reports whether an interface kind is declared for the direction
// ("read", "write" or "events") and is not prohibited, with a reason if not.
func (c *ConnectorManifest) Permitted(direction, kind string) (Interface, error) {
	for _, p := range c.Spec.Interfaces.Prohibited {
		if p.Kind == kind {
			return Interface{}, fmt.Errorf("interface %q is prohibited: %s", kind, p.Reason)
		}
	}
	var list []Interface
	switch direction {
	case DirectionRead:
		list = c.Spec.Interfaces.Read
	case DirectionWrite:
		list = c.Spec.Interfaces.Write
	case "events":
		list = c.Spec.Interfaces.Events
	default:
		return Interface{}, fmt.Errorf("unknown direction %q", direction)
	}
	for _, i := range list {
		if i.Kind == kind {
			return i, nil
		}
	}
	return Interface{}, fmt.Errorf("interface %q is not declared for %s", kind, direction)
}

// HasEntity reports whether the connector declares the entity.
func (c *ConnectorManifest) HasEntity(name string) bool {
	for _, e := range c.Spec.Entities {
		if e == name {
			return true
		}
	}
	return false
}

func (c *ConnectorManifest) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(c.TypeMeta, KindConnectorManifest, &es)
	validateMeta(c.Metadata, true, &es)
	s := c.Spec
	if s.System == "" {
		es.add("spec.system", "is required")
	}
	if !oneOf(s.Runtime, RuntimeCamelJava, RuntimeDebezium, RuntimeFilesEDI, RuntimeScreen, RuntimeWasm, RuntimeContainer) {
		es.add("spec.runtime", "unknown runtime %q", s.Runtime)
	}
	if len(s.Auth.Methods) == 0 {
		es.add("spec.auth.methods", "at least one method is required")
	}
	if !secretRefRE.MatchString(s.Auth.SecretRef) {
		es.add("spec.auth.secretRef", "must be a secret-store reference such as openbao://path, got %q", s.Auth.SecretRef)
	}

	prohibited := map[string]bool{}
	for i, p := range s.Interfaces.Prohibited {
		if p.Kind == "" {
			es.add(fmt.Sprintf("spec.interfaces.prohibited[%d].kind", i), "is required")
		}
		if p.Reason == "" {
			es.add(fmt.Sprintf("spec.interfaces.prohibited[%d].reason", i), "is required so the verifier can explain refusals")
		}
		prohibited[p.Kind] = true
	}
	checkList := func(dir string, list []Interface) {
		for i, in := range list {
			path := fmt.Sprintf("spec.interfaces.%s[%d]", dir, i)
			if in.Kind == "" {
				es.add(path+".kind", "is required")
			}
			if prohibited[in.Kind] {
				es.add(path+".kind", "interface %q is both permitted and prohibited", in.Kind)
			}
			if in.Simulation != "" && dir != DirectionWrite {
				es.add(path+".simulation", "only write interfaces can declare a simulation mode")
			}
		}
	}
	checkList(DirectionRead, s.Interfaces.Read)
	checkList(DirectionWrite, s.Interfaces.Write)
	checkList("events", s.Interfaces.Events)

	for i, ev := range s.Events {
		path := fmt.Sprintf("spec.events[%d]", i)
		if ev.Name == "" {
			es.add(path+".name", "is required")
		}
		if _, err := c.Permitted("events", ev.Interface); err != nil {
			es.add(path+".interface", "%v", err)
		}
	}

	ops := map[string]Operation{}
	for i, op := range s.Operations {
		path := fmt.Sprintf("spec.operations[%d]", i)
		if !nameRE.MatchString(op.Name) {
			es.add(path+".name", "must be lowercase alphanumerics and dashes, got %q", op.Name)
		}
		if _, dup := ops[op.Name]; dup {
			es.add(path+".name", "duplicate operation %q", op.Name)
		}
		ops[op.Name] = op
		if !oneOf(op.Direction, DirectionRead, DirectionWrite) {
			es.add(path+".direction", "must be read or write, got %q", op.Direction)
			continue
		}
		if _, err := c.Permitted(op.Direction, op.Interface); err != nil {
			es.add(path+".interface", "%v", err)
		}
		switch {
		case !validRisk(op.Risk):
			es.add(path+".risk", "must be read, low or high, got %q", op.Risk)
		case op.Direction == DirectionWrite && op.Risk == RiskRead:
			es.add(path+".risk", "write operations must be low or high risk")
		case op.Direction == DirectionRead && op.Risk != RiskRead:
			es.add(path+".risk", "read operations must have risk \"read\"")
		}
	}
	for i, op := range s.Operations {
		if op.Compensation == "" {
			continue
		}
		path := fmt.Sprintf("spec.operations[%d].compensation", i)
		comp, ok := ops[op.Compensation]
		switch {
		case op.Direction != DirectionWrite:
			es.add(path, "only write operations can declare a compensation")
		case !ok:
			es.add(path, "unknown operation %q", op.Compensation)
		case comp.Direction != DirectionWrite:
			es.add(path, "compensation %q must be a write operation", op.Compensation)
		}
	}

	if s.Limits.MaxConcurrentCalls <= 0 {
		es.add("spec.limits.maxConcurrentCalls", "must be positive")
	}
	if s.Limits.RequestsPerSecond <= 0 {
		es.add("spec.limits.requestsPerSecond", "must be positive")
	}
	return es
}
