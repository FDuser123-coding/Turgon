package v1alpha1

import (
	"fmt"
	"regexp"
	"strings"
)

// SlotContract states which entities, events and actions a tool must
// provide to fill a stack slot such as CRM or ERP (architecture §18.1).
// Recipes and extensions talk to slots, never to tools, so a tool can be
// replaced by rebinding the slot.
type SlotContract struct {
	TypeMeta
	Metadata ObjectMeta       `json:"metadata"`
	Spec     SlotContractSpec `json:"spec"`
}

type SlotContractSpec struct {
	Model    string           `json:"model,omitempty"`
	Entities []string         `json:"entities"`
	Events   []string         `json:"events,omitempty"`
	Actions  []ContractAction `json:"actions,omitempty"`
}

type ContractAction struct {
	Name   string `json:"name"`
	Entity string `json:"entity,omitempty"`
	Risk   string `json:"risk"`
}

func (c *SlotContract) GetTypeMeta() TypeMeta { return c.TypeMeta }
func (c *SlotContract) GetMeta() ObjectMeta   { return c.Metadata }

func (c *SlotContract) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(c.TypeMeta, KindSlotContract, &es)
	validateMeta(c.Metadata, true, &es)
	if len(c.Spec.Entities) == 0 {
		es.add("spec.entities", "at least one entity is required")
	}
	for i, a := range c.Spec.Actions {
		path := fmt.Sprintf("spec.actions[%d]", i)
		if !nameRE.MatchString(a.Name) {
			es.add(path+".name", "must be lowercase alphanumerics and dashes, got %q", a.Name)
		}
		if !validRisk(a.Risk) {
			es.add(path+".risk", "must be read, low or high, got %q", a.Risk)
		}
	}
	return es
}

// Plugin is a signed, installable unit that fills a slot or extends a stack
// (architecture §18.2–18.4). Its permissions double as the consent an
// administrator approves at install time.
type Plugin struct {
	TypeMeta
	Metadata ObjectMeta `json:"metadata"`
	Spec     PluginSpec `json:"spec"`
}

// Plugin types (architecture §18.2).
const (
	PluginConnector      = "connector"
	PluginLogic          = "logic"
	PluginRecipePack     = "recipe-pack"
	PluginModelExtension = "model-extension"
	PluginPolicyPack     = "policy-pack"
	PluginUIPanel        = "ui-panel"
	PluginAgent          = "agent"
	PluginApp            = "app"
)

// Plugin runtimes (isolation tiers, architecture §18.3).
const (
	PluginRuntimeWasm        = "wasm"
	PluginRuntimeContainer   = "container"
	PluginRuntimeExternal    = "external"
	PluginRuntimeDeclarative = "declarative"
	PluginRuntimeIframe      = "iframe"
)

type PluginSpec struct {
	Type    string `json:"type"`
	Runtime string `json:"runtime"`
	// World is the WIT world a Wasm component targets, e.g. turgon:stack/logic-plugin@0.1.0.
	World string `json:"world,omitempty"`
	// Implements lists slot contracts a connector plugin satisfies, e.g. ["erp@1"].
	Implements []string `json:"implements,omitempty"`
	// Connector names the ConnectorManifest a connector plugin packages.
	Connector   string            `json:"connector,omitempty"`
	Subscribes  []string          `json:"subscribes,omitempty"`
	Permissions PluginPermissions `json:"permissions,omitempty"`
	Limits      *PluginLimits     `json:"limits,omitempty"`
	UI          *PluginUI         `json:"ui,omitempty"`
}

type PluginPermissions struct {
	Entities *EntityPermissions  `json:"entities,omitempty"`
	Network  *NetworkPermissions `json:"network,omitempty"`
	// Secrets are handles into the host's secret store, never values.
	Secrets []string `json:"secrets,omitempty"`
}

type EntityPermissions struct {
	Read []string `json:"read,omitempty"`
	// Propose lists Entity or Entity.field targets the plugin may propose
	// changes to. Plugins never write directly; proposals pass the write guard.
	Propose []string `json:"propose,omitempty"`
}

type NetworkPermissions struct {
	Allow []string `json:"allow,omitempty"`
}

type PluginLimits struct {
	MemoryMB  int `json:"memoryMB"`
	TimeoutMs int `json:"timeoutMs"`
}

type PluginUI struct {
	Panel string `json:"panel"`
}

var hostRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+(:\d+)?$`)

func (p *Plugin) GetTypeMeta() TypeMeta { return p.TypeMeta }
func (p *Plugin) GetMeta() ObjectMeta   { return p.Metadata }

// allowedRuntimes maps each plugin type to the isolation tiers it may use.
var allowedRuntimes = map[string][]string{
	PluginConnector:      {PluginRuntimeContainer, PluginRuntimeWasm},
	PluginLogic:          {PluginRuntimeWasm},
	PluginRecipePack:     {PluginRuntimeDeclarative},
	PluginModelExtension: {PluginRuntimeDeclarative},
	PluginPolicyPack:     {PluginRuntimeDeclarative},
	PluginUIPanel:        {PluginRuntimeIframe},
	PluginAgent:          {PluginRuntimeWasm, PluginRuntimeContainer, PluginRuntimeExternal},
	PluginApp:            {PluginRuntimeContainer},
}

func (p *Plugin) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(p.TypeMeta, KindPlugin, &es)
	validateMeta(p.Metadata, true, &es)
	if p.Metadata.Publisher == "" {
		es.add("metadata.publisher", "is required")
	}
	s := p.Spec
	runtimes, ok := allowedRuntimes[s.Type]
	if !ok {
		es.add("spec.type", "unknown plugin type %q", s.Type)
	} else if !oneOf(s.Runtime, runtimes...) {
		es.add("spec.runtime", "a %s plugin must run in one of %v, got %q", s.Type, runtimes, s.Runtime)
	}
	if s.Runtime == PluginRuntimeWasm && s.World == "" {
		es.add("spec.world", "is required for wasm plugins")
	}
	if (s.Runtime == PluginRuntimeWasm || s.Runtime == PluginRuntimeContainer) &&
		(s.Limits == nil || s.Limits.MemoryMB <= 0 || s.Limits.TimeoutMs <= 0) {
		es.add("spec.limits", "memoryMB and timeoutMs must be set for sandboxed plugins")
	}
	if s.Type == PluginConnector {
		if len(s.Implements) == 0 {
			es.add("spec.implements", "a connector plugin must implement at least one slot contract")
		}
		if s.Connector == "" {
			es.add("spec.connector", "a connector plugin must name its connector manifest")
		}
	}
	if s.Permissions.Network != nil {
		for i, h := range s.Permissions.Network.Allow {
			if strings.Contains(h, "*") || !hostRE.MatchString(h) {
				es.add(fmt.Sprintf("spec.permissions.network.allow[%d]", i),
					"must be an explicit host name without wildcards, got %q", h)
			}
		}
	}
	for i, sec := range s.Permissions.Secrets {
		if !handleRE.MatchString(sec) {
			es.add(fmt.Sprintf("spec.permissions.secrets[%d]", i),
				"must be a secret handle (lowercase name), never a value")
		}
	}
	if s.UI != nil && !strings.HasPrefix(s.UI.Panel, "ui://") {
		es.add("spec.ui.panel", "must be a ui:// resource URI, got %q", s.UI.Panel)
	}
	return es
}

// StackBlueprint binds slots to plugins and adds extensions, recipes and
// policies. The compiler verifies and deploys it like a single recipe, which
// is how a whole stack becomes a one-click install (architecture §18.5).
type StackBlueprint struct {
	TypeMeta
	Metadata ObjectMeta    `json:"metadata"`
	Spec     BlueprintSpec `json:"spec"`
}

type BlueprintSpec struct {
	Model      string                 `json:"model,omitempty"`
	Slots      map[string]SlotBinding `json:"slots"`
	Extensions []string               `json:"extensions,omitempty"`
	Recipes    []string               `json:"recipes,omitempty"`
	Policies   []string               `json:"policies,omitempty"`
}

type SlotBinding struct {
	Plugin  string `json:"plugin"`
	Version string `json:"version,omitempty"`
}

func (b *StackBlueprint) GetTypeMeta() TypeMeta { return b.TypeMeta }
func (b *StackBlueprint) GetMeta() ObjectMeta   { return b.Metadata }

func (b *StackBlueprint) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(b.TypeMeta, KindStackBlueprint, &es)
	validateMeta(b.Metadata, false, &es)
	if len(b.Spec.Slots) == 0 {
		es.add("spec.slots", "at least one slot is required")
	}
	for name, s := range b.Spec.Slots {
		if !nameRE.MatchString(name) {
			es.add("spec.slots."+name, "slot names must be lowercase alphanumerics and dashes")
		}
		if s.Plugin == "" {
			es.add("spec.slots."+name+".plugin", "is required")
		}
	}
	return es
}

// PolicyPack carries Rego policies evaluated by OPA (architecture §9, Appendix C).
type PolicyPack struct {
	TypeMeta
	Metadata ObjectMeta     `json:"metadata"`
	Spec     PolicyPackSpec `json:"spec"`
}

type PolicyPackSpec struct {
	Rego string `json:"rego"`
}

func (p *PolicyPack) GetTypeMeta() TypeMeta { return p.TypeMeta }
func (p *PolicyPack) GetMeta() ObjectMeta   { return p.Metadata }

var regoPackageRE = regexp.MustCompile(`(?m)^\s*package\s+[a-zA-Z_][a-zA-Z0-9_.]*\s*$`)

func (p *PolicyPack) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(p.TypeMeta, KindPolicyPack, &es)
	validateMeta(p.Metadata, false, &es)
	if !regoPackageRE.MatchString(p.Spec.Rego) {
		es.add("spec.rego", "must be a Rego module with a package declaration")
	}
	return es
}
