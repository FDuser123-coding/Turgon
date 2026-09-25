package v1alpha1

import (
	"strings"
	"testing"
)

func hasError(es FieldErrors, path, substr string) bool {
	for _, e := range es {
		if e.Path == path && strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}

func validConnector() *ConnectorManifest {
	return &ConnectorManifest{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindConnectorManifest},
		Metadata: ObjectMeta{Name: "erp", Version: "1.0.0"},
		Spec: ConnectorSpec{
			System:  "ERP",
			Runtime: RuntimeCamelJava,
			Auth:    ConnectorAuth{Methods: []string{"basic"}, SecretRef: "openbao://erp"},
			Interfaces: Interfaces{
				Read:       []Interface{{Kind: "api"}},
				Write:      []Interface{{Kind: "api"}},
				Prohibited: []ProhibitedInterface{{Kind: "db", Reason: "license"}},
			},
			Operations: []Operation{
				{Name: "create", Direction: DirectionWrite, Interface: "api", Risk: RiskHigh, Compensation: "cancel"},
				{Name: "cancel", Direction: DirectionWrite, Interface: "api", Risk: RiskHigh},
			},
			Limits: Limits{MaxConcurrentCalls: 1, RequestsPerSecond: 1},
		},
	}
}

func TestConnectorValid(t *testing.T) {
	if es := validConnector().Validate(); len(es) > 0 {
		t.Fatal(es)
	}
}

func TestConnectorRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ConnectorManifest)
		path   string
		substr string
	}{
		{"secret value instead of reference", func(c *ConnectorManifest) { c.Spec.Auth.SecretRef = "hunter2" }, "spec.auth.secretRef", "reference"},
		{"permitted and prohibited", func(c *ConnectorManifest) {
			c.Spec.Interfaces.Write = append(c.Spec.Interfaces.Write, Interface{Kind: "db"})
		}, "spec.interfaces.write[1].kind", "both permitted and prohibited"},
		{"operation on prohibited interface", func(c *ConnectorManifest) { c.Spec.Operations[0].Interface = "db" }, "spec.operations[0].interface", "prohibited"},
		{"read-risk write", func(c *ConnectorManifest) { c.Spec.Operations[0].Risk = RiskRead }, "spec.operations[0].risk", "low or high"},
		{"unknown compensation", func(c *ConnectorManifest) { c.Spec.Operations[0].Compensation = "undo" }, "spec.operations[0].compensation", "unknown operation"},
		{"prohibited without reason", func(c *ConnectorManifest) { c.Spec.Interfaces.Prohibited[0].Reason = "" }, "spec.interfaces.prohibited[0].reason", "required"},
		{"zero limits", func(c *ConnectorManifest) { c.Spec.Limits.RequestsPerSecond = 0 }, "spec.limits.requestsPerSecond", "positive"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := validConnector()
			c.mutate(m)
			if es := m.Validate(); !hasError(es, c.path, c.substr) {
				t.Fatalf("want %s: ...%s..., got %v", c.path, c.substr, es)
			}
		})
	}
}

func TestPluginPermissions(t *testing.T) {
	p := &Plugin{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindPlugin},
		Metadata: ObjectMeta{Name: "x", Version: "1.0.0", Publisher: "acme"},
		Spec: PluginSpec{
			Type: PluginLogic, Runtime: PluginRuntimeWasm, World: "turgon:stack/logic-plugin@0.1.0",
			Permissions: PluginPermissions{
				Network: &NetworkPermissions{Allow: []string{"*.example.com", "api.example.com"}},
				Secrets: []string{"sk_live_abc123=="},
			},
		},
	}
	es := p.Validate()
	for _, want := range []struct{ path, substr string }{
		{"spec.permissions.network.allow[0]", "without wildcards"},
		{"spec.permissions.secrets[0]", "never a value"},
		{"spec.limits", "memoryMB"},
	} {
		if !hasError(es, want.path, want.substr) {
			t.Errorf("missing %s error in %v", want.path, es)
		}
	}
	if hasError(es, "spec.permissions.network.allow[1]", "") {
		t.Error("explicit host rejected")
	}

	p.Spec.Runtime = PluginRuntimeContainer
	if !hasError(p.Validate(), "spec.runtime", "must run in") {
		t.Error("logic plugin allowed outside the wasm sandbox")
	}
}

func TestRecipeStepUnion(t *testing.T) {
	r := &Recipe{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindRecipe},
		Metadata: ObjectMeta{Name: "r"},
		Spec: RecipeSpec{
			Trigger: Trigger{Source: "a", Event: "E"},
			Steps: []Step{
				{},
				{Write: &WriteStep{Target: "b", Operation: "op"}},
			},
		},
	}
	es := r.Validate()
	if !hasError(es, "spec.steps[0]", "exactly one") {
		t.Errorf("empty step accepted: %v", es)
	}
	if !hasError(es, "spec.steps[1].write.idempotencyKey", "required") {
		t.Errorf("write without idempotency key accepted: %v", es)
	}
}
