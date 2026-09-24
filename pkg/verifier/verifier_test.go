package verifier

import (
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
)

func load(t *testing.T) *catalog.Catalog {
	t.Helper()
	cat, err := catalog.Load("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func recipe(t *testing.T, cat *catalog.Catalog, name string) *v1alpha1.Recipe {
	t.Helper()
	r, err := cat.Recipe(name)
	if err != nil {
		t.Fatal(err)
	}
	cp := *r
	cp.Spec.Steps = nil
	for _, s := range r.Spec.Steps {
		// Deep-copy step payloads so tests can mutate them.
		switch {
		case s.Map != nil:
			m := *s.Map
			cp.Spec.Steps = append(cp.Spec.Steps, v1alpha1.Step{Map: &m})
		case s.Resolve != nil:
			rs := *s.Resolve
			cp.Spec.Steps = append(cp.Spec.Steps, v1alpha1.Step{Resolve: &rs})
		case s.Write != nil:
			w := *s.Write
			cp.Spec.Steps = append(cp.Spec.Steps, v1alpha1.Step{Write: &w})
		}
	}
	return &cp
}

func findings(r *Report, sev Severity, substr string) []Finding {
	var out []Finding
	for _, f := range r.Findings {
		if f.Severity == sev && strings.Contains(f.Message, substr) {
			out = append(out, f)
		}
	}
	for _, c := range r.Children {
		out = append(out, findings(c, sev, substr)...)
	}
	return out
}

func TestCertifiedRecipeIsL0(t *testing.T) {
	cat := load(t)
	rep := New(cat, Options{}).Recipe(recipe(t, cat, "salesforce-won-deals-to-sap-orders"))
	if !rep.Deployable || rep.Level != "L0" {
		t.Fatalf("got %s deployable=%v, findings %+v", rep.Level, rep.Deployable, rep.Findings)
	}
	if got := rep.Resolution.Connectors["sap-ecc"].Metadata.Version; got != "0.4.0" {
		t.Errorf("sap-ecc resolved to %s", got)
	}
}

func TestUncertifiedRecipeIsAtBestL1(t *testing.T) {
	cat := load(t)
	r := recipe(t, cat, "salesforce-won-deals-to-sap-orders")
	r.Metadata.Certification = ""
	if rep := New(cat, Options{}).Recipe(r); rep.Level != "L1" || !rep.Deployable {
		t.Fatalf("got %s deployable=%v", rep.Level, rep.Deployable)
	}
}

func TestProhibitedInterfaceBlocks(t *testing.T) {
	cat := load(t)
	sap, _ := cat.Connector("sap-ecc", "")
	evil := *sap
	evil.Metadata.Version = "0.4.1"
	evil.Spec.Operations = append([]v1alpha1.Operation{}, sap.Spec.Operations...)
	// A connector that quietly routes order creation through ODP-RFC.
	for i := range evil.Spec.Operations {
		if evil.Spec.Operations[i].Name == "create-sales-order" {
			evil.Spec.Operations[i].Interface = "odp-rfc"
		}
	}
	if err := cat.Add(&evil); err != nil {
		t.Fatal(err)
	}
	rep := New(cat, Options{}).Recipe(recipe(t, cat, "salesforce-won-deals-to-sap-orders"))
	if rep.Deployable || rep.Level != "L3" {
		t.Fatalf("got %s deployable=%v", rep.Level, rep.Deployable)
	}
	if len(findings(rep, SeverityError, "SAP Note 3255746")) == 0 {
		t.Fatalf("refusal does not explain why: %+v", rep.Findings)
	}
}

func TestMissingConnectorIsL3(t *testing.T) {
	cat := load(t)
	r := recipe(t, cat, "salesforce-won-deals-to-sap-orders")
	r.Spec.Steps[2].Write.Target = "ibm-i"
	rep := New(cat, Options{}).Recipe(r)
	if rep.Level != "L3" || rep.Deployable {
		t.Fatalf("got %s deployable=%v", rep.Level, rep.Deployable)
	}
}

func TestMissingMappingIsL2(t *testing.T) {
	cat := load(t)
	r := recipe(t, cat, "salesforce-won-deals-to-sap-orders")
	r.Spec.Steps[0].Map.Mapping = "sf-opportunity-to-order@9"
	rep := New(cat, Options{}).Recipe(r)
	if rep.Level != "L2" || rep.Deployable {
		t.Fatalf("got %s deployable=%v", rep.Level, rep.Deployable)
	}
}

func TestLowConfidenceFieldsGoToReview(t *testing.T) {
	cat := load(t)
	m, _ := cat.Mapping("sf-opportunity-to-order@3")
	ai := *m
	ai.Metadata.Version = "3.1.0"
	ai.Spec.Fields = append(append([]v1alpha1.FieldMapping{}, m.Spec.Fields...),
		v1alpha1.FieldMapping{Target: "salesOrg", Expression: "Owner.Region__c", Origin: v1alpha1.OriginAI, Confidence: 0.93},
		v1alpha1.FieldMapping{Target: "incoterms", Expression: "Incoterms__c", Origin: v1alpha1.OriginAI, Confidence: 0.99},
	)
	if err := cat.Add(&ai); err != nil {
		t.Fatal(err)
	}
	r := recipe(t, cat, "salesforce-won-deals-to-sap-orders")
	r.Metadata.Certification = ""
	rep := New(cat, Options{}).Recipe(r)
	// 0.93 is above the general threshold but below the write threshold.
	if rep.Deployable || rep.Level != "L1" || len(rep.ReviewQueue) != 1 || rep.ReviewQueue[0].Target != "salesOrg" {
		t.Fatalf("got %s deployable=%v queue=%+v", rep.Level, rep.Deployable, rep.ReviewQueue)
	}

	// Once a person approves the field, the recipe deploys.
	ai.Spec.Fields[len(ai.Spec.Fields)-2].Approved = true
	if rep := New(cat, Options{}).Recipe(r); !rep.Deployable {
		t.Fatalf("approved field still blocks: %+v", rep.ReviewQueue)
	}
}

func TestPolicyAndCapacityChecks(t *testing.T) {
	cat := load(t)
	r := recipe(t, cat, "salesforce-won-deals-to-sap-orders")
	r.Spec.Steps[2].Write.Approval = v1alpha1.ApprovalNone
	r.Spec.Capacity.PeakPerSecond = 50
	r.Spec.Policies = append(r.Spec.Policies, "no-such-policy")
	rep := New(cat, Options{}).Recipe(r)
	for _, substr := range []string{"cannot disable approval", "exceeds sap-ecc's declared limit", "no-such-policy"} {
		if len(findings(rep, SeverityError, substr)) == 0 {
			t.Errorf("no error containing %q: %+v", substr, rep.Findings)
		}
	}
}

func TestEntityMismatchBetweenMapAndWrite(t *testing.T) {
	cat := load(t)
	r := recipe(t, cat, "salesforce-won-deals-to-sap-orders")
	r.Spec.Steps[2].Write.Operation = "cancel-sales-order"
	r.Spec.Steps[2].Write.Compensation = "create-sales-order"
	if rep := New(cat, Options{}).Recipe(r); !rep.Deployable {
		t.Fatalf("same-entity operation rejected: %+v", rep.Findings)
	}
	r.Spec.Steps[0].Map.To = "model.Invoice"
	rep := New(cat, Options{}).Recipe(r)
	if len(findings(rep, SeverityError, "expects SalesOrder but the preceding map step produces Invoice")) == 0 {
		t.Fatalf("entity mismatch not caught: %+v", rep.Findings)
	}
}

func TestSlotRecipeStandaloneExplainsSlots(t *testing.T) {
	cat := load(t)
	rep := New(cat, Options{}).Recipe(recipe(t, cat, "shopify-orders-to-sap"))
	if rep.Deployable || len(findings(rep, SeverityError, "is a stack slot")) != 2 {
		t.Fatalf("findings: %+v", rep.Findings)
	}
}

func blueprint(t *testing.T, cat *catalog.Catalog) *v1alpha1.StackBlueprint {
	t.Helper()
	o, err := cat.Find("eu-distributor-core")
	if err != nil {
		t.Fatal(err)
	}
	b := *o.(*v1alpha1.StackBlueprint)
	b.Spec.Slots = map[string]v1alpha1.SlotBinding{}
	for k, v := range o.(*v1alpha1.StackBlueprint).Spec.Slots {
		b.Spec.Slots[k] = v
	}
	return &b
}

func TestBlueprintVerifies(t *testing.T) {
	cat := load(t)
	rep := New(cat, Options{}).Blueprint(blueprint(t, cat))
	if !rep.Deployable {
		t.Fatalf("blueprint blocked: %+v", rep.Errors())
	}
	if rep.Level != "L1" { // shopify-orders-to-sap is not certified
		t.Errorf("level = %s, want L1", rep.Level)
	}
	if len(rep.Resolution.Slots) != 5 || len(rep.Resolution.Extensions) != 1 {
		t.Errorf("resolution = %+v", rep.Resolution)
	}
}

func TestSlotSwapIsReverifiedAgainstContract(t *testing.T) {
	cat := load(t)
	// A cheaper ERP that cannot create sales orders does not fit the erp slot.
	lite := &v1alpha1.ConnectorManifest{
		TypeMeta: v1alpha1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: v1alpha1.KindConnectorManifest},
		Metadata: v1alpha1.ObjectMeta{Name: "erp-lite", Version: "1.0.0"},
		Spec: v1alpha1.ConnectorSpec{
			System: "ERP Lite", Runtime: v1alpha1.RuntimeCamelJava,
			Auth:       v1alpha1.ConnectorAuth{Methods: []string{"api-key"}, SecretRef: "openbao://erp-lite"},
			Interfaces: v1alpha1.Interfaces{Read: []v1alpha1.Interface{{Kind: "rest-api"}}},
			Entities:   []string{"Customer", "SalesOrder"},
			Operations: []v1alpha1.Operation{{Name: "get-customer", Direction: "read", Interface: "rest-api", Risk: "read"}},
			Limits:     v1alpha1.Limits{MaxConcurrentCalls: 1, RequestsPerSecond: 5},
		},
	}
	plugin := &v1alpha1.Plugin{
		TypeMeta: v1alpha1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: v1alpha1.KindPlugin},
		Metadata: v1alpha1.ObjectMeta{Name: "erp-lite", Version: "1.0.0", Publisher: "lite"},
		Spec: v1alpha1.PluginSpec{
			Type: v1alpha1.PluginConnector, Runtime: v1alpha1.PluginRuntimeContainer,
			Connector: "erp-lite", Implements: []string{"erp@1"},
			Limits: &v1alpha1.PluginLimits{MemoryMB: 128, TimeoutMs: 1000},
		},
	}
	for _, o := range []v1alpha1.Object{lite, plugin} {
		if err := cat.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	b := blueprint(t, cat)
	b.Spec.Slots["erp"] = v1alpha1.SlotBinding{Plugin: "erp-lite"}
	rep := New(cat, Options{}).Blueprint(b)
	if rep.Deployable {
		t.Fatal("incompatible slot swap accepted")
	}
	if len(findings(rep, SeverityError, "missing action create-sales-order")) == 0 {
		t.Fatalf("errors: %+v", rep.Errors())
	}
}

func TestExtensionPermissionsCheckedAgainstSlots(t *testing.T) {
	cat := load(t)
	b := blueprint(t, cat)
	delete(b.Spec.Slots, "erp") // nothing provides SalesOrder any more
	b.Spec.Recipes = nil
	rep := New(cat, Options{}).Blueprint(b)
	for _, substr := range []string{"asks for entity SalesOrder", "subscribes to model.SalesOrder.created"} {
		if len(findings(rep, SeverityError, substr)) == 0 {
			t.Errorf("no error containing %q: %+v", substr, rep.Errors())
		}
	}
}

func TestConnectionsCannotWidenInterfaces(t *testing.T) {
	cat := load(t)
	rep := New(cat, Options{}).Recipe(recipe(t, cat, "shop-orders-to-erp"))
	if !rep.Deployable || rep.Resolution.Connections["erp-db"] == nil {
		t.Fatalf("baseline: %+v", rep.Errors())
	}
	if !rep.Resolution.Connectors["erp-db"].HasEntity("SalesOrder") {
		t.Fatal("connection entities not merged into the effective manifest")
	}

	conn, _ := cat.Connection("erp-db")
	sneaky := *conn
	sneaky.Metadata.Version = "0.0.1"
	sneaky.Spec.Operations = append([]v1alpha1.Operation{}, conn.Spec.Operations...)
	sneaky.Spec.Operations[0].Interface = "pg-copy" // not a write interface postgres declares
	if err := cat.Add(&sneaky); err != nil {
		t.Fatal(err)
	}
	rep = New(cat, Options{}).Recipe(recipe(t, cat, "shop-orders-to-erp"))
	if rep.Deployable || rep.Level != "L3" || len(findings(rep, SeverityError, `interface "pg-copy" is not declared for write`)) == 0 {
		t.Fatalf("got %s deployable=%v: %+v", rep.Level, rep.Deployable, rep.Findings)
	}
}

func TestInvalidJSONataIsRejected(t *testing.T) {
	cat := load(t)
	m, _ := cat.Mapping("shop-order-to-sales-order@1")
	bad := *m
	bad.Metadata.Version = "1.0.1"
	bad.Spec.Fields = append([]v1alpha1.FieldMapping{}, m.Spec.Fields...)
	bad.Spec.Fields[0].Expression = "$substring(order_number, "
	if err := cat.Add(&bad); err != nil {
		t.Fatal(err)
	}
	rep := New(cat, Options{}).Recipe(recipe(t, cat, "shop-orders-to-erp"))
	if rep.Deployable || len(findings(rep, SeverityError, "invalid JSONata expression")) == 0 {
		t.Fatalf("findings: %+v", rep.Findings)
	}
}

func TestPolicyPacksMustCompileAndPassTheirTests(t *testing.T) {
	cat := load(t)
	add := func(name, rego string) {
		t.Helper()
		err := cat.Add(&v1alpha1.PolicyPack{
			TypeMeta: v1alpha1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: v1alpha1.KindPolicyPack},
			Metadata: v1alpha1.ObjectMeta{Name: name, Version: "1.0.0"},
			Spec:     v1alpha1.PolicyPackSpec{Rego: rego},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	add("broken-syntax", "package turgon.writeback\nallow if {")
	add("wrong-test", "package turgon.writeback\nrequire_approval if input.action.amount > 10\ntest_small_amounts_skip_approval if { not require_approval with input as {\"action\": {\"amount\": 20}} }")

	r := recipe(t, cat, "shop-orders-to-erp")
	r.Spec.Policies = []string{"writeback-default", "broken-syntax"}
	if rep := New(cat, Options{}).Recipe(r); rep.Deployable || len(findings(rep, SeverityError, "do not compile")) == 0 {
		t.Fatalf("syntax error: %+v", rep.Findings)
	}
	r.Spec.Policies = []string{"writeback-default", "wrong-test"}
	rep := New(cat, Options{}).Recipe(r)
	if rep.Deployable || len(findings(rep, SeverityError, "test_small_amounts_skip_approval")) == 0 {
		t.Fatalf("failing policy test: %+v", rep.Findings)
	}
	r.Spec.Policies = []string{"mask-personal-data"}
	if rep := New(cat, Options{}).Recipe(r); len(findings(rep, SeverityWarning, "built-in write-back default")) == 0 {
		t.Fatalf("no writeback pack: %+v", rep.Findings)
	}
}
