package compiler

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/api/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

func compileNamed(t *testing.T, cat *catalog.Catalog, name string) *RuntimeSpec {
	t.Helper()
	obj, err := cat.Find(name)
	if err != nil {
		t.Fatal(err)
	}
	rt, rep, err := Compile(cat, obj, verifier.Options{})
	if err != nil {
		t.Fatalf("compile %s: %v (%+v)", name, err, rep.Errors())
	}
	return rt
}

func TestCompileIsReproducible(t *testing.T) {
	var first []byte
	for i := 0; i < 5; i++ {
		cat, err := catalog.Load("../../examples")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(compileNamed(t, cat, "eu-distributor-core"))
		if first == nil {
			first = b
		} else if string(b) != string(first) {
			t.Fatal("compiling the same inputs produced different output")
		}
	}
}

func TestDigestTracksInputs(t *testing.T) {
	cat, _ := catalog.Load("../../examples")
	before := compileNamed(t, cat, "salesforce-won-deals-to-sap-orders").Metadata.Digest

	m, _ := cat.Mapping("sf-opportunity-to-order@3")
	changed := *m
	changed.Metadata.Version = "3.0.1"
	changed.Spec.Fields = append([]v1alpha1.FieldMapping{}, m.Spec.Fields...)
	changed.Spec.Fields[0].Expression = "Id & '-SF'"
	if err := cat.Add(&changed); err != nil {
		t.Fatal(err)
	}
	after := compileNamed(t, cat, "salesforce-won-deals-to-sap-orders").Metadata.Digest
	if before == after {
		t.Fatal("digest did not change when a mapping changed")
	}
}

func TestRecipeRuntimeSpec(t *testing.T) {
	cat, _ := catalog.Load("../../examples")
	rt := compileNamed(t, cat, "salesforce-won-deals-to-sap-orders")
	if rt.Metadata.Level != "L0" || len(rt.Spec.Workflows) != 1 {
		t.Fatalf("metadata %+v, %d workflows", rt.Metadata, len(rt.Spec.Workflows))
	}
	w := rt.Spec.Workflows[0].Steps[2].Write
	if w == nil || w.Simulation != "testrun" || w.Compensation != "cancel-sales-order" || !w.Metered || w.Risk != "high" {
		t.Fatalf("write step = %+v", w)
	}
	var sap *ConnectorConfig
	for i := range rt.Spec.Connectors {
		if rt.Spec.Connectors[i].Name == "sap-ecc" {
			sap = &rt.Spec.Connectors[i]
		}
	}
	if sap == nil || sap.SecretRef != "openbao://sap/ecc/prod" || len(sap.Prohibited) != 2 {
		t.Fatalf("sap connector = %+v", sap)
	}
	if len(rt.Spec.Tools) != 1 || rt.Spec.Tools[0].Name != "create_sales_order" {
		t.Fatalf("tools = %+v", rt.Spec.Tools)
	}
}

func TestBlueprintToolsFollowSlots(t *testing.T) {
	cat, _ := catalog.Load("../../examples")
	rt := compileNamed(t, cat, "eu-distributor-core")
	tools := map[string]Tool{}
	for _, tl := range rt.Spec.Tools {
		tools[tl.Name] = tl
	}
	if tl, ok := tools["create_sales_order"]; !ok || tl.Endpoint != "erp" || tl.Risk != "high" {
		t.Errorf("create_sales_order = %+v", tl)
	}
	// get-customer is offered by both crm and erp, so both are qualified.
	for _, name := range []string{"crm_get_customer", "erp_get_customer"} {
		if _, ok := tools[name]; !ok {
			t.Errorf("missing tool %s in %v", name, tools)
		}
	}
	// A connector named directly by a recipe shares its slot's deployment.
	seen := map[string]bool{}
	for _, c := range rt.Spec.Connectors {
		if seen[c.Name] {
			t.Errorf("connector %s deployed twice", c.Name)
		}
		seen[c.Name] = true
	}
	if len(rt.Spec.Connectors) != 5 || len(rt.Spec.Plugins) != 6 {
		t.Errorf("%d connectors, %d plugins", len(rt.Spec.Connectors), len(rt.Spec.Plugins))
	}
}

func TestCompileRefusesBlockedRecipe(t *testing.T) {
	cat, _ := catalog.Load("../../examples")
	obj, _ := cat.Find("shopify-orders-to-sap") // slot-based; not deployable alone
	rt, rep, err := Compile(cat, obj, verifier.Options{})
	if !errors.Is(err, ErrNotDeployable) || rt != nil || rep == nil {
		t.Fatalf("rt=%v err=%v", rt, err)
	}
}

func TestConnectionConfigIsCompiledIn(t *testing.T) {
	cat, _ := catalog.Load("../../examples")
	rt := compileNamed(t, cat, "shop-orders-to-erp")
	byEndpoint := map[string]ConnectorConfig{}
	for _, c := range rt.Spec.Connectors {
		byEndpoint[c.Endpoint] = c
	}
	erp := byEndpoint["erp-db"]
	if erp.Name != "postgres" || erp.Connection != "erp-db" || erp.SecretRef != "openbao://erp-db/dsn" ||
		!strings.Contains(string(erp.Config), "erp.sales_orders") {
		t.Fatalf("erp-db = %+v", erp)
	}
	if w := rt.Spec.Workflows[0].Steps[2].Write; w.Simulation != "rollback" || w.Entity != "SalesOrder" {
		t.Fatalf("write = %+v", w)
	}
}

func TestPoliciesAreCompiledIn(t *testing.T) {
	cat, _ := catalog.Load("../../examples")
	rt := compileNamed(t, cat, "shop-orders-to-erp")
	if len(rt.Spec.Policies) != 1 || !strings.Contains(rt.Spec.Policies[0].Rego, "package porter.writeback") {
		t.Fatalf("policies = %+v", rt.Spec.Policies)
	}
	if got := rt.Spec.Workflows[0].Policies; len(got) != 1 || got[0] != "writeback-default" {
		t.Fatalf("workflow policies = %v", got)
	}
}
