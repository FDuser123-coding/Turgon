package console

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIntegrationsEndpoint(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}, Catalogs: []string{"../../examples"}})
	var m Integrations
	if err := json.Unmarshal(do(t, s, "GET", "/api/integrations", "", nil).Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if m.Error != "" {
		t.Fatal(m.Error)
	}
	flows := map[string]Flow{}
	for _, f := range m.Flows {
		flows[f.Name] = f
	}
	systems := map[string]System{}
	for _, sys := range m.Systems {
		systems[sys.Name] = sys
	}

	// A flow reads as: the event that starts it, then its steps as the
	// compiled spec runs them.
	sf := flows["salesforce-won-deals-to-erp"]
	if !sf.Deployable || sf.Level != "L1" || sf.Trigger.System != "salesforce-prod" || sf.Trigger.Event != "Opportunity.ClosedWon" ||
		sf.Trigger.Delivery != "polling" || !strings.Contains(sf.Description, "Salesforce") || sf.SLO != "p95 5s" {
		t.Fatalf("salesforce flow: %+v", sf)
	}
	var kinds []string
	for _, st := range sf.Steps {
		kinds = append(kinds, st.Kind)
	}
	if strings.Join(kinds, ",") != "map,resolve,write,map,write" || strings.Join(sf.Targets, ",") != "erp-db,salesforce-prod" {
		t.Fatalf("steps %v, targets %v", kinds, sf.Targets)
	}
	w := sf.Steps[2]
	if w.System != "erp-db" || w.Operation != "create-sales-order" || w.Risk != "high" || w.Simulation != "rollback" ||
		w.Compensation != "cancel-sales-order" || sf.Steps[0].Fields != 6 {
		t.Fatalf("write step: %+v", w)
	}

	// How each event arrives.
	for flow, want := range map[string]string{"stripe-payments-to-erp": "webhook", "shop-orders-to-erp": "outbox",
		"shop-order-rows-to-erp": "change capture", "salesforce-won-deals-to-erp-cdc": "subscription"} {
		if got := flows[flow].Trigger.Delivery; got != want {
			t.Errorf("%s arrives by %q, want %q", flow, got, want)
		}
	}

	// Systems, with their role and what they emit and receive.
	shop := systems["shop-db"]
	if shop.Role != "source" || shop.Connector != "postgres" || shop.Product != "PostgreSQL 13-17" || len(shop.Events) != 2 || shop.Description == "" {
		t.Fatalf("shop-db: %+v", shop)
	}
	erp := systems["erp-db"]
	if erp.Role != "target" || strings.Join(erp.Operations, ",") != "create-sales-order,record-payment" || len(erp.Flows) < 5 {
		t.Fatalf("erp-db: %+v", erp)
	}
	if systems["salesforce-prod"].Role != "both" {
		t.Fatalf("salesforce-prod: %+v", systems["salesforce-prod"])
	}

	// A recipe for a stack slot cannot run on its own; its slots are not
	// systems.
	if f := flows["shopify-orders-to-sap"]; f.Deployable || !strings.Contains(f.Problem, "slot") {
		t.Fatalf("slot recipe: %+v", f)
	}
	if _, ok := systems["commerce"]; ok {
		t.Fatal("a stack slot is listed as a system")
	}
}

func TestIntegrationsWithoutCatalog(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}})
	var m Integrations
	_ = json.Unmarshal(do(t, s, "GET", "/api/integrations", "", nil).Body.Bytes(), &m)
	if m.Systems == nil || m.Flows == nil || len(m.Flows) != 0 {
		t.Fatalf("%+v", m)
	}
}
