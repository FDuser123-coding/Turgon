package meta

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

func opportunity(fields ...Field) Catalog {
	return Catalog{Endpoint: "salesforce-prod", Connector: "salesforce", Objects: []Object{
		{Name: "Opportunity", Kind: "sobject", Fields: fields},
		{Name: "Account", Kind: "sobject", Fields: []Field{{Name: "Id", Type: "id", Key: true}}},
	}, Uses: []Use{{Object: "Opportunity", Field: "ERP_Order_Number__c", By: "operation update-opportunity"}}}
}

func TestDigestIgnoresOrderAndUses(t *testing.T) {
	a := opportunity(Field{Name: "Amount", Type: "currency"}, Field{Name: "Id", Type: "id"})
	b := opportunity(Field{Name: "Id", Type: "id"}, Field{Name: "Amount", Type: "currency"})
	b.Objects[0], b.Objects[1] = b.Objects[1], b.Objects[0]
	b.Uses = nil
	if a.Digest() != b.Digest() {
		t.Fatal("equal catalogs differ")
	}
	if a.Objects[0].Name != "Opportunity" {
		t.Fatal("Digest reordered its receiver")
	}
	c := opportunity(Field{Name: "Amount", Type: "double"}, Field{Name: "Id", Type: "id"})
	if a.Digest() == c.Digest() {
		t.Fatal("a type change kept the digest")
	}
}

func TestDiff(t *testing.T) {
	old := opportunity(
		Field{Name: "Id", Type: "id", Key: true},
		Field{Name: "Amount", Type: "currency"},
		Field{Name: "CloseDate", Type: "date"},
		Field{Name: "Name", Type: "string", Length: 120},
		Field{Name: "StageName", Type: "picklist", Required: true},
		Field{Name: "ERP_Order_Number__c", Type: "string", Length: 20},
	)
	new2 := opportunity(
		Field{Name: "Id", Type: "id", Key: true},
		Field{Name: "Amount", Type: "double"},
		Field{Name: "Name", Type: "string", Length: 80, Required: true},
		Field{Name: "StageName", Type: "picklist"},
		Field{Name: "ERP_Order_Number__c", Type: "string", Length: 20, ReadOnly: true},
		Field{Name: "Region__c", Type: "string"},
	)
	new2.Objects = []Object{new2.Objects[0], {Name: "Quote", Kind: "sobject"}}
	var got []string
	for _, c := range Diff(old, new2) {
		s := c.String()
		if c.Breaking {
			s += " !"
		}
		got = append(got, s)
	}
	want := []string{
		"object-removed Account !",
		"field-removed Opportunity.CloseDate (was date) !",
		"became-read-only Opportunity.ERP_Order_Number__c !",
		"became-required Opportunity.Name !",
		"length-shrunk Opportunity.Name (120 -> 80) !",
		"field-added Opportunity.Region__c (string)",
		"no-longer-required Opportunity.StageName",
		"type-changed Opportunity.Amount (currency -> double) !",
		"object-added Quote",
	}
	if strings.Join(sortStrings(got), "\n") != strings.Join(sortStrings(want), "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if len(Diff(old, old)) != 0 {
		t.Fatal("a catalog differs from itself")
	}
}

func sortStrings(s []string) []string {
	out := append([]string(nil), s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestUsageAndAnnotate(t *testing.T) {
	c := opportunity(Field{Name: "Id", Type: "id"}, Field{Name: "Amount", Type: "currency"}, Field{Name: "CloseDate", Type: "date"},
		Field{Name: "ERP_Order_Number__c", Type: "string"})
	spec := &compiler.RuntimeSpec{}
	spec.Spec.Workflows = []compiler.Workflow{{Steps: []compiler.WorkflowStep{
		{Map: &compiler.MapConfig{Mapping: "sf-opportunity-to-order@3", From: "salesforce.Opportunity", Fields: map[string]string{
			"netValue": "Amount", "orderDate": "$substring(CloseDate, 0, 10)", "externalId": "Id", "note": "'Amount'"}}},
		{Map: &compiler.MapConfig{Mapping: "other@1", From: "hubspot-crm.Deal", Fields: map[string]string{"x": "Amount"}}},
	}}}
	u := Usage(c, spec)
	if got := strings.Join(u["Opportunity.Amount"], "; "); got != "mapping sf-opportunity-to-order@3 (netValue); mapping sf-opportunity-to-order@3 (note)" {
		t.Fatalf("Amount used by %q", got)
	}
	if got := strings.Join(u["Opportunity.CloseDate"], "; "); got != "mapping sf-opportunity-to-order@3 (orderDate)" {
		t.Fatalf("CloseDate used by %q", got)
	}
	if got := strings.Join(u["Opportunity.ERP_Order_Number__c"], ""); got != "operation update-opportunity" {
		t.Fatalf("ERP field used by %q", got)
	}

	changes := Annotate([]Change{{Kind: FieldRemoved, Object: "Opportunity", Field: "CloseDate", Breaking: true},
		{Kind: ObjectRemoved, Object: "Opportunity", Breaking: true}}, u, nil)
	if len(changes[0].UsedBy) != 1 || len(changes[1].UsedBy) != 5 {
		t.Fatalf("annotated %+v", changes)
	}

	// A new required field breaks what creates records, though nothing reads it.
	c.Uses = append(c.Uses, Use{Object: "Opportunity", Field: "Amount", By: "operation create-opportunity", Creates: true},
		Use{Object: "Opportunity", Field: "Region__c", By: "operation tag-region", Creates: true})
	cr := Creators(c)
	changes = Annotate([]Change{{Kind: FieldAdded, Object: "Opportunity", Field: "Region__c", Breaking: true},
		{Kind: FieldAdded, Object: "Opportunity", Field: "Note__c"}}, Usage(c, nil), cr)
	if got := strings.Join(changes[0].UsedBy, "; "); got != "operation create-opportunity (creates); operation tag-region" {
		t.Fatalf("required field used by %q", got)
	}
	if changes[1].UsedBy != nil {
		t.Fatalf("optional field used by %v", changes[1].UsedBy)
	}
}

// A mapping reading an event reads the object the event's records come
// from, whatever the mapping calls the entity.
func TestUsageThroughTheTriggeringEvent(t *testing.T) {
	c := Catalog{Endpoint: "erp-db", Connector: "postgres", Events: map[string]string{"Customer.Changed": "erp.customers"},
		Objects: []Object{{Name: "erp.customers", Fields: []Field{{Name: "id"}, {Name: "name"}, {Name: "vat_id"}}}}}
	spec := &compiler.RuntimeSpec{}
	spec.Spec.Workflows = []compiler.Workflow{
		{Trigger: compiler.WorkflowSource{Endpoint: "erp-db", Event: "Customer.Changed"}, Steps: []compiler.WorkflowStep{
			{Map: &compiler.MapConfig{Mapping: "customer-to-account@1", From: "erp-db.Customer", Fields: map[string]string{"Name": "name", "VAT": "$trim(vat_id)"}}},
		}},
		// Another endpoint's event: its mapping does not read erp-db.
		{Trigger: compiler.WorkflowSource{Endpoint: "shop-db", Event: "Customer.Changed"}, Steps: []compiler.WorkflowStep{
			{Map: &compiler.MapConfig{Mapping: "other@1", From: "erp-db.Customer", Fields: map[string]string{"x": "id"}}},
		}},
	}
	u := Usage(c, spec, nil)
	if strings.Join(u["erp.customers.name"], ";") != "mapping customer-to-account@1 (Name)" ||
		strings.Join(u["erp.customers.vat_id"], ";") != "mapping customer-to-account@1 (VAT)" || u["erp.customers.id"] != nil {
		t.Fatalf("usage %v", u)
	}
	missing := MissingUses(Catalog{Uses: []Use{{Object: "erp.orders", Field: "x", By: "operation b"}, {Object: "erp.orders", Field: "y", By: "operation a"},
		{Object: "erp.customers", Field: "nope", By: "operation a"}}, Objects: c.Objects})
	if len(missing) != 2 || missing[0].String() != "erp.orders; used by operation a, operation b" || missing[1].String() != "erp.customers.nope; used by operation a" {
		t.Fatalf("missing %v", missing)
	}
}

// A field the new catalog lacks is still known to be read by the mappings
// that read it in the old one: its removal breaks them.
func TestCompareKnowsWhatUsedARemovedField(t *testing.T) {
	old := Catalog{Endpoint: "shop-db", Connector: "postgres", Events: map[string]string{"Order.Placed": "shop.orders"},
		Objects: []Object{{Name: "shop.orders", Fields: []Field{{Name: "order_number"}, {Name: "total", Type: "numeric"}}}}}
	new := old
	new.Objects = []Object{{Name: "shop.orders", Fields: []Field{{Name: "order_number"}, {Name: "total_amount", Type: "numeric"}}}}
	spec := &compiler.RuntimeSpec{}
	spec.Spec.Workflows = []compiler.Workflow{{Trigger: compiler.WorkflowSource{Endpoint: "shop-db", Event: "Order.Placed"}, Steps: []compiler.WorkflowStep{
		{Map: &compiler.MapConfig{Mapping: "shop-order-to-sales-order@1.0.0", From: "shop-db.Order", Fields: map[string]string{"netValue": "$number(total)"}}}}}}
	changes := Compare(old, new, spec)
	if len(changes) != 2 || changes[0].Kind != FieldRemoved || changes[0].Field != "total" || !changes[0].Breaks() ||
		strings.Join(changes[0].UsedBy, ";") != "mapping shop-order-to-sales-order@1.0.0 (netValue)" || changes[1].Breaks() {
		t.Fatalf("changes %+v", changes)
	}
}

func TestSampledObjects(t *testing.T) {
	fields := SampleFields([]map[string]any{
		{"id": json.Number("7"), "total": "12.50", "customer": map[string]any{"email": "a@b"}, "note": nil, "created_at": "2026-10-01T09:00:00Z"},
		{"id": 8.0, "total": 12.5, "tags": []any{"x"}, "note": nil, "created_at": "2026-10-01T09:05:00Z"},
	})
	got := map[string]string{}
	for _, f := range fields {
		got[f.Name] = f.Type
	}
	want := map[string]string{"id": "number", "total": "number|string", "customer": "object", "note": "", "created_at": "datetime", "tags": "array"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("fields %v", got)
	}

	// A field missing from a later sample is not seen, not removed; a field
	// first seen with a value is not a type change.
	old := Catalog{Objects: []Object{{Name: "Order.Created", Sampled: true, Fields: []Field{{Name: "id", Type: "number"}, {Name: "note"}, {Name: "coupon", Type: "string"}}}}}
	new := Catalog{Objects: []Object{{Name: "Order.Created", Sampled: true, Fields: []Field{{Name: "id", Type: "string"}, {Name: "note", Type: "string"}}}}}
	var kinds []string
	for _, c := range Diff(old, new) {
		kinds = append(kinds, fmt.Sprintf("%s %s %v", c.Kind, c.Field, c.Breaking))
	}
	if strings.Join(kinds, ",") != "field-not-seen coupon false,type-changed id true" {
		t.Fatalf("changes %v", kinds)
	}
}
