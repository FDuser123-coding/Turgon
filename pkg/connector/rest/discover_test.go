package rest

import (
	"context"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

// An API without a schema: an event's fields are those its list returns,
// and a mapping reading the event reads them.
func TestDiscoverSamplesTheList(t *testing.T) {
	shop, c := newShop(t)
	ctx := context.Background()
	cat, err := c.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Objects) != 0 || len(cat.Uses) != 0 || cat.Events["Order.Created"] != "Order.Created" {
		t.Fatalf("empty list: %+v", cat)
	}

	shop.AddOrder(order(1001))
	o := order(1002)
	o["note"], o["discount_code"] = "gift", "AUTUMN"
	shop.AddOrder(o)
	cat, err = c.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	orders, ok := cat.Object("Order.Created")
	if !ok || !orders.Sampled || !strings.Contains(orders.Label, "GET /orders.json, 2 item(s)") {
		t.Fatalf("objects %+v", cat.Objects)
	}
	types := map[string]string{}
	for _, f := range orders.Fields {
		types[f.Name] = f.Type
	}
	if types["subtotal_price"] != "string" || types["created_at"] != "datetime" || types["line_items"] != "array" || types["note"] != "string" {
		t.Fatalf("fields %v", types)
	}
	if id, _ := orders.Field("id"); !id.Key || id.Type != "number" {
		t.Fatalf("id %+v", id)
	}

	spec := &compiler.RuntimeSpec{}
	spec.Spec.Workflows = []compiler.Workflow{{Trigger: compiler.WorkflowSource{Endpoint: "shopify-store", Event: "Order.Created"},
		Steps: []compiler.WorkflowStep{{Map: &compiler.MapConfig{Mapping: "shopify-order-to-order@1", From: "shopify-store.Order",
			Fields: map[string]string{"netValue": "$number(subtotal_price)", "coupon": "discount_code"}}}}}}
	cat.Endpoint = "shopify-store"
	if by := meta.Usage(cat, spec)["Order.Created.subtotal_price"]; len(by) != 1 || by[0] != "mapping shopify-order-to-order@1 (netValue)" {
		t.Fatalf("usage %v", meta.Usage(cat, spec))
	}

	// Only one order had a discount code: a sample without it does not see
	// it, which may not mean the API dropped it.
	before := cat
	shop.Close()
	shop2, c2 := newShop(t)
	shop2.AddOrder(order(1003))
	after, err := c2.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	after.Endpoint = "shopify-store"
	changes := meta.Compare(before, after, spec)
	if len(changes) != 1 || changes[0].Kind != meta.FieldNotSeen || changes[0].Field != "discount_code" || changes[0].Breaks() ||
		strings.Join(changes[0].UsedBy, ";") != "mapping shopify-order-to-order@1 (coupon)" {
		t.Fatalf("changes %+v", changes)
	}
}
