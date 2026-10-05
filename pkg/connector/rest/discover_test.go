package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/shoptest"
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

// With an OpenAPI description, what the connection reads and writes comes
// from its schemas, and a write made stricter breaks what uses it.
func TestDiscoverFromOpenAPI(t *testing.T) {
	doc, err := os.ReadFile("testdata/shop-openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = w.Write(doc)
	}))
	defer srv.Close()
	shop := shoptest.New("shpat_test")
	defer shop.Close()
	cfg := shopConfig(shop.URL())
	cfg.OpenAPI = srv.URL + "/openapi.yaml"
	cfg.Operations["create-order"] = Operation{Method: "POST", Path: "/orders.json", Wrap: "order", Result: "order",
		Fields: map[string]string{"email": "email", "line_items": "lines", "note_attributes\\.x": "x"}}
	c, err := New(cfg, "shpat_test", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cat, err := c.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cat.Events["Order.Created"] != "Order" {
		t.Fatalf("events %v (objects %+v)", cat.Events, cat.Objects)
	}
	order, _ := cat.Object("Order")
	types := map[string]meta.Field{}
	for _, f := range order.Fields {
		types[f.Name] = f
	}
	if order.Sampled || order.Kind != "schema" || order.Label != "An order." ||
		types["id"].Type != "integer(int64)" || !types["id"].ReadOnly || types["id"].Required || types["created_at"].Required ||
		types["created_at"].Type != "string(date-time)" || types["currency"].Length != 3 ||
		types["line_items"].Type != "array<LineItem>" || types["customer"].Type != "Customer|integer" ||
		types["note"].Type != "string" || types["note"].Length != 5000 {
		t.Fatalf("Order %+v", types)
	}
	create, ok := cat.Object("OrderCreate")
	if !ok {
		t.Fatalf("objects %+v", cat.Objects)
	}
	if li, _ := create.Field("line_items"); !li.Required {
		t.Fatalf("line_items %+v", li)
	}
	if _, ok := cat.Object("OrderUpdate"); !ok {
		t.Fatal("the update's request body is missing")
	}
	usage := meta.Usage(cat)
	if strings.Join(usage["OrderCreate.email"], ";") != "operation create-order" || strings.Join(usage["OrderUpdate.note"], ";") != "operation update-order" ||
		strings.Join(usage["Order.name"], ";") != "operation get-order" {
		t.Fatalf("usage %v", usage)
	}
	cr := meta.Creators(cat)
	if strings.Join(cr["OrderCreate"], ";") != "operation create-order" || strings.Join(cr["OrderUpdate"], ";") != "operation update-order" || cr["Order"] != nil {
		t.Fatalf("writers %v", cr)
	}
	if m := meta.MissingUses(cat); len(m) != 1 || m[0].Field != "note_attributes.x" {
		t.Fatalf("missing %v", m)
	}

	// A new API version requires a phone on every new order.
	mu.Lock()
	doc = []byte(strings.Replace(string(doc), "required: [line_items]", "required: [line_items, phone]", 1))
	mu.Unlock()
	after, err := c.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	changes := meta.Compare(cat, after)
	if len(changes) != 1 || changes[0].Kind != meta.BecameRequired || changes[0].Field != "phone" || !changes[0].Breaks() ||
		strings.Join(changes[0].UsedBy, ";") != "operation create-order (writes)" {
		t.Fatalf("changes %+v", changes)
	}
}

func TestOpenAPIPathsAndRefs(t *testing.T) {
	sp, err := parseSpec([]byte(`{"openapi": "3.1.0", "servers": [{"url": "https://api.stripe.com/"}],
		"paths": {"/v1/invoices/{invoice}": {"post": {"requestBody": {"content": {"application/x-www-form-urlencoded":
			{"schema": {"type": "object", "properties": {"metadata": {"anyOf": [{"type": "object"}, {"enum": [""], "type": "string"}]}}}}}}}}},
		"components": {"schemas": {"loop": {"$ref": "#/components/schemas/loop"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	where, op := sp.operation("POST", "", "/v1/invoices/{{invoiceId}}")
	if op == nil || where != "POST /v1/invoices/{invoice}" {
		t.Fatalf("operation %q %v", where, op)
	}
	if _, op := sp.operation("GET", "", "/v1/invoices/{{invoiceId}}"); op != nil {
		t.Fatal("matched another method")
	}
	o, ok := sp.describe(sp.content(op, true), where+" request", "request body", true)
	if !ok || o.Name != "POST /v1/invoices/{invoice} request" || o.Fields[0].Type != "object|string" {
		t.Fatalf("object %+v", o)
	}
	if m, _ := sp.resolve(map[string]any{"$ref": "#/components/schemas/loop"}, ""); m != nil {
		t.Fatal("a $ref cycle resolved")
	}
	if _, err := parseSpec([]byte(`{"swagger": "2.0"}`)); err == nil || !strings.Contains(err.Error(), "OpenAPI 3") {
		t.Fatalf("swagger 2: %v", err)
	}
	if topField(`metadata.erp_id`) != "metadata" || topField(`customerid_account@odata\.bind`) != "customerid_account@odata.bind" {
		t.Fatal("topField")
	}
}
