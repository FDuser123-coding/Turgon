package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

func TestRequiredFieldsFromRefusals(t *testing.T) {
	const pre = "writeguard: write payload invalid: POST /v1/customers: HTTP "
	for name, tc := range map[string]struct {
		err  string
		want []string
	}{
		"stripe":          {pre + `400: {"error": {"type": "invalid_request_error", "message": "Missing required param: email.", "param": "email"}}`, []string{"email"}},
		"rails field map": {pre + `422: {"errors": {"email": ["can't be blank"], "phone": ["is invalid"]}}`, []string{"email"}},
		"problem details": {pre + `400: {"type": "https://example.net/validation", "title": "Your request is not valid.",
			"invalid-params": [{"name": "taxId", "reason": "must not be empty"}, {"name": "age", "reason": "must be a positive integer"}]}`, []string{"taxId"}},
		"error list":              {pre + `422: {"errors": [{"field": "billing_address.country", "code": "missing_field"}]}`, []string{"billing_address"}},
		"json:api":                {pre + `422: {"errors": [{"source": {"pointer": "/data/attributes/vatNumber"}, "detail": "can't be blank"}]}`, []string{"vatNumber"}},
		"message only":            {pre + `400: {"message": "Property 'region' is required"}`, []string{"region"}},
		"plain text":              {pre + `400: missing required field: costCenter`, []string{"costCenter"}},
		"error name is no field":  {pre + `400: {"name": "ValidationError", "message": "email is required"}`, nil},
		"not a validation answer": {pre + `403: {"error": {"message": "Missing required scope: write_customers", "param": "scope"}}`, nil},
		"invalid, not missing":    {pre + `422: {"errors": {"email": ["is invalid"]}}`, nil},
		"truncated body":          {pre + `422: {"errors": {"email": ["can't be blank"], "addr…`, nil},
	} {
		got := requiredFields(tc.err)
		if len(got) == 0 {
			got = nil
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}

func TestLearnRefusalsMarksTheWrittenObject(t *testing.T) {
	c := &Conn{cfg: Config{Operations: map[string]Operation{
		"create-customer": {Method: "POST", Path: "/customers", Wrap: "customer"},
		"get-customer":    {Method: "GET", Path: "/customers/{{id}}"},
	}}}
	cat := meta.Catalog{
		Objects: []meta.Object{
			{Name: "/customers", Kind: "records", Sampled: true, Fields: []meta.Field{{Name: "email", Type: "string"}, {Name: "id", Type: "number"}}},
		},
		Uses: []meta.Use{{Object: "/customers", By: "operation create-customer (writes)", Creates: true}, {Object: "/customers", By: "operation get-customer"}},
	}
	at := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	c.LearnRefusals(&cat, []meta.Refusal{
		{Operation: "create-customer", At: at, Error: "POST /customers: HTTP 422: " + `{"errors": {"email": ["can't be blank"], "taxId": ["is required"]}}`},
		{Operation: "create-customer", At: at, Error: "POST /customers: HTTP 400: " + `{"errors": {"customer": "Required parameter missing or invalid"}}`},
		{Operation: "get-customer", At: at, Error: "GET /customers/1: HTTP 400: missing required field: x"},
		{Operation: "gone", At: at, Error: "HTTP 400: missing required field: y"},
	})
	o, _ := cat.Object("/customers")
	email, _ := o.Field("email")
	tax, _ := o.Field("taxId")
	if !email.Required || email.Type != "string" || !tax.Required || len(o.Fields) != 3 ||
		email.RequiredBy != "operation create-customer was refused without it on 2026-11-01" {
		t.Fatalf("fields: %+v", o.Fields)
	}

	// A later sample does not have taxId and cannot say it is required:
	// what was learned is kept, and the drift shows it as breaking for
	// every operation that creates the object.
	next := meta.Catalog{Objects: []meta.Object{{Name: "/customers", Kind: "records", Sampled: true, Fields: []meta.Field{{Name: "email", Type: "string"}, {Name: "id", Type: "number"}}}},
		Uses: cat.Uses}
	meta.KeepRequired(cat, &next)
	if n, _ := next.Object("/customers"); len(n.Fields) != 3 {
		t.Fatalf("kept: %+v", n.Fields)
	}
	if ch := meta.Compare(cat, next); len(ch) != 0 {
		t.Fatalf("unchanged catalog drifted: %v", ch)
	}
	before := meta.Catalog{Objects: []meta.Object{{Name: "/customers", Kind: "records", Sampled: true, Fields: []meta.Field{{Name: "email", Type: "string"}, {Name: "id", Type: "number"}}}},
		Uses: cat.Uses}
	var breaking []string
	for _, ch := range meta.Compare(before, cat) {
		if ch.Breaks() {
			breaking = append(breaking, ch.String())
			if !reflect.DeepEqual(ch.UsedBy, []string{"operation create-customer (writes)"}) {
				t.Errorf("%s used by %q", ch, ch.UsedBy)
			}
		}
	}
	if !reflect.DeepEqual(breaking, []string{
		"became-required /customers.email (operation create-customer was refused without it on 2026-11-01)",
		"field-added /customers.taxId (required: operation create-customer was refused without it on 2026-11-01)",
	}) {
		t.Fatalf("breaking: %v", breaking)
	}
}

// A store that starts requiring tags refuses the note update the way
// Shopify does; the refusal names tags, not the request's order wrapper.
func TestShopifyRefusalNamesTheMissingField(t *testing.T) {
	shop, c := newShop(t)
	id := shop.AddOrder(order(1001))
	shop.Require("tags")
	_, err := c.Commit(context.Background(), "update-order", "k", json.RawMessage(fmt.Sprintf(`{"orderId": %d, "note": "x"}`, id)))
	if err == nil {
		t.Fatal("the update without tags was taken")
	}
	if got := requiredFields(err.Error()); !reflect.DeepEqual(got, []string{"tags"}) {
		t.Fatalf("%v from %v", got, err)
	}
	cat, err := c.Discover(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	c.LearnRefusals(&cat, []meta.Refusal{{Operation: "update-order", Error: "writeguard: commit shop.update-order: write payload invalid: PUT /orders/1.json: HTTP 422: {\"errors\":{\"tags\":[\"can't be blank\"]}}", At: time.Now()}})
	o, _ := cat.Object(cat.Events["Order.Created"])
	if f, ok := o.Field("tags"); !ok || !f.Required {
		t.Fatalf("%s: %+v", o.Name, o.Fields)
	}
}
