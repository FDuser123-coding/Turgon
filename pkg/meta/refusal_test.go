package meta

import (
	"reflect"
	"testing"
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
		got := RequiredFields(tc.err)
		if len(got) == 0 {
			got = nil
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}
