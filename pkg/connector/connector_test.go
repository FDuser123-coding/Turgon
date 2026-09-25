package connector

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConfigSecretRefs(t *testing.T) {
	cfg := json.RawMessage(`{"baseURL":"https://api.stripe.com","events":{
		"Invoice.Paid":{"path":"/v1/events","webhook":{"signature":"stripe","secretRef":"openbao://stripe-billing/webhook-secret"}},
		"Invoice.Voided":{"webhook":{"secretRef":"openbao://stripe-billing/webhook-secret"}},
		"Other":{"list":[{"secretRef":"openbao://a/b"}],"secretRef":""}}}`)
	if got := strings.Join(ConfigSecretRefs(cfg), ","); got != "openbao://a/b,openbao://stripe-billing/webhook-secret" {
		t.Fatal(got)
	}
	if got := ConfigSecretRefs(json.RawMessage(`not json`)); len(got) != 0 {
		t.Fatal(got)
	}
}
