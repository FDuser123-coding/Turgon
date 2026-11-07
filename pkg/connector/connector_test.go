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

func TestVersionIDNamesTheRecordAndItsChange(t *testing.T) {
	id := VersionID("006A000001", 1762369200000)
	if id != "006A000001@1762369200000" || RecordID(id) != "006A000001" {
		t.Fatalf("%q -> %q", id, RecordID(id))
	}
	for _, plain := range []string{"42", "evt_1", "ada@example.com", "@5", "a@b@c"} {
		if RecordID(plain) != plain {
			t.Errorf("RecordID(%q) = %q", plain, RecordID(plain))
		}
	}
	if RecordID(VersionID("ada@example.com", 7)) != "ada@example.com" {
		t.Error("an ID with an @ of its own")
	}
}
