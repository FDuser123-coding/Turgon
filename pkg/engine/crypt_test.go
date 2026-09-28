package engine

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/pkg/identity"
)

func key(b byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(b), 32)))
}

func TestPayloadsAreEncryptedAndRotate(t *testing.T) {
	old, err := NewCodec("2025-a:" + key('a'))
	if err != nil {
		t.Fatal(err)
	}
	dc := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), old)
	p, err := dc.ToPayloads(map[string]any{"customerRef": "ada@lovelace-gmbh.example", "amount": 1500})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(p.Payloads[0].Data), "lovelace") || string(p.Payloads[0].Metadata["encoding"]) != "binary/encrypted" ||
		string(p.Payloads[0].Metadata["turgon-key-id"]) != "2025-a" {
		t.Fatalf("not encrypted: %v", p.Payloads[0])
	}

	// A new key encrypts; the old one still decrypts what it wrote.
	rotated, _ := NewCodec("2026-b:" + key('b') + ",2025-a:" + key('a'))
	rdc := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), rotated)
	var got map[string]any
	if err := rdc.FromPayloads(p, &got); err != nil || got["customerRef"] != "ada@lovelace-gmbh.example" {
		t.Fatalf("rotated decode: %v %v", got, err)
	}
	if q, _ := rdc.ToPayloads("x"); string(q.Payloads[0].Metadata["turgon-key-id"]) != "2026-b" {
		t.Fatal("the first key does not encrypt")
	}

	// Without the key, or with the payload changed, nothing decrypts.
	other, _ := NewCodec("2026-b:" + key('b'))
	if _, err := other.Decode(p.Payloads); err == nil || !strings.Contains(err.Error(), `"2025-a"`) {
		t.Fatalf("missing key: %v", err)
	}
	tampered := &commonpb.Payload{Metadata: p.Payloads[0].Metadata, Data: append([]byte{}, p.Payloads[0].Data...)}
	tampered.Data[len(tampered.Data)-1] ^= 1
	if _, err := old.Decode([]*commonpb.Payload{tampered}); err == nil {
		t.Fatal("a tampered payload decrypted")
	}
	// The key ID is authenticated: relabelling a payload breaks it.
	both, _ := NewCodec("2025-a:" + key('a') + ",2026-b:" + key('b'))
	relabelled := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/encrypted"), "turgon-key-id": []byte("2026-b")}, Data: p.Payloads[0].Data}
	if _, err := both.Decode([]*commonpb.Payload{relabelled}); err == nil {
		t.Fatal("a relabelled payload decrypted")
	}

	// Histories from before encryption was turned on still read.
	plain, _ := converter.GetDefaultDataConverter().ToPayloads("before")
	var s string
	if err := dc.FromPayloads(plain, &s); err != nil || s != "before" {
		t.Fatalf("plain payload: %q %v", s, err)
	}
}

func TestPayloadKeySpecs(t *testing.T) {
	for _, bad := range []string{"", "nokey", "a:" + base64.StdEncoding.EncodeToString([]byte("short")), "a:!!!", "a b:" + key('a'), "a:" + key('a') + ",a:" + key('b')} {
		if _, err := NewCodec(bad); err == nil {
			t.Errorf("%q accepted", bad)
		} else if strings.Contains(err.Error(), key('a')) {
			t.Errorf("error quotes the key: %v", err)
		}
	}
}

// Failure messages can quote records; with encryption they are encrypted
// too, and the console decodes them, details included.
func TestFailuresAreEncryptedAndStillRead(t *testing.T) {
	dcBefore, fcBefore := dataConverter, failureConverter
	t.Cleanup(func() { dataConverter, failureConverter = dcBefore, fcBefore })
	c, _ := NewCodec("k1:" + key('k'))
	EncryptPayloads(c)

	u := Unresolved{Entity: "Customer", System: "shopify-store", Ref: "edsger@lovelace-gmbh.example",
		Suggestions: []identity.Suggestion{{Master: "C-100", Score: 0.88}}}
	err := temporal.NewNonRetryableApplicationError(`Customer "edsger@lovelace-gmbh.example" has no master record`, ErrTypeUnresolved, nil, u)
	f := FailureConverter().ErrorToFailure(err)
	if raw, _ := f.Marshal(); strings.Contains(string(raw), "edsger") {
		t.Fatal("the failure stores the record in clear")
	}
	var app *temporal.ApplicationError
	back := FailureConverter().FailureToError(f)
	var got Unresolved
	if !errors.As(back, &app) || app.Type() != ErrTypeUnresolved || !strings.Contains(app.Message(), "edsger") || app.Details(&got) != nil || got.Suggestions[0].Master != "C-100" {
		t.Fatalf("decoded %v %+v", back, got)
	}
}
