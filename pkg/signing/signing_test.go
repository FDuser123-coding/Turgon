package signing

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

func testSpec() *compiler.RuntimeSpec {
	s := &compiler.RuntimeSpec{
		Kind:     compiler.KindRuntimeSpec,
		Metadata: compiler.RuntimeMeta{Name: "shop-orders-to-erp", Level: "L1"},
		Spec: compiler.RuntimeBody{Policies: []compiler.PolicyRef{{Name: "writeback-default", Version: "1.0.0",
			Rego: "package turgon.writeback\ndefault allow := false\nallow if input.amount < 10000"}}},
	}
	s.Metadata.Digest = compiler.Digest(s.Spec)
	return s
}

func keys(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	privPEM, pubPEM, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	priv, err := ParsePrivateKey(privPEM)
	if err != nil {
		t.Fatal(err)
	}
	pubs, err := ParsePublicKeys(pubPEM)
	if err != nil || len(pubs) != 1 {
		t.Fatal(pubs, err)
	}
	return priv, pubs[0]
}

func TestSignedSpecVerifies(t *testing.T) {
	priv, pub := keys(t)
	s := testSpec()
	if err := Sign(s, priv); err != nil {
		t.Fatal(err)
	}
	id, err := Verify(s, []ed25519.PublicKey{pub})
	if err != nil || id != KeyID(pub) {
		t.Fatalf("verify: %q %v", id, err)
	}
}

func TestTamperingIsRefused(t *testing.T) {
	priv, pub := keys(t)
	trusted := []ed25519.PublicKey{pub}

	// Loosening a policy breaks the digest...
	s := testSpec()
	_ = Sign(s, priv)
	s.Spec.Policies[0].Rego = strings.Replace(s.Spec.Policies[0].Rego, "10000", "10000000", 1)
	if _, err := Verify(s, trusted); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("edited policy: %v", err)
	}
	// ...and recomputing the digest breaks the signature.
	s.Metadata.Digest = compiler.Digest(s.Spec)
	if _, err := Verify(s, trusted); !errors.Is(err, ErrUnsigned) {
		t.Fatalf("edited policy, digest recomputed: %v", err)
	}
	// Relabelling the certification level breaks it too.
	s = testSpec()
	_ = Sign(s, priv)
	s.Metadata.Level = "L3"
	if _, err := Verify(s, trusted); !errors.Is(err, ErrUnsigned) {
		t.Fatalf("relabelled: %v", err)
	}
	// Unsigned, or signed by someone else.
	if _, err := Verify(testSpec(), trusted); !errors.Is(err, ErrUnsigned) {
		t.Fatalf("unsigned: %v", err)
	}
	other, _ := keys(t)
	s = testSpec()
	_ = Sign(s, other)
	if _, err := Verify(s, trusted); !errors.Is(err, ErrUnsigned) {
		t.Fatalf("untrusted signer: %v", err)
	}
	// A spec whose body does not match its digest is never signed.
	s = testSpec()
	s.Spec.Policies[0].Rego += "\n"
	if err := Sign(s, priv); err == nil {
		t.Fatal("signed a spec with a stale digest")
	}
}

func TestKeyRotation(t *testing.T) {
	oldPriv, oldPub := keys(t)
	newPriv, newPub := keys(t)
	s := testSpec()
	_ = Sign(s, oldPriv)
	_ = Sign(s, newPriv)
	_ = Sign(s, newPriv) // re-signing replaces, not duplicates
	if len(s.Metadata.Signatures) != 2 {
		t.Fatalf("signatures = %d", len(s.Metadata.Signatures))
	}
	for _, trusted := range [][]ed25519.PublicKey{{oldPub}, {newPub}} {
		if _, err := Verify(s, trusted); err != nil {
			t.Fatal(err)
		}
	}
}

func TestParseErrors(t *testing.T) {
	if _, err := ParsePrivateKey([]byte("not a key")); err == nil {
		t.Error("garbage private key accepted")
	}
	if _, err := ParsePublicKeys([]byte("not a key")); err == nil {
		t.Error("garbage public keys accepted")
	}
	_, pubPEM, _ := GenerateKey()
	if _, err := ParsePrivateKey(pubPEM); err == nil {
		t.Error("a public key accepted as a private key")
	}
}
