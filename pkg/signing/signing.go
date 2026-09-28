// Package signing signs compiled runtime specs and verifies them before
// they run. A spec carries everything a worker enforces, the policy packs
// included, and its digest only shows it was not changed by accident:
// whoever can edit it can recompute the digest. A signature shows the spec
// came from the pipeline that holds the signing key, so loosening a policy
// or relabelling a spec's certification level means going through that
// pipeline.
//
// Keys are Ed25519, stored as PEM (PKCS #8 private keys, PKIX public
// keys). A spec may carry several signatures, e.g. during key rotation.
package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

// ErrUnsigned: no signature by a trusted key.
var ErrUnsigned = errors.New("spec is not signed by a trusted key")

// GenerateKey returns a new key pair as PEM.
func GenerateKey() (privatePEM, publicPEM []byte, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), nil
}

// ParsePrivateKey reads an Ed25519 private key from PEM.
func ParsePrivateKey(data []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("signing key: want a PEM \"PRIVATE KEY\" block")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("signing key: %w", err)
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("signing key: not an Ed25519 key")
	}
	return priv, nil
}

// ParsePublicKeys reads every Ed25519 public key in a PEM bundle.
func ParsePublicKeys(data []byte) ([]ed25519.PublicKey, error) {
	var keys []ed25519.PublicKey
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "PUBLIC KEY" {
			return nil, fmt.Errorf("trusted keys: unexpected PEM block %q", block.Type)
		}
		k, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("trusted keys: %w", err)
		}
		pub, ok := k.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("trusted keys: not an Ed25519 key")
		}
		keys = append(keys, pub)
	}
	if len(keys) == 0 && strings.TrimSpace(string(data)) != "" {
		return nil, errors.New("trusted keys: no PEM public key found")
	}
	return keys, nil
}

// KeyID names a public key: the first 16 hex digits of its SHA-256.
func KeyID(pub ed25519.PublicKey) string {
	der, _ := x509.MarshalPKIXPublicKey(pub)
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:8])
}

// message is what a signature covers: the spec's identity, certification
// level and body digest, so none can change without breaking it.
func message(s *compiler.RuntimeSpec) []byte {
	return []byte("turgon-runtime-spec-v1\n" + s.Metadata.Name + "\n" + s.Metadata.Level + "\n" + s.Metadata.Digest + "\n")
}

// Sign adds a signature to the spec, replacing an earlier one by the same
// key. The spec's digest must match its body.
func Sign(s *compiler.RuntimeSpec, priv ed25519.PrivateKey) error {
	if got := compiler.Digest(s.Spec); got != s.Metadata.Digest {
		return fmt.Errorf("refusing to sign: the body hashes to %s, not the spec's digest %s", got, s.Metadata.Digest)
	}
	id := KeyID(priv.Public().(ed25519.PublicKey))
	sig := compiler.Signature{KeyID: id, Algorithm: "ed25519", Value: base64.StdEncoding.EncodeToString(ed25519.Sign(priv, message(s)))}
	kept := s.Metadata.Signatures[:0]
	for _, old := range s.Metadata.Signatures {
		if old.KeyID != id {
			kept = append(kept, old)
		}
	}
	s.Metadata.Signatures = append(kept, sig)
	return nil
}

// Verify checks the digest and that a trusted key signed the spec, and
// returns that key's ID.
func Verify(s *compiler.RuntimeSpec, trusted []ed25519.PublicKey) (string, error) {
	if got := compiler.Digest(s.Spec); got != s.Metadata.Digest {
		return "", fmt.Errorf("digest mismatch (spec says %s, body hashes to %s)", s.Metadata.Digest, got)
	}
	byID := map[string]ed25519.PublicKey{}
	for _, k := range trusted {
		byID[KeyID(k)] = k
	}
	var tried []string
	for _, sig := range s.Metadata.Signatures {
		pub, ok := byID[sig.KeyID]
		if !ok || sig.Algorithm != "ed25519" {
			tried = append(tried, sig.KeyID)
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(sig.Value)
		if err == nil && ed25519.Verify(pub, message(s), raw) {
			return sig.KeyID, nil
		}
		tried = append(tried, sig.KeyID+" (invalid)")
	}
	if len(tried) == 0 {
		return "", ErrUnsigned
	}
	return "", fmt.Errorf("%w: signatures by %s", ErrUnsigned, strings.Join(tried, ", "))
}
