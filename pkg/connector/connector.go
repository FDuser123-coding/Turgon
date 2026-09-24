// Package connector defines what the runtime needs from a connector
// implementation: a write target for the write guard and, optionally, an
// event source. Connectors are built from the compiled runtime spec by
// factories registered per connector name (architecture §7.1, AD-04: generic
// workers configured by data, no per-customer code).
package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Event is one business event read from a source system.
type Event struct {
	// ID is unique within the source and stable across re-reads.
	ID string `json:"id"`
	// Position orders events within a source; cursors store it.
	Position int64           `json:"position"`
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload"`
}

// Instance is a connector bound to one system.
type Instance interface {
	writeguard.Target
	Close()
}

// Source is implemented by instances that emit events.
type Source interface {
	// Poll returns up to limit events named event with Position > after,
	// in Position order.
	Poll(ctx context.Context, event string, after int64, limit int) ([]Event, error)
}

// SecretResolver turns a secret reference into its value at runtime.
// Values are never written to specs, logs or audit records.
type SecretResolver interface {
	Resolve(ctx context.Context, ref string) (string, error)
}

// Factory builds an instance from its compiled configuration.
type Factory func(ctx context.Context, cfg compiler.ConnectorConfig, secrets SecretResolver) (Instance, error)

// Registry maps connector names to factories.
type Registry map[string]Factory

// EnvSecrets resolves references from environment variables, for
// development and CI: "openbao://shop-db/dsn" reads PORTER_SECRET_SHOP_DB_DSN.
// Production resolves references against OpenBao or a cloud secret manager.
type EnvSecrets struct{}

// EnvName returns the variable EnvSecrets reads for ref.
func EnvName(ref string) string {
	if i := strings.Index(ref, "://"); i >= 0 {
		ref = ref[i+3:]
	}
	var b strings.Builder
	b.WriteString("PORTER_SECRET_")
	for _, r := range ref {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToUpper(r))
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (EnvSecrets) Resolve(_ context.Context, ref string) (string, error) {
	name := EnvName(ref)
	if v, ok := os.LookupEnv(name); ok {
		return v, nil
	}
	return "", fmt.Errorf("secret %s: set %s", ref, name)
}

// StaticSecrets resolves references from a map; for tests.
type StaticSecrets map[string]string

func (s StaticSecrets) Resolve(_ context.Context, ref string) (string, error) {
	if v, ok := s[ref]; ok {
		return v, nil
	}
	return "", fmt.Errorf("secret %s: not found", ref)
}
