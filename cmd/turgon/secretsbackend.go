package main

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/secrets"
)

// secretBackend is where connection secrets come from. TURGON_SECRETS=env
// (the default) reads TURGON_SECRET_* variables; openbao reads OpenBao or
// HashiCorp Vault, configured by TURGON_OPENBAO_* (or BAO_*/VAULT_*).
type secretBackend interface {
	connector.SecretResolver
	// Where says where a reference is read from.
	Where(ref string) string
	// Missing says how to provide a reference that could not be read.
	Missing(ref string) string
}

func openSecrets() (secretBackend, error) {
	switch b := os.Getenv("TURGON_SECRETS"); b {
	case "", "env":
		return envBackend{}, nil
	case "openbao", "vault":
		o, err := secrets.NewOpenBao(secrets.ConfigFromEnv())
		if err != nil {
			return nil, err
		}
		return openBaoBackend{o}, nil
	default:
		return nil, fmt.Errorf("TURGON_SECRETS must be env or openbao, got %q", b)
	}
}

type envBackend struct{ connector.EnvSecrets }

func (envBackend) Where(ref string) string { return connector.EnvName(ref) }
func (envBackend) Missing(ref string) string {
	return fmt.Sprintf("Set %s to the secret for %s.", connector.EnvName(ref), ref)
}

type openBaoBackend struct{ *secrets.OpenBao }

func (b openBaoBackend) Missing(ref string) string {
	return fmt.Sprintf("Put the secret in OpenBao at %s, readable by the token Turgon signs in with.", b.Where(ref))
}

// specSecretRefs lists every secret reference a spec's connectors use.
func specSecretRefs(spec *compiler.RuntimeSpec) []string {
	seen := map[string]bool{}
	for _, c := range spec.Spec.Connectors {
		if c.SecretRef != "" {
			seen[c.SecretRef] = true
		}
		for _, ref := range connector.ConfigSecretRefs(c.Config) {
			seen[ref] = true
		}
	}
	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// checkSecrets signs in to the secret manager and reads each reference
// (OpenBao only; environment variables are checked per connector).
func checkSecrets(ctx context.Context, b secretBackend, refs []string) []connector.CheckResult {
	if o, ok := b.(openBaoBackend); ok {
		return o.Check(ctx, refs)
	}
	return nil
}
