package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/secrets"
)

// secretBackend is where connection secrets come from. TURGON_SECRETS=env
// (the default) reads TURGON_SECRET_* variables; openbao reads OpenBao or
// HashiCorp Vault, configured by TURGON_OPENBAO_* (or BAO_*/VAULT_*); aws,
// azure and gcp read the cloud's secret manager.
type secretBackend interface {
	connector.SecretResolver
	// Where says where a reference is read from.
	Where(ref string) string
	// Missing says how to provide a reference that could not be read.
	Missing(ref string) string
	// Fingerprints reads every reference now (not from a cache) and
	// returns a hash of each value, to notice rotations.
	Fingerprints(ctx context.Context, refs []string) (fingerprints, error)
	// Rotates reports whether values can change while Turgon runs.
	Rotates() bool
}

// fingerprints are SHA-256 hashes of secret values by reference: enough to
// tell that a value changed, without keeping it.
type fingerprints map[string]string

func (f fingerprints) changed(now fingerprints) []string {
	var out []string
	for ref, h := range now {
		if f[ref] != h {
			out = append(out, ref)
		}
	}
	sort.Strings(out)
	return out
}

func fingerprint(ctx context.Context, r connector.SecretResolver, refs []string) (fingerprints, error) {
	out := fingerprints{}
	for _, ref := range refs {
		v, err := r.Resolve(ctx, ref)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(v))
		out[ref] = hex.EncodeToString(sum[:])
	}
	return out, nil
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
	case "aws", "azure", "gcp":
		c, err := openCloud(b)
		if err != nil {
			return nil, err
		}
		return cloudBackend{c}, nil
	default:
		return nil, fmt.Errorf("TURGON_SECRETS must be env, openbao, aws, azure or gcp, got %q", b)
	}
}

// openCloud configures a cloud secret manager from TURGON_* variables.
// Credentials come from the platform: IRSA or EKS Pod Identity, AKS
// workload identity or a managed identity, GKE Workload Identity.
func openCloud(kind string) (*secrets.Cloud, error) {
	prefix := os.Getenv("TURGON_SECRETS_PREFIX")
	ttl, _ := time.ParseDuration(os.Getenv("TURGON_SECRETS_TTL"))
	ctx := context.Background()
	switch kind {
	case "aws":
		return secrets.NewAWS(ctx, secrets.AWSConfig{Region: os.Getenv("TURGON_AWS_REGION"), Prefix: prefix,
			Endpoint: os.Getenv("TURGON_AWS_SECRETS_ENDPOINT"), TTL: ttl})
	case "azure":
		return secrets.NewAzure(secrets.AzureConfig{VaultURL: os.Getenv("TURGON_AZURE_VAULT_URL"), Prefix: prefix, TTL: ttl,
			APIVersion: os.Getenv("TURGON_AZURE_API_VERSION")})
	default:
		return secrets.NewGCP(ctx, secrets.GCPConfig{Project: os.Getenv("TURGON_GCP_PROJECT"), Prefix: prefix,
			Endpoint: os.Getenv("TURGON_GCP_SECRETS_ENDPOINT"), TTL: ttl})
	}
}

type envBackend struct{ connector.EnvSecrets }

// Environment variables cannot change inside a running process.
func (envBackend) Rotates() bool { return false }
func (b envBackend) Fingerprints(ctx context.Context, refs []string) (fingerprints, error) {
	return fingerprint(ctx, b, refs)
}

func (envBackend) Where(ref string) string { return connector.EnvName(ref) }
func (envBackend) Missing(ref string) string {
	return fmt.Sprintf("Set %s to the secret for %s.", connector.EnvName(ref), ref)
}

type openBaoBackend struct{ *secrets.OpenBao }

func (openBaoBackend) Rotates() bool { return true }
func (b openBaoBackend) Fingerprints(ctx context.Context, refs []string) (fingerprints, error) {
	b.Forget() // read what OpenBao holds now, not the cache
	return fingerprint(ctx, b, refs)
}

func (b openBaoBackend) Missing(ref string) string {
	return fmt.Sprintf("Put the secret in OpenBao at %s, readable by the token Turgon signs in with.", b.Where(ref))
}

type cloudBackend struct{ *secrets.Cloud }

func (cloudBackend) Rotates() bool { return true }
func (b cloudBackend) Fingerprints(ctx context.Context, refs []string) (fingerprints, error) {
	b.Forget()
	return fingerprint(ctx, b, refs)
}
func (b cloudBackend) Missing(ref string) string {
	return fmt.Sprintf("Put the secret in %s, readable by the identity Turgon runs as.", b.Where(ref))
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
	for _, p := range spec.Spec.Plugins {
		for _, ref := range p.Secrets {
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
	switch b := b.(type) {
	case openBaoBackend:
		return b.Check(ctx, refs)
	case cloudBackend:
		return b.Check(ctx, refs)
	}
	return nil
}
