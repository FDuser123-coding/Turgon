package connector

import (
	"bytes"
	"context"
	"sync"
)

// TokenStore keeps credentials a system rotates while Turgon uses them,
// such as an OAuth 2.0 refresh token that QuickBooks Online or Xero
// replaces on every use. The secret a connection names only seeds them:
// once rotated, the current token lives here, where every worker finds it.
type TokenStore interface {
	// UpdateToken calls fn with the value stored under key (nil when there
	// is none) while holding a lock all workers share, so only one of them
	// spends a single-use token. What fn returns is stored, unless it is
	// nil; an error from fn stores nothing.
	UpdateToken(ctx context.Context, key string, fn func(current []byte) ([]byte, error)) error
}

type tokenResolver struct {
	SecretResolver
	TokenStore
}

// WithTokens returns a resolver that also hands connectors store, for the
// tokens their systems rotate.
func WithTokens(r SecretResolver, store TokenStore) SecretResolver {
	return tokenResolver{r, store}
}

// Tokens returns the token store a resolver carries, if any.
func Tokens(r SecretResolver) (TokenStore, bool) {
	ts, ok := r.(TokenStore)
	return ts, ok && ts != nil
}

// MemoryTokens keeps tokens in memory, for tests. A process that restarts
// loses the rotated token, and the system then refuses the seed: production
// keeps them in Turgon's database (pgstore.TokenVault).
type MemoryTokens struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (t *MemoryTokens) UpdateToken(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = map[string][]byte{}
	}
	next, err := fn(bytes.Clone(t.m[key]))
	if err != nil || next == nil {
		return err
	}
	t.m[key] = bytes.Clone(next)
	return nil
}
