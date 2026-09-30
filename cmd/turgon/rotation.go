package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
)

// rotation replaces a process's connections when the secrets they use
// change, or on SIGHUP: `turgon run` rebuilds its worker generation,
// `turgon mcp` its agent server. New connections are checked first, and
// kept out if they are worse than the current ones (a wrong password put
// in the secret manager).
type rotation[T any] struct {
	out     io.Writer
	log     audit.Recorder
	spec    *compiler.RuntimeSpec
	secrets secretBackend
	refs    []string
	prints  fingerprints
	refused string // the last refusal reported, not repeated at every check

	// connect opens connections with the secrets as they are now.
	connect func() (T, error)
	// regressions returns the checks the current connections pass and
	// next fails, for the given endpoints (all of them if none).
	regressions func(ctx context.Context, next T, endpoints []string) []string
	// discard closes connections that will not be used.
	discard func(T)
	// use switches to next; false if that failed, having said why.
	use func(next T) bool
}

// newRotation reads the secrets' fingerprints, before the first
// connections are opened with them.
func newRotation[T any](ctx context.Context, out io.Writer, log audit.Recorder, spec *compiler.RuntimeSpec, secrets secretBackend) (*rotation[T], error) {
	r := &rotation[T]{out: out, log: log, spec: spec, secrets: secrets, refs: specSecretRefs(spec)}
	p, err := secrets.Fingerprints(ctx, r.refs)
	if err != nil {
		return nil, err
	}
	r.prints = p
	return r, nil
}

// every returns a channel ticking every d, or nil if the secrets cannot
// change or d is 0; and a function stopping it.
func (r *rotation[T]) every(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 || !r.secrets.Rotates() {
		return nil, func() {}
	}
	t := time.NewTicker(d)
	fmt.Fprintf(r.out, "turgon: checking %d secret(s) for rotation every %s\n", len(r.refs), d)
	return t.C, t.Stop
}

// hup reconnects with every secret as it is now.
func (r *rotation[T]) hup(ctx context.Context) {
	p, err := r.secrets.Fingerprints(ctx, r.refs)
	if r.reload(ctx, "reload requested (SIGHUP)", nil) && err == nil {
		r.prints = p
	}
}

// refresh reads the secrets again and reconnects if one changed. A refused
// change is tried again at the next refresh.
func (r *rotation[T]) refresh(ctx context.Context) {
	p, err := r.secrets.Fingerprints(ctx, r.refs)
	if err != nil {
		// The secret manager is unreachable: keep going with what the
		// connectors have.
		fmt.Fprintf(r.out, "turgon: checking secrets for rotation: %v\n", err)
		return
	}
	if changed := r.prints.changed(p); len(changed) > 0 &&
		r.reload(ctx, fmt.Sprintf("secret %s changed", strings.Join(changed, ", ")), changed) {
		r.prints = p
	}
}

// reload opens new connections and switches to them if they are no worse
// than the current ones; if the new secrets do not work, the current
// connections are kept.
func (r *rotation[T]) reload(ctx context.Context, why string, changed []string) bool {
	next, err := r.connect()
	if err != nil {
		fmt.Fprintf(r.out, "turgon: %s, but the new connections failed; keeping the current ones: %v\n", why, err)
		return false
	}
	// Connectors may connect lazily: check the ones whose secrets changed
	// (all of them on SIGHUP) before switching.
	if msgs := r.regressions(ctx, next, endpointsUsing(r.spec, changed)); len(msgs) > 0 {
		r.discard(next)
		key := why + "\x00" + strings.Join(msgs, "\x00")
		if key == r.refused {
			return false
		}
		r.refused = key
		fmt.Fprintf(r.out, "turgon: %s, but the new connections failed their checks; keeping the current ones: %s\n", why, strings.Join(msgs, "; "))
		r.record("connections.reload-refused", map[string]any{"spec": r.spec.Metadata.Name, "reason": why, "secrets": changed, "failed": msgs})
		return false
	}
	if !r.use(next) {
		return false
	}
	r.refused = ""
	fmt.Fprintf(r.out, "turgon: %s; reconnected\n", why)
	r.record("connections.reloaded", map[string]any{"spec": r.spec.Metadata.Name, "reason": why, "secrets": changed})
	return true
}

func (r *rotation[T]) record(action string, data map[string]any) {
	if _, err := r.log.Record("turgon", action, data); err != nil {
		fmt.Fprintf(r.out, "turgon: audit: %v\n", err)
	}
}
