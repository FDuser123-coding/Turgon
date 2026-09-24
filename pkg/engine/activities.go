package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/mapping"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Resolver looks up identity cross-references (architecture §7.3).
type Resolver interface {
	Xref(ctx context.Context, entity, system, sourceID string) (string, bool, error)
}

// Activities holds the runtime dependencies activities use. It is safe for
// concurrent use.
type Activities struct {
	Guard    *writeguard.Guard
	Resolver Resolver

	mu      sync.Mutex
	mappers map[string]*mapping.Mapper
}

func nonRetryable(kind string, err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), kind, err)
}

// Map applies a mapping to the current document.
func (x *Activities) Map(_ context.Context, in MapInput) (map[string]any, error) {
	m, err := x.mapper(in.Config)
	if err != nil {
		return nil, nonRetryable(ErrTypeMapping, err)
	}
	out, err := m.Apply(in.Doc)
	if err != nil {
		return nil, nonRetryable(ErrTypeMapping, err)
	}
	return out, nil
}

func (x *Activities) mapper(cfg compiler.MapConfig) (*mapping.Mapper, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.mappers == nil {
		x.mappers = map[string]*mapping.Mapper{}
	}
	// Mapping references are immutable versions, so the reference is a
	// safe cache key.
	if m, ok := x.mappers[cfg.Mapping]; ok {
		return m, nil
	}
	m, err := mapping.Compile(cfg.Fields)
	if err != nil {
		return nil, fmt.Errorf("mapping %s: %w", cfg.Mapping, err)
	}
	x.mappers[cfg.Mapping] = m
	return m, nil
}

// Resolve links the document to a master record. The document names the
// source record in <entity>Ref (customerRef) and gains <entity>Id.
func (x *Activities) Resolve(ctx context.Context, in ResolveInput) (map[string]any, error) {
	entity := strings.TrimPrefix(in.Config.Entity, "model.")
	field := lowerFirst(entity)
	switch in.Config.Strategy {
	case "exact":
	case "splink":
		return nil, nonRetryable(ErrTypeUnresolved, errors.New("probabilistic (splink) identity resolution is not available in this runtime yet; use the exact strategy"))
	default:
		return nil, nonRetryable(ErrTypeUnresolved, fmt.Errorf("unknown strategy %q", in.Config.Strategy))
	}
	if x.Resolver == nil {
		return nil, nonRetryable(ErrTypeUnresolved, errors.New("no identity resolver configured"))
	}
	ref, ok := in.Doc[field+"Ref"].(string)
	if !ok || ref == "" {
		return nil, nonRetryable(ErrTypeUnresolved, fmt.Errorf("document has no %sRef to resolve", field))
	}
	id, found, err := x.Resolver.Xref(ctx, entity, in.System, ref)
	if err != nil {
		return nil, err // transient: retry
	}
	if !found {
		return nil, nonRetryable(ErrTypeUnresolved, fmt.Errorf("%s %q from %s has no master record; it needs a data steward", entity, ref, in.System))
	}
	out := make(map[string]any, len(in.Doc)+1)
	for k, v := range in.Doc {
		out[k] = v
	}
	out[field+"Id"] = id
	return out, nil
}

// PrepareWrite builds the write request and runs the guard's first phase.
func (x *Activities) PrepareWrite(ctx context.Context, in PrepareInput) (PrepareOutput, error) {
	req, err := buildRequest(in)
	if err != nil {
		return PrepareOutput{}, nonRetryable(ErrTypeInvalid, err)
	}
	p, err := x.Guard.Prepare(ctx, req)
	if err != nil {
		return PrepareOutput{}, classify(err)
	}
	return PrepareOutput{Request: req, Prepared: p}, nil
}

// CommitWrite runs the guard's second phase.
func (x *Activities) CommitWrite(ctx context.Context, in CommitInput) (writeguard.Outcome, error) {
	out, err := x.Guard.Commit(ctx, in.Request, in.Approval)
	if err != nil && !(errors.Is(err, writeguard.ErrUnconfirmed) && out.Status == writeguard.StatusCommitted) {
		return out, classify(err)
	}
	return out, err
}

// Compensate undoes a committed write.
func (x *Activities) Compensate(ctx context.Context, in CompensateInput) error {
	return x.Guard.Compensate(ctx, in.Request, in.Result)
}

func classify(err error) error {
	switch {
	case errors.Is(err, writeguard.ErrDenied):
		return nonRetryable(ErrTypeDenied, err)
	case errors.Is(err, writeguard.ErrRejected):
		return nonRetryable(ErrTypeRejected, err)
	case errors.Is(err, writeguard.ErrInvalid):
		return nonRetryable(ErrTypeInvalid, err)
	default:
		return err
	}
}

// buildRequest turns a compiled write step into a guard request. Recipe
// writes act as the recipe's own service identity.
func buildRequest(in PrepareInput) (writeguard.Request, error) {
	c := in.Config
	key, err := Render(c.IdempotencyKey, in.Source, in.Doc)
	if err != nil {
		return writeguard.Request{}, err
	}
	payload, err := json.Marshal(in.Doc)
	if err != nil {
		return writeguard.Request{}, err
	}
	amount, _ := in.Doc["netValue"].(float64)
	return writeguard.Request{
		Target:          c.Endpoint,
		Operation:       c.Operation,
		Tool:            strings.ReplaceAll(c.Operation, "-", "_"),
		Risk:            c.Risk,
		Subject:         policy.Subject{ID: "porter/recipe/" + in.Workflow, Roles: []string{policy.RoleOperator}},
		IdempotencyKey:  key,
		Payload:         payload,
		Entity:          c.Entity,
		Amount:          amount,
		Simulate:        c.Simulation != "",
		Compensation:    c.Compensation,
		RequireApproval: c.Approval == "required",
		Reason:          fmt.Sprintf("recipe %s step %s", in.Workflow, in.Step),
	}, nil
}

func lowerFirst(s string) string {
	for i, r := range s {
		return string(unicode.ToLower(r)) + s[i+len(string(r)):]
	}
	return s
}
