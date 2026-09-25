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

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/mapping"
	"github.com/fduser123-coding/turgon/pkg/notify"
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
	// Audit records automatic identity matches.
	Audit audit.Recorder
	// Notifier tells people when a run needs them; nil sends nothing.
	Notifier notify.Notifier

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

// Matcher finds master records a source record may be, and links it with
// its identifying attributes (implemented by pgstore).
type Matcher interface {
	Candidates(ctx context.Context, entity string, attrs identity.Attributes) ([]identity.Candidate, error)
	Link(ctx context.Context, entity, system, sourceID, masterID string, attrs identity.Attributes) error
}

// Resolve links the document to a master record. The document names the
// source record in <entity>Ref (customerRef) and gains <entity>Id.
//
// A known cross-reference is used as it is. Otherwise the record is scored
// against records already linked (pkg/identity). With the probabilistic
// strategy a certain, unambiguous match is linked and audited; anything
// else goes to a data steward with the best suggestions.
func (x *Activities) Resolve(ctx context.Context, in ResolveInput) (map[string]any, error) {
	entity := strings.TrimPrefix(in.Config.Entity, "model.")
	field := lowerFirst(entity)
	switch in.Config.Strategy {
	case v1alpha1.StrategyExact, v1alpha1.StrategyProbabilistic, v1alpha1.StrategySplink:
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
		attrs := identity.Extract(in.Doc, in.Config.Match)
		var suggestions []identity.Suggestion
		m, canMatch := x.Resolver.(Matcher)
		if canMatch && len(attrs) > 0 {
			candidates, err := m.Candidates(ctx, entity, attrs)
			if err != nil {
				return nil, err // transient: retry
			}
			suggestions = identity.Suggest(attrs, candidates, 3)
		}
		master, certain := identity.Decide(suggestions, in.Config.AutoMatchAbove)
		if !certain || in.Config.Strategy != v1alpha1.StrategyProbabilistic {
			msg := fmt.Sprintf("%s %q from %s has no master record; it needs a data steward", entity, ref, in.System)
			return nil, temporal.NewNonRetryableApplicationError(msg, ErrTypeUnresolved, nil,
				Unresolved{Entity: entity, System: in.System, Ref: ref, Attributes: attrs, Suggestions: suggestions})
		}
		if err := m.Link(ctx, entity, in.System, ref, master, attrs); err != nil {
			return nil, err
		}
		if x.Audit != nil {
			if _, err := x.Audit.Record("turgon/resolver", "xref.matched", map[string]any{
				"entity": entity, "system": in.System, "ref": ref, "master": master,
				"score": suggestions[0].Score, "reasons": suggestions[0].Reasons, "threshold": in.Config.AutoMatchAbove,
			}); err != nil {
				return nil, err
			}
		}
		id = master
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
	amount := AmountOf(in.Doc)
	return writeguard.Request{
		Recipe:          in.Workflow,
		Target:          c.Endpoint,
		Operation:       c.Operation,
		Tool:            strings.ReplaceAll(c.Operation, "-", "_"),
		Risk:            c.Risk,
		Subject:         policy.Subject{ID: "turgon/recipe/" + in.Workflow, Roles: []string{policy.RoleOperator}},
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

// amountFields name a document's monetary amount, in order of preference;
// policy compares it with approval thresholds.
var amountFields = []string{"netValue", "amount", "totalAmount"}

// AmountOf returns a document's monetary amount, or 0 if it has none.
func AmountOf(doc map[string]any) float64 {
	for _, f := range amountFields {
		switch v := doc[f].(type) {
		case float64:
			return v
		case json.Number:
			n, _ := v.Float64()
			return n
		}
	}
	return 0
}

func lowerFirst(s string) string {
	for i, r := range s {
		return string(unicode.ToLower(r)) + s[i+len(string(r)):]
	}
	return s
}
