package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/identity"
)

// matchResolver has no cross-reference, one linked candidate, and
// optionally a trained model.
type matchResolver struct {
	model  *identity.Model
	linked string
}

func (r *matchResolver) Xref(context.Context, string, string, string) (string, bool, error) {
	return "", false, nil
}

func (r *matchResolver) Candidates(context.Context, string, identity.Attributes) ([]identity.Candidate, error) {
	return []identity.Candidate{{Master: "C-7", Attributes: identity.Attributes{"email": "r7-1@relay.marketplace.example", "domain": "relay.marketplace.example"}}}, nil
}

func (r *matchResolver) Link(_ context.Context, _, _, _, master string, _ identity.Attributes) error {
	r.linked = master
	return nil
}

func (r *matchResolver) MatchModel(context.Context, string) (identity.Model, bool, error) {
	if r.model == nil {
		return identity.Model{}, false, nil
	}
	return *r.model, true, nil
}

// A relay order from an unknown buyer shares only the marketplace's relay
// domain with a known customer. With the default model and auto-matching
// above 0.8 it is linked to that customer; with a model trained on this
// deployment, which learned the relay domain says little, it goes to a
// data steward.
func TestResolveUsesTheTrainedModel(t *testing.T) {
	in := ResolveInput{
		Config: compiler.ResolveConfig{Entity: "model.Customer", Strategy: v1alpha1.StrategyProbabilistic, AutoMatchAbove: 0.8,
			Match: []v1alpha1.MatchField{{Field: "customerRef", Kind: v1alpha1.MatchEmail}, {Field: "customerRef", Kind: v1alpha1.MatchDomain}}},
		System: "marketplace",
		Doc:    map[string]any{"customerRef": "r9-1@relay.marketplace.example"},
	}

	def := &matchResolver{}
	if _, err := (&Activities{Resolver: def}).Resolve(context.Background(), in); err != nil || def.linked != "C-7" {
		t.Fatalf("default model: linked %q, err %v", def.linked, err)
	}

	weak := identity.Default
	weak.Levels = map[string]identity.Probabilities{}
	for k, v := range identity.Default.Levels {
		weak.Levels[k] = v
	}
	weak.Levels[identity.DomainSame] = identity.Probabilities{M: 0.34, U: 0.25}
	trained := &matchResolver{model: &weak}
	_, err := (&Activities{Resolver: trained}).Resolve(context.Background(), in)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != ErrTypeUnresolved || trained.linked != "" {
		t.Fatalf("trained model: linked %q, err %v", trained.linked, err)
	}
}

type fakeSplink struct {
	out    []identity.Suggestion
	err    error
	entity string
	attrs  identity.Attributes
}

func (f *fakeSplink) Suggest(_ context.Context, entity string, a identity.Attributes, _ int) ([]identity.Suggestion, identity.SplinkModel, error) {
	f.entity, f.attrs = entity, a
	return f.out, identity.SplinkModel{Records: 40, Masters: 31}, f.err
}

type auditRecord struct {
	action string
	detail map[string]any
}

type memAudit struct{ records []auditRecord }

func (m *memAudit) Record(_ string, action string, detail any) (audit.Entry, error) {
	m.records = append(m.records, auditRecord{action, detail.(map[string]any)})
	return audit.Entry{}, nil
}

// With strategy splink the Splink service scores the record: a certain
// match is linked and audited with the model; anything else, and an
// entity the service has no model for yet, goes to a data steward.
func TestResolveWithSplink(t *testing.T) {
	in := ResolveInput{
		Config: compiler.ResolveConfig{Entity: "model.Customer", Strategy: v1alpha1.StrategySplink, AutoMatchAbove: 0.95,
			Match: []v1alpha1.MatchField{{Field: "email", Kind: v1alpha1.MatchEmail}}},
		System: "shop",
		Doc:    map[string]any{"customerRef": "S-1", "email": "Ada@Acme.io"},
	}
	unresolved := func(t *testing.T, err error) Unresolved {
		t.Helper()
		var app *temporal.ApplicationError
		var u Unresolved
		if !errors.As(err, &app) || app.Type() != ErrTypeUnresolved || app.Details(&u) != nil {
			t.Fatalf("want an unresolved failure, got %v", err)
		}
		return u
	}

	t.Run("certain match is linked", func(t *testing.T) {
		r, sp, au := &matchResolver{}, &fakeSplink{out: []identity.Suggestion{{Master: "C-1", Score: 0.99, Reasons: []string{"same email address"}}, {Master: "C-2", Score: 0.1}}}, &memAudit{}
		out, err := (&Activities{Resolver: r, Splink: sp, Audit: au}).Resolve(context.Background(), in)
		if err != nil || out["customerId"] != "C-1" || r.linked != "C-1" {
			t.Fatalf("out %v linked %q err %v", out, r.linked, err)
		}
		if sp.entity != "Customer" || sp.attrs["email"] != "ada@acme.io" {
			t.Fatalf("asked %q %v", sp.entity, sp.attrs)
		}
		if len(au.records) != 1 || au.records[0].action != "xref.matched" || au.records[0].detail["model"] != "splink (40 records of 31 masters)" {
			t.Fatalf("audit %+v", au.records)
		}
	})
	t.Run("ambiguous goes to a steward with the suggestions", func(t *testing.T) {
		r := &matchResolver{}
		sp := &fakeSplink{out: []identity.Suggestion{{Master: "C-1", Score: 0.97}, {Master: "C-2", Score: 0.6}}}
		_, err := (&Activities{Resolver: r, Splink: sp}).Resolve(context.Background(), in)
		u := unresolved(t, err)
		if r.linked != "" || len(u.Suggestions) != 2 || u.Suggestions[0].Master != "C-1" || u.Ref != "S-1" {
			t.Fatalf("linked %q, %+v", r.linked, u)
		}
	})
	t.Run("below the threshold goes to a steward", func(t *testing.T) {
		r := &matchResolver{}
		_, err := (&Activities{Resolver: r, Splink: &fakeSplink{out: []identity.Suggestion{{Master: "C-1", Score: 0.9}}}}).Resolve(context.Background(), in)
		unresolved(t, err)
		if r.linked != "" {
			t.Fatal("linked below the threshold")
		}
	})
	t.Run("no model yet: steward, with the built-in suggestions", func(t *testing.T) {
		r := &matchResolver{}
		_, err := (&Activities{Resolver: r, Splink: &fakeSplink{err: identity.ErrNoSplinkModel}}).Resolve(context.Background(), in)
		unresolved(t, err)
		if r.linked != "" || !strings.Contains(err.Error(), "no model") {
			t.Fatalf("linked %q err %v", r.linked, err)
		}
	})
	t.Run("not configured: steward, saying so", func(t *testing.T) {
		r := &matchResolver{}
		_, err := (&Activities{Resolver: r}).Resolve(context.Background(), in)
		unresolved(t, err)
		if r.linked != "" || !strings.Contains(err.Error(), "TURGON_SPLINK_URL") {
			t.Fatalf("linked %q err %v", r.linked, err)
		}
	})
	t.Run("service away: retried", func(t *testing.T) {
		_, err := (&Activities{Resolver: &matchResolver{}, Splink: &fakeSplink{err: errors.New("connection refused")}}).Resolve(context.Background(), in)
		var app *temporal.ApplicationError
		if err == nil || errors.As(err, &app) {
			t.Fatalf("want a retryable error, got %v", err)
		}
	})
	t.Run("refused: not retried", func(t *testing.T) {
		_, err := (&Activities{Resolver: &matchResolver{}, Splink: &fakeSplink{err: &identity.SplinkError{Status: 401, Message: "a bearer token is required"}}}).Resolve(context.Background(), in)
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ErrTypeInvalid {
			t.Fatalf("got %v", err)
		}
	})
}
