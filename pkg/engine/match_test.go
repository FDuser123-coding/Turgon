package engine

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
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
