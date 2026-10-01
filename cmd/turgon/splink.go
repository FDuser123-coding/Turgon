package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/identity"
)

// splinkFromEnv is the Splink service resolve steps with strategy splink
// use (TURGON_SPLINK_URL, TURGON_SPLINK_TOKEN); nil when none is set.
func splinkFromEnv() (*identity.Splink, error) {
	raw := strings.TrimSpace(os.Getenv("TURGON_SPLINK_URL"))
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("TURGON_SPLINK_URL %q: want http(s)://host:port", raw)
	}
	return &identity.Splink{URL: raw, Token: os.Getenv("TURGON_SPLINK_TOKEN")}, nil
}

// splinkEntities lists the entities the spec's resolve steps match with
// strategy splink.
func splinkEntities(spec *compiler.RuntimeSpec) []string {
	seen := map[string]bool{}
	for _, w := range spec.Spec.Workflows {
		for _, st := range w.Steps {
			if st.Resolve != nil && st.Resolve.Strategy == v1alpha1.StrategySplink {
				seen[strings.TrimPrefix(st.Resolve.Entity, "model.")] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for e := range seen {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// checkSplink checks the Splink service a spec's splink resolve steps
// need: that it is configured, answers, and has a model per entity.
func checkSplink(ctx context.Context, spec *compiler.RuntimeSpec) []connector.CheckResult {
	entities := splinkEntities(spec)
	if len(entities) == 0 {
		return nil
	}
	s, err := splinkFromEnv()
	if err != nil {
		return []connector.CheckResult{connector.Fail("splink", err.Error(), "Set TURGON_SPLINK_URL to the Splink service's URL, such as http://turgon-splink:8080.")}
	}
	if s == nil {
		return []connector.CheckResult{connector.Fail("splink", "resolve steps use strategy splink but TURGON_SPLINK_URL is not set",
			"Deploy the Splink service (deploy/splink, or splink.enabled in the Helm chart) and set TURGON_SPLINK_URL on the workers; until then unmatched records go to a data steward.")}
	}
	models, err := s.Health(ctx)
	if err != nil {
		return []connector.CheckResult{connector.Fail("splink", err.Error(), connector.NetworkFix(err, "the Splink service at "+s.URL))}
	}
	var out []connector.CheckResult
	for _, e := range entities {
		m, ok := models[e]
		if !ok {
			out = append(out, connector.Pass("splink "+e, "no model yet: records go to a data steward until links train one"))
			continue
		}
		trained := strings.Join(m.Trained, ", ")
		if trained == "" {
			trained = "none, starting values"
		}
		out = append(out, connector.Pass("splink "+e, fmt.Sprintf("%d records of %d masters, trained: %s, at %s", m.Records, m.Masters, trained, m.TrainedAt)))
	}
	return out
}
