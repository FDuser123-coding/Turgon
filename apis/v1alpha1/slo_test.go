package v1alpha1

import (
	"strings"
	"testing"
)

func TestSLOLatencyMustBeADuration(t *testing.T) {
	for latency, ok := range map[string]bool{"2s": true, "500ms": true, "1m30s": true, "2 seconds": false, "-1s": false, "0s": false} {
		r := Recipe{Spec: RecipeSpec{
			Trigger: Trigger{Source: "s", Event: "E"},
			Steps:   []Step{{Map: &MapStep{From: "a", To: "b", Mapping: "m@1"}}},
			SLO:     &SLO{P95Latency: latency},
		}}
		bad := strings.Contains(r.Validate().Error(), "spec.slo.p95Latency")
		if bad == ok {
			t.Errorf("p95Latency %q: rejected = %v", latency, bad)
		}
	}
}
