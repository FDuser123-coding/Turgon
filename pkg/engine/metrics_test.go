package engine

import (
	"fmt"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/fduser123-coding/turgon/pkg/metrics"
)

// metricValue sums a metric's samples whose labels include want: counters
// and gauges by value, histograms by observation count.
func metricValue(t *testing.T, name string, want map[string]string) float64 {
	t.Helper()
	return gather(t, name, want, false)
}

// histogramSum sums a histogram's observed values.
func histogramSum(t *testing.T, name string, want map[string]string) float64 {
	t.Helper()
	return gather(t, name, want, true)
}

func gather(t *testing.T, name string, want map[string]string, sums bool) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	sum := 0.0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	next:
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			for k, v := range want {
				if labels[k] != v {
					continue next
				}
			}
			switch f.GetType() {
			case dto.MetricType_COUNTER:
				sum += m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				sum += m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				if sums {
					sum += m.GetHistogram().GetSampleSum()
				} else {
					sum += float64(m.GetHistogram().GetSampleCount())
				}
			}
		}
	}
	return sum
}

// A run records its outcome, its active time and the approval it waited
// for; the write guard records the write and how long the target took.
func TestRunsAndWritesAreMeasured(t *testing.T) {
	f, shop, id := newShopifyFixture(t)
	wf := "shopify-store-orders-to-erp"
	before := map[string]float64{
		"completed": metricValue(t, "turgon_runs_finished_total", map[string]string{"workflow": wf, "outcome": "completed"}),
		"approved":  metricValue(t, "turgon_approvals_total", map[string]string{"workflow": wf, "decision": "approved"}),
		"active":    metricValue(t, "turgon_run_active_seconds", map[string]string{"workflow": wf}),
		"committed": metricValue(t, "turgon_writes_total", map[string]string{"target": "erp-db", "operation": "create-sales-order", "status": "committed"}),
		"duration":  metricValue(t, "turgon_write_duration_seconds", map[string]string{"target": "erp-db", "operation": "create-sales-order"}),
		"polled":    metricValue(t, "turgon_events_total", map[string]string{"workflow": wf, "via": "poll"}),
	}
	activeBefore := histogramSum(t, "turgon_run_active_seconds", map[string]string{"workflow": wf})
	if _, err, _ := f.run(f.dispatch()[0], approve("03-write")); err != nil {
		t.Fatal(err)
	}
	after := map[string]float64{
		"completed": metricValue(t, "turgon_runs_finished_total", map[string]string{"workflow": wf, "outcome": "completed"}),
		"approved":  metricValue(t, "turgon_approvals_total", map[string]string{"workflow": wf, "decision": "approved"}),
		"active":    metricValue(t, "turgon_run_active_seconds", map[string]string{"workflow": wf}),
		"committed": metricValue(t, "turgon_writes_total", map[string]string{"target": "erp-db", "operation": "create-sales-order", "status": "committed"}),
		"duration":  metricValue(t, "turgon_write_duration_seconds", map[string]string{"target": "erp-db", "operation": "create-sales-order"}),
		"polled":    metricValue(t, "turgon_events_total", map[string]string{"workflow": wf, "via": "poll"}),
	}
	for k := range before {
		if after[k]-before[k] != 1 {
			t.Errorf("%s: %v → %v, want one more (order %d)", k, before[k], after[k], id)
		}
	}
	// The hour the approval took is recorded as waiting, not active time.
	if w := histogramSum(t, "turgon_approval_wait_seconds", map[string]string{"workflow": wf}); w < 3600 {
		t.Errorf("approval wait = %vs, want the simulated hour", w)
	}
	if active := histogramSum(t, "turgon_run_active_seconds", map[string]string{"workflow": wf}) - activeBefore; active >= 60 {
		t.Errorf("active time = %vs; the approval wait was counted", active)
	}
	_ = shop
}

// A failed run records its reason.
func TestFailedRunsRecordTheirReason(t *testing.T) {
	f, shop, _ := newShopifyFixture(t)
	shop.FailUpdates = true
	wf := "shopify-store-orders-to-erp"
	before := metricValue(t, "turgon_runs_finished_total", map[string]string{"workflow": wf, "outcome": "failed", "reason": ErrTypeInvalid})
	_, err, _ := f.run(f.dispatch()[0], approve("03-write"))
	if err == nil {
		t.Fatal("run did not fail")
	}
	if got := metricValue(t, "turgon_runs_finished_total", map[string]string{"workflow": wf, "outcome": "failed", "reason": ErrTypeInvalid}); got-before != 1 {
		t.Fatalf("failed runs %v → %v (%s)", before, got, fmt.Sprint(err))
	}
	if n := metricValue(t, "turgon_writes_total", map[string]string{"target": "erp-db", "status": "compensated"}); n < 1 {
		t.Fatal("the compensation was not counted")
	}
}
