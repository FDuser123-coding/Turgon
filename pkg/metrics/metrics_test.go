package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTemporalHandlerKeepsOneLabelSetPerName(t *testing.T) {
	h := Temporal().WithTags(map[string]string{"task_queue": "q", "workflow": "w"})
	h.Counter("test_events_total").Inc(2)
	// Different tags later: missing ones record as "", extra ones are
	// dropped, and nothing panics.
	h.WithTags(map[string]string{"extra": "x"}).Counter("test_events_total").Inc(1)
	Temporal().Counter("test_events_total").Inc(1)
	h.Gauge("test_level").Update(3)
	h.Timer("test_latency").Record(1500 * time.Millisecond)
	// A name already used by a metric of another kind records nowhere.
	h.Gauge("test_events_total").Update(7)
	h.Counter("test_negative_total").Inc(-5)

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`test_events_total{task_queue="q",workflow="w"} 3`,
		`test_events_total{task_queue="",workflow=""} 1`,
		`test_level{task_queue="q",workflow="w"} 3`,
		`test_latency_seconds_sum{task_queue="q",workflow="w"} 1.5`,
		`test_negative_total{task_queue="q",workflow="w"} 0`,
		"go_goroutines",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}
