package console

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/audit"
)

// failedRuns wraps fakeRuns with finished runs, and retries them.
type failedRuns struct {
	*fakeRuns
	status  map[string]string
	retried []string
	fail    error
}

func (f *failedRuns) Get(ctx context.Context, id string) (RunDetail, error) {
	if s, ok := f.status[id]; ok {
		return RunDetail{RunSummary: RunSummary{ID: id, Status: s}, FailureType: "TurgonRejected"}, nil
	}
	return f.fakeRuns.Get(ctx, id)
}

func (f *failedRuns) Retry(_ context.Context, id string) error {
	if f.fail != nil {
		return f.fail
	}
	f.retried = append(f.retried, id)
	f.status[id] = "running"
	return nil
}

func operatorAuth() ProxyAuth {
	a := proxyAuth()
	a.OperatorGroup = "turgon-operators"
	return a
}

func TestOperatorsRetryFailedRuns(t *testing.T) {
	runs := &failedRuns{fakeRuns: newRuns(), status: map[string]string{
		"hubspot-won-deals-to-erp/1": "failed", "hubspot-won-deals-to-erp/2": "completed",
	}}
	var log bytes.Buffer
	s := New(Config{Runs: runs, Auth: operatorAuth(), Retry: runs, Recorder: audit.New(&log)})
	body := `{"id":"hubspot-won-deals-to-erp/1","note":"approver was on holiday; asking again"}`

	for name, c := range map[string]struct {
		body, groups string
		code         int
	}{
		"approvers are not operators": {body, "turgon-approvers", http.StatusForbidden},
		"a reason is required":        {strings.Replace(body, "approver was on holiday; asking again", " ", 1), "turgon-operators", http.StatusBadRequest},
		"unknown run":                 {strings.Replace(body, "/1", "/9", 1), "turgon-operators", http.StatusNotFound},
		"completed runs stay done":    {strings.Replace(body, "/1", "/2", 1), "turgon-operators", http.StatusConflict},
		"waiting runs are not failed": {`{"id":"shop-orders-to-erp/7","note":"x"}`, "turgon-operators", http.StatusConflict},
		"unknown fields":              {strings.Replace(body, `"note"`, `"spec":"x","note"`, 1), "turgon-operators", http.StatusBadRequest},
	} {
		if rec := do(t, s, "POST", "/api/runs/retry", c.body, as("olga@example.com", c.groups)); rec.Code != c.code {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
	if len(runs.retried) != 0 || log.Len() != 0 {
		t.Fatalf("refused requests changed state: %v\n%s", runs.retried, log.String())
	}

	rec := do(t, s, "POST", "/api/runs/retry", body, as("olga@example.com", "turgon-operators"))
	if rec.Code != 200 || len(runs.retried) != 1 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	entries := log.String()
	if !strings.Contains(entries, `"actor":"olga@example.com","action":"run.retried"`) || !strings.Contains(entries, "on holiday") ||
		!strings.Contains(entries, `"failure":"TurgonRejected"`) {
		t.Fatalf("audit:\n%s", entries)
	}
	// Running again: a second retry is refused.
	if rec := do(t, s, "POST", "/api/runs/retry", body, as("olga@example.com", "turgon-operators")); rec.Code != http.StatusConflict {
		t.Fatalf("second retry: %d", rec.Code)
	}
}

func TestRetryFailuresAreAudited(t *testing.T) {
	runs := &failedRuns{fakeRuns: newRuns(), status: map[string]string{"x/1": "timed_out"}, fail: errors.New("workflow execution already started")}
	var log bytes.Buffer
	s := New(Config{Runs: runs, Auth: operatorAuth(), Retry: runs, Recorder: audit.New(&log)})
	rec := do(t, s, "POST", "/api/runs/retry", `{"id":"x/1","note":"retry"}`, as("olga@example.com", "turgon-operators"))
	if rec.Code != http.StatusBadGateway || !strings.Contains(log.String(), `"action":"run.retry-failed"`) {
		t.Fatalf("%d %s\n%s", rec.Code, rec.Body, log.String())
	}
	if _, err := audit.Verify(strings.NewReader(log.String())); err != nil {
		t.Fatal(err)
	}
}

func TestRetryNeedsTheAuditLog(t *testing.T) {
	runs := &failedRuns{fakeRuns: newRuns(), status: map[string]string{"x/1": "failed"}}
	s := New(Config{Runs: runs, Auth: operatorAuth(), Retry: runs})
	rec := do(t, s, "POST", "/api/runs/retry", `{"id":"x/1","note":"retry"}`, as("olga@example.com", "turgon-operators"))
	if rec.Code != http.StatusServiceUnavailable || len(runs.retried) != 0 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}
