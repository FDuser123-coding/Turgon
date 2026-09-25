package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// Operators retry failed runs from the console: after an approval nobody
// answered in time, or once the cause of a failure is fixed. A retried run
// starts again from its event with the same idempotency keys, so writes
// that already happened are not repeated.

// Retrier starts a failed run again.
type Retrier interface {
	Retry(ctx context.Context, id string) error
}

// Retryable reports whether a run in this status can be started again.
func Retryable(status string) bool {
	switch status {
	case "failed", "timed_out", "terminated", "canceled":
		return true
	}
	return false
}

// RetryRequest asks to start a failed run again.
type RetryRequest struct {
	ID   string `json:"id"`
	Note string `json:"note"`
}

func (s *Server) retryRun(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r)
	if !user.Has(RoleOperator) {
		writeError(w, http.StatusForbidden, "you are not an operator")
		return
	}
	if s.cfg.Retry == nil || s.cfg.Recorder == nil {
		writeError(w, http.StatusServiceUnavailable, "retrying runs needs Turgon's state database for the audit log; start the console with --database-url")
		return
	}
	var req RetryRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid retry: "+err.Error())
		return
	}
	req.Note = strings.TrimSpace(req.Note)
	if req.ID == "" || req.Note == "" || len(req.Note) > 500 {
		writeError(w, http.StatusBadRequest, "id and a note (why, at most 500 characters) are required")
		return
	}
	run, err := s.cfg.Runs.Get(r.Context(), req.ID)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "run not found")
		return
	case err != nil:
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	case !Retryable(run.Status):
		writeError(w, http.StatusConflict, "only failed runs can be retried; this one is "+run.Status)
		return
	}
	// Recorded before the retry, so no retry is ever unaudited.
	data := map[string]any{"runs": []string{req.ID}, "reason": req.Note, "status": run.Status}
	if run.FailureType != "" {
		data["failure"] = run.FailureType
	}
	if _, err := s.record(user.ID, "run.retried", data); err != nil {
		writeError(w, http.StatusBadGateway, "audit log: "+err.Error())
		return
	}
	if err := s.cfg.Retry.Retry(r.Context(), req.ID); err != nil {
		_, _ = s.record(user.ID, "run.retry-failed", map[string]any{"runs": []string{req.ID}, "error": err.Error()})
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": req.ID})
}
