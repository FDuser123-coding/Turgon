package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// The approval ledger holds, for each write waiting for a person, the run
// asking and the runs (later changes of the record) waiting on its
// decision. A run that ends without closing its request (terminated from
// Temporal, say) leaves it held until it expires; operators release it.

// Ledger is the approval ledger (pgstore.Store).
type Ledger = writeguard.ApprovalLedgerAdmin

// Waker wakes runs that waited on a released request.
type Waker interface {
	Wake(ctx context.Context, key string, waiters []string) int
}

// Wake implements Waker on Temporal.
func (t TemporalRuns) Wake(ctx context.Context, key string, waiters []string) int {
	return engine.WakeWaiters(ctx, t.Client, key, waiters)
}

// HeldRequest is an open request, with the status of the run holding it.
type HeldRequest struct {
	writeguard.HeldApproval
	// HolderStatus is the holding run's status ("running", "terminated",
	// ...), or "missing" when Temporal no longer knows it.
	HolderStatus string `json:"holderStatus"`
	// Orphaned: the run holding it is not running, so nobody will close it.
	Orphaned bool `json:"orphaned"`
}

func (s *Server) ledger(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Ledger == nil {
		writeJSON(w, http.StatusOK, []HeldRequest{})
		return
	}
	held, err := s.cfg.Ledger.OpenApprovals()
	if err != nil {
		writeError(w, http.StatusBadGateway, "approval ledger: "+err.Error())
		return
	}
	out := make([]HeldRequest, 0, len(held))
	for _, h := range held {
		// One run Temporal cannot describe does not hide the others; it
		// is not called orphaned, since it may still run.
		status, err := s.holderStatus(r.Context(), h.Holder)
		if err != nil {
			out = append(out, HeldRequest{HeldApproval: h, HolderStatus: "unknown"})
			continue
		}
		out = append(out, HeldRequest{HeldApproval: h, HolderStatus: status, Orphaned: status != "running"})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) holderStatus(ctx context.Context, run string) (string, error) {
	d, err := s.cfg.Runs.Get(ctx, run)
	if errors.Is(err, ErrNotFound) {
		return "missing", nil
	}
	if err != nil {
		return "", err
	}
	return d.Status, nil
}

// ReleaseRequest asks to release an orphaned request.
type ReleaseRequest struct {
	Key    string `json:"key"`
	Holder string `json:"holder"`
	Note   string `json:"note"`
}

// ReleaseResult says how many waiting runs were woken.
type ReleaseResult struct {
	Waiters int `json:"waiters"`
	Woken   int `json:"woken"`
}

func (s *Server) releaseRequest(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r)
	if !user.Has(RoleOperator) {
		writeError(w, http.StatusForbidden, "you are not an operator")
		return
	}
	if s.cfg.Ledger == nil || s.cfg.Waker == nil || s.cfg.Recorder == nil {
		writeError(w, http.StatusServiceUnavailable, "releasing requests needs Turgon's state database; start the console with --database-url")
		return
	}
	var req ReleaseRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid release: "+err.Error())
		return
	}
	req.Note = strings.TrimSpace(req.Note)
	if req.Key == "" || req.Holder == "" || req.Note == "" || utf8.RuneCountInString(req.Note) > 500 {
		writeError(w, http.StatusBadRequest, "key, holder and a note (why, at most 500 characters) are required")
		return
	}
	status, err := s.holderStatus(r.Context(), req.Holder)
	if err != nil {
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	if status == "running" {
		writeError(w, http.StatusConflict, "the run holding this request is still running: approve or reject its write instead")
		return
	}
	// Recorded before the release, so no release is ever unaudited.
	if _, err := s.record(user.ID, "approval.released", map[string]any{"key": req.Key, "holder": req.Holder, "holderStatus": status, "reason": req.Note}); err != nil {
		writeError(w, http.StatusBadGateway, "audit log: "+err.Error())
		return
	}
	waiters, released, err := s.cfg.Ledger.ReleaseApproval(req.Key, req.Holder)
	if err != nil || !released {
		// The release was audited before it was tried: say it failed.
		status, msg := http.StatusConflict, "that run no longer holds this request"
		if err != nil {
			status, msg = http.StatusBadGateway, "approval ledger: "+err.Error()
		}
		if _, aerr := s.record(user.ID, "approval.release-failed", map[string]any{"key": req.Key, "holder": req.Holder, "error": msg}); aerr != nil {
			msg += "; audit log: " + aerr.Error()
		}
		writeError(w, status, msg)
		return
	}
	writeJSON(w, http.StatusOK, ReleaseResult{Waiters: len(waiters), Woken: s.cfg.Waker.Wake(r.Context(), req.Key, waiters)})
}
