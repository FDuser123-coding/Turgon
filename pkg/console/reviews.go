package console

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// Mapping review (architecture §7.4): fields an AI or a person mapped with
// too little confidence wait in the catalog's review queue until a data
// steward approves or rejects them here. A review holds for one expression
// of one mapping version.

// ReviewStore keeps reviews (implemented by pgstore).
type ReviewStore interface {
	Put(ctx context.Context, rv verifier.Review) error
	All(ctx context.Context) (verifier.Reviews, error)
}

// ReviewRequest is a steward's decision on a queued field.
type ReviewRequest struct {
	Mapping    string `json:"mapping"`
	Target     string `json:"target"`
	Expression string `json:"expression"`
	Decision   string `json:"decision"`
	Note       string `json:"note,omitempty"`
}

// verifierOptions applies the stored reviews.
func (s *Server) verifierOptions(ctx context.Context) (verifier.Options, error) {
	if s.cfg.Reviews == nil {
		return verifier.Options{}, nil
	}
	rv, err := s.cfg.Reviews.All(ctx)
	return verifier.Options{Reviews: rv}, err
}

func (s *Server) reviewField(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r)
	if !user.Has(RoleSteward) {
		writeError(w, http.StatusForbidden, "you are not a data steward")
		return
	}
	if s.cfg.Reviews == nil || s.cfg.Recorder == nil {
		writeError(w, http.StatusServiceUnavailable, "mapping reviews need Turgon's state database; start the console with --database-url")
		return
	}
	var req ReviewRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid review: "+err.Error())
		return
	}
	req.Note = strings.TrimSpace(req.Note)
	switch {
	case req.Decision != verifier.ReviewApproved && req.Decision != verifier.ReviewRejected:
		writeError(w, http.StatusBadRequest, "decision must be approved or rejected")
		return
	case req.Decision == verifier.ReviewRejected && req.Note == "":
		writeError(w, http.StatusBadRequest, "say why the mapping is wrong, so its author can fix it")
		return
	case len(req.Note) > 1000:
		writeError(w, http.StatusBadRequest, "the note is too long")
		return
	}
	// Only fields the queue holds can be reviewed, with the expression the
	// catalog has now: the console cannot approve arbitrary mappings, nor
	// an expression that changed since the page was loaded.
	if !s.inReviewQueue(req) {
		writeError(w, http.StatusConflict, "this field is no longer waiting for review with this expression; reload")
		return
	}
	rv := verifier.Review{Mapping: req.Mapping, Target: req.Target, Expression: req.Expression,
		Decision: req.Decision, Reviewer: user.ID, Note: req.Note, At: time.Now().UTC()}
	if err := s.cfg.Reviews.Put(r.Context(), rv); err != nil {
		writeError(w, http.StatusBadGateway, "state database: "+err.Error())
		return
	}
	if _, err := s.record(user.ID, "mapping.reviewed", map[string]any{
		"mapping": rv.Mapping, "target": rv.Target, "expression": rv.Expression,
		"expressionSha256": verifier.ExpressionDigest(rv.Expression), "decision": rv.Decision, "note": rv.Note,
	}); err != nil {
		writeError(w, http.StatusBadGateway, "audit log: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rv)
}

func (s *Server) inReviewQueue(req ReviewRequest) bool {
	if len(s.cfg.Catalogs) == 0 {
		return false
	}
	cat, err := catalog.Load(s.cfg.Catalogs...)
	if err != nil {
		return false
	}
	// Verified without the stored reviews, so a field can be reviewed
	// again (a rejection reconsidered).
	v := verifier.New(cat, verifier.Options{})
	for _, obj := range cat.All() {
		for _, it := range queued(v, obj) {
			if it.Mapping == req.Mapping && it.Target == req.Target && it.Expression == req.Expression {
				return true
			}
		}
	}
	return false
}
