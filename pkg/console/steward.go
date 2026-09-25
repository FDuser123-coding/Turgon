package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/identity"
)

// The data-steward queue (architecture §7.3): runs that stopped because a
// source record has no master record. A steward links the record to its
// master record; the console stores the cross-reference and retries every
// run that was waiting on it.

// UnresolvedRun is a failed run waiting for a steward.
type UnresolvedRun struct {
	ID       string            `json:"id"`
	Workflow string            `json:"workflow"`
	Started  time.Time         `json:"started"`
	Failed   time.Time         `json:"failed"`
	Link     engine.Unresolved `json:"link"`
	Event    *connector.Event  `json:"event,omitempty"`
}

// StewardItem is one missing link and the runs waiting on it, with the
// newest run's identifying attributes and suggested master records.
type StewardItem struct {
	linkKey
	Attributes  identity.Attributes   `json:"attributes,omitempty"`
	Suggestions []identity.Suggestion `json:"suggestions,omitempty"`
	Since       time.Time             `json:"since"`
	Runs        []UnresolvedRun       `json:"runs"`
}

// linkKey names a source record.
type linkKey struct {
	Entity string `json:"entity"`
	System string `json:"system"`
	Ref    string `json:"ref"`
}

func keyOf(u engine.Unresolved) linkKey { return linkKey{u.Entity, u.System, u.Ref} }

// StewardRuns is what the queue needs from the workflow engine.
type StewardRuns interface {
	// Unresolved lists runs whose latest attempt failed for want of a
	// master record, newest first.
	Unresolved(ctx context.Context, limit int) ([]UnresolvedRun, error)
	// Retry starts a failed run again for the same event.
	Retry(ctx context.Context, id string) error
}

// XrefStore stores identity cross-references with the source record's
// identifying attributes, which later records are matched against.
type XrefStore interface {
	Link(ctx context.Context, entity, system, sourceID, masterID string, attrs identity.Attributes) error
}

// Unresolved implements StewardRuns on Temporal.
func (t TemporalRuns) Unresolved(ctx context.Context, limit int) ([]UnresolvedRun, error) {
	resp, err := t.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: t.Namespace,
		PageSize:  int32(limit),
		Query:     fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Failed'", engine.WorkflowName),
	})
	if err != nil {
		return nil, err
	}
	dc := converter.GetDefaultDataConverter()
	seen := map[string]bool{}
	var out []UnresolvedRun
	for _, e := range resp.Executions {
		id := e.GetExecution().GetWorkflowId()
		if seen[id] {
			continue
		}
		seen[id] = true
		// A run retried since may have moved on: only its latest attempt counts.
		desc, err := t.Client.DescribeWorkflowExecution(ctx, id, "")
		if err != nil || desc.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
			continue
		}
		info := desc.GetWorkflowExecutionInfo()
		run := UnresolvedRun{ID: id, Workflow: workflowName(id), Started: info.GetStartTime().AsTime(), Failed: info.GetCloseTime().AsTime()}
		found := false
		it := t.Client.GetWorkflowHistory(ctx, id, info.GetExecution().GetRunId(), false, enums.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
		for it.HasNext() && !found {
			ev, err := it.Next()
			if err != nil {
				break
			}
			a := ev.GetWorkflowExecutionFailedEventAttributes()
			for c := a.GetFailure(); c != nil && !found; c = c.GetCause() {
				app := c.GetApplicationFailureInfo()
				if app.GetType() == engine.ErrTypeUnresolved && dc.FromPayloads(app.GetDetails(), &run.Link) == nil && run.Link.Ref != "" {
					found = true
				}
			}
		}
		if !found {
			continue
		}
		if in, err := (engine.TemporalStarter{Client: t.Client}).Input(ctx, id); err == nil {
			run.Event = &in.Event
		}
		out = append(out, run)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Failed.After(out[j].Failed) })
	return out, nil
}

// Retry implements StewardRuns on Temporal.
func (t TemporalRuns) Retry(ctx context.Context, id string) error {
	_, err := (engine.TemporalStarter{Client: t.Client}).Retry(ctx, id, nil)
	return err
}

// group collects runs by the link they wait for, oldest wait first. Runs
// arrive newest first, so each item carries its newest run's suggestions.
func group(runs []UnresolvedRun) []StewardItem {
	byLink := map[linkKey]*StewardItem{}
	var order []linkKey
	for _, r := range runs {
		k := keyOf(r.Link)
		it, ok := byLink[k]
		if !ok {
			it = &StewardItem{linkKey: k, Attributes: r.Link.Attributes, Suggestions: r.Link.Suggestions, Since: r.Failed}
			byLink[k] = it
			order = append(order, k)
		}
		it.Runs = append(it.Runs, r)
		if r.Failed.Before(it.Since) {
			it.Since = r.Failed
		}
	}
	out := make([]StewardItem, 0, len(order))
	for _, k := range order {
		out = append(out, *byLink[k])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Since.Before(out[j].Since) })
	return out
}

func (s *Server) stewardQueue(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Steward == nil {
		writeJSON(w, http.StatusOK, []StewardItem{})
		return
	}
	runs, err := s.cfg.Steward.Unresolved(r.Context(), 200)
	if err != nil {
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, group(runs))
}

// Link is a steward's decision.
type Link struct {
	linkKey
	Master string `json:"master"`
	Note   string `json:"note,omitempty"`
}

// LinkResult reports what a link did.
type LinkResult struct {
	Retried []string          `json:"retried"`
	Failed  map[string]string `json:"failed,omitempty"`
}

var masterRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,199}$`)

func (s *Server) stewardLink(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r)
	if !user.Has(RoleSteward) {
		writeError(w, http.StatusForbidden, "you are not a data steward")
		return
	}
	if s.cfg.Steward == nil || s.cfg.Xref == nil || s.cfg.Recorder == nil {
		writeError(w, http.StatusServiceUnavailable, "the steward queue needs Turgon's state database; start the console with --database-url")
		return
	}
	var l Link
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&l); err != nil {
		writeError(w, http.StatusBadRequest, "invalid link: "+err.Error())
		return
	}
	l.Master, l.Note = strings.TrimSpace(l.Master), strings.TrimSpace(l.Note)
	if l.Entity == "" || l.System == "" || l.Ref == "" || !masterRE.MatchString(l.Master) || len(l.Note) > 500 {
		writeError(w, http.StatusBadRequest, "entity, system, ref and a master ID (letters, digits and . _ : @ / -) are required")
		return
	}
	// Only links the queue is waiting for can be made here, so the console
	// cannot be used to rewrite arbitrary cross-references.
	runs, err := s.cfg.Steward.Unresolved(r.Context(), 200)
	if err != nil {
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	var waiting []string
	var attrs identity.Attributes
	for _, run := range runs {
		if keyOf(run.Link) == l.linkKey {
			waiting = append(waiting, run.ID)
			if attrs == nil {
				attrs = run.Link.Attributes // the newest run's
			}
		}
	}
	if len(waiting) == 0 {
		writeError(w, http.StatusConflict, "no run is waiting on this record any more; reload")
		return
	}
	// The record's attributes come from the run, not the request, so later
	// matches learn from what the source system said.
	if err := s.cfg.Xref.Link(r.Context(), l.Entity, l.System, l.Ref, l.Master, attrs); err != nil {
		writeError(w, http.StatusBadGateway, "state database: "+err.Error())
		return
	}
	if _, err := s.record(user.ID, "xref.linked", map[string]any{
		"entity": l.Entity, "system": l.System, "ref": l.Ref, "master": l.Master, "note": l.Note, "runs": waiting,
	}); err != nil {
		writeError(w, http.StatusBadGateway, "audit log: "+err.Error())
		return
	}
	res := LinkResult{Retried: []string{}}
	for _, id := range waiting {
		if err := s.cfg.Steward.Retry(r.Context(), id); err != nil {
			if res.Failed == nil {
				res.Failed = map[string]string{}
			}
			res.Failed[id] = err.Error()
			continue
		}
		res.Retried = append(res.Retried, id)
	}
	if len(res.Retried) > 0 {
		if _, err := s.record(user.ID, "run.retried", map[string]any{"runs": res.Retried, "reason": "linked " + l.Ref + " to " + l.Master}); err != nil {
			writeError(w, http.StatusBadGateway, "audit log: "+err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) record(actor, action string, data any) (audit.Entry, error) {
	return s.cfg.Recorder.Record(actor, action, data)
}
