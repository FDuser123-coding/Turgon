package console

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/engine"
)

// RunSummary is one integration run as listed in the console.
type RunSummary struct {
	ID       string                  `json:"id"`
	RunID    string                  `json:"runId"`
	Workflow string                  `json:"workflow"`
	Status   string                  `json:"status"`
	Started  time.Time               `json:"started"`
	Closed   *time.Time              `json:"closed,omitempty"`
	Pending  *engine.PendingApproval `json:"pending,omitempty"`
}

// RunDetail adds what a run was started with and how it ended.
type RunDetail struct {
	RunSummary
	SpecDigest  string            `json:"specDigest,omitempty"`
	Event       *connector.Event  `json:"event,omitempty"`
	Result      *engine.RunResult `json:"result,omitempty"`
	Failure     string            `json:"failure,omitempty"`
	FailureType string            `json:"failureType,omitempty"`
}

// Runs is what the console needs from the workflow engine.
type Runs interface {
	List(ctx context.Context, limit int) ([]RunSummary, error)
	Get(ctx context.Context, id string) (RunDetail, error)
	Pending(ctx context.Context, id string) (*engine.PendingApproval, error)
	Signal(ctx context.Context, id string, sig engine.ApprovalSignal) error
}

// ErrNotFound is returned for unknown runs.
var ErrNotFound = errors.New("run not found")

// TemporalRuns reads runs from Temporal.
type TemporalRuns struct {
	Client    client.Client
	Namespace string
}

func status(s enums.WorkflowExecutionStatus) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "WORKFLOW_EXECUTION_STATUS_"))
}

func workflowName(id string) string {
	if i := strings.LastIndex(id, "/"); i > 0 {
		return id[:i]
	}
	return id
}

func (t TemporalRuns) List(ctx context.Context, limit int) ([]RunSummary, error) {
	resp, err := t.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: t.Namespace,
		PageSize:  int32(limit),
		Query:     fmt.Sprintf("WorkflowType = '%s'", engine.WorkflowName),
	})
	if err != nil {
		return nil, err
	}
	runs := make([]RunSummary, 0, len(resp.Executions))
	for _, e := range resp.Executions {
		r := RunSummary{
			ID: e.GetExecution().GetWorkflowId(), RunID: e.GetExecution().GetRunId(),
			Workflow: workflowName(e.GetExecution().GetWorkflowId()), Status: status(e.GetStatus()),
			Started: e.GetStartTime().AsTime(),
		}
		if ct := e.GetCloseTime(); ct != nil && !ct.AsTime().IsZero() {
			c := ct.AsTime()
			r.Closed = &c
		}
		runs = append(runs, r)
	}
	sort.SliceStable(runs, func(i, j int) bool { return runs[i].Started.After(runs[j].Started) })

	// Ask running runs whether they wait for a person, a few at a time.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := range runs {
		if runs[i].Status != "running" {
			continue
		}
		wg.Add(1)
		go func(r *RunSummary) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if p, err := t.Pending(ctx, r.ID); err == nil {
				r.Pending = p
			}
		}(&runs[i])
	}
	wg.Wait()
	return runs, nil
}

func (t TemporalRuns) Pending(ctx context.Context, id string) (*engine.PendingApproval, error) {
	return engine.Pending(ctx, t.Client, id)
}

func (t TemporalRuns) Signal(ctx context.Context, id string, sig engine.ApprovalSignal) error {
	return t.Client.SignalWorkflow(ctx, id, "", engine.SignalApproval, sig)
}

func (t TemporalRuns) Get(ctx context.Context, id string) (RunDetail, error) {
	desc, err := t.Client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		var nf *serviceerror.NotFound
		if errors.As(err, &nf) {
			return RunDetail{}, ErrNotFound
		}
		return RunDetail{}, err
	}
	info := desc.GetWorkflowExecutionInfo()
	d := RunDetail{RunSummary: RunSummary{
		ID: id, RunID: info.GetExecution().GetRunId(), Workflow: workflowName(id),
		Status: status(info.GetStatus()), Started: info.GetStartTime().AsTime(),
	}}
	if ct := info.GetCloseTime(); ct != nil && !ct.AsTime().IsZero() {
		c := ct.AsTime()
		d.Closed = &c
	}
	if in, err := (engine.TemporalStarter{Client: t.Client}).Input(ctx, id); err == nil {
		d.SpecDigest, d.Event = in.SpecDigest, &in.Event
	}
	dc := converter.GetDefaultDataConverter()
	it := t.Client.GetWorkflowHistory(ctx, id, d.RunID, false, enums.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			break
		}
		if a := ev.GetWorkflowExecutionCompletedEventAttributes(); a != nil {
			var res engine.RunResult
			if dc.FromPayloads(a.GetResult(), &res) == nil {
				d.Result = &res
			}
		}
		if a := ev.GetWorkflowExecutionFailedEventAttributes(); a != nil {
			// Temporal wraps the cause ("activity error"); the application
			// error with its type and message is the innermost useful one.
			for c := a.GetFailure(); c != nil; c = c.GetCause() {
				if info := c.GetApplicationFailureInfo(); info != nil && info.GetType() != "" {
					d.FailureType, d.Failure = info.GetType(), c.GetMessage()
				} else if d.Failure == "" {
					d.Failure = c.GetMessage()
				}
			}
		}
	}
	if d.Status == "running" {
		d.Pending, _ = t.Pending(ctx, id)
	}
	return d, nil
}
