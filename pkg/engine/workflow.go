package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// DefaultApprovalTimeout is how long a run waits for a person.
const DefaultApprovalTimeout = 72 * time.Hour

// a is used only to name activity methods in ExecuteActivity calls.
var a *Activities

type committedWrite struct {
	step    string
	request writeguard.Request
	result  json.RawMessage
}

// IntegrationWorkflow interprets one compiled workflow for one event.
func IntegrationWorkflow(ctx workflow.Context, in RunInput) (RunResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        8,
			NonRetryableErrorTypes: []string{ErrTypeDenied, ErrTypeRejected, ErrTypeInvalid, ErrTypeUnresolved, ErrTypeMapping},
		},
	})
	logger := workflow.GetLogger(ctx)
	timeout := in.ApprovalTimeout
	if timeout <= 0 {
		timeout = DefaultApprovalTimeout
	}

	var source map[string]any
	if err := json.Unmarshal(in.Event.Payload, &source); err != nil {
		return RunResult{}, temporal.NewNonRetryableApplicationError("event payload is not a JSON object", ErrTypeInvalid, err)
	}
	doc := source

	var pending *PendingApproval
	if err := workflow.SetQueryHandler(ctx, QueryPending, func() (*PendingApproval, error) { return pending, nil }); err != nil {
		return RunResult{}, err
	}
	approvals := workflow.GetSignalChannel(ctx, SignalApproval)

	var result RunResult
	var done []committedWrite
	fail := func(err error) (RunResult, error) {
		if len(done) == 0 {
			return result, err
		}
		logger.Warn("step failed; compensating committed writes", "error", err, "writes", len(done))
		var errs []error
		for i := len(done) - 1; i >= 0; i-- {
			w := done[i]
			cerr := workflow.ExecuteActivity(ctx, a.Compensate, CompensateInput{Request: w.request, Result: w.result}).Get(ctx, nil)
			if cerr != nil {
				errs = append(errs, fmt.Errorf("compensate %s: %w", w.step, cerr))
			}
		}
		if len(errs) == 0 {
			// Every write was undone: report the step's own failure.
			return result, err
		}
		// Some writes could not be undone; an operator must act.
		return result, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("step failed and %d compensation(s) failed: %v", len(errs), errors.Join(errs...)),
			ErrTypeCompensationFailed, err)
	}

	for _, step := range in.Workflow.Steps {
		switch {
		// Results decode into a fresh map: decoding into doc would merge
		// keys into it (and into source, which doc starts as).
		case step.Map != nil:
			var next map[string]any
			if err := workflow.ExecuteActivity(ctx, a.Map, MapInput{Config: *step.Map, Doc: doc}).Get(ctx, &next); err != nil {
				return fail(err)
			}
			doc = next
		case step.Resolve != nil:
			var next map[string]any
			in := ResolveInput{Config: *step.Resolve, System: in.Workflow.Trigger.Endpoint, Doc: doc}
			if err := workflow.ExecuteActivity(ctx, a.Resolve, in).Get(ctx, &next); err != nil {
				return fail(err)
			}
			doc = next
		case step.Write != nil:
			var prep PrepareOutput
			pin := PrepareInput{Workflow: in.Workflow.Name, Step: step.Name, Config: *step.Write, Source: source, Doc: doc}
			if err := workflow.ExecuteActivity(ctx, a.PrepareWrite, pin).Get(ctx, &prep); err != nil {
				return fail(err)
			}
			rec := WriteRecord{Step: step.Name, Endpoint: step.Write.Endpoint, Operation: step.Write.Operation}
			if d := prep.Prepared.Duplicate; d != nil {
				rec.Status, rec.Result = d.Status, d.Result
				// Written before this run: not ours to compensate.
				result.Writes = append(result.Writes, rec)
				doc = withOutput(doc, step.Write.Output, d.Result)
				continue
			}

			var approval *policy.Approval
			if prep.Prepared.NeedsApproval {
				pending = &PendingApproval{
					Step: step.Name, Request: prep.Request, Preview: prep.Prepared.Preview,
					Reasons: prep.Prepared.Decision.Reasons, Since: workflow.Now(ctx),
				}
				approval = awaitApproval(ctx, approvals, step.Name, timeout)
				pending = nil
			}

			var out writeguard.Outcome
			cin := CommitInput{Request: prep.Request, Approval: approval}
			if err := workflow.ExecuteActivity(ctx, a.CommitWrite, cin).Get(ctx, &out); err != nil {
				return fail(err)
			}
			rec.Status, rec.Result = out.Status, out.Result
			result.Writes = append(result.Writes, rec)
			done = append(done, committedWrite{step: step.Name, request: prep.Request, result: out.Result})
			doc = withOutput(doc, step.Write.Output, out.Result)
		}
	}
	return result, nil
}

// withOutput returns a copy of doc with a write's result stored under field.
func withOutput(doc map[string]any, field string, result json.RawMessage) map[string]any {
	if field == "" {
		return doc
	}
	next := make(map[string]any, len(doc)+1)
	for k, v := range doc {
		next[k] = v
	}
	var v any
	if json.Unmarshal(result, &v) == nil {
		next[field] = v
	}
	return next
}

// awaitApproval blocks until an approval signal for step arrives or the
// timeout passes; a timeout is recorded as a rejection.
func awaitApproval(ctx workflow.Context, ch workflow.ReceiveChannel, step string, timeout time.Duration) *policy.Approval {
	tctx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	timer := workflow.NewTimer(tctx, timeout)
	var got *policy.Approval
	for got == nil {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
			var sig ApprovalSignal
			c.Receive(ctx, &sig)
			if sig.Step != step && sig.Step != "" {
				workflow.GetLogger(ctx).Warn("ignoring approval for another step", "step", sig.Step, "waiting", step)
				return
			}
			status := sig.Status
			if status != policy.ApprovalApproved {
				status = policy.ApprovalRejected
			}
			got = &policy.Approval{Status: status, By: sig.By}
		})
		sel.AddFuture(timer, func(workflow.Future) {
			got = &policy.Approval{Status: policy.ApprovalRejected, By: "porter/approval-timeout"}
		})
		sel.Select(ctx)
	}
	return got
}
