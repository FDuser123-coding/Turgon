package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// activityRetry retries activities that failed for a passing reason;
// decisions and invalid input are final.
var activityRetry = &temporal.RetryPolicy{
	InitialInterval:        time.Second,
	BackoffCoefficient:     2,
	MaximumInterval:        time.Minute,
	MaximumAttempts:        8,
	NonRetryableErrorTypes: []string{ErrTypeDenied, ErrTypeRejected, ErrTypeInvalid, ErrTypeUnresolved, ErrTypeMapping},
}

// localStepsVersion gates running map and resolve steps as local
// activities, which older runs' histories recorded as activities.
const localStepsVersion = "local-map-resolve"

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
		RetryPolicy:         activityRetry,
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
	failedStep := ""
	fail := func(err error) (RunResult, error) {
		if n, ok := stewardNeeded(err); ok {
			tell(ctx, in.Workflow.Name, n)
		}
		if len(done) == 0 {
			return result, err
		}
		logger.Warn("step failed; compensating committed writes", "error", err, "writes", len(done))
		var errs []error
		var causes []string
		for i := len(done) - 1; i >= 0; i-- {
			w := done[i]
			cerr := workflow.ExecuteActivity(ctx, a.Compensate, CompensateInput{Request: w.request, Result: w.result}).Get(ctx, nil)
			if cerr != nil {
				errs = append(errs, fmt.Errorf("compensate %s: %w", w.step, cerr))
				causes = append(causes, fmt.Sprintf("undo %s: %s", w.step, cause(cerr)))
			}
		}
		if len(errs) == 0 {
			// Every write was undone: report the step's own failure.
			return result, err
		}
		// Some writes could not be undone; an operator must act.
		tell(ctx, in.Workflow.Name, compensationFailed(failedStep, cause(err), causes))
		return result, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("step failed and %d compensation(s) failed: %v", len(errs), errors.Join(errs...)),
			ErrTypeCompensationFailed, err)
	}

	// Map and resolve steps are short and touch no target system, so they
	// run as local activities: in the worker, recorded in the workflow's
	// own task instead of a round trip each through the Temporal server.
	// Runs started before this change replay them as they ran.
	local := workflow.GetVersion(ctx, localStepsVersion, workflow.DefaultVersion, 1) == 1
	step := func(name string, input, out any) error {
		if !local {
			return workflow.ExecuteActivity(ctx, name, input).Get(ctx, out)
		}
		lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         activityRetry,
		})
		return workflow.ExecuteLocalActivity(lctx, name, input).Get(lctx, out)
	}

	for _, st := range in.Workflow.Steps {
		failedStep = st.Name
		switch {
		// Results decode into a fresh map: decoding into doc would merge
		// keys into it (and into source, which doc starts as).
		case st.Map != nil:
			var next map[string]any
			if err := step("Map", MapInput{Config: *st.Map, Doc: doc}, &next); err != nil {
				return fail(err)
			}
			doc = next
		case st.Resolve != nil:
			var next map[string]any
			rin := ResolveInput{Config: *st.Resolve, System: in.Workflow.Trigger.Endpoint, Doc: doc}
			if err := step("Resolve", rin, &next); err != nil {
				return fail(err)
			}
			doc = next
		case st.Write != nil:
			var prep PrepareOutput
			pin := PrepareInput{Workflow: in.Workflow.Name, Step: st.Name, Config: *st.Write, Source: source, Doc: doc}
			if err := workflow.ExecuteActivity(ctx, a.PrepareWrite, pin).Get(ctx, &prep); err != nil {
				return fail(err)
			}
			rec := WriteRecord{Step: st.Name, Endpoint: st.Write.Endpoint, Operation: st.Write.Operation}
			if d := prep.Prepared.Duplicate; d != nil {
				rec.Status, rec.Result = d.Status, d.Result
				// Written before this run: not ours to compensate.
				result.Writes = append(result.Writes, rec)
				doc = withOutput(doc, st.Write.Output, d.Result)
				continue
			}

			var approval *policy.Approval
			if prep.Prepared.NeedsApproval {
				pending = &PendingApproval{
					Step: st.Name, Digest: RequestDigest(prep.Request), Request: prep.Request,
					Preview: prep.Prepared.Preview, Reasons: prep.Prepared.Decision.Reasons, Since: workflow.Now(ctx),
				}
				tell(ctx, in.Workflow.Name, approvalPending(*pending, pending.Since.Add(timeout)))
				approval = awaitApproval(ctx, approvals, *pending, timeout)
				if approval.By == approvalTimeoutActor {
					tell(ctx, in.Workflow.Name, approvalTimedOut(*pending))
				}
				pending = nil
			}

			var out writeguard.Outcome
			cin := CommitInput{Request: prep.Request, Approval: approval}
			if err := workflow.ExecuteActivity(ctx, a.CommitWrite, cin).Get(ctx, &out); err != nil {
				return fail(err)
			}
			rec.Status, rec.Result = out.Status, out.Result
			result.Writes = append(result.Writes, rec)
			done = append(done, committedWrite{step: st.Name, request: prep.Request, result: out.Result})
			doc = withOutput(doc, st.Write.Output, out.Result)
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

// RequestDigest identifies a write request's exact content.
func RequestDigest(req writeguard.Request) string {
	b, _ := json.Marshal(req)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// approvalTimeoutActor rejects approvals nobody answered in time.
const approvalTimeoutActor = "turgon/approval-timeout"

// awaitApproval blocks until a decision for the pending request arrives or
// the timeout passes; a timeout is recorded as a rejection. Signals for
// another step or another version of the request are ignored, so a decision
// sent early, or about a different request, can never approve this one.
func awaitApproval(ctx workflow.Context, ch workflow.ReceiveChannel, p PendingApproval, timeout time.Duration) *policy.Approval {
	tctx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	timer := workflow.NewTimer(tctx, timeout)
	var got *policy.Approval
	for got == nil {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
			var sig ApprovalSignal
			c.Receive(ctx, &sig)
			if sig.Step != p.Step || sig.Digest != p.Digest {
				workflow.GetLogger(ctx).Warn("ignoring approval that does not match the pending request",
					"step", sig.Step, "waiting", p.Step, "by", sig.By)
				return
			}
			status := sig.Status
			if status != policy.ApprovalApproved {
				status = policy.ApprovalRejected
			}
			got = &policy.Approval{Status: status, By: sig.By, Note: sig.Note}
		})
		sel.AddFuture(timer, func(workflow.Future) {
			got = &policy.Approval{Status: policy.ApprovalRejected, By: approvalTimeoutActor,
				Note: fmt.Sprintf("nobody decided within %s", timeout)}
		})
		sel.Select(ctx)
	}
	return got
}
