package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
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

// approvalLedgerVersion gates the approval ledger: a run whose write
// another run already asks to approve waits on that decision.
const approvalLedgerVersion = "approval-ledger"

// approvalNewerVersion gates telling the holder of a request the waiting
// run's own request (SignalApprovalNewer) rather than only its ID.
const approvalNewerVersion = "approval-newer"

// ledgerReleaseVersion gates closing a run's requests when the run ends
// (on a disconnected context, newest waiter first), rather than right
// after each write's commit.
const ledgerReleaseVersion = "ledger-release-at-end"

// approvalNoticeVersion gates telling approvers of a newer change
// (approval.newer-change), which runs that received the signal before the
// notice existed replay without.
const approvalNoticeVersion = "approval-newer-notice"

// skippedMemoVersion gates recording a skipped run's reason in its memo
// (MemoSkipped), which runs skipped before it replay without.
const skippedMemoVersion = "skipped-memo"

// unlinkVersion gates removing a write's link when a saga undoes the
// write, which runs that compensated before it replay without.
const unlinkVersion = "unlink-on-undo"

// MemoSkipped is the memo field holding why a run was skipped, so a list
// of runs shows it without reading each run's result.
const MemoSkipped = "turgon.skipped"

// ledgerMargin keeps a request in the ledger a little past its run's
// timeout, after which another run may take it over.
const ledgerMargin = time.Hour

// DefaultApprovalTimeout is how long a run waits for a person.
const DefaultApprovalTimeout = 72 * time.Hour

// a is used only to name activity methods in ExecuteActivity calls.
var a *Activities

type committedWrite struct {
	step    string
	request writeguard.Request
	result  json.RawMessage
	// link is the cross-reference the write made, removed when the write
	// is undone.
	link *LinkInput
}

// IntegrationWorkflow interprets one compiled workflow for one event.
func IntegrationWorkflow(ctx workflow.Context, in RunInput) (res RunResult, err error) {
	// Metrics go through the workflow's metrics handler, which the SDK
	// silences during replay; they add nothing to the history.
	started, waited := workflow.Now(ctx), time.Duration(0)
	defer func() { recordRun(ctx, in.Workflow.Name, err, workflow.Now(ctx).Sub(started)-waited) }()

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
	waiting := workflow.GetSignalChannel(ctx, SignalApprovalWaiting)
	newer := workflow.GetSignalChannel(ctx, SignalApprovalNewer)
	closed := workflow.GetSignalChannel(ctx, SignalApprovalClosed)
	self := workflow.GetInfo(ctx).WorkflowExecution.ID
	// held are the ledger keys of the requests this run holds open, closed
	// when the run ends (see release).
	var held []string
	// release closes a request and wakes the runs that waited on it.
	// Runs from before ledgerReleaseVersion closed it right after its
	// commit, on the run's own context, and woke waiters oldest first.
	release := func(key string, atEnd bool) {
		rctx := ctx
		if atEnd {
			// A cancelled run still closes what it holds.
			rctx, _ = workflow.NewDisconnectedContext(ctx)
		}
		var waiters []string
		if err := workflow.ExecuteActivity(rctx, a.CloseApproval, LedgerInput{Key: key, Run: self}).Get(rctx, &waiters); err != nil {
			logger.Warn("could not close the request for approval; it expires on its own", "key", key, "error", err)
		}
		if atEnd {
			// The newest change is woken first, so it is the one that asks
			// again after a rejection.
			slices.Reverse(waiters)
		}
		for _, w := range waiters {
			// A waiter that ended meanwhile has nothing to wake.
			_ = workflow.SignalExternalWorkflow(rctx, w, "", SignalApprovalClosed, key).Get(rctx, nil)
		}
	}
	// Requests are closed once the run has ended, its compensations
	// included: a waiter woken earlier could find a write that a later
	// failure of this run then undoes.
	defer func() {
		for _, key := range held {
			release(key, true)
		}
	}()

	var result RunResult
	var done []committedWrite
	failedStep := ""
	failedWrite := func() *compiler.WriteConfig {
		for _, st := range in.Workflow.Steps {
			if st.Name == failedStep {
				return st.Write
			}
		}
		return nil
	}
	refused := func(err error, undone int) {
		if n, ok := writeRefused(failedStep, failedWrite(), err, undone); ok &&
			workflow.GetVersion(ctx, notifyRefusedVersion, workflow.DefaultVersion, 1) == 1 {
			tell(ctx, in.Workflow.Name, n)
		}
	}
	fail := func(err error) (RunResult, error) {
		if n, ok := stewardNeeded(err); ok {
			tell(ctx, in.Workflow.Name, n)
		}
		if len(done) == 0 {
			refused(err, 0)
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
				continue
			}
			// The record the write made is gone or cancelled: its link
			// would lead the target's event about the undo back to the
			// source, which the run just failed to update.
			if w.link != nil && workflow.GetVersion(ctx, unlinkVersion, workflow.DefaultVersion, 1) == 1 {
				if uerr := workflow.ExecuteActivity(ctx, a.UnlinkWrite, *w.link).Get(ctx, nil); uerr != nil {
					logger.Warn("removing the link of an undone write", "step", w.step, "error", uerr)
				}
			}
		}
		if len(errs) == 0 {
			// Every write was undone: report the step's own failure.
			refused(err, len(done))
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
	// Asked only when a write has plugins, so runs without plugins keep
	// their histories as they were.
	var plugins *bool
	pluginsOn := func() bool {
		if plugins == nil {
			on := workflow.GetVersion(ctx, pluginsVersion, workflow.DefaultVersion, 1) == 1
			plugins = &on
		}
		return *plugins
	}
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

	// link records a written record as a cross-reference, when the step
	// asks (only new specs do, so runs from before replay as they ran).
	link := func(w *compiler.WriteConfig, res json.RawMessage) (*LinkInput, error) {
		if w.Link == nil {
			return nil, nil
		}
		lin := LinkInput{Link: *w.Link, System: w.Endpoint, Result: res, Doc: doc}
		return &lin, workflow.ExecuteActivity(ctx, a.LinkWrite, lin).Get(ctx, nil)
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
			if _, into := st.Resolve.Fields(); st.Resolve.IfUnknown == v1alpha1.IfUnknownSkip && doc[into] == nil {
				// A record Turgon never linked: nothing of it to update.
				entity := strings.TrimPrefix(st.Resolve.Entity, "model.")
				result.Skipped = fmt.Sprintf("%s: a %s from %s without a cross-reference", st.Name, entity, in.Workflow.Trigger.Endpoint)
				logger.Info("run skipped", "reason", result.Skipped)
				if workflow.GetVersion(ctx, skippedMemoVersion, workflow.DefaultVersion, 1) == 1 {
					if err := workflow.UpsertMemo(ctx, map[string]any{MemoSkipped: result.Skipped}); err != nil {
						logger.Warn("recording the skip in the run's memo", "error", err)
					}
				}
				return result, nil
			}
		case st.Write != nil:
			var prep PrepareOutput
			pin := PrepareInput{Workflow: in.Workflow.Name, Step: st.Name, Config: *st.Write, Source: source, Doc: doc}
			stepKey := "" // the request this step holds
			for {
				prep = PrepareOutput{}
				if err := workflow.ExecuteActivity(ctx, a.PrepareWrite, pin).Get(ctx, &prep); err != nil {
					return fail(err)
				}
				if prep.Prepared.Duplicate != nil || !prep.Prepared.NeedsApproval ||
					workflow.GetVersion(ctx, approvalLedgerVersion, workflow.DefaultVersion, 1) != 1 {
					break
				}
				// One request per write: if another run (an earlier change
				// of the record) already asks, wait on its decision, then
				// prepare again. An approval makes this write a duplicate;
				// a rejection lets this run ask with its own data.
				key := writeguard.ApprovalKey(prep.Request)
				lin := LedgerInput{Key: key, Run: self, Until: workflow.Now(ctx).Add(timeout + ledgerMargin)}
				var holder string
				if err := workflow.ExecuteActivity(ctx, a.OpenApproval, lin).Get(ctx, &holder); err != nil {
					return fail(err)
				}
				if holder == "" {
					stepKey = key
					break
				}
				logger.Info("waiting on another run's request for approval", "key", key, "holder", holder)
				if workflow.GetVersion(ctx, approvalNewerVersion, workflow.DefaultVersion, 1) == 1 {
					_ = workflow.SignalExternalWorkflow(ctx, holder, "", SignalApprovalNewer, NewerChange{Run: self, Request: prep.Request}).Get(ctx, nil)
				} else {
					_ = workflow.SignalExternalWorkflow(ctx, holder, "", SignalApprovalWaiting, self).Get(ctx, nil)
				}
				since := workflow.Now(ctx)
				awaitClosed(ctx, closed, timeout+ledgerMargin)
				waited += workflow.Now(ctx).Sub(since)
			}
			rec := WriteRecord{Step: st.Name, Endpoint: st.Write.Endpoint, Operation: st.Write.Operation}
			if d := prep.Prepared.Duplicate; d != nil {
				rec.Status, rec.Result = d.Status, d.Result
				// Written before this run: not ours to compensate.
				result.Writes = append(result.Writes, rec)
				doc = withOutput(doc, st.Write.Output, d.Result)
				if _, err := link(st.Write, d.Result); err != nil {
					return fail(err)
				}
				continue
			}

			var approval *policy.Approval
			if prep.Prepared.NeedsApproval {
				pending = &PendingApproval{
					Step: st.Name, Digest: RequestDigest(prep.Request), Request: prep.Request,
					Preview: prep.Prepared.Preview, Reasons: prep.Prepared.Decision.Reasons, Since: workflow.Now(ctx),
				}
				tell(ctx, in.Workflow.Name, approvalPending(*pending, pending.Since.Add(timeout)))
				onNewer := func(c NewerChange) {
					// Runs from before the notice replay without it.
					if workflow.GetVersion(ctx, approvalNoticeVersion, workflow.DefaultVersion, 1) == 1 {
						tell(ctx, in.Workflow.Name, newerChangeWaiting(*pending, c))
					}
				}
				wr := waitingRuns{ids: waiting, changes: newer, onNewer: onNewer, key: writeguard.ApprovalKey(prep.Request)}
				approval = awaitApproval(ctx, approvals, wr, pending, timeout)
				wait := workflow.Now(ctx).Sub(pending.Since)
				waited += wait
				recordApproval(ctx, in.Workflow.Name, approval, wait)
				if approval.By == approvalTimeoutActor {
					tell(ctx, in.Workflow.Name, approvalTimedOut(*pending))
				}
				pending = nil
			}

			var out writeguard.Outcome
			cin := CommitInput{Request: prep.Request, Approval: approval}
			err := workflow.ExecuteActivity(ctx, a.CommitWrite, cin).Get(ctx, &out)
			if stepKey != "" {
				if workflow.GetVersion(ctx, ledgerReleaseVersion, workflow.DefaultVersion, 1) == 1 {
					held = append(held, stepKey) // closed when the run ends
				} else {
					release(stepKey, false)
				}
			}
			if err != nil {
				return fail(err)
			}
			rec.Status, rec.Result = out.Status, out.Result
			result.Writes = append(result.Writes, rec)
			done = append(done, committedWrite{step: st.Name, request: prep.Request, result: out.Result})
			doc = withOutput(doc, st.Write.Output, out.Result)
			lin, err := link(st.Write, out.Result)
			if err != nil {
				return fail(err)
			}
			done[len(done)-1].link = lin
			if len(st.Write.Plugins) > 0 && out.Status == writeguard.StatusCommitted && pluginsOn() {
				runPlugins(ctx, in.Workflow.Name, st.Name, st.Write, modelRecord(prep.Request.Payload, out.Result))
			}
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
// waitingRuns are the channels on which later changes' runs announce that
// they wait on a request: by ID (SignalApprovalWaiting), or with their own
// request (SignalApprovalNewer), which onNewer, if set, tells people about.
type waitingRuns struct {
	ids, changes workflow.ReceiveChannel
	onNewer      func(NewerChange)
	// key is the pending request's ledger key: a change for another write
	// (announced to an earlier step of this run) is not listed.
	key string
}

// Runs that start waiting on the decision meanwhile are listed in p.Waiting.
func awaitApproval(ctx workflow.Context, ch workflow.ReceiveChannel, w waitingRuns, p *PendingApproval, timeout time.Duration) *policy.Approval {
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
		add := func(run string) bool {
			for _, w := range p.Waiting {
				if w == run {
					return false
				}
			}
			p.Waiting = append(p.Waiting, run)
			return true
		}
		sel.AddReceive(w.ids, func(c workflow.ReceiveChannel, _ bool) {
			var run string
			c.Receive(ctx, &run)
			add(run)
		})
		sel.AddReceive(w.changes, func(c workflow.ReceiveChannel, _ bool) {
			var nc NewerChange
			c.Receive(ctx, &nc)
			if w.key != "" && writeguard.ApprovalKey(nc.Request) != w.key {
				return
			}
			if add(nc.Run) && w.onNewer != nil {
				w.onNewer(nc)
			}
		})
		sel.AddFuture(timer, func(workflow.Future) {
			got = &policy.Approval{Status: policy.ApprovalRejected, By: approvalTimeoutActor,
				Note: fmt.Sprintf("nobody decided within %s", timeout)}
		})
		sel.Select(ctx)
	}
	return got
}

// awaitClosed blocks until the request this run waits on is decided, or
// the request's time in the ledger passes (its run ended without closing
// it), whichever comes first.
func awaitClosed(ctx workflow.Context, closed workflow.ReceiveChannel, limit time.Duration) {
	tctx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(closed, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, nil) })
	sel.AddFuture(workflow.NewTimer(tctx, limit), func(workflow.Future) {})
	sel.Select(ctx)
}

// recordRun counts a finished run by outcome and records how long it took,
// not counting the time it waited for people, which recipes' latency SLOs
// leave out.
func recordRun(ctx workflow.Context, name string, err error, active time.Duration) {
	outcome, reason := "completed", ""
	if err != nil {
		outcome, reason = "failed", "error"
		var app *temporal.ApplicationError
		if errors.As(err, &app) && app.Type() != "" {
			reason = app.Type()
		}
	}
	m := workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"workflow": name})
	m.WithTags(map[string]string{"outcome": outcome, "reason": reason}).Counter("turgon_runs_finished_total").Inc(1)
	m.Timer("turgon_run_active").Record(active)
}

// recordApproval counts a decision and how long people took to make it.
func recordApproval(ctx workflow.Context, name string, a *policy.Approval, wait time.Duration) {
	decision := string(a.Status)
	if a.By == approvalTimeoutActor {
		decision = "timed_out"
	}
	m := workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"workflow": name})
	m.WithTags(map[string]string{"decision": decision}).Counter("turgon_approvals_total").Inc(1)
	m.Timer("turgon_approval_wait").Record(wait)
}
