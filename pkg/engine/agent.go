package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// AgentWriteWorkflowName runs one write an agent asked for through a tool
// (architecture §8, figure 4): policy, validation, simulation, a person's
// approval when policy requires it, then the commit. It is durable, so a
// write can wait days for its approver.
const AgentWriteWorkflowName = "turgon.agent-write"

// AgentStep is the step name agent writes wait under for approval.
const AgentStep = "agent-write"

// AgentWriteInput starts an agent write. The request is complete: the MCP
// server builds it from the tool call and the authenticated identity.
type AgentWriteInput struct {
	SpecDigest      string             `json:"specDigest"`
	Request         writeguard.Request `json:"request"`
	ApprovalTimeout time.Duration      `json:"approvalTimeout,omitempty"`
}

// PrepareAgentInput is the input of the PrepareAgentWrite activity.
type PrepareAgentInput struct {
	Request writeguard.Request `json:"request"`
}

// PrepareAgentWrite runs the guard's first phase for an agent's request.
func (x *Activities) PrepareAgentWrite(ctx context.Context, in PrepareAgentInput) (writeguard.Prepared, error) {
	if !x.Guard.Has(in.Request.Target) {
		return writeguard.Prepared{}, nonRetryable(ErrTypeInvalid, fmt.Errorf("this worker has no connector for %s", in.Request.Target))
	}
	p, err := x.Guard.Prepare(ctx, in.Request)
	if errors.Is(err, writeguard.ErrDenied) && len(p.Decision.Reasons) > 0 {
		// Tell the agent why.
		err = fmt.Errorf("%w: %s", err, strings.Join(p.Decision.Reasons, "; "))
	}
	if err != nil {
		return p, classify(err)
	}
	return p, nil
}

// AgentWriteWorkflow performs one agent write. Its result lists the write
// like a recipe run's, so the console shows both alike.
func AgentWriteWorkflow(ctx workflow.Context, in AgentWriteInput) (RunResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        8,
			NonRetryableErrorTypes: []string{ErrTypeDenied, ErrTypeRejected, ErrTypeInvalid},
		},
	})
	timeout := in.ApprovalTimeout
	if timeout <= 0 {
		timeout = DefaultApprovalTimeout
	}
	var pending *PendingApproval
	if err := workflow.SetQueryHandler(ctx, QueryPending, func() (*PendingApproval, error) { return pending, nil }); err != nil {
		return RunResult{}, err
	}
	req := in.Request
	rec := WriteRecord{Step: AgentStep, Endpoint: req.Target, Operation: req.Operation}

	var prep writeguard.Prepared
	if err := workflow.ExecuteActivity(ctx, a.PrepareAgentWrite, PrepareAgentInput{Request: req}).Get(ctx, &prep); err != nil {
		return RunResult{}, err
	}
	if d := prep.Duplicate; d != nil {
		rec.Status, rec.Result = d.Status, d.Result
		return RunResult{Writes: []WriteRecord{rec}}, nil
	}
	var approval *policy.Approval
	if prep.NeedsApproval {
		pending = &PendingApproval{
			Step: AgentStep, Digest: RequestDigest(req), Request: req,
			Preview: prep.Preview, Reasons: prep.Decision.Reasons, Since: workflow.Now(ctx),
		}
		name := "agent/" + req.Tool
		tell(ctx, name, approvalPending(*pending, pending.Since.Add(timeout)))
		approval = awaitApproval(ctx, workflow.GetSignalChannel(ctx, SignalApproval), *pending, timeout)
		if approval.By == approvalTimeoutActor {
			tell(ctx, name, approvalTimedOut(*pending))
		}
		pending = nil
	}
	var out writeguard.Outcome
	if err := workflow.ExecuteActivity(ctx, a.CommitWrite, CommitInput{Request: req, Approval: approval}).Get(ctx, &out); err != nil {
		return RunResult{}, err
	}
	rec.Status, rec.Result = out.Status, out.Result
	return RunResult{Writes: []WriteRecord{rec}}, nil
}

// AgentWriteState is where an agent write stands.
type AgentWriteState string

const (
	AgentWriteCommitted AgentWriteState = "committed"
	AgentWritePending   AgentWriteState = "pending_approval"
	AgentWriteRunning   AgentWriteState = "running"
	AgentWriteDenied    AgentWriteState = "denied"
	AgentWriteRejected  AgentWriteState = "rejected"
	AgentWriteInvalid   AgentWriteState = "invalid"
	AgentWriteFailed    AgentWriteState = "failed"
)

// AgentWriteStatus reports an agent write to the agent.
type AgentWriteStatus struct {
	State   AgentWriteState  `json:"state"`
	Write   *WriteRecord     `json:"write,omitempty"`
	Pending *PendingApproval `json:"pending,omitempty"`
	Message string           `json:"message,omitempty"`
}

// ErrRequestConflict means a request ID was already used for a different request.
var ErrRequestConflict = errors.New("request ID already used for a different request")

// AgentWrites starts agent writes on Temporal and reports on them.
type AgentWrites struct {
	Client    client.Client
	TaskQueue string
	// Wait is how long Submit waits for a write to finish before reporting
	// it as still in progress. Default 15s.
	Wait time.Duration
}

// Submit starts the write under id, or finds the one already started under
// it, and reports its state. Submitting the same request again is how an
// agent checks on a write; it never writes twice. A different request
// under the same id is refused with ErrRequestConflict.
func (w AgentWrites) Submit(ctx context.Context, id string, in AgentWriteInput) (AgentWriteStatus, error) {
	opts := client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: w.TaskQueue,
		// An agent write runs once: a rejected or failed request is not
		// asked again under the same id, the agent must make a new one.
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
	run, err := w.Client.ExecuteWorkflow(ctx, opts, AgentWriteWorkflowName, in)
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &already) {
		prior, err := w.Input(ctx, id)
		if err != nil {
			return AgentWriteStatus{}, err
		}
		if !sameRequest(prior.Request, in.Request) {
			return AgentWriteStatus{}, ErrRequestConflict
		}
		run = w.Client.GetWorkflow(ctx, id, "")
	} else if err != nil {
		return AgentWriteStatus{}, err
	}
	return w.await(ctx, id, run, w.Wait)
}

// ErrUnknownWrite is returned by Status for an ID no write was started under.
var ErrUnknownWrite = errors.New("no agent write with this ID")

// Status reports on a write already started, waiting at most a moment.
func (w AgentWrites) Status(ctx context.Context, id string) (AgentWriteStatus, error) {
	desc, err := w.Client.DescribeWorkflowExecution(ctx, id, "")
	var nf *serviceerror.NotFound
	if errors.As(err, &nf) {
		return AgentWriteStatus{}, ErrUnknownWrite
	}
	if err != nil {
		return AgentWriteStatus{}, err
	}
	if desc.GetWorkflowExecutionInfo().GetType().GetName() != AgentWriteWorkflowName {
		return AgentWriteStatus{}, ErrUnknownWrite
	}
	return w.await(ctx, id, w.Client.GetWorkflow(ctx, id, ""), time.Second)
}

// await waits up to wait for a write to finish, then reports where it stands.
func (w AgentWrites) await(ctx context.Context, id string, run client.WorkflowRun, wait time.Duration) (AgentWriteStatus, error) {
	if wait <= 0 {
		wait = 15 * time.Second
	}
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var res RunResult
	err := run.Get(wctx, &res)
	if err != nil && ctx.Err() == nil && !isRunFailure(err) {
		// The wait ended (Temporal may end its long poll a little before
		// our deadline) or the call failed: ask whether the write is still
		// running, waiting for a person or retrying the target.
		desc, derr := w.Client.DescribeWorkflowExecution(ctx, id, "")
		if derr != nil {
			return AgentWriteStatus{}, errors.Join(err, derr)
		}
		if desc.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			if p, perr := Pending(ctx, w.Client, id); perr == nil && p != nil {
				return AgentWriteStatus{State: AgentWritePending, Pending: p}, nil
			}
			return AgentWriteStatus{State: AgentWriteRunning}, nil
		}
		// It finished just now: fetch how.
		err = run.Get(ctx, &res)
	}
	switch {
	case err == nil:
		st := AgentWriteStatus{State: AgentWriteCommitted}
		if len(res.Writes) > 0 {
			st.Write = &res.Writes[0]
		}
		return st, nil
	case ctx.Err() != nil:
		return AgentWriteStatus{}, ctx.Err()
	}
	st := AgentWriteStatus{State: AgentWriteFailed, Message: err.Error()}
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		st.Message = app.Message()
		switch app.Type() {
		case ErrTypeDenied:
			st.State = AgentWriteDenied
		case ErrTypeRejected:
			st.State = AgentWriteRejected
		case ErrTypeInvalid:
			st.State = AgentWriteInvalid
		}
	}
	return st, nil
}

// isRunFailure reports whether err is a run's own failure, as opposed to a
// failure to wait for it.
func isRunFailure(err error) bool {
	var failed *temporal.WorkflowExecutionError
	return errors.As(err, &failed)
}

// sameRequest reports whether b asks for the same write as a. Roles are
// not compared: they may change between an agent's calls about one write.
func sameRequest(a, b writeguard.Request) bool {
	return a.Target == b.Target && a.Operation == b.Operation && a.IdempotencyKey == b.IdempotencyKey &&
		a.Subject.ID == b.Subject.ID && a.Subject.OnBehalfOf == b.Subject.OnBehalfOf &&
		a.Reason == b.Reason && bytes.Equal(a.Payload, b.Payload)
}

// Input returns the input an agent write was started with.
func (w AgentWrites) Input(ctx context.Context, id string) (AgentWriteInput, error) {
	var in AgentWriteInput
	it := w.Client.GetWorkflowHistory(ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !it.HasNext() {
		return in, fmt.Errorf("agent write %s has no history", id)
	}
	ev, err := it.Next()
	if err != nil {
		return in, err
	}
	attrs := ev.GetWorkflowExecutionStartedEventAttributes()
	if attrs == nil {
		return in, fmt.Errorf("agent write %s: first event is %s", id, ev.GetEventType())
	}
	err = DataConverter().FromPayloads(attrs.GetInput(), &in)
	return in, err
}
