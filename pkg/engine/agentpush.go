package engine

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// An agent can ask to be told, by an HTTP POST to a URL of its own (A2A
// push notifications), when its write waits for approval and when it ends.
// The write's workflow keeps the URLs and sends the notifications itself:
// they survive restarts, every agent server sees them, and they go out the
// moment the state changes, even days later.

const (
	// SignalPushSet adds or replaces a PushConfig; SignalPushDelete removes
	// one by ID; QueryPush lists them.
	SignalPushSet    = "agent-push-set"
	SignalPushDelete = "agent-push-delete"
	QueryPush        = "agent-push"
	// ActivityAgentPush delivers one notification. `turgon run` registers
	// it (agent.Pusher), since the payload is an A2A task.
	ActivityAgentPush = "AgentPush"
	// ErrTypePushRefused marks a notification the receiver refused for
	// good (a 4xx), or a URL Turgon will not call; it is not retried.
	ErrTypePushRefused = "push-refused"
	// MaxPushConfigs caps the URLs per write.
	MaxPushConfigs = 5

	// AgentPushWorkflowName delivers a write's outcome notifications,
	// detached from the write, so a receiver that is down does not delay
	// the outcome for the agents that poll or stream.
	AgentPushWorkflowName = "turgon.agent-push"

	// pushVersion gates notifications, which writes started by older
	// workers did not send.
	pushVersion = "agent-push"
)

// PushConfig is where to notify an agent, and how to authenticate.
type PushConfig struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Token string `json:"token,omitempty"`
	// Schemes and Credentials authenticate Turgon to the receiver
	// (Authorization: Bearer <credentials>).
	Schemes     []string `json:"schemes,omitempty"`
	Credentials string   `json:"credentials,omitempty"`
}

// AgentPushInput is one notification to deliver.
type AgentPushInput struct {
	Config     PushConfig       `json:"config"`
	WorkflowID string           `json:"workflowId"`
	Tool       string           `json:"tool"`
	Status     AgentWriteStatus `json:"status"`
}

// pushes keeps a write's push configurations, changed by signals.
type pushes struct {
	on      bool
	configs []PushConfig
}

func startPushes(ctx workflow.Context, initial []PushConfig) (*pushes, error) {
	p := &pushes{on: workflow.GetVersion(ctx, pushVersion, workflow.DefaultVersion, 1) == 1}
	if !p.on {
		return p, nil
	}
	for _, c := range initial {
		p.set(c)
	}
	if err := workflow.SetQueryHandler(ctx, QueryPush, func() ([]PushConfig, error) { return p.configs, nil }); err != nil {
		return nil, err
	}
	set, del := workflow.GetSignalChannel(ctx, SignalPushSet), workflow.GetSignalChannel(ctx, SignalPushDelete)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			sel := workflow.NewSelector(ctx)
			sel.AddReceive(set, func(ch workflow.ReceiveChannel, _ bool) {
				var c PushConfig
				ch.Receive(ctx, &c)
				p.set(c)
			})
			sel.AddReceive(del, func(ch workflow.ReceiveChannel, _ bool) {
				var id string
				ch.Receive(ctx, &id)
				p.delete(id)
			})
			sel.Select(ctx)
		}
	})
	return p, nil
}

func (p *pushes) set(c PushConfig) {
	for i := range p.configs {
		if p.configs[i].ID == c.ID {
			p.configs[i] = c
			return
		}
	}
	if len(p.configs) < MaxPushConfigs {
		p.configs = append(p.configs, c)
	}
}

func (p *pushes) delete(id string) {
	for i := range p.configs {
		if p.configs[i].ID == id {
			p.configs = append(p.configs[:i], p.configs[i+1:]...)
			return
		}
	}
}

// notify tells every registered URL that the write waits for approval,
// and waits for them: the outcome is then always sent after it. Like other
// notifications it is best effort: a receiver that is down never holds up
// the approval.
func (p *pushes) notify(ctx workflow.Context, tool string, st AgentWriteStatus) {
	if !p.on || len(p.configs) == 0 {
		return
	}
	deliver(ctx, p.inputs(ctx, tool, st))
}

// outcome hands the write's outcome to a detached workflow that delivers
// it, and returns once that has started.
func (p *pushes) outcome(ctx workflow.Context, tool string, st AgentWriteStatus) {
	if !p.on || len(p.configs) == 0 {
		return
	}
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:        id + "#outcome",
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
	})
	f := workflow.ExecuteChildWorkflow(cctx, AgentPushWorkflowName, p.inputs(ctx, tool, st))
	if err := f.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("could not start notifying the agent", "error", err)
	}
}

func (p *pushes) inputs(ctx workflow.Context, tool string, st AgentWriteStatus) []AgentPushInput {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	var out []AgentPushInput
	for _, c := range p.configs {
		out = append(out, AgentPushInput{Config: c, WorkflowID: id, Tool: tool, Status: st})
	}
	return out
}

// AgentPushWorkflow delivers notifications, retrying each for a few
// minutes.
func AgentPushWorkflow(ctx workflow.Context, in []AgentPushInput) error {
	deliver(ctx, in)
	return nil
}

func deliver(ctx workflow.Context, in []AgentPushInput) {
	nctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute, MaximumAttempts: 6,
			NonRetryableErrorTypes: []string{ErrTypePushRefused},
		},
	})
	var sent []workflow.Future
	for _, n := range in {
		sent = append(sent, workflow.ExecuteActivity(nctx, ActivityAgentPush, n))
	}
	for i, f := range sent {
		if err := f.Get(nctx, nil); err != nil {
			workflow.GetLogger(ctx).Warn("could not notify the agent", "config", in[i].Config.ID, "error", err)
		}
	}
}

// ErrWriteFinished is returned when push settings change on a write that
// has already ended.
var ErrWriteFinished = errors.New("the write has already ended")

// SetPush adds or replaces a push configuration of a running write.
func (w AgentWrites) SetPush(ctx context.Context, id string, c PushConfig) error {
	return finished(w.Client.SignalWorkflow(ctx, id, "", SignalPushSet, c))
}

// DeletePush removes a push configuration of a running write.
func (w AgentWrites) DeletePush(ctx context.Context, id, configID string) error {
	return finished(w.Client.SignalWorkflow(ctx, id, "", SignalPushDelete, configID))
}

// Push lists a write's push configurations.
func (w AgentWrites) Push(ctx context.Context, id string) ([]PushConfig, error) {
	v, err := w.Client.QueryWorkflow(ctx, id, "", QueryPush)
	var nf *serviceerror.NotFound
	switch {
	case errors.As(err, &nf):
		return nil, ErrUnknownWrite
	case err != nil:
		var qf *serviceerror.QueryFailed
		if errors.As(err, &qf) { // a write started before notifications existed
			return nil, nil
		}
		return nil, err
	}
	var out []PushConfig
	err = v.Get(&out)
	return out, err
}

// Watch waits up to wait for a write to end, then reports where it stands.
func (w AgentWrites) Watch(ctx context.Context, id string, wait time.Duration) (AgentWriteStatus, error) {
	return w.await(ctx, id, w.Client.GetWorkflow(ctx, id, ""), wait)
}

func finished(err error) error {
	var nf *serviceerror.NotFound
	if errors.As(err, &nf) {
		return ErrWriteFinished
	}
	return err
}
