package engine

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

// TaskQueueFor returns the default task queue for a spec. Each spec has
// its own queue: a worker only has the connectors of the spec it runs, so
// tasks for another spec must never reach it.
func TaskQueueFor(spec *compiler.RuntimeSpec) string {
	return "porter-" + spec.Metadata.Name
}

// Register adds the workflow and activities to a Temporal worker.
func Register(w worker.Registry, acts *Activities) {
	w.RegisterWorkflowWithOptions(IntegrationWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	w.RegisterActivity(acts)
}

// TemporalStarter starts runs on a Temporal cluster.
type TemporalStarter struct {
	Client    client.Client
	TaskQueue string
}

// Start starts the run unless a run with this ID is running or completed.
// A failed run may be started again (see Retry); idempotency keys make the
// second attempt safe.
func (s TemporalStarter) Start(ctx context.Context, id string, in RunInput) error {
	in.TaskQueue = s.TaskQueue
	_, err := s.Client.ExecuteWorkflow(ctx, s.options(id), WorkflowName, in)
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &already) {
		return nil
	}
	return err
}

func (s TemporalStarter) options(id string) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                       id,
		TaskQueue:                s.TaskQueue,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}
}

// Input returns the input a run was started with.
func (s TemporalStarter) Input(ctx context.Context, id string) (RunInput, error) {
	it := s.Client.GetWorkflowHistory(ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !it.HasNext() {
		return RunInput{}, fmt.Errorf("run %s has no history", id)
	}
	ev, err := it.Next()
	if err != nil {
		return RunInput{}, err
	}
	attrs := ev.GetWorkflowExecutionStartedEventAttributes()
	if attrs == nil {
		return RunInput{}, fmt.Errorf("run %s: first event is %s", id, ev.GetEventType())
	}
	var in RunInput
	if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &in); err != nil {
		return RunInput{}, err
	}
	return in, nil
}

// Retry starts a failed run again for the same event. If spec is not nil,
// the run uses that spec's definition of the workflow, for example after a
// mapping fix.
func (s TemporalStarter) Retry(ctx context.Context, id string, spec *compiler.RuntimeSpec) (RunInput, error) {
	in, err := s.Input(ctx, id)
	if err != nil {
		return in, err
	}
	if spec != nil {
		found := false
		for _, wf := range spec.Spec.Workflows {
			if wf.Name == in.Workflow.Name {
				in.Workflow, in.SpecDigest, found = wf, spec.Metadata.Digest, true
			}
		}
		if !found {
			return in, fmt.Errorf("spec %s has no workflow %s", spec.Metadata.Name, in.Workflow.Name)
		}
	}
	if s.TaskQueue == "" {
		s.TaskQueue = in.TaskQueue
	}
	if s.TaskQueue == "" {
		return in, fmt.Errorf("run %s does not record its task queue; pass --task-queue", id)
	}
	in.TaskQueue = s.TaskQueue
	opts := s.options(id)
	// Without this the SDK returns the existing run instead of an error.
	opts.WorkflowExecutionErrorWhenAlreadyStarted = true
	_, err = s.Client.ExecuteWorkflow(ctx, opts, WorkflowName, in)
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &already) {
		return in, fmt.Errorf("run %s is running or completed; only failed runs can be retried", id)
	}
	return in, err
}
