package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/plugin"
)

// Logic plugins (architecture §18.4) react to the entities a run writes:
// after a write commits, every plugin subscribed to the model event it
// causes (model.SalesOrder.created) runs once, with the written record.
// They read and propose changes through the write guard; a plugin failing
// never fails the run.

// ErrTypePlugin marks a plugin's own failure: a trap, a limit it hit or
// the error it returned. It is not retried.
const ErrTypePlugin = "TurgonPlugin"

// pluginsVersion gates plugin runs, which older workers did not schedule.
const pluginsVersion = "logic-plugins"

// PluginInput is one model event for one plugin.
type PluginInput struct {
	Plugin   string          `json:"plugin"`
	Event    string          `json:"event"`
	Entity   string          `json:"entity"`
	Record   json.RawMessage `json:"record"`
	Workflow string          `json:"workflow"`
	RunID    string          `json:"runId"`
	Step     string          `json:"step"`
}

// PluginRunner runs logic plugins; pkg/plugin/runner implements it.
type PluginRunner interface {
	Run(ctx context.Context, in PluginInput) error
}

// RunPlugin runs one plugin on one event.
func (x *Activities) RunPlugin(ctx context.Context, in PluginInput) error {
	if x.Plugins == nil {
		return nonRetryable(ErrTypePlugin, errors.New("this worker runs no plugins"))
	}
	err := x.Plugins.Run(ctx, in)
	if errors.Is(err, plugin.ErrPlugin) {
		return nonRetryable(ErrTypePlugin, err)
	}
	return err
}

// runPlugins runs the plugins subscribed to a committed write's event, in
// parallel, and waits for them. Their failures are logged, not returned.
func runPlugins(ctx workflow.Context, wf string, step string, w *compiler.WriteConfig, result json.RawMessage) {
	pctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 10 * time.Second, MaximumAttempts: 3,
			NonRetryableErrorTypes: []string{ErrTypePlugin},
		},
	})
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	var runs []workflow.Future
	for _, p := range w.Plugins {
		in := PluginInput{Plugin: p, Event: w.Event, Entity: w.Entity, Record: result, Workflow: wf, RunID: id, Step: step}
		runs = append(runs, workflow.ExecuteActivity(pctx, a.RunPlugin, in))
	}
	for i, f := range runs {
		if err := f.Get(pctx, nil); err != nil {
			workflow.GetLogger(ctx).Warn("plugin failed", "plugin", w.Plugins[i], "event", w.Event, "error", err)
		}
	}
}
