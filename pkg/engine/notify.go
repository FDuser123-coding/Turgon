package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/notify"
)

// Notify tells people a run needs them. Without a notifier it does nothing.
func (a *Activities) Notify(ctx context.Context, n notify.Notification) error {
	if a.Notifier == nil {
		return nil
	}
	return a.Notifier.Notify(ctx, n)
}

// notifyVersion gates notifications, which runs started by older workers
// did not send: replaying such a run must not find an activity it never
// scheduled.
const notifyVersion = "notify-people"

// tell sends a notification and waits a little for it. A notification is
// best effort: when every channel keeps failing the run goes on, so a chat
// outage never holds up a write or an approval.
func tell(ctx workflow.Context, workflowName string, n notify.Notification) {
	if workflow.GetVersion(ctx, notifyVersion, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		return
	}
	n.Workflow = workflowName
	n.RunID = workflow.GetInfo(ctx).WorkflowExecution.ID
	n.At = workflow.Now(ctx)
	nctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 5,
		},
	})
	if err := workflow.ExecuteActivity(nctx, a.Notify, n).Get(nctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("could not notify", "kind", n.Kind, "error", err)
	}
}

// The messages name what is waiting and where to act, but carry no payload
// values: chat channels are not the place for customer data. The console
// shows the details to the people allowed to see them.

func approvalPending(p PendingApproval, deadline time.Time) notify.Notification {
	r := p.Request
	facts := map[string]string{
		"Target":        r.Target,
		"Operation":     r.Operation,
		"Risk":          r.Risk,
		"Entity":        r.Entity,
		"Step":          p.Step,
		"Reference":     r.IdempotencyKey,
		"Why approval":  strings.Join(p.Reasons, "; "),
		"Decide before": deadline.UTC().Format("2006-01-02 15:04 MST"),
	}
	if r.Amount != 0 {
		facts["Amount"] = strconv.FormatFloat(r.Amount, 'f', 2, 64)
	}
	if r.Subject.Agent {
		facts["Requested by"] = "agent " + r.Subject.ID
		if r.Subject.OnBehalfOf != "" {
			facts["Requested by"] += " for " + r.Subject.OnBehalfOf
		}
	}
	return notify.Notification{
		Kind: notify.ApprovalPending, Step: p.Step,
		Title: fmt.Sprintf("Approval needed: %s on %s", r.Operation, r.Target),
		Text:  "A run is waiting for someone to approve or reject this write. Nothing is written until then.",
		Facts: notify.SortedFacts(facts),
	}
}

func approvalTimedOut(p PendingApproval) notify.Notification {
	return notify.Notification{
		Kind: notify.ApprovalTimedOut, Step: p.Step,
		Title: fmt.Sprintf("Approval timed out: %s on %s", p.Request.Operation, p.Request.Target),
		Text:  "Nobody decided in time, so the write was rejected and the run stopped. Retry the run to ask again.",
		Facts: notify.SortedFacts(map[string]string{"Step": p.Step, "Reference": p.Request.IdempotencyKey}),
	}
}

func stewardNeeded(err error) (notify.Notification, bool) {
	var app *temporal.ApplicationError
	var u Unresolved
	if !errors.As(err, &app) || app.Type() != ErrTypeUnresolved || app.Details(&u) != nil {
		return notify.Notification{}, false
	}
	facts := map[string]string{"Entity": u.Entity, "System": u.System, "Suggestions": strconv.Itoa(len(u.Suggestions))}
	if len(u.Suggestions) > 0 {
		s := u.Suggestions[0]
		facts["Best suggestion"] = fmt.Sprintf("%s (%.0f%%: %s)", s.Master, s.Score*100, strings.Join(s.Reasons, ", "))
	}
	return notify.Notification{
		Kind:  notify.StewardNeeded,
		Title: fmt.Sprintf("A %s from %s needs a data steward", u.Entity, u.System),
		Text:  "The record matches no master record with enough certainty. Link it in the steward queue and the run continues.",
		Facts: notify.SortedFacts(facts),
	}, true
}

func compensationFailed(step, stepErr string, undo []string) notify.Notification {
	return notify.Notification{
		Kind: notify.CompensationFailed, Step: step,
		Title: "Writes could not be undone",
		Text:  "A step failed and undoing the writes before it failed too, so the systems disagree. An operator must reconcile them.",
		Facts: notify.SortedFacts(map[string]string{"Failed step": step, "Step error": clip(stepErr), "Undo errors": clip(strings.Join(undo, "; "))}),
	}
}

// cause is an activity error's own message, without Temporal's wrapping.
func cause(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Message() != "" {
		return app.Message()
	}
	return err.Error()
}

func clip(s string) string {
	if r := []rune(s); len(r) > 300 {
		return string(r[:299]) + "…"
	}
	return s
}
