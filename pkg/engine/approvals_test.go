package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/fduser123-coding/turgon/pkg/metrics"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// testRun is the workflow ID Temporal's test environment gives a run.
const testRun = "default-test-workflow-id"

// A run whose write another run already asks to approve opens no second
// request: it tells that run, waits on its decision, and once the write is
// done finds it a duplicate, without anyone being asked.
func TestALaterChangeWaitsOnTheOpenRequest(t *testing.T) {
	f := newFixture(t)
	f.publish(1101, "ada@example.com", "100.00")
	runs := f.dispatch()
	key := "erp-db/create-sales-order/SHOP-1101"
	if holder, err := f.store.OpenApproval(key, "earlier-run", time.Now().Add(time.Hour)); err != nil || holder != "" {
		t.Fatalf("open: %q, %v", holder, err)
	}

	var s testsuite.WorkflowTestSuite
	s.SetMetricsHandler(metrics.Temporal())
	env := s.NewTestWorkflowEnvironment()
	Register(env, f.rt.Activities)
	told := 0
	env.OnSignalExternalWorkflow(mock.Anything, "earlier-run", "", SignalApprovalWaiting, testRun).Return(nil).Run(func(mock.Arguments) { told++ })
	var pendingWhileWaiting *PendingApproval
	env.RegisterDelayedCallback(func() {
		if v, err := env.QueryWorkflow(QueryPending); err == nil {
			_ = v.Get(&pendingWhileWaiting)
		}
		// The earlier run's write is approved and committed.
		if err := f.store.Complete(key, writeguard.Outcome{Status: writeguard.StatusCommitted, Result: []byte(`{"id":"SO-1"}`)}); err != nil {
			t.Error(err)
		}
		waiters, err := f.store.CloseApproval(key, "earlier-run")
		if err != nil || len(waiters) != 1 || waiters[0] != testRun {
			t.Errorf("waiters %v, %v", waiters, err)
		}
		env.SignalWorkflow(SignalApprovalClosed, key)
	}, time.Hour)
	env.ExecuteWorkflow(IntegrationWorkflow, runs[0])
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("completed %v, %v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	var res RunResult
	_ = env.GetWorkflowResult(&res)
	if len(res.Writes) != 1 || res.Writes[0].Status != writeguard.StatusDuplicate || told != 1 || pendingWhileWaiting != nil {
		t.Fatalf("writes %+v, told %d, pending while waiting %+v", res.Writes, told, pendingWhileWaiting)
	}
	if n := f.orders("true"); n != 0 {
		t.Fatalf("%d orders written by the waiting run", n)
	}
}

// When the request it waited on is rejected, a waiting run asks with its
// own data.
func TestAWaitingRunAsksOnceTheOpenRequestIsRejected(t *testing.T) {
	f := newFixture(t)
	f.publish(1102, "ada@example.com", "100.00")
	runs := f.dispatch()
	key := "erp-db/create-sales-order/SHOP-1102"
	_, _ = f.store.OpenApproval(key, "earlier-run", time.Now().Add(time.Hour))

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	Register(env, f.rt.Activities)
	env.OnSignalExternalWorkflow(mock.Anything, "earlier-run", "", SignalApprovalWaiting, testRun).Return(nil)
	env.RegisterDelayedCallback(func() {
		_, _ = f.store.CloseApproval(key, "earlier-run") // rejected: nothing stored
		env.SignalWorkflow(SignalApprovalClosed, key)
	}, time.Hour)
	var asked *PendingApproval
	env.RegisterDelayedCallback(func() {
		if v, err := env.QueryWorkflow(QueryPending); err == nil {
			_ = v.Get(&asked)
		}
		sig := approve("03-write")
		if asked != nil {
			sig.Digest = asked.Digest
		}
		env.SignalWorkflow(SignalApproval, sig)
	}, 2*time.Hour)
	env.ExecuteWorkflow(IntegrationWorkflow, runs[0])
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("completed %v, %v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	var res RunResult
	_ = env.GetWorkflowResult(&res)
	if asked == nil || len(res.Writes) != 1 || res.Writes[0].Status != writeguard.StatusCommitted || f.orders("external_id = 'SHOP-1102'") != 1 {
		t.Fatalf("asked %+v, writes %+v", asked, res.Writes)
	}
	// Its own request is closed: nobody holds the write any more.
	if holder, _ := f.store.OpenApproval(key, "next-run", time.Now().Add(time.Hour)); holder != "" {
		t.Fatalf("the request is still held by %s", holder)
	}
}

// The run holding the request lists the runs that wait on it, and wakes
// them once its write is decided and stored.
func TestTheOpenRequestListsAndWakesItsWaiters(t *testing.T) {
	f := newFixture(t)
	f.publish(1103, "ada@example.com", "100.00")
	runs := f.dispatch()
	key := "erp-db/create-sales-order/SHOP-1103"

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	Register(env, f.rt.Activities)
	woken := 0
	env.OnSignalExternalWorkflow(mock.Anything, "later-run", "", SignalApprovalClosed, key).Return(nil).Run(func(mock.Arguments) { woken++ })
	var seen *PendingApproval
	env.RegisterDelayedCallback(func() {
		// A later change's run finds the request open and waits on it.
		if holder, err := f.store.OpenApproval(key, "later-run", time.Now().Add(time.Hour)); err != nil || holder != testRun {
			t.Errorf("holder %q, %v", holder, err)
		}
		env.SignalWorkflow(SignalApprovalWaiting, "later-run")
	}, time.Hour)
	env.RegisterDelayedCallback(func() {
		if v, err := env.QueryWorkflow(QueryPending); err == nil {
			_ = v.Get(&seen)
		}
		sig := approve("03-write")
		if seen != nil {
			sig.Digest = seen.Digest
		}
		env.SignalWorkflow(SignalApproval, sig)
	}, 2*time.Hour)
	env.ExecuteWorkflow(IntegrationWorkflow, runs[0])
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("completed %v, %v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	if seen == nil || len(seen.Waiting) != 1 || seen.Waiting[0] != "later-run" || woken != 1 {
		t.Fatalf("pending %+v, woken %d", seen, woken)
	}
	if out, _ := f.store.Lookup(key); out == nil || out.Status != writeguard.StatusCommitted {
		t.Fatalf("outcome %+v", out)
	}
}
