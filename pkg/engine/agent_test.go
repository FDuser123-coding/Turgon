package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// agentOrder is what an agent sends to the create_sales_order tool.
func agentOrder(key string, roles ...string) AgentWriteInput {
	payload, _ := json.Marshal(map[string]any{
		"externalId": "AGENT-" + key, "customerId": "C-100", "orderDate": "2026-09-24",
		"netValue": 480.0, "currency": "EUR", "lines": []any{map[string]any{"material": "M-1", "quantity": 4}},
	})
	return AgentWriteInput{Request: writeguard.Request{
		Target: "erp-db", Operation: "create-sales-order", Tool: "create_sales_order", Risk: "high",
		Subject:        policy.Subject{ID: "claude", Agent: true, OnBehalfOf: "ada@example.com", Roles: roles},
		IdempotencyKey: "agent:claude:" + key, Payload: payload, Entity: "SalesOrder", Amount: 480,
		Simulate: true, Reason: "customer asked by phone",
	}}
}

// runAgent executes one agent write in Temporal's test environment.
func (f *fixture) runAgent(in AgentWriteInput, signals ...ApprovalSignal) (RunResult, error, []PendingApproval) {
	f.t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	Register(env, f.rt.Activities)
	var seen []PendingApproval
	for i, sig := range signals {
		sig := sig
		env.RegisterDelayedCallback(func() {
			if v, err := env.QueryWorkflow(QueryPending); err == nil {
				var p *PendingApproval
				if v.Get(&p) == nil && p != nil {
					seen = append(seen, *p)
					sig.Digest = p.Digest
				}
			}
			env.SignalWorkflow(SignalApproval, sig)
		}, time.Duration(i+1)*time.Hour)
	}
	env.ExecuteWorkflow(AgentWriteWorkflow, in)
	if !env.IsWorkflowCompleted() {
		f.t.Fatal("workflow did not complete")
	}
	var res RunResult
	err := env.GetWorkflowError()
	if err == nil {
		_ = env.GetWorkflowResult(&res)
	}
	return res, err, seen
}

func TestAgentWriteWaitsForApprovalThenCommits(t *testing.T) {
	f := newFixture(t)
	res, err, pending := f.runAgent(agentOrder("1", policy.RoleOperator), approve(AgentStep))
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("%d approvals asked, want 1", len(pending))
	}
	p := pending[0]
	if p.Request.Subject.OnBehalfOf != "ada@example.com" || len(p.Preview) == 0 || !strings.Contains(strings.Join(p.Reasons, ","), "high-risk") {
		t.Fatalf("approver saw %+v", p)
	}
	if len(res.Writes) != 1 || res.Writes[0].Status != writeguard.StatusCommitted || f.orders("external_id = 'AGENT-1' AND customer_id = 'C-100'") != 1 {
		t.Fatalf("result %+v, orders %d", res, f.orders("true"))
	}
	log := f.auditLog.String()
	if !strings.Contains(log, `"actor":"claude for ada@example.com","action":"writeback.committed"`) || !strings.Contains(log, "customer asked by phone") {
		t.Errorf("commit not audited as the agent with its reason:\n%s", log)
	}

	// The same request again finds the committed write and writes nothing.
	res, err, pending = f.runAgent(agentOrder("1", policy.RoleOperator))
	if err != nil || len(pending) != 0 || res.Writes[0].Status != writeguard.StatusDuplicate || f.orders("true") != 1 {
		t.Fatalf("retry: err %v, pending %d, result %+v, orders %d", err, len(pending), res, f.orders("true"))
	}
}

func TestAgentWithoutOperatorRoleCannotAskForWrites(t *testing.T) {
	f := newFixture(t)
	_, err, pending := f.runAgent(agentOrder("2", policy.RoleReader), approve(AgentStep))
	if errType(err) != ErrTypeDenied || len(pending) != 0 || f.orders("true") != 0 {
		t.Fatalf("err %v (%s), pending %d, orders %d", err, errType(err), len(pending), f.orders("true"))
	}
	if !strings.Contains(err.Error(), "agents need the integration-operator role to write") {
		t.Errorf("the agent is not told why: %v", err)
	}
	if !strings.Contains(f.auditLog.String(), "agents need the integration-operator role to write") {
		t.Error("denial reason not audited")
	}
}

func TestRejectedAgentWriteWritesNothing(t *testing.T) {
	f := newFixture(t)
	reject := ApprovalSignal{Step: AgentStep, Status: "rejected", By: "controller@customer", Note: "no PO"}
	_, err, pending := f.runAgent(agentOrder("3", policy.RoleOperator), reject)
	if errType(err) != ErrTypeRejected || len(pending) != 1 || f.orders("true") != 0 {
		t.Fatalf("err %v (%s), pending %d, orders %d", err, errType(err), len(pending), f.orders("true"))
	}
}

func TestAgentWriteToUnknownTargetFailsAtOnce(t *testing.T) {
	f := newFixture(t)
	in := agentOrder("4", policy.RoleOperator)
	in.Request.Target = "sap-ecc"
	_, err, _ := f.runAgent(in)
	if errType(err) != ErrTypeInvalid || !strings.Contains(err.Error(), "no connector for sap-ecc") {
		t.Fatalf("err %v (%s)", err, errType(err))
	}
}

func TestSameRequestIgnoresRoles(t *testing.T) {
	a, b := agentOrder("5", policy.RoleOperator).Request, agentOrder("5").Request
	if !sameRequest(a, b) {
		t.Error("a change of roles made the request different")
	}
	b.Payload = json.RawMessage(`{"externalId":"AGENT-6"}`)
	if sameRequest(a, b) {
		t.Error("a different record is the same request")
	}
}

func TestAgentWriteNotifiesRegisteredURLs(t *testing.T) {
	f := newFixture(t)
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	recorded := pushEnv(env, f.rt.Activities, "down")

	in := agentOrder("6", policy.RoleOperator)
	in.Push = []PushConfig{{ID: "start", URL: "https://agent.example/hook", Token: "t1"}}
	// Before the approval: one added and one removed by signal, and one
	// whose receiver refuses every notification.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalPushSet, PushConfig{ID: "later", URL: "https://agent.example/other"})
		env.SignalWorkflow(SignalPushSet, PushConfig{ID: "gone", URL: "https://agent.example/gone"})
		env.SignalWorkflow(SignalPushSet, PushConfig{ID: "down", URL: "https://agent.example/down"})
		env.SignalWorkflow(SignalPushDelete, "gone")
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow(QueryPush)
		var configs []PushConfig
		if err != nil || v.Get(&configs) != nil || len(configs) != 3 {
			t.Errorf("configs %+v, err %v", configs, err)
		}
		v, _ = env.QueryWorkflow(QueryPending)
		var p *PendingApproval
		_ = v.Get(&p)
		sig := approve(AgentStep)
		sig.Digest = p.Digest
		env.SignalWorkflow(SignalApproval, sig)
	}, time.Hour)
	env.ExecuteWorkflow(AgentWriteWorkflow, in)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a refused notification failed the write: %v", err)
	}
	sent := *recorded

	// The approval request and the outcome reached the three URLs
	// registered then; "gone" was removed first.
	var got []string
	for _, s := range sent {
		got = append(got, s.Config.ID+":"+string(s.Status.State))
		if s.WorkflowID == "" || s.Tool != "create_sales_order" {
			t.Errorf("notification %+v", s)
		}
	}
	sort.Strings(got)
	want := []string{"down:committed", "down:pending_approval", "later:committed", "later:pending_approval", "start:committed", "start:pending_approval"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	for _, s := range sent {
		if s.Status.State == AgentWriteCommitted && (s.Status.Write == nil || s.Status.Write.Status != writeguard.StatusCommitted) {
			t.Errorf("outcome without the write: %+v", s.Status)
		}
		if s.Status.State == AgentWritePending && (s.Status.Pending == nil || len(s.Status.Pending.Preview) == 0) {
			t.Errorf("approval notification without the preview: %+v", s.Status)
		}
	}
}

func TestAgentWriteNotifiesItsFailure(t *testing.T) {
	f := newFixture(t)
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	sent := pushEnv(env, f.rt.Activities)
	in := agentOrder("7", policy.RoleReader)
	in.Push = []PushConfig{{ID: "a", URL: "https://agent.example/hook"}}
	env.ExecuteWorkflow(AgentWriteWorkflow, in)
	if errType(env.GetWorkflowError()) != ErrTypeDenied {
		t.Fatalf("err %v", env.GetWorkflowError())
	}
	if len(*sent) != 1 || (*sent)[0].Status.State != AgentWriteDenied || !strings.Contains((*sent)[0].Status.Message, "integration-operator") {
		t.Fatalf("sent %+v", *sent)
	}
}

// pushEnv registers the agent write workflow with a notification activity
// that records what it sends, and refuses the configs named. The outcome
// goes to a detached workflow, which the test environment does not run:
// its input is recorded when it starts, then delivered by running it on
// its own.
func pushEnv(env *testsuite.TestWorkflowEnvironment, acts *Activities, refuse ...string) *[]AgentPushInput {
	var mu sync.Mutex
	var sent []AgentPushInput
	env.RegisterWorkflowWithOptions(AgentWriteWorkflow, workflow.RegisterOptions{Name: AgentWriteWorkflowName})
	env.RegisterWorkflowWithOptions(func(workflow.Context, []AgentPushInput) error { return nil }, workflow.RegisterOptions{Name: AgentPushWorkflowName})
	env.RegisterActivity(acts)
	record := func(_ context.Context, in AgentPushInput) error {
		mu.Lock()
		sent = append(sent, in)
		mu.Unlock()
		if slices.Contains(refuse, in.Config.ID) {
			return temporal.NewNonRetryableApplicationError("404 Not Found", ErrTypePushRefused, nil)
		}
		return nil
	}
	env.RegisterActivityWithOptions(record, activity.RegisterOptions{Name: ActivityAgentPush})
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var in []AgentPushInput
		if info.WorkflowType.Name != AgentPushWorkflowName || !strings.HasSuffix(info.WorkflowExecution.ID, "#outcome") || args.Get(&in) != nil {
			panic(fmt.Sprintf("unexpected child workflow %+v", info))
		}
		var s testsuite.WorkflowTestSuite
		child := s.NewTestWorkflowEnvironment()
		child.RegisterActivityWithOptions(record, activity.RegisterOptions{Name: ActivityAgentPush})
		child.ExecuteWorkflow(AgentPushWorkflow, in)
		if err := child.GetWorkflowError(); err != nil {
			panic(err)
		}
	})
	return &sent
}
