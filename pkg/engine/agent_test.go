package engine

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

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
