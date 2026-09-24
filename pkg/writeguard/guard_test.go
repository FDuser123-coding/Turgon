package writeguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/api/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/policy"
)

// fakeSAP simulates BAPI test-run mode and numbers created documents.
type fakeSAP struct {
	mu        sync.Mutex
	next      int
	docs      map[string]string // doc number -> status
	commits   int
	failOps   map[string]error
	simulated int
}

func newFakeSAP() *fakeSAP {
	return &fakeSAP{next: 4500000001, docs: map[string]string{}, failOps: map[string]error{}}
}

func (s *fakeSAP) Simulate(_ context.Context, op string, payload json.RawMessage) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.simulated++
	if op != "create-sales-order" {
		return nil, ErrSimulationUnsupported
	}
	return json.RawMessage(fmt.Sprintf(`{"testrun":true,"input":%s}`, payload)), nil
}

func (s *fakeSAP) Commit(_ context.Context, op, key string, payload json.RawMessage) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failOps[op]; err != nil {
		return nil, err
	}
	s.commits++
	switch op {
	case "create-sales-order", "create-delivery":
		doc := fmt.Sprint(s.next)
		s.next++
		s.docs[doc] = "open"
		return json.RawMessage(fmt.Sprintf(`{"document":%q}`, doc)), nil
	case "cancel-sales-order", "cancel-delivery":
		var r struct{ Document string }
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, err
		}
		s.docs[r.Document] = "cancelled"
		return json.RawMessage(`{"cancelled":true}`), nil
	}
	return nil, fmt.Errorf("unknown op %s", op)
}

func (s *fakeSAP) Confirm(_ context.Context, _ string, result json.RawMessage) error {
	var r struct{ Document string }
	_ = json.Unmarshal(result, &r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Document != "" && s.docs[r.Document] == "" {
		return errors.New("document not found on read-back")
	}
	return nil
}

type harness struct {
	guard    *Guard
	sap      *fakeSAP
	log      *bytes.Buffer
	approver *recordingApprover
}

type recordingApprover struct {
	mu       sync.Mutex
	decision string
	seen     []ApprovalRequest
}

func (a *recordingApprover) Approve(_ context.Context, req ApprovalRequest) (policy.Approval, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = append(a.seen, req)
	return policy.Approval{Status: a.decision, By: "approver@customer"}, nil
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{sap: newFakeSAP(), log: &bytes.Buffer{}, approver: &recordingApprover{decision: policy.ApprovalApproved}}
	g, err := New(Config{
		Targets: map[string]TargetConfig{
			"erp": {Target: h.sap, Limits: v1alpha1.Limits{MaxConcurrentCalls: 4, RequestsPerSecond: 1000}, Metered: true},
		},
		Policy:   policy.WritebackDefault{},
		Approver: h.approver,
		Audit:    audit.New(h.log),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.guard = g
	return h
}

func (h *harness) actions(t *testing.T) []string {
	t.Helper()
	if _, err := audit.Verify(bytes.NewReader(h.log.Bytes())); err != nil {
		t.Fatalf("audit chain broken: %v", err)
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(h.log.String()), "\n") {
		var e audit.Entry
		_ = json.Unmarshal([]byte(line), &e)
		out = append(out, e.Action)
	}
	return out
}

func agentOrder(key string) Request {
	return Request{
		Target: "erp", Operation: "create-sales-order", Tool: "create_sales_order", Risk: v1alpha1.RiskHigh,
		Subject:        policy.Subject{ID: "agent-7", Agent: true, OnBehalfOf: "alice", Roles: []string{policy.RoleOperator}},
		IdempotencyKey: key, Payload: json.RawMessage(`{"customer":"C1","netValue":1200}`),
		Entity: "SalesOrder", Amount: 1200, Simulate: true, Compensation: "cancel-sales-order",
	}
}

// The governed write-back sequence of architecture §8, Figure 4.
func TestGovernedWriteBack(t *testing.T) {
	h := newHarness(t)
	out, err := h.guard.Execute(context.Background(), agentOrder("006Qy00000AbCdE"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != StatusCommitted || !strings.Contains(string(out.Result), "4500000001") {
		t.Fatalf("outcome = %+v", out)
	}
	if len(h.approver.seen) != 1 || !strings.Contains(string(h.approver.seen[0].Preview), `"testrun":true`) {
		t.Fatalf("approver did not see a simulated preview: %+v", h.approver.seen)
	}
	if got := h.guard.Metered()["erp"]; got != 1 {
		t.Errorf("metered = %d, want 1", got)
	}
	want := []string{"policy.decision", "writeback.simulated", "writeback.approval", "policy.decision", "writeback.committed"}
	if got := h.actions(t); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("audit trail = %v, want %v", got, want)
	}
	if !strings.Contains(h.log.String(), `"actor":"agent-7 for alice"`) {
		t.Error("audit does not record the user the agent acted for")
	}
}

func TestRetriesNeverDuplicate(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	first, err := h.guard.Execute(ctx, agentOrder("opp-1"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		again, err := h.guard.Execute(ctx, agentOrder("opp-1"))
		if err != nil || again.Status != StatusDuplicate || string(again.Result) != string(first.Result) {
			t.Fatalf("retry %d: %+v %v", i, again, err)
		}
	}
	if h.sap.commits != 1 || len(h.approver.seen) != 1 {
		t.Fatalf("commits=%d approvals=%d, want 1 and 1", h.sap.commits, len(h.approver.seen))
	}
}

func TestConcurrentRetriesCommitOnce(t *testing.T) {
	h := newHarness(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.guard.Execute(context.Background(), agentOrder("opp-race"))
		}()
	}
	wg.Wait()
	if h.sap.commits != 1 {
		t.Fatalf("commits = %d, want 1", h.sap.commits)
	}
}

func TestRejectedApprovalDoesNotWrite(t *testing.T) {
	h := newHarness(t)
	h.approver.decision = policy.ApprovalRejected
	out, err := h.guard.Execute(context.Background(), agentOrder("opp-2"))
	if !errors.Is(err, ErrRejected) || out.Status != StatusRejected || h.sap.commits != 0 {
		t.Fatalf("out=%+v err=%v commits=%d", out, err, h.sap.commits)
	}
	// Rejection releases the key so a corrected request can be resubmitted.
	h.approver.decision = policy.ApprovalApproved
	if out, err := h.guard.Execute(context.Background(), agentOrder("opp-2")); err != nil || out.Status != StatusCommitted {
		t.Fatalf("resubmission: %+v %v", out, err)
	}
}

func TestPolicyDeniesWithoutRole(t *testing.T) {
	h := newHarness(t)
	req := agentOrder("x")
	req.Risk = v1alpha1.RiskLow
	req.Subject.Roles = nil
	out, err := h.guard.Execute(context.Background(), req)
	if !errors.Is(err, ErrDenied) || out.Status != StatusDenied || h.sap.commits != 0 || h.sap.simulated != 0 {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestSagaCompensatesOnFailure(t *testing.T) {
	h := newHarness(t)
	h.sap.failOps["create-delivery"] = errors.New("BAPI returned E: plant blocked")
	ctx := context.Background()
	saga := h.guard.NewSaga("opp-3")

	order, err := saga.Write(ctx, agentOrder("opp-3"))
	if err != nil {
		t.Fatal(err)
	}
	delivery := agentOrder("opp-3")
	delivery.Operation, delivery.Compensation, delivery.Risk = "create-delivery", "cancel-delivery", v1alpha1.RiskLow
	if _, err := saga.Write(ctx, delivery); err == nil {
		t.Fatal("expected the delivery step to fail")
	}
	var r struct{ Document string }
	_ = json.Unmarshal(order.Result, &r)
	if h.sap.docs[r.Document] != "cancelled" {
		t.Fatalf("order %s not compensated: %v", r.Document, h.sap.docs)
	}
	acts := strings.Join(h.actions(t), ",")
	if !strings.Contains(acts, "writeback.failed") || !strings.HasSuffix(acts, "writeback.committed,saga.compensated") {
		t.Fatalf("audit trail = %s", acts)
	}
}

func TestSagaRequiresCompensation(t *testing.T) {
	h := newHarness(t)
	req := agentOrder("opp-4")
	req.Compensation = ""
	if _, err := h.guard.NewSaga("s").Write(context.Background(), req); err == nil {
		t.Fatal("saga accepted a step without an undo action")
	}
}

func TestCircuitBreakerOpensAndRecovers(t *testing.T) {
	now := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	sap := newFakeSAP()
	sap.failOps["create-sales-order"] = errors.New("RFC connection reset")
	g, err := New(Config{
		Targets: map[string]TargetConfig{"erp": {Target: sap, Limits: v1alpha1.Limits{MaxConcurrentCalls: 1, RequestsPerSecond: 1e6}}},
		Policy:  policy.WritebackDefault{},
		Approver: ApproverFunc(func(context.Context, ApprovalRequest) (policy.Approval, error) {
			return policy.Approval{Status: policy.ApprovalApproved}, nil
		}),
		Audit:   audit.New(&bytes.Buffer{}),
		Breaker: BreakerConfig{Failures: 3, Cooldown: time.Minute},
		Now:     clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := g.Execute(ctx, agentOrder(fmt.Sprint("k", i))); errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("circuit opened early at attempt %d", i)
		}
	}
	if _, err := g.Execute(ctx, agentOrder("k3")); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	now = now.Add(time.Minute)
	delete(sap.failOps, "create-sales-order")
	if out, err := g.Execute(ctx, agentOrder("k4")); err != nil || out.Status != StatusCommitted {
		t.Fatalf("trial request after cooldown: %+v %v", out, err)
	}
}

func TestGovernorHoldsDeclaredRate(t *testing.T) {
	g := newGovernor(20, 2, time.Now)
	start := time.Now()
	for i := 0; i < 30; i++ { // 20 burst + 10 at 20/s ≈ 0.5s
		release, err := g.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("30 requests at 20/s took %v", elapsed)
	}
}

func TestGovernorHonoursContext(t *testing.T) {
	g := newGovernor(1, 1, time.Now)
	release, _ := g.acquire(context.Background())
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := g.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}
