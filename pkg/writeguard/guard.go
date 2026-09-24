// Package writeguard makes writes into systems of record safe by default
// (architecture §8). Every write passes, in order: idempotency, policy,
// validation, simulation, approval, the per-target rate governor and
// circuit breaker, the commit, read-your-writes confirmation, metering and
// audit. Sagas chain writes and run compensations when a later step fails.
package writeguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/api/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/policy"
)

var (
	// ErrSimulationUnsupported is returned by targets that cannot dry-run an operation.
	ErrSimulationUnsupported = errors.New("simulation not supported")
	// ErrDenied is returned when policy does not allow the write.
	ErrDenied = errors.New("write denied by policy")
	// ErrRejected is returned when an approver rejects the write.
	ErrRejected = errors.New("write rejected by approver")
	// ErrInvalid is returned when a payload fails validation.
	ErrInvalid = errors.New("write payload invalid")
	// ErrUnconfirmed is returned when a committed write cannot be read back.
	ErrUnconfirmed = errors.New("write committed but not confirmed")
)

// Target is a system of record, reached through a connector.
type Target interface {
	// Simulate dry-runs op (test-run mode, validate-only API or sandbox) and
	// returns a preview of the resulting document, or ErrSimulationUnsupported.
	Simulate(ctx context.Context, op string, payload json.RawMessage) (json.RawMessage, error)
	// Commit performs op. key is passed through so targets that support
	// idempotent requests can de-duplicate on their side too.
	Commit(ctx context.Context, op, key string, payload json.RawMessage) (json.RawMessage, error)
}

// Confirmer is implemented by targets that can read back a committed write.
type Confirmer interface {
	Confirm(ctx context.Context, op string, result json.RawMessage) error
}

// Approver asks a person (console, chat integration) to approve a write.
type Approver interface {
	Approve(ctx context.Context, req ApprovalRequest) (policy.Approval, error)
}

// ApproverFunc adapts a function to Approver.
type ApproverFunc func(ctx context.Context, req ApprovalRequest) (policy.Approval, error)

func (f ApproverFunc) Approve(ctx context.Context, req ApprovalRequest) (policy.Approval, error) {
	return f(ctx, req)
}

// ApprovalRequest is what an approver sees.
type ApprovalRequest struct {
	Request Request         `json:"request"`
	Preview json.RawMessage `json:"preview,omitempty"`
	Reasons []string        `json:"reasons,omitempty"`
}

// Validator checks a payload against the semantic model and business rules.
type Validator func(ctx context.Context, req Request) error

// TargetConfig registers a target with its manifest-declared limits.
type TargetConfig struct {
	Target  Target
	Limits  v1alpha1.Limits
	Metered bool
}

// Config assembles a guard.
type Config struct {
	Targets  map[string]TargetConfig
	Policy   policy.Decider
	Approver Approver
	Audit    audit.Recorder
	Store    Store
	Validate Validator
	Breaker  BreakerConfig
	// Now overrides the clock; for tests.
	Now func() time.Time
}

// Request is one write.
type Request struct {
	Target         string          `json:"target"`
	Operation      string          `json:"operation"`
	Tool           string          `json:"tool,omitempty"`
	Risk           string          `json:"risk"`
	Subject        policy.Subject  `json:"subject"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Payload        json.RawMessage `json:"payload"`
	Entity         string          `json:"entity,omitempty"`
	Amount         float64         `json:"amount,omitempty"`
	Simulate       bool            `json:"simulate,omitempty"`
	Compensation   string          `json:"compensation,omitempty"`
	// Recipe names the recipe making the write, if any.
	Recipe string `json:"recipe,omitempty"`
	// RequireApproval asks a person even when policy would not.
	RequireApproval bool   `json:"requireApproval,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// Status of a write.
type Status string

const (
	StatusCommitted Status = "committed"
	StatusDuplicate Status = "duplicate"
	StatusDenied    Status = "denied"
	StatusRejected  Status = "rejected"
	StatusFailed    Status = "failed"
)

// Outcome is the result of a write.
type Outcome struct {
	Status   Status           `json:"status"`
	Result   json.RawMessage  `json:"result,omitempty"`
	Preview  json.RawMessage  `json:"preview,omitempty"`
	Approval *policy.Approval `json:"approval,omitempty"`
	Reasons  []string         `json:"reasons,omitempty"`
}

type target struct {
	TargetConfig
	gov *governor
	brk *breaker
}

// Guard executes governed writes. It is safe for concurrent use.
type Guard struct {
	cfg     Config
	targets map[string]*target
	mu      sync.Mutex
	metered map[string]int
}

// New validates the configuration and returns a guard.
func New(cfg Config) (*Guard, error) {
	if cfg.Policy == nil {
		return nil, errors.New("writeguard: a policy decider is required")
	}
	if cfg.Audit == nil {
		return nil, errors.New("writeguard: an audit recorder is required; every write is audited")
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	g := &Guard{cfg: cfg, targets: map[string]*target{}, metered: map[string]int{}}
	for name, tc := range cfg.Targets {
		if tc.Target == nil {
			return nil, fmt.Errorf("writeguard: target %q has no implementation", name)
		}
		if tc.Limits.RequestsPerSecond <= 0 || tc.Limits.MaxConcurrentCalls <= 0 {
			return nil, fmt.Errorf("writeguard: target %q needs positive rate and concurrency limits", name)
		}
		g.targets[name] = &target{
			TargetConfig: tc,
			gov:          newGovernor(tc.Limits.RequestsPerSecond, tc.Limits.MaxConcurrentCalls, cfg.Now),
			brk:          newBreaker(cfg.Breaker, cfg.Now),
		}
	}
	return g, nil
}

// Metered returns how many documents each metered target has created, for
// licensing such as SAP digital access.
func (g *Guard) Metered() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int, len(g.metered))
	for k, v := range g.metered {
		out[k] = v
	}
	return out
}

func storeKey(req Request) string {
	return req.Target + "/" + req.Operation + "/" + req.IdempotencyKey
}

func (g *Guard) actor(req Request) string {
	if req.Subject.Agent && req.Subject.OnBehalfOf != "" {
		return req.Subject.ID + " for " + req.Subject.OnBehalfOf
	}
	return req.Subject.ID
}

// Prepared is the result of the first phase of a write: everything up to
// the point where a person may have to approve it.
type Prepared struct {
	// Duplicate is set when the write already committed under this key.
	Duplicate     *Outcome        `json:"duplicate,omitempty"`
	Decision      policy.Decision `json:"decision"`
	Preview       json.RawMessage `json:"preview,omitempty"`
	NeedsApproval bool            `json:"needsApproval"`
}

func (g *Guard) target(req Request) (*target, error) {
	t, ok := g.targets[req.Target]
	if !ok {
		return nil, fmt.Errorf("writeguard: unknown target %q", req.Target)
	}
	if req.IdempotencyKey == "" {
		return nil, errors.New("writeguard: an idempotency key is required")
	}
	return t, nil
}

func input(req Request) policy.Input {
	return policy.Input{
		Recipe:  req.Recipe,
		Tool:    policy.Tool{Name: req.Tool, Risk: req.Risk},
		Subject: req.Subject,
		Action:  policy.Action{Entity: req.Entity, Amount: req.Amount},
	}
}

func (g *Guard) decide(ctx context.Context, actor string, in policy.Input) (policy.Decision, error) {
	dec, err := g.cfg.Policy.Decide(ctx, in)
	if err != nil {
		return dec, fmt.Errorf("writeguard: policy: %w", err)
	}
	_, err = g.cfg.Audit.Record(actor, "policy.decision", map[string]any{"input": in, "decision": dec})
	return dec, err
}

// Prepare runs the checks before approval: de-duplication, policy,
// validation and simulation. It does not claim the idempotency key, so a
// workflow can wait for approval for as long as it needs to.
func (g *Guard) Prepare(ctx context.Context, req Request) (Prepared, error) {
	t, err := g.target(req)
	if err != nil {
		return Prepared{}, err
	}
	actor := g.actor(req)
	prior, err := g.cfg.Store.Lookup(storeKey(req))
	if err != nil {
		return Prepared{}, err
	}
	if prior != nil {
		dup := *prior
		dup.Status = StatusDuplicate
		_, err := g.cfg.Audit.Record(actor, "writeback.duplicate", map[string]any{"target": req.Target, "operation": req.Operation, "key": req.IdempotencyKey})
		return Prepared{Duplicate: &dup}, err
	}

	dec, err := g.decide(ctx, actor, input(req))
	if err != nil {
		return Prepared{}, err
	}
	p := Prepared{Decision: dec, NeedsApproval: dec.RequireApproval || req.RequireApproval}
	// A request that is denied and would not become allowed through approval
	// stops here, before anyone is asked. High-risk writes are denied until
	// approved; an explicit denial cannot be approved away.
	if dec.Denied || (!dec.Allow && !p.NeedsApproval) {
		return p, ErrDenied
	}

	if g.cfg.Validate != nil {
		if err := g.cfg.Validate(ctx, req); err != nil {
			_, _ = g.cfg.Audit.Record(actor, "writeback.invalid", map[string]any{"target": req.Target, "operation": req.Operation, "error": err.Error()})
			return p, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}

	if req.Simulate {
		preview, err := t.Target.Simulate(ctx, req.Operation, req.Payload)
		switch {
		case errors.Is(err, ErrSimulationUnsupported):
		case err != nil:
			_, _ = g.cfg.Audit.Record(actor, "writeback.simulation-failed", map[string]any{"target": req.Target, "operation": req.Operation, "error": err.Error()})
			return p, fmt.Errorf("writeguard: simulation: %w", err)
		default:
			p.Preview = preview
			if _, err := g.cfg.Audit.Record(actor, "writeback.simulated", map[string]any{"target": req.Target, "operation": req.Operation, "preview": preview}); err != nil {
				return p, err
			}
		}
	}
	return p, nil
}

// Commit performs a prepared write. approval must be supplied when Prepare
// reported NeedsApproval. Policy is evaluated again with the approval, so a
// decision can never be carried over from a different request.
func (g *Guard) Commit(ctx context.Context, req Request, approval *policy.Approval) (Outcome, error) {
	t, err := g.target(req)
	if err != nil {
		return Outcome{Status: StatusFailed}, err
	}
	actor := g.actor(req)
	key := storeKey(req)

	prior, err := g.cfg.Store.Begin(key)
	if err != nil {
		return Outcome{Status: StatusFailed}, err
	}
	if prior != nil {
		out := *prior
		out.Status = StatusDuplicate
		_, aerr := g.cfg.Audit.Record(actor, "writeback.duplicate", map[string]any{"target": req.Target, "operation": req.Operation, "key": req.IdempotencyKey})
		return out, aerr
	}
	committed := false
	defer func() {
		if !committed {
			_ = g.cfg.Store.Abort(key)
		}
	}()

	in := input(req)
	if approval != nil {
		if _, err := g.cfg.Audit.Record(approval.By, "writeback.approval", map[string]any{"target": req.Target, "operation": req.Operation, "key": req.IdempotencyKey, "status": approval.Status, "note": approval.Note}); err != nil {
			return Outcome{Status: StatusFailed}, err
		}
		if approval.Status != policy.ApprovalApproved {
			return Outcome{Status: StatusRejected, Approval: approval}, ErrRejected
		}
		in.Approval = *approval
	}
	dec, err := g.decide(ctx, actor, in)
	if err != nil {
		return Outcome{Status: StatusFailed}, err
	}
	if (dec.RequireApproval || req.RequireApproval) && approval == nil {
		return Outcome{Status: StatusDenied, Reasons: dec.Reasons}, fmt.Errorf("%w: approval required", ErrDenied)
	}
	if !dec.Allow {
		return Outcome{Status: StatusDenied, Approval: approval, Reasons: dec.Reasons}, ErrDenied
	}

	result, err := g.commit(ctx, t, actor, req.Operation, req.IdempotencyKey, req)
	if err != nil {
		return Outcome{Status: StatusFailed, Approval: approval}, err
	}
	out := Outcome{Status: StatusCommitted, Result: result, Approval: approval}
	if err := g.cfg.Store.Complete(key, out); err != nil {
		return out, err
	}
	committed = true

	if c, ok := t.Target.(Confirmer); ok {
		if err := c.Confirm(ctx, req.Operation, result); err != nil {
			_, _ = g.cfg.Audit.Record(actor, "writeback.unconfirmed", map[string]any{"target": req.Target, "operation": req.Operation, "error": err.Error()})
			return out, fmt.Errorf("%w: %v", ErrUnconfirmed, err)
		}
	}
	return out, nil
}

// Execute runs one governed write in-process: Prepare, ask the configured
// approver if needed, then Commit.
func (g *Guard) Execute(ctx context.Context, req Request) (Outcome, error) {
	p, err := g.Prepare(ctx, req)
	if err != nil {
		status := StatusFailed
		if errors.Is(err, ErrDenied) {
			status = StatusDenied
		}
		return Outcome{Status: status, Preview: p.Preview, Reasons: p.Decision.Reasons}, err
	}
	if p.Duplicate != nil {
		return *p.Duplicate, nil
	}
	var approval *policy.Approval
	if p.NeedsApproval {
		if g.cfg.Approver == nil {
			return Outcome{Status: StatusDenied, Preview: p.Preview, Reasons: p.Decision.Reasons}, fmt.Errorf("%w: approval required but no approver is configured", ErrDenied)
		}
		a, err := g.cfg.Approver.Approve(ctx, ApprovalRequest{Request: req, Preview: p.Preview, Reasons: p.Decision.Reasons})
		if err != nil {
			return Outcome{Status: StatusFailed, Preview: p.Preview}, fmt.Errorf("writeguard: approval: %w", err)
		}
		approval = &a
	}
	out, err := g.Commit(ctx, req, approval)
	out.Preview = p.Preview
	return out, err
}

// commit runs a write through the governor and breaker, then meters and audits it.
func (g *Guard) commit(ctx context.Context, t *target, actor, op, key string, req Request) (json.RawMessage, error) {
	if err := t.brk.allow(); err != nil {
		return nil, fmt.Errorf("writeguard: %s: %w", req.Target, err)
	}
	release, err := t.gov.acquire(ctx)
	if err != nil {
		return nil, err
	}
	result, err := t.Target.Commit(ctx, op, key, req.Payload)
	release()
	if err != nil {
		t.brk.failure()
		_, _ = g.cfg.Audit.Record(actor, "writeback.failed", map[string]any{"target": req.Target, "operation": op, "key": key, "error": err.Error()})
		return nil, fmt.Errorf("writeguard: commit %s.%s: %w", req.Target, op, err)
	}
	t.brk.success()
	if t.Metered {
		g.mu.Lock()
		g.metered[req.Target]++
		g.mu.Unlock()
	}
	_, err = g.cfg.Audit.Record(actor, "writeback.committed", map[string]any{
		"target": req.Target, "operation": op, "key": key, "reason": req.Reason,
		"result": result, "metered": t.Metered,
	})
	return result, err
}

// Compensate undoes a committed write given the result it returned.
func (g *Guard) Compensate(ctx context.Context, req Request, result json.RawMessage) error {
	return g.compensate(ctx, req, result)
}

// compensate undoes a committed write. Compensations are automatic: they
// skip policy and approval but still pass the governor, breaker and audit,
// and are idempotent under a derived key.
func (g *Guard) compensate(ctx context.Context, req Request, result json.RawMessage) error {
	t, ok := g.targets[req.Target]
	if !ok {
		return fmt.Errorf("writeguard: unknown target %q", req.Target)
	}
	if req.Compensation == "" {
		return fmt.Errorf("writeguard: %s.%s has no compensation", req.Target, req.Operation)
	}
	creq := req
	creq.Operation = req.Compensation
	creq.IdempotencyKey = req.IdempotencyKey + "#compensate"
	creq.Payload = result
	creq.Reason = "compensating " + req.Operation
	key := storeKey(creq)
	prior, err := g.cfg.Store.Begin(key)
	if err != nil {
		return err
	}
	if prior != nil {
		return nil
	}
	res, err := g.commit(ctx, t, "porter/saga", creq.Operation, creq.IdempotencyKey, creq)
	if err != nil {
		_ = g.cfg.Store.Abort(key)
		return err
	}
	return g.cfg.Store.Complete(key, Outcome{Status: StatusCommitted, Result: res})
}
