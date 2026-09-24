package writeguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/policy"
)

// ErrNotFound is returned by readers when no record has the given ID.
var ErrNotFound = errors.New("record not found")

// Reader is implemented by targets that answer reads by record ID.
type Reader interface {
	Read(ctx context.Context, op, id string) (json.RawMessage, error)
}

// ReadRequest is one read on behalf of a person or agent.
type ReadRequest struct {
	Target    string         `json:"target"`
	Operation string         `json:"operation"`
	Tool      string         `json:"tool,omitempty"`
	Subject   policy.Subject `json:"subject"`
	Entity    string         `json:"entity,omitempty"`
	ID        string         `json:"id"`
}

// Read performs a governed read: policy decides, the target's rate
// governor and circuit breaker apply as for writes, and the audit log
// records who read what (never the data returned). Agents never see the
// target's credentials; the connector uses them on their behalf.
func (g *Guard) Read(ctx context.Context, req ReadRequest) (json.RawMessage, error) {
	t, ok := g.targets[req.Target]
	if !ok {
		return nil, fmt.Errorf("writeguard: unknown target %q", req.Target)
	}
	r, ok := t.Target.(Reader)
	if !ok {
		return nil, fmt.Errorf("writeguard: target %q cannot answer reads", req.Target)
	}
	actor := g.actor(Request{Subject: req.Subject})
	in := policy.Input{
		Tool:    policy.Tool{Name: req.Tool, Risk: v1alpha1.RiskRead},
		Subject: req.Subject,
		Action:  policy.Action{Entity: req.Entity},
	}
	dec, err := g.decide(ctx, actor, in)
	if err != nil {
		return nil, err
	}
	if dec.Denied || !dec.Allow {
		return nil, fmt.Errorf("%w: %v", ErrDenied, dec.Reasons)
	}
	if err := t.brk.allow(); err != nil {
		return nil, fmt.Errorf("writeguard: %s: %w", req.Target, err)
	}
	release, err := t.gov.acquire(ctx)
	if err != nil {
		return nil, err
	}
	data, err := r.Read(ctx, req.Operation, req.ID)
	release()
	switch {
	case errors.Is(err, ErrNotFound):
		t.brk.success() // the system answered
	case err != nil:
		t.brk.failure()
	default:
		t.brk.success()
	}
	found := err == nil
	if _, aerr := g.cfg.Audit.Record(actor, "read", map[string]any{
		"target": req.Target, "operation": req.Operation, "tool": req.Tool, "id": req.ID, "found": found,
	}); aerr != nil {
		return nil, aerr
	}
	return data, err
}
