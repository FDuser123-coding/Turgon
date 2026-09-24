package writeguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Saga chains governed writes. If a write fails, every write already
// committed in the saga is compensated in reverse order.
type Saga struct {
	g    *Guard
	id   string
	done []step
}

type step struct {
	req    Request
	result json.RawMessage
}

// NewSaga starts a saga. id should derive from the triggering source record.
func (g *Guard) NewSaga(id string) *Saga { return &Saga{g: g, id: id} }

// Write executes one step. Every step must declare its compensation up front.
// On failure the saga compensates and returns the step's error joined with
// any compensation errors.
func (s *Saga) Write(ctx context.Context, req Request) (Outcome, error) {
	if req.Compensation == "" {
		return Outcome{Status: StatusFailed}, fmt.Errorf("saga %s: %s.%s must declare a compensation", s.id, req.Target, req.Operation)
	}
	out, err := s.g.Execute(ctx, req)
	if err != nil && !(errors.Is(err, ErrUnconfirmed) && out.Status == StatusCommitted) {
		if cerr := s.Compensate(ctx); cerr != nil {
			return out, errors.Join(err, cerr)
		}
		return out, err
	}
	if out.Status == StatusCommitted || out.Status == StatusDuplicate {
		s.done = append(s.done, step{req: req, result: out.Result})
	}
	return out, err
}

// Compensate undoes every committed step, newest first, and clears the saga.
func (s *Saga) Compensate(ctx context.Context) error {
	var errs []error
	for i := len(s.done) - 1; i >= 0; i-- {
		st := s.done[i]
		if err := s.g.compensate(ctx, st.req, st.result); err != nil {
			errs = append(errs, fmt.Errorf("saga %s: compensate %s.%s: %w", s.id, st.req.Target, st.req.Operation, err))
		}
	}
	s.done = nil
	if _, err := s.g.cfg.Audit.Record("porter/saga", "saga.compensated", map[string]any{"saga": s.id, "errors": len(errs)}); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
