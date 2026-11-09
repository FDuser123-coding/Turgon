package writeguard

import (
	"time"
)

// ApprovalLedger keeps, for each write, the run whose request for approval
// is open, and the runs waiting on its decision. When a record changes
// while a write of it waits for a person, the run for the change waits on
// that request instead of opening a second one: an approval makes its
// write a duplicate, and a rejection lets it ask with the newer data.
// A Store may implement it; without one, every run asks.
type ApprovalLedger interface {
	// OpenApproval records run as asking for the approval of key until
	// the given time, unless another run's request is still open: then it
	// returns that run, and records run as waiting on it. An empty result
	// means run holds the request.
	OpenApproval(key, run string, until time.Time) (holder string, err error)
	// CloseApproval ends run's request for key and returns the runs that
	// waited on it, to be woken. A run that does not hold the request
	// stops waiting on it.
	CloseApproval(key, run string) (waiters []string, err error)
}

// Ledger returns the store's approval ledger, or nil when it keeps none.
func (g *Guard) Ledger() ApprovalLedger {
	l, _ := g.cfg.Store.(ApprovalLedger)
	return l
}

// ApprovalKey names the write a request makes, as the ledger keys it.
func ApprovalKey(req Request) string { return storeKey(req) }

type openApproval struct {
	holder  string
	until   time.Time
	waiters []string
}

func (s *MemoryStore) OpenApproval(key, run string, until time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.approvals == nil {
		s.approvals = map[string]*openApproval{}
	}
	a := s.approvals[key]
	if a == nil {
		s.approvals[key] = &openApproval{holder: run, until: until}
		return "", nil
	}
	if a.holder == run || !time.Now().Before(a.until) {
		a.holder, a.until, a.waiters = run, until, without(a.waiters, run)
		return "", nil
	}
	a.waiters = append(without(a.waiters, run), run)
	return a.holder, nil
}

func (s *MemoryStore) CloseApproval(key, run string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.approvals[key]
	if a == nil {
		return nil, nil
	}
	if a.holder != run {
		a.waiters = without(a.waiters, run)
		return nil, nil
	}
	delete(s.approvals, key)
	return a.waiters, nil
}

func without(list []string, s string) []string {
	out := list[:0:0]
	for _, x := range list {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}

var _ ApprovalLedger = (*MemoryStore)(nil)
