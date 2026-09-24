package writeguard

import (
	"errors"
	"sync"
)

// ErrInFlight is returned when a write with the same idempotency key is
// already being processed.
var ErrInFlight = errors.New("a write with this idempotency key is in progress")

// Store is the de-duplication store keyed by source identity, so retries
// never create duplicates. Production uses Postgres; MemoryStore is for
// tests and single-node prototypes.
type Store interface {
	// Begin claims key. It returns the stored outcome if the key already
	// completed, or ErrInFlight if another caller holds it.
	Begin(key string) (*Outcome, error)
	// Complete records the outcome and releases the claim.
	Complete(key string, out Outcome) error
	// Abort releases the claim without recording an outcome, so the write
	// can be retried (for example after a denial or a failed commit).
	Abort(key string) error
}

// MemoryStore is an in-process Store.
type MemoryStore struct {
	mu       sync.Mutex
	done     map[string]Outcome
	inflight map[string]bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{done: map[string]Outcome{}, inflight: map[string]bool{}}
}

func (s *MemoryStore) Begin(key string) (*Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if out, ok := s.done[key]; ok {
		return &out, nil
	}
	if s.inflight[key] {
		return nil, ErrInFlight
	}
	s.inflight[key] = true
	return nil, nil
}

func (s *MemoryStore) Complete(key string, out Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, key)
	s.done[key] = out
	return nil
}

func (s *MemoryStore) Abort(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, key)
	return nil
}
