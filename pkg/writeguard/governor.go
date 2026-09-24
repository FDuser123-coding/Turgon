package writeguard

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned while a target's circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit open: target is failing, writes paused")

// governor enforces a target's declared requests-per-second and concurrency
// limits, so agent traffic cannot overwhelm a legacy core (architecture §8).
type governor struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	sem    chan struct{}
	now    func() time.Time
}

func newGovernor(rps float64, concurrency int, now func() time.Time) *governor {
	if concurrency < 1 {
		concurrency = 1
	}
	burst := rps
	if burst < 1 {
		burst = 1
	}
	return &governor{rate: rps, burst: burst, tokens: burst, last: now(), sem: make(chan struct{}, concurrency), now: now}
}

// acquire blocks until a request may start and returns its release func.
func (g *governor) acquire(ctx context.Context) (func(), error) {
	select {
	case g.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-g.sem }
	for {
		wait := g.take()
		if wait == 0 {
			return release, nil
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			release()
			return nil, ctx.Err()
		}
	}
}

// take consumes a token, or returns how long to wait for one.
func (g *governor) take() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	g.tokens += now.Sub(g.last).Seconds() * g.rate
	if g.tokens > g.burst {
		g.tokens = g.burst
	}
	g.last = now
	if g.tokens >= 1 {
		g.tokens--
		return 0
	}
	return time.Duration((1 - g.tokens) / g.rate * float64(time.Second))
}

// BreakerConfig tunes the per-target circuit breaker.
type BreakerConfig struct {
	// Failures is the number of consecutive failures that opens the circuit. Default 5.
	Failures int
	// Cooldown is how long the circuit stays open before a trial request. Default 30s.
	Cooldown time.Duration
}

type breaker struct {
	mu       sync.Mutex
	cfg      BreakerConfig
	failures int
	openedAt time.Time
	open     bool
	trial    bool
	now      func() time.Time
}

func newBreaker(cfg BreakerConfig, now func() time.Time) *breaker {
	if cfg.Failures <= 0 {
		cfg.Failures = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 30 * time.Second
	}
	return &breaker{cfg: cfg, now: now}
}

// allow reports whether a request may proceed. After the cooldown a single
// trial request is let through (half-open).
func (b *breaker) allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return nil
	}
	if !b.trial && b.now().Sub(b.openedAt) >= b.cfg.Cooldown {
		b.trial = true
		return nil
	}
	return ErrCircuitOpen
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures, b.open, b.trial = 0, false, false
}

func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.trial || b.failures >= b.cfg.Failures {
		b.open, b.trial, b.openedAt = true, false, b.now()
	}
}
