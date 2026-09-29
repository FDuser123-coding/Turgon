package engine

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/metrics"
)

// StreamInbox is an inbox that also keeps where each subscription resumes.
type StreamInbox interface {
	Inbox
	// DeliverStream stores events and the position to resume after them in
	// one transaction, and returns how many events were new.
	DeliverStream(ctx context.Context, source string, events []connector.Event, resume []byte) (int, error)
	// StreamResume returns where a subscription resumes (nil: from now).
	StreamResume(ctx context.Context, source string) ([]byte, error)
}

// Locker gives a named lock to one worker at a time, so a deployment's
// replicas keep one subscription per event between them.
type Locker interface {
	// TryLock takes the lock if it is free. The worker holds it until
	// release, or until its connection to the lock's store is lost.
	TryLock(ctx context.Context, name string) (release func(), ok bool, err error)
}

// Streams keeps the runtime's subscriptions open: each event that arrives
// over one is stored in the inbox with the position after it, and the
// dispatcher starts its runs from there. A subscription that fails is
// reopened from the stored position, with backoff.
type Streams struct {
	Runtime *Runtime
	Inbox   StreamInbox
	// Locker, if set, lets one worker subscribe per event; the others
	// stand by and take over when it stops.
	Locker Locker
	// Delivered is called after new events are stored, to wake the
	// dispatcher.
	Delivered func()
	// Log reports subscription errors.
	Log func(format string, args ...any)
	// MinBackoff and MaxBackoff bound the wait before reopening a failed
	// subscription (defaults 1s and 1m); Standby is how often a worker
	// without the lock tries to take it (default 10s).
	MinBackoff, MaxBackoff, Standby time.Duration
}

// Run keeps every subscription open until ctx ends.
func (s *Streams) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for endpoint, events := range s.Runtime.Streams {
		for event := range events {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.run(ctx, endpoint, event)
			}()
		}
	}
	wg.Wait()
}

func (s *Streams) run(ctx context.Context, endpoint, event string) {
	minB, maxB, standby := or(s.MinBackoff, time.Second), or(s.MaxBackoff, time.Minute), or(s.Standby, 10*time.Second)
	src := s.Runtime.Sources[endpoint].(connector.Streamer)
	source := InboxSource(endpoint, event)
	var workflows []string
	for _, wf := range s.Runtime.Spec.Spec.Workflows {
		if wf.Trigger.Endpoint == endpoint && wf.Trigger.Event == event {
			workflows = append(workflows, wf.Name)
		}
	}
	sort.Strings(workflows)
	backoff := minB
	for ctx.Err() == nil {
		release := func() {}
		if s.Locker != nil {
			r, ok, err := s.Locker.TryLock(ctx, "stream:"+source)
			if err != nil {
				s.log("turgon: subscription %s: lock: %v", source, err)
			}
			if err != nil || !ok {
				sleep(ctx, standby)
				continue
			}
			release = r
		}
		resume, err := s.Inbox.StreamResume(ctx, source)
		if err == nil {
			err = src.Stream(ctx, event, resume, func(events []connector.Event, next []byte) error {
				n, err := s.Inbox.DeliverStream(ctx, source, events, next)
				if err != nil {
					return err
				}
				for _, wf := range workflows {
					polled(wf, nil)
				}
				backoff = minB
				if n > 0 && s.Delivered != nil {
					s.Delivered()
				}
				return nil
			})
		}
		release()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("the subscription ended")
		}
		for _, wf := range workflows {
			metrics.PollErrors.WithLabelValues(wf).Inc()
		}
		s.log("turgon: subscription %s: %v (reopening in %s)", source, err, backoff)
		sleep(ctx, backoff)
		backoff = min(backoff*2, maxB)
	}
}

func (s *Streams) log(format string, args ...any) {
	if s.Log != nil {
		s.Log(format, args...)
	}
}

func or(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
