package pgstore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

func TestStreamPositionsAreStoredWithTheirEvents(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const src = "salesforce-prod/Opportunity.Changed"
	if resume, err := s.StreamResume(ctx, src); err != nil || resume != nil {
		t.Fatalf("fresh: %x %v", resume, err)
	}
	ev := func(id string) connector.Event { return connector.Event{ID: id, Payload: json.RawMessage(`{}`)} }
	if n, err := s.DeliverStream(ctx, src, []connector.Event{ev("a"), ev("b")}, []byte{0, 0, 1}); err != nil || n != 2 {
		t.Fatalf("deliver: %d %v", n, err)
	}
	// A keepalive carries only a newer position; a replayed batch adds nothing.
	if n, err := s.DeliverStream(ctx, src, nil, []byte{0, 0, 2}); err != nil || n != 0 {
		t.Fatalf("keepalive: %d %v", n, err)
	}
	if n, _ := s.DeliverStream(ctx, src, []connector.Event{ev("b"), ev("c")}, []byte{0, 0, 3}); n != 1 {
		t.Fatalf("replay: %d", n)
	}
	if resume, _ := s.StreamResume(ctx, src); string(resume) != string([]byte{0, 0, 3}) {
		t.Fatalf("resume: %x", resume)
	}
	if got, _ := s.Inbox(ctx, src, "Opportunity.Changed", 0, 10); len(got) != 3 {
		t.Fatalf("inbox: %+v", got)
	}
	// Events that cannot be stored leave the position where it was.
	bad := connector.Event{ID: "d", Payload: json.RawMessage(`not json`)}
	if _, err := s.DeliverStream(ctx, src, []connector.Event{bad}, []byte{0, 0, 4}); err == nil {
		t.Fatal("stored an invalid payload")
	}
	if resume, _ := s.StreamResume(ctx, src); string(resume) != string([]byte{0, 0, 3}) {
		t.Fatalf("position moved past events that were not stored: %x", resume)
	}
}

func TestTryLockIsHeldByOneWorker(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	release, ok, err := s.TryLock(ctx, "stream:x")
	if err != nil || !ok {
		t.Fatalf("first: %v %v", ok, err)
	}
	if _, ok, err := s.TryLock(ctx, "stream:x"); err != nil || ok {
		t.Fatalf("second worker got the lock: %v %v", ok, err)
	}
	if r, ok, _ := s.TryLock(ctx, "stream:y"); !ok {
		t.Fatal("another name is locked too")
	} else {
		r()
	}
	release()
	r, ok, _ := s.TryLock(ctx, "stream:x")
	if !ok {
		t.Fatal("not free after release")
	}
	r()
}
