package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
)

type memCursors map[string]int64

func (m memCursors) Cursor(_ context.Context, name string) (int64, error) { return m[name], nil }
func (m memCursors) SetCursor(_ context.Context, name string, pos int64) error {
	m[name] = pos
	return nil
}

type memInbox struct{ got []connector.Event }

func (m *memInbox) Deliver(_ context.Context, _ string, evs []connector.Event) (int, error) {
	m.got = append(m.got, evs...)
	return len(evs), nil
}
func (m *memInbox) Inbox(context.Context, string, string, int64, int) ([]connector.Event, error) {
	return nil, nil
}

// skipSource answers its pages in turn, each read from the cursor.
type skipSource struct {
	pages  [][]connector.Event
	afters []int64
}

func (s *skipSource) Poll(_ context.Context, _ string, after int64, _ int) ([]connector.Event, error) {
	s.afters = append(s.afters, after)
	if len(s.pages) == 0 {
		return nil, nil
	}
	p := s.pages[0]
	s.pages = s.pages[1:]
	return p, nil
}

func ev(id string, pos int64) connector.Event {
	return connector.Event{ID: id, Position: pos, Name: "SalesOrder.Deleted", Payload: json.RawMessage(`{}`)}
}

func skip(pos int64) connector.Event {
	return connector.Event{Position: pos, Name: "SalesOrder.Deleted", Skip: true}
}

// A skip marker moves the cursor past items the source left out and starts
// no run, whether the events are polled or reconciled into an inbox.
func TestSkipMarkersMoveTheCursorOnly(t *testing.T) {
	ctx := context.Background()
	wf := compiler.Workflow{Name: "deleted"}
	rt := &Runtime{Spec: &compiler.RuntimeSpec{}}

	src := &skipSource{pages: [][]connector.Event{{ev("a", 3), skip(9)}, {skip(20)}}}
	cur, rec := memCursors{}, &recorder{}
	d := &Dispatcher{Runtime: rt, Cursors: cur, Starter: rec}
	read := func(after int64) ([]connector.Event, error) { return src.Poll(ctx, "", after, 10) }
	if n, err := d.drain(ctx, wf, "c", read); err != nil || n != 1 || len(rec.ids) != 1 || cur["c"] != 9 {
		t.Fatalf("drain: %d %v, runs %v, cursor %d", n, err, rec.ids, cur["c"])
	}
	if n, err := d.drain(ctx, wf, "c", read); err != nil || n != 0 || rec.calls != 1 || cur["c"] != 20 {
		t.Fatalf("drain of a lone skip: %d %v, starts %d, cursor %d", n, err, rec.calls, cur["c"])
	}

	src = &skipSource{pages: [][]connector.Event{{skip(5)}, {ev("b", 7), skip(12)}}}
	inbox := &memInbox{}
	d = &Dispatcher{Runtime: rt, Cursors: cur, Starter: rec, Inbox: inbox}
	if err := d.reconcile(ctx, src, wf, "r", "inbox", 10); err != nil {
		t.Fatal(err)
	}
	if len(inbox.got) != 1 || inbox.got[0].ID != "b" || cur["r"] != 12 {
		t.Fatalf("reconciled %+v, cursor %d", inbox.got, cur["r"])
	}
	// Each poll read from past the last marker; the third found nothing.
	if len(src.afters) != 3 || src.afters[1] != 5 || src.afters[2] != 12 {
		t.Fatalf("polled after %v", src.afters)
	}
}

type xrefs struct{ put []string }

func (x *xrefs) Xref(context.Context, string, string, string) (string, bool, error) {
	return "", false, nil
}
func (x *xrefs) PutXref(_ context.Context, entity, system, sourceID, masterID string) error {
	x.put = append(x.put, entity+"/"+system+"/"+sourceID+"->"+masterID)
	return nil
}

// A link records a string or a number. An object at the ID path is a
// wrong path, which fails instead of linking by its printout.
func TestLinkWriteNeedsScalarIDs(t *testing.T) {
	store := &xrefs{}
	x := &Activities{Resolver: store}
	link := v1alpha1.WriteLink{Entity: "model.SalesOrder", ID: "d.OrderID", Master: "shopifyId"}
	doc := map[string]any{"shopifyId": float64(1794323503001)}
	if err := x.LinkWrite(context.Background(), LinkInput{Link: link, System: "exact-online", Result: json.RawMessage(`{"d":{"OrderID":"b8d0"}}`), Doc: doc}); err != nil {
		t.Fatal(err)
	}
	if len(store.put) != 1 || store.put[0] != "SalesOrder/exact-online/b8d0->1794323503001" {
		t.Fatalf("put %v", store.put)
	}
	for name, res := range map[string]string{"object": `{"d":{"OrderID":{"a":1}}}`, "array": `{"d":{"OrderID":[1]}}`, "bool": `{"d":{"OrderID":true}}`, "missing": `{"d":{}}`} {
		if err := x.LinkWrite(context.Background(), LinkInput{Link: link, System: "exact-online", Result: json.RawMessage(res), Doc: doc}); err == nil {
			t.Errorf("%s: linked", name)
		}
	}
	if len(store.put) != 1 {
		t.Fatalf("put %v", store.put)
	}
}
