package pgstore

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// One request per write, whatever the number of workers asking at once;
// waiters are handed back on close, and a request past its time is taken
// over.
func TestApprovalLedger(t *testing.T) {
	s := newStore(t)
	key := "erp/create/" + strings.ReplaceAll(t.Name(), "/", "-")
	soon := time.Now().Add(time.Hour)
	var wg sync.WaitGroup
	holders := make([]string, 8)
	for i := range holders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := s.OpenApproval(key, "run-"+string(rune('a'+i)), soon)
			if err != nil {
				t.Error(err)
			}
			holders[i] = h
		}(i)
	}
	wg.Wait()
	held, holder := 0, ""
	for i, h := range holders {
		if h == "" {
			held++
			holder = "run-" + string(rune('a'+i))
		}
	}
	if held != 1 {
		t.Fatalf("%d runs hold the request: %v", held, holders)
	}
	if w, err := s.CloseApproval(key, "nobody"); err != nil || w != nil {
		t.Fatalf("a stranger closing: %v, %v", w, err)
	}
	w, err := s.CloseApproval(key, holder)
	if err != nil || len(w) != 7 {
		t.Fatalf("waiters %v, %v", w, err)
	}
	if h, _ := s.OpenApproval(key, w[0], soon); h != "" {
		t.Fatalf("after close: held by %q", h)
	}
	if h, _ := s.OpenApproval(key, w[1], soon); h != w[0] {
		t.Fatalf("second: %q", h)
	}
	gone := key + "/expired"
	_, _ = s.OpenApproval(gone, "crashed", time.Now().Add(-time.Second))
	if h, _ := s.OpenApproval(gone, "next", soon); h != "" {
		t.Fatalf("expired request still held by %q", h)
	}
}
