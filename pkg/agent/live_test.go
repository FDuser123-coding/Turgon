package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// fakeConn is a connector instance that counts closes and passes or fails
// its connection check.
type fakeConn struct {
	writeguard.Target
	closed atomic.Int32
	ok     bool
}

func (f *fakeConn) Close() { f.closed.Add(1) }
func (f *fakeConn) Check(context.Context) []connector.CheckResult {
	if f.ok {
		return []connector.CheckResult{connector.Pass("connect", "")}
	}
	return []connector.CheckResult{connector.Fail("connect", "password authentication failed", "")}
}

// fakeServer answers every MCP request with its name, after release is
// closed if it has one.
func fakeServer(name string, conn *fakeConn, release chan struct{}) *Server {
	return &Server{
		auth:      DevAuth{Identity: Identity{Agent: "test"}},
		instances: map[string]connector.Instance{"erp-db": conn},
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if release != nil {
				<-release
			}
			_, _ = io.WriteString(w, name)
		}),
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestLiveSwapsWithoutDroppingRequests(t *testing.T) {
	oldConn, newConn := &fakeConn{ok: true}, &fakeConn{ok: true}
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	live := NewLive(fakeServer("old", oldConn, release))
	srv := httptest.NewServer(live)
	defer srv.Close()
	defer unblock() // before srv.Close, which waits for the request

	// A request in flight on the old server when the secret rotates.
	inflight := make(chan string, 1)
	go func() {
		_, body := get(t, srv.URL+"/mcp")
		inflight <- body
	}()
	time.Sleep(100 * time.Millisecond)
	live.Swap(fakeServer("new", newConn, nil))

	// New requests go to the new server at once.
	if _, body := get(t, srv.URL+"/mcp"); body != "new" {
		t.Fatalf("after the swap: %q", body)
	}
	// The old server's connections stay open for the request it serves.
	time.Sleep(100 * time.Millisecond)
	if oldConn.closed.Load() != 0 {
		t.Fatal("the old connections were closed under a request in flight")
	}
	unblock()
	if body := <-inflight; body != "old" {
		t.Fatalf("the request in flight got %q", body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for oldConn.closed.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the old connections were never closed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	live.Close()
	if oldConn.closed.Load() != 1 || newConn.closed.Load() != 1 {
		t.Fatalf("closed old %d, new %d times", oldConn.closed.Load(), newConn.closed.Load())
	}
	if code, _ := get(t, srv.URL+"/mcp"); code != http.StatusServiceUnavailable {
		t.Fatalf("after Close: %d", code)
	}
}

func TestVerifyChecksTheNamedEndpoints(t *testing.T) {
	s := fakeServer("s", &fakeConn{ok: false}, nil)
	if got := s.Verify(context.Background(), "erp-db"); len(got["erp-db"]) != 1 || got["erp-db"][0].OK {
		t.Fatalf("Verify(erp-db) = %v", got)
	}
	if got := s.Verify(context.Background(), "other"); len(got) != 0 {
		t.Fatalf("Verify(other) = %v", got)
	}
	if got := s.Verify(context.Background()); len(got) != 1 {
		t.Fatalf("Verify() = %v", got)
	}
}
