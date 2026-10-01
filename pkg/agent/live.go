package agent

import (
	"net/http"
	"sync"
	"sync/atomic"
)

// Live serves one Server at a time and switches to another without
// dropping requests: `turgon mcp` builds a new Server when a secret
// rotates. MCP runs stateless and A2A tasks are Temporal workflows, so
// nothing a client holds refers to a particular Server.
type Live struct {
	cur     atomic.Pointer[liveServer]
	retired sync.WaitGroup
}

type liveServer struct {
	s      *Server
	mu     sync.RWMutex // read-held by each request it serves
	closed bool
}

// NewLive serves s.
func NewLive(s *Server) *Live {
	l := &Live{}
	l.cur.Store(&liveServer{s: s})
	return l
}

// Current returns the Server new requests go to.
func (l *Live) Current() *Server { return l.cur.Load().s }

// Swap sends new requests to s, and closes the previous Server once the
// requests it is serving have finished.
func (l *Live) Swap(s *Server) {
	old := l.cur.Swap(&liveServer{s: s})
	l.retired.Add(1)
	go func() {
		defer l.retired.Done()
		old.retire()
	}()
}

// Close closes the current Server, after the previous ones.
func (l *Live) Close() {
	l.retired.Wait()
	l.cur.Load().retire()
}

func (g *liveServer) retire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		g.s.Close()
	}
}

func (l *Live) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for {
		g := l.cur.Load()
		g.mu.RLock()
		if g.closed {
			g.mu.RUnlock()
			if l.cur.Load() == g { // Live is closed: shutting down
				http.Error(w, "shutting down", http.StatusServiceUnavailable)
				return
			}
			continue // swapped out while this request waited: take the new one
		}
		func() {
			defer g.mu.RUnlock()
			g.s.ServeHTTP(w, r)
		}()
		return
	}
}
