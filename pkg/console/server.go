// Package console serves Porter's web console (architecture §12): runs,
// the approval queue with dry-run previews, the audit log with chain
// verification, and verifier reports for the catalog, including fields
// waiting in the mapping review queue.
package console

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/api/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// The React app is built into dist by `make console`; see console/.
//
//go:embed all:dist
var dist embed.FS

// Config assembles a console server.
type Config struct {
	Runs      Runs
	Auth      Authenticator
	Catalogs  []string
	AuditLogs []string
	// Assets overrides the embedded web app; for tests and development.
	Assets fs.FS
}

// Server is the console HTTP handler.
type Server struct {
	cfg Config
	mux *http.ServeMux
}

// New returns a console server.
func New(cfg Config) *Server {
	if cfg.Assets == nil {
		sub, _ := fs.Sub(dist, "dist")
		cfg.Assets = sub
	}
	s := &Server{cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/me", s.me)
	s.mux.HandleFunc("GET /api/runs", s.listRuns)
	s.mux.HandleFunc("GET /api/runs/{id...}", s.getRun)
	s.mux.HandleFunc("POST /api/decisions", s.decide)
	s.mux.HandleFunc("GET /api/audit", s.audit)
	s.mux.HandleFunc("GET /api/catalog", s.catalog)
	s.mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "no such endpoint")
	})
	s.mux.HandleFunc("/", s.static)
	return s
}

// ServeHTTP authenticates every request and applies security headers.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Cache-Control", "no-store")
	user, err := s.cfg.Auth.Authenticate(r)
	if err != nil || !user.Has(RoleViewer) {
		writeError(w, http.StatusUnauthorized, "sign in through your identity provider to use the console")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		if err := sameOrigin(r); err != nil {
			writeError(w, http.StatusForbidden, err.Error())
			return
		}
	}
	s.mux.ServeHTTP(w, withUser(r, user))
}

// sameOrigin rejects cross-site state changes: they must be JSON (which a
// plain HTML form cannot send) and, when the browser says where they came
// from, come from this host.
func sameOrigin(r *http.Request) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return errors.New("state-changing requests must be application/json")
	}
	if o := r.Header.Get("Origin"); o != "" {
		u, err := url.Parse(o)
		if err != nil || u.Host != r.Host {
			return errors.New("cross-origin request refused")
		}
	}
	return nil
}

// Serve listens on addr. Dev authentication is refused on anything but a
// loopback address.
func (s *Server) Serve(addr string) error {
	if _, dev := s.cfg.Auth.(DevAuth); dev {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return fmt.Errorf("dev authentication only listens on loopback addresses, not %s", addr)
		}
	}
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, userFrom(r))
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 500 {
		limit = n
	}
	runs, err := s.cfg.Runs.List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	d, err := s.cfg.Runs.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "run not found")
	case err != nil:
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
	default:
		writeJSON(w, http.StatusOK, d)
	}
}

// Decision is an approver's answer to a pending approval.
type Decision struct {
	RunID    string `json:"runId"`
	Step     string `json:"step"`
	Digest   string `json:"digest"`
	Decision string `json:"decision"` // approve or reject
	Note     string `json:"note,omitempty"`
}

func (s *Server) decide(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r)
	if !user.Has(RoleApprover) {
		writeError(w, http.StatusForbidden, "you are not an approver")
		return
	}
	var d Decision
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		writeError(w, http.StatusBadRequest, "invalid decision: "+err.Error())
		return
	}
	status := map[string]string{"approve": "approved", "reject": "rejected"}[d.Decision]
	if status == "" || d.RunID == "" || d.Step == "" || d.Digest == "" {
		writeError(w, http.StatusBadRequest, "runId, step, digest and decision (approve or reject) are required")
		return
	}
	// The decision must be about what the run is waiting for right now.
	p, err := s.cfg.Runs.Pending(r.Context(), d.RunID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	if p == nil || p.Step != d.Step || p.Digest != d.Digest {
		writeError(w, http.StatusConflict, "this run is no longer waiting for that decision; reload")
		return
	}
	if p.Request.Subject.ID == user.ID || p.Request.Subject.OnBehalfOf == user.ID {
		writeError(w, http.StatusForbidden, "you cannot approve a write made on your own behalf")
		return
	}
	sig := engine.ApprovalSignal{Step: d.Step, Digest: d.Digest, Status: status, By: user.ID, Note: d.Note}
	if err := s.cfg.Runs.Signal(r.Context(), d.RunID, sig); err != nil {
		writeError(w, http.StatusBadGateway, "workflow engine: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

// AuditLog is one audit file's verification result and latest entries.
type AuditLog struct {
	File    string        `json:"file"`
	OK      bool          `json:"ok"`
	Error   string        `json:"error,omitempty"`
	Count   uint64        `json:"count"`
	Head    string        `json:"head,omitempty"`
	Entries []audit.Entry `json:"entries"`
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 2000 {
		limit = n
	}
	out := make([]AuditLog, 0, len(s.cfg.AuditLogs))
	for _, path := range s.cfg.AuditLogs {
		out = append(out, readAudit(path, limit))
	}
	writeJSON(w, http.StatusOK, out)
}

func readAudit(path string, limit int) AuditLog {
	l := AuditLog{File: path, Entries: []audit.Entry{}}
	data, err := os.ReadFile(path)
	if err != nil {
		l.Error = err.Error()
		return l
	}
	last, err := audit.Verify(strings.NewReader(string(data)))
	l.OK = err == nil
	if err != nil {
		l.Error = err.Error()
	}
	if last != nil {
		l.Count, l.Head = last.Seq, last.Hash
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for i := len(lines) - 1; i >= 0 && len(l.Entries) < limit; i-- {
		var e audit.Entry
		if json.Unmarshal([]byte(lines[i]), &e) == nil {
			l.Entries = append(l.Entries, e)
		}
	}
	return l
}

// CatalogReport is the verifier's view of the catalog.
type CatalogReport struct {
	Error   string             `json:"error,omitempty"`
	Reports []*verifier.Report `json:"reports"`
}

func (s *Server) catalog(w http.ResponseWriter, r *http.Request) {
	out := CatalogReport{Reports: []*verifier.Report{}}
	if len(s.cfg.Catalogs) == 0 {
		writeJSON(w, http.StatusOK, out)
		return
	}
	cat, err := catalog.Load(s.cfg.Catalogs...)
	if err != nil {
		out.Error = err.Error()
		writeJSON(w, http.StatusOK, out)
		return
	}
	v := verifier.New(cat, verifier.Options{})
	for _, obj := range cat.All() {
		switch obj.GetTypeMeta().Kind {
		case v1alpha1.KindRecipe, v1alpha1.KindStackBlueprint:
			out.Reports = append(out.Reports, v.Verify(obj))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// static serves the single-page app, falling back to index.html.
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name != "" {
		if f, err := s.cfg.Assets.Open(name); err == nil {
			st, _ := f.Stat()
			f.Close()
			if st != nil && !st.IsDir() {
				if strings.HasPrefix(name, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				http.ServeFileFS(w, r, s.cfg.Assets, name)
				return
			}
		}
	}
	if _, err := fs.Stat(s.cfg.Assets, "index.html"); err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "The console web app is not built into this binary. Run `make console` and rebuild; the API is at /api/.")
		return
	}
	http.ServeFileFS(w, r, s.cfg.Assets, "index.html")
}
