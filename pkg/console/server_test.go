package console

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

type fakeRuns struct {
	pending map[string]*engine.PendingApproval
	signals []engine.ApprovalSignal
}

func (f *fakeRuns) List(context.Context, int) ([]RunSummary, error) {
	var out []RunSummary
	for id, p := range f.pending {
		out = append(out, RunSummary{ID: id, Workflow: workflowName(id), Status: "running", Started: time.Now(), Pending: p})
	}
	return out, nil
}

func (f *fakeRuns) Get(_ context.Context, id string) (RunDetail, error) {
	p, ok := f.pending[id]
	if !ok {
		return RunDetail{}, ErrNotFound
	}
	return RunDetail{RunSummary: RunSummary{ID: id, Status: "running", Pending: p}}, nil
}

func (f *fakeRuns) Pending(_ context.Context, id string) (*engine.PendingApproval, error) {
	return f.pending[id], nil
}

func (f *fakeRuns) Signal(_ context.Context, id string, sig engine.ApprovalSignal) error {
	f.signals = append(f.signals, sig)
	delete(f.pending, id)
	return nil
}

func newRuns() *fakeRuns {
	return &fakeRuns{pending: map[string]*engine.PendingApproval{
		"shop-orders-to-erp/7": {Step: "03-write", Digest: "abc123", Request: writeguard.Request{
			Target: "erp-db", Operation: "create-sales-order", Subject: policy.Subject{ID: "turgon/recipe/shop-orders-to-erp"},
		}},
		"agent-orders/1": {Step: "01-write", Digest: "def456", Request: writeguard.Request{
			Subject: policy.Subject{ID: "agent-7", Agent: true, OnBehalfOf: "alice@example.com"},
		}},
	}}
}

func proxyAuth() ProxyAuth {
	return ProxyAuth{Trusted: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, ApproverGroup: "turgon-approvers"}
}

func do(t *testing.T, s *Server, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "10.1.2.3:5555"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func as(user, groups string) map[string]string {
	return map[string]string{"X-Auth-Request-Email": user, "X-Auth-Request-Groups": groups, "Content-Type": "application/json"}
}

func TestAuthentication(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: proxyAuth()})
	if rec := do(t, s, "GET", "/api/runs", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no identity: %d", rec.Code)
	}
	// Identity headers from an untrusted address are ignored.
	req := httptest.NewRequest("GET", "/api/me", nil)
	req.RemoteAddr = "203.0.113.9:1234"
	req.Header.Set("X-Auth-Request-Email", "mallory@example.com")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("spoofed header accepted: %d", rec.Code)
	}
	rec = do(t, s, "GET", "/api/me", "", as("bob@example.com", "sales, turgon-approvers"))
	var u User
	_ = json.Unmarshal(rec.Body.Bytes(), &u)
	if rec.Code != 200 || u.ID != "bob@example.com" || !u.Has(RoleApprover) {
		t.Fatalf("me = %d %+v", rec.Code, u)
	}
	if h := rec.Header().Get("Content-Security-Policy"); !strings.Contains(h, "frame-ancestors 'none'") {
		t.Errorf("missing CSP: %q", h)
	}
}

func TestDecisions(t *testing.T) {
	runs := newRuns()
	s := New(Config{Runs: runs, Auth: proxyAuth()})
	approve := `{"runId":"shop-orders-to-erp/7","step":"03-write","digest":"abc123","decision":"approve","note":"ok"}`

	cases := []struct {
		name string
		body string
		hdr  map[string]string
		code int
	}{
		{"viewer cannot approve", approve, as("vera@example.com", "sales"), http.StatusForbidden},
		{"form posts are refused", approve, map[string]string{"X-Auth-Request-Email": "bob@example.com", "X-Auth-Request-Groups": "turgon-approvers", "Content-Type": "application/x-www-form-urlencoded"}, http.StatusForbidden},
		{"cross-origin is refused", approve, map[string]string{"X-Auth-Request-Email": "bob@example.com", "X-Auth-Request-Groups": "turgon-approvers", "Content-Type": "application/json", "Origin": "https://evil.example"}, http.StatusForbidden},
		{"stale digest conflicts", strings.Replace(approve, "abc123", "old", 1), as("bob@example.com", "turgon-approvers"), http.StatusConflict},
		{"unknown fields rejected", strings.Replace(approve, `"note"`, `"by":"ceo","note"`, 1), as("bob@example.com", "turgon-approvers"), http.StatusBadRequest},
		{"no self-approval", `{"runId":"agent-orders/1","step":"01-write","digest":"def456","decision":"approve"}`, as("alice@example.com", "turgon-approvers"), http.StatusForbidden},
	}
	for _, c := range cases {
		if rec := do(t, s, "POST", "/api/decisions", c.body, c.hdr); rec.Code != c.code {
			t.Errorf("%s: %d %s", c.name, rec.Code, rec.Body)
		}
	}
	if len(runs.signals) != 0 {
		t.Fatalf("signals sent by refused requests: %+v", runs.signals)
	}

	rec := do(t, s, "POST", "/api/decisions", approve, as("bob@example.com", "turgon-approvers"))
	if rec.Code != 200 || len(runs.signals) != 1 {
		t.Fatalf("approve: %d %s", rec.Code, rec.Body)
	}
	// The approver's identity comes from authentication, never the request.
	if sig := runs.signals[0]; sig.By != "bob@example.com" || sig.Status != "approved" || sig.Digest != "abc123" || sig.Note != "ok" {
		t.Fatalf("signal = %+v", sig)
	}
	if rec := do(t, s, "POST", "/api/decisions", approve, as("bob@example.com", "turgon-approvers")); rec.Code != http.StatusConflict {
		t.Fatalf("double decision: %d", rec.Code)
	}
}

func TestRunsEndpoints(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}})
	rec := do(t, s, "GET", "/api/runs/shop-orders-to-erp/7", "", nil)
	var d RunDetail
	_ = json.Unmarshal(rec.Body.Bytes(), &d)
	if rec.Code != 200 || d.Pending == nil || d.Pending.Digest != "abc123" {
		t.Fatalf("get run: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, s, "GET", "/api/runs/nope/1", "", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("missing run: %d", rec.Code)
	}
	if rec := do(t, s, "GET", "/api/nope", "", nil); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "no such endpoint") {
		t.Fatalf("unknown API path: %d", rec.Code)
	}
}

func TestAuditEndpointReportsTampering(t *testing.T) {
	dir := t.TempDir()
	good, bad := filepath.Join(dir, "good.jsonl"), filepath.Join(dir, "bad.jsonl")
	for _, p := range []string{good, bad} {
		var buf bytes.Buffer
		l := audit.New(&buf)
		for i := 0; i < 3; i++ {
			_, _ = l.Record("turgon", "writeback.committed", map[string]int{"n": i})
		}
		data := buf.Bytes()
		if p == bad {
			data = bytes.Replace(data, []byte(`"n":1`), []byte(`"n":9`), 1)
		}
		_ = os.WriteFile(p, data, 0o600)
	}
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}, Audit: []AuditSource{AuditFile(good), AuditFile(bad)}})
	var logs []AuditLog
	_ = json.Unmarshal(do(t, s, "GET", "/api/audit", "", nil).Body.Bytes(), &logs)
	if len(logs) != 2 || !logs[0].OK || logs[0].Count != 3 || logs[0].Entries[0].Seq != 3 {
		t.Fatalf("good log = %+v", logs[0])
	}
	if logs[1].OK || !strings.Contains(logs[1].Error, "tampered") {
		t.Fatalf("tampered log reported as %+v", logs[1])
	}
}

func TestCatalogEndpoint(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}, Catalogs: []string{"../../examples"}})
	var c CatalogReport
	_ = json.Unmarshal(do(t, s, "GET", "/api/catalog", "", nil).Body.Bytes(), &c)
	subjects := map[string]string{}
	for _, r := range c.Reports {
		subjects[r.Subject] = r.Level
	}
	if c.Error != "" || subjects["Recipe/shop-orders-to-erp@1.0.0"] != "L1" || subjects["StackBlueprint/eu-distributor-core"] != "L1" {
		t.Fatalf("catalog = %s %+v", c.Error, subjects)
	}
}

func TestStaticAppFallback(t *testing.T) {
	assets := fstest.MapFS{
		"index.html":      {Data: []byte("<!doctype html><div id=root></div>")},
		"assets/app-1.js": {Data: []byte("console.log(1)")},
	}
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}, Assets: assets})
	if rec := do(t, s, "GET", "/approvals/some/deep/link", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "root") {
		t.Fatalf("SPA fallback: %d", rec.Code)
	}
	if rec := do(t, s, "GET", "/assets/app-1.js", "", nil); !strings.Contains(rec.Header().Get("Cache-Control"), "immutable") {
		t.Fatalf("asset caching: %q", rec.Header().Get("Cache-Control"))
	}
	empty := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}, Assets: fstest.MapFS{}})
	if rec := do(t, empty, "GET", "/", "", nil); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "make console") {
		t.Fatalf("unbuilt app: %d %s", rec.Code, rec.Body)
	}
}

func TestDevAuthIsLoopbackOnly(t *testing.T) {
	s := New(Config{Runs: newRuns(), Auth: DevAuth{User: "dev"}})
	if err := s.Serve("0.0.0.0:0"); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("err = %v", err)
	}
}
