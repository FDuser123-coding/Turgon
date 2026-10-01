package plugin

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// recorder is a Host that answers from fixed data and records results the
// probe publishes.
type recorder struct {
	mu        sync.Mutex
	entities  map[string]string
	proposals []string
	published map[string][][]byte
	requests  []HTTPRequest
	fail      error // returned by Get, as a host failure
}

func (r *recorder) Get(_ context.Context, kind, id string) (string, error) {
	if r.fail != nil {
		return "", r.fail
	}
	switch kind {
	case "Secret":
		return "", &ErrorCode{Kind: Denied}
	case "Broken":
		return "", &ErrorCode{Kind: Invalid, Message: "no read operation for Broken"}
	}
	v, ok := r.entities[kind+"/"+id]
	if !ok {
		return "", &ErrorCode{Kind: NotFound}
	}
	return v, nil
}

func (r *recorder) ProposeChange(_ context.Context, kind, id, patch string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proposals = append(r.proposals, kind+"/"+id+" "+patch)
	return `{"state":"pending_approval"}`, nil
}

func (r *recorder) Publish(_ context.Context, topic string, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published == nil {
		r.published = map[string][][]byte{}
	}
	r.published[topic] = append(r.published[topic], payload)
	return nil
}

func (r *recorder) Send(_ context.Context, req HTTPRequest) (*HTTPResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
	switch {
	case strings.Contains(req.URL, "denied"):
		return nil, &ErrorCode{Kind: Denied}
	case strings.Contains(req.URL, "down"):
		return nil, &ErrorCode{Kind: Unavailable, Message: "connection refused"}
	}
	hs := []Header{{"Content-Type", "application/json"}}
	for _, h := range req.Headers {
		hs = append(hs, Header{"Echo-" + h.Name, h.Value})
	}
	return &HTTPResponse{Status: 201, Headers: hs, Body: append([]byte(req.Method+" "), req.Body...)}, nil
}

func (r *recorder) result(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	res := r.published["result"]
	if len(res) == 0 {
		t.Fatal("the probe published no result")
	}
	return string(res[len(res)-1])
}

func load(t *testing.T, file string, limits Limits) *Module {
	t.Helper()
	b, err := os.ReadFile("testdata/" + file)
	if err != nil {
		t.Fatal(err)
	}
	m, err := Load(context.Background(), b, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close(context.Background()) })
	return m
}

var small = Limits{MemoryMB: 16, Timeout: 2 * time.Second}

func TestPluginCallsTheHost(t *testing.T) {
	m := load(t, "probe.wasm", small)
	if strings.Join(m.Imports, ",") != "entities.get,entities.propose-change,events.publish" {
		t.Fatalf("imports %v", m.Imports)
	}
	ctx := context.Background()
	h := &recorder{entities: map[string]string{"Customer/C-100": `{"id":"C-100","credit_limit":5000}`}}
	for event, want := range map[string]string{
		"get Customer C-100": `ok {"id":"C-100","credit_limit":5000}`,
		"get Customer C-404": "err not-found",
		"get Secret s":       "err denied",
		"get Broken b":       "err invalid no read operation for Broken",
		`propose SalesOrder SO-1 {"creditStatus":"ok"}`: `ok {"state":"pending_approval"}`,
		"publish audit": "ok published",
	} {
		if err := m.Handle(ctx, []byte(event), h); err != nil {
			t.Fatalf("%s: %v", event, err)
		}
		if got := h.result(t); got != want {
			t.Errorf("%s: %q, want %q", event, got, want)
		}
	}
	if len(h.proposals) != 1 || h.proposals[0] != `SalesOrder/SO-1 {"creditStatus":"ok"}` {
		t.Fatalf("proposals %v", h.proposals)
	}
	// A list<u8> crosses as bytes, not text.
	if p := h.published["audit"]; len(p) != 1 || len(p[0]) != 256 || p[0][255] != 255 {
		t.Fatalf("published %v", p)
	}
}

func TestPluginFailuresAndLimits(t *testing.T) {
	ctx := context.Background()
	m := load(t, "probe.wasm", Limits{MemoryMB: 8, Timeout: 300 * time.Millisecond})
	h := &recorder{}
	for event, want := range map[string]string{
		"fail no-credit-data": "plugin: no-credit-data",
		"trap":                "plugin: wasm error: unreachable",
		"loop":                "plugin: ran longer than 300ms",
		"grow":                "plugin:",
		"spam":                "plugin: more than 64 host calls",
		"dance":               `plugin: unknown command "dance"`,
	} {
		start := time.Now()
		err := m.Handle(ctx, []byte(event), h)
		if err == nil || !errors.Is(err, ErrPlugin) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", event, err)
		}
		if time.Since(start) > 3*time.Second {
			t.Errorf("%s took %s", event, time.Since(start))
		}
	}
	// Each event runs in a fresh instance: a plugin that ran out of memory
	// or time does not poison the next event.
	h.entities = map[string]string{"Customer/C-1": "{}"}
	if err := m.Handle(ctx, []byte("get Customer C-1"), h); err != nil || h.result(t) != "ok {}" {
		t.Fatalf("after failures: %v", err)
	}
	// A host failure is not the plugin's: it is retried.
	h.fail = errors.New("connection refused")
	err := m.Handle(ctx, []byte("get Customer C-1"), h)
	if err == nil || errors.Is(err, ErrPlugin) || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("host failure: %v", err)
	}
}

func TestLoadRefusesWhatTheWorldDoesNotOffer(t *testing.T) {
	ctx := context.Background()
	wasi, _ := os.ReadFile("testdata/probe-wasi.wasm")
	if _, err := Load(ctx, wasi, small); err == nil || !strings.Contains(err.Error(), "imports WASI") {
		t.Fatalf("WASI module: %v", err)
	}
	if _, err := Load(ctx, []byte("not wasm at all"), small); err == nil {
		t.Fatal("garbage loaded")
	}
	probe, _ := os.ReadFile("testdata/probe.wasm")
	if _, err := Load(ctx, probe, Limits{}); err == nil {
		t.Fatal("loaded without limits")
	}
	// A module that needs more memory than its limit starts, but cannot run.
	m, err := Load(ctx, probe, Limits{MemoryMB: 1, Timeout: time.Second})
	if err == nil {
		err = m.Handle(ctx, []byte("get Customer C-1"), &recorder{})
		m.Close(ctx)
	}
	if err == nil {
		t.Fatal("ran within 1 MB")
	}
}

// The same probe as a component, as `wasm-tools component new` wraps it
// (with its shim and fixup glue modules), runs the same.
func TestComponentsRunTheirCoreModule(t *testing.T) {
	m := load(t, "probe.component.wasm", small)
	h := &recorder{entities: map[string]string{"Customer/C-1": `{"id":"C-1"}`}}
	if err := m.Handle(context.Background(), []byte("get Customer C-1"), h); err != nil || h.result(t) != `ok {"id":"C-1"}` {
		t.Fatalf("%v", err)
	}
}

func TestWorld020(t *testing.T) {
	m := load(t, "probe2.wasm", small)
	if m.World != "turgon:stack/logic-plugin@0.2.0" || strings.Join(m.Imports, ",") != "entities.get,events.publish,http.send" {
		t.Fatalf("world %s, imports %v", m.World, m.Imports)
	}
	ctx := context.Background()
	h := &recorder{}
	for event, want := range map[string]string{
		"http POST https://api.example.com/score Authorization Bearer {{secret:key}}\n{\"customer\":\"C-1\"}": `ok 201 [Content-Type=application/json,Echo-Authorization=Bearer {{secret:key}}] POST {"customer":"C-1"}`,
		"http GET https://denied.example.com/":       "err denied",
		"http GET https://down.example.com/":         "err unavailable connection refused",
		"get Customer C-404":                         "err not-found",
		"publish credit.checked {\"status\":\"ok\"}": "ok published",
	} {
		if err := m.Handle(ctx, []byte(event), h); err != nil {
			t.Fatalf("%s: %v", event, err)
		}
		if got := h.result(t); got != want {
			t.Errorf("%s:\n got %q\nwant %q", event, got, want)
		}
	}
	if string(h.published["credit.checked"][0]) != `{"status":"ok"}` {
		t.Fatalf("published %q", h.published["credit.checked"])
	}
	// Requests count as host calls (the runner caps requests lower).
	if err := m.Handle(ctx, []byte("many-http https://api.example.com/"), h); err != nil || h.result(t) != "sent 40" {
		t.Fatalf("many requests: %v %q", err, h.result(t))
	}
}

// A 0.1.0 plugin sees unavailable as invalid: its world has no such case.
func TestWorld010SeesUnavailableAsInvalid(t *testing.T) {
	m := load(t, "probe.wasm", small)
	if m.World != "turgon:stack/logic-plugin@0.1.0" {
		t.Fatalf("world %s", m.World)
	}
	h := &unavailable{recorder{}}
	if err := m.Handle(context.Background(), []byte("get Customer C-1"), h); err != nil || h.result(t) != "err invalid unavailable: the ERP is down" {
		t.Fatalf("%v %q", err, h.result(t))
	}
}

type unavailable struct{ recorder }

func (u *unavailable) Get(context.Context, string, string) (string, error) {
	return "", &ErrorCode{Kind: Unavailable, Message: "the ERP is down"}
}
