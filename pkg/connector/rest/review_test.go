package rest_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
)

// recorder answers every request with body and keeps what it was sent.
type recorder struct {
	mu     sync.Mutex
	bodies []string
	urls   []string
	body   string
	srv    *httptest.Server
}

func newRecorder(t *testing.T, body string) *recorder {
	r := &recorder{body: body}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.bodies, r.urls = append(r.bodies, string(b)), append(r.urls, req.URL.String())
		out := r.body
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, out)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// A list absent from the answer's top level is an error, not "nothing":
// an error body or a misspelled items path would otherwise make every poll
// empty and every existing lookup miss (and so every retry create again).
func TestAbsentTopLevelListIsAnError(t *testing.T) {
	api := newRecorder(t, `{"error": "temporarily unavailable"}`)
	cfg := rest.Config{BaseURL: api.srv.URL, Auth: rest.Auth{Type: "none"},
		Events: map[string]rest.Event{"E": {Path: "/x", Items: "value", ID: "id", Position: "n"}},
		Operations: map[string]rest.Operation{"create": {Method: "POST", Path: "/x",
			Existing: &rest.Existing{Path: "/x", Params: map[string]string{"$filter": "k eq '{{idempotencyKey}}'"}, Items: "value"}}}}
	c, err := rest.New(cfg, "", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Poll(context.Background(), "E", 0, 10); err == nil {
		t.Fatal("a poll read an error body as an empty list")
	}
	if _, err := c.Commit(context.Background(), "create", "k1", json.RawMessage(`{}`)); err == nil {
		t.Fatal("the existing lookup read an error body as no record, and created")
	}
	if len(api.bodies) != 2 { // the poll and the lookup, no create
		t.Fatalf("requests %v", api.urls)
	}
}

// Constants are copied into each body: the configuration keeps them as
// configured; they are not captured, confirmed or restored; and one that
// overlaps a mapped field is refused.
func TestConstantsStayConstant(t *testing.T) {
	api := newRecorder(t, `{"ok": true}`)
	op := rest.Operation{Method: "POST", Path: "/x", Fields: map[string]string{"Contact.Name": "name"},
		Constants: map[string]any{"Meta": map[string]any{"Source": "turgon"}}, KeyField: "Ref"}
	cfg := rest.Config{BaseURL: api.srv.URL, Auth: rest.Auth{Type: "none"}, Operations: map[string]rest.Operation{"create": op}}
	c, err := rest.New(cfg, "", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := c.Commit(context.Background(), "create", "key-"+name, json.RawMessage(`{"name": "`+name+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	want := `{"Contact":{"Name":"b"},"Meta":{"Source":"turgon"},"Ref":"key-b"}`
	if api.bodies[1] != want {
		t.Fatalf("second body %s, want %s", api.bodies[1], want)
	}
	if m := op.Constants["Meta"].(map[string]any); len(m) != 1 {
		t.Fatalf("the configuration's constant changed: %v", m)
	}
	for name, bad := range map[string]rest.Operation{
		"constant over a mapped field": {Method: "POST", Path: "/x", Fields: map[string]string{"Contact.Name": "n"}, Constants: map[string]any{"Contact": map[string]any{}}},
		"no body with constants":       {Method: "POST", Path: "/x", BodyFormat: "none", Constants: map[string]any{"A": 1}},
		"keyField on a delete":         {Method: "DELETE", Path: "/x/{{id}}", KeyField: "Ref"},
		"keyField also mapped":         {Method: "POST", Path: "/x", Fields: map[string]string{"Ref": "r"}, KeyField: "Ref"},
	} {
		cfg := rest.Config{BaseURL: api.srv.URL, Auth: rest.Auth{Type: "none"}, Operations: map[string]rest.Operation{"op": bad}}
		if _, err := rest.New(cfg, "", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A quote in a value inside a quoted literal is doubled wherever the
// placeholder sits in it, so it cannot end the literal.
func TestQuotesInsideLiteralsAreDoubled(t *testing.T) {
	api := newRecorder(t, `{"value": []}`)
	cfg := rest.Config{BaseURL: api.srv.URL, Auth: rest.Auth{Type: "none"}, Operations: map[string]rest.Operation{"create": {
		Method: "POST", Path: "/x",
		Existing: &rest.Existing{Path: "/x", Params: map[string]string{"$filter": "ref eq 'SHOP-{{idempotencyKey}}'"}, Items: "value"}}}}
	c, err := rest.New(cfg, "", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), "create", "x' or 1 eq 1 or 'a", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(api.urls[0], "%27SHOP-x%27%27+or+1+eq+1+or+%27%27a%27") {
		t.Fatalf("lookup %s", api.urls[0])
	}
}
