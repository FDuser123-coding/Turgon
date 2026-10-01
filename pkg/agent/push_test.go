package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
)

func pushSpec() *compiler.RuntimeSpec {
	spec := &compiler.RuntimeSpec{}
	spec.Spec.Tools = []compiler.Tool{{Name: "create_sales_order", Risk: "high", Entity: "SalesOrder", Endpoint: "erp-db"}}
	return spec
}

func refused(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == engine.ErrTypePushRefused && app.NonRetryable()
}

func TestPusherPostsTheTask(t *testing.T) {
	type got struct {
		token, auth string
		task        map[string]any
	}
	seen := make(chan got, 1)
	status := http.StatusOK
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var g got
		g.token, g.auth = r.Header.Get("X-A2A-Notification-Token"), r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&g.task)
		seen <- g
		w.WriteHeader(status)
	}))
	defer recv.Close()
	p := NewPusher(pushSpec(), PushGuard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}})
	in := engine.AgentPushInput{
		Config:     engine.PushConfig{ID: "a", URL: recv.URL + "/hook", Token: "tok", Schemes: []string{"bearer"}, Credentials: "cred"},
		WorkflowID: "create_sales_order/claude:r-1", Tool: "create_sales_order",
		Status: engine.AgentWriteStatus{State: engine.AgentWriteRejected, Message: "no PO"},
	}
	if err := p.Push(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	g := <-seen
	st, _ := g.task["status"].(map[string]any)
	if g.token != "tok" || g.auth != "Bearer cred" || g.task["kind"] != "task" || g.task["id"] != in.WorkflowID || st["state"] != "rejected" {
		t.Fatalf("received %+v", g)
	}

	// A receiver's 4xx (or a redirect) is final; a 5xx is retried.
	for code, final := range map[int]bool{http.StatusNotFound: true, http.StatusFound: true, http.StatusServiceUnavailable: false, http.StatusTooManyRequests: false} {
		status = code
		err := p.Push(context.Background(), in)
		<-seen
		if err == nil || refused(err) != final {
			t.Errorf("%d: %v", code, err)
		}
	}
}

func TestPusherOnlyReachesPermittedAddresses(t *testing.T) {
	hit := false
	recv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit = true }))
	defer recv.Close()
	// https to a loopback address the operators did not allow: stopped
	// when connecting, whatever the URL says.
	p := NewPusher(pushSpec(), PushGuard{})
	for _, u := range []string{recv.URL + "/hook", strings.Replace(recv.URL, "127.0.0.1", "localhost", 1) + "/hook"} {
		err := p.Push(context.Background(), engine.AgentPushInput{Config: engine.PushConfig{URL: u}, Tool: "create_sales_order",
			Status: engine.AgentWriteStatus{State: engine.AgentWriteCommitted}})
		if !refused(err) {
			t.Errorf("%s: %v", u, err)
		}
	}
	// Plain http, even to an allowed address that is not loopback.
	err := p.Push(context.Background(), engine.AgentPushInput{Config: engine.PushConfig{URL: "http://8.8.8.8/hook"}, Tool: "x"})
	if !refused(err) {
		t.Errorf("plain http: %v", err)
	}
	if hit {
		t.Fatal("a refused notification reached the receiver")
	}
}

type fakeResolver map[string][]netip.Addr

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if ips, ok := f[host]; ok {
		return ips, nil
	}
	return nil, errors.New("no such host")
}

func TestPushGuardChecksEveryAddressOfAName(t *testing.T) {
	g := PushGuard{Resolver: fakeResolver{
		"agents.example.com": {netip.MustParseAddr("93.184.216.34")},
		"split.example.com":  {netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("10.1.2.3")},
		"cgnat.example.com":  {netip.MustParseAddr("100.64.0.9")},
		"nat64.example.com":  {netip.MustParseAddr("64:ff9b::a01:203")},
	}}
	ctx := context.Background()
	if err := g.CheckURL(ctx, "https://agents.example.com/a2a/hook"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"https://split.example.com/", "https://cgnat.example.com/", "https://nat64.example.com/", "https://nowhere.example.com/", "https://0.0.0.0/", "https://[::1]/"} {
		if err := g.CheckURL(ctx, u); err == nil {
			t.Errorf("%s accepted", u)
		}
	}
	allow, err := ParseAllow("10.20.0.0/16, 192.168.7.7")
	if err != nil || len(allow) != 2 || !allow[1].Contains(netip.MustParseAddr("192.168.7.7")) {
		t.Fatalf("ParseAllow: %v, %v", allow, err)
	}
	g.Allow = allow
	if err := g.CheckURL(ctx, "http://10.20.3.4:8080/hook"); err != nil {
		t.Fatalf("allowed range: %v", err)
	}
	if _, err := ParseAllow("10.0.0.0/33"); err == nil {
		t.Fatal("bad CIDR accepted")
	}
}
