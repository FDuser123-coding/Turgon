package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/a2a"

	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

var pending = engine.AgentWriteStatus{State: engine.AgentWritePending, Pending: &engine.PendingApproval{Step: engine.AgentStep, Reasons: []string{"high-risk tool"}}}

func committed() engine.AgentWriteStatus {
	return engine.AgentWriteStatus{State: engine.AgentWriteCommitted,
		Write: &engine.WriteRecord{Status: writeguard.StatusCommitted, Result: json.RawMessage(`{"id": 7, "external_id": "AGENT-1"}`)}}
}

func orderArgs(requestID string) map[string]any {
	return map[string]any{"requestId": requestID, "record": order(), "reason": "customer asked"}
}

func TestStreamFollowsAWriteToItsOutcome(t *testing.T) {
	w := &fakeWrites{status: pending, watch: []engine.AgentWriteStatus{pending, {State: engine.AgentWriteRunning}, committed()}}
	url, _ := setupWith(t, "127.0.0.0/8", w)
	c, card := a2aClient(t, url, operator)
	if !card.Capabilities.Streaming || !card.Capabilities.PushNotifications {
		t.Fatalf("capabilities %+v", card.Capabilities)
	}
	var got []string
	var artifact map[string]any
	for ev, err := range c.SendStreamingMessage(context.Background(), ask("create_sales_order", orderArgs("s-1"))) {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case *a2a.Task:
			got = append(got, "task:"+string(e.Status.State))
		case *a2a.TaskStatusUpdateEvent:
			s := "status:" + string(e.Status.State)
			if e.Final {
				s += ":final"
			}
			got = append(got, s)
		case *a2a.TaskArtifactUpdateEvent:
			got = append(got, "artifact")
			artifact = dataOf(t, e.Artifact.Parts)
		default:
			t.Fatalf("event %T", ev)
		}
	}
	// pending → pending is no change; running is.
	want := "task:working status:working artifact status:completed:final"
	if strings.Join(got, " ") != want {
		t.Fatalf("events %v, want %s", got, want)
	}
	if artifact["external_id"] != "AGENT-1" {
		t.Fatalf("artifact %v", artifact)
	}
}

func TestStreamAnswersAReadWithItsReply(t *testing.T) {
	url, _ := setupWith(t, "127.0.0.0/8", &fakeWrites{})
	c, _ := a2aClient(t, url, operator)
	n := 0
	for ev, err := range c.SendStreamingMessage(context.Background(), ask("erp_db_get_customer", map[string]any{"id": "C-100"})) {
		if err != nil {
			t.Fatal(err)
		}
		m, ok := ev.(*a2a.Message)
		if !ok || dataOf(t, m.Parts)["name"] == nil {
			t.Fatalf("event %#v", ev)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("%d events", n)
	}
}

func TestStreamEndsAtItsLimitAndResubscribeTakesOver(t *testing.T) {
	w := &fakeWrites{status: pending}
	url, _ := setupWith(t, "127.0.0.0/8", w, func(o *Options) { o.StreamLimit = 300 * time.Millisecond })
	c, _ := a2aClient(t, url, operator)
	ctx := context.Background()
	start := time.Now()
	var last a2a.Event
	for ev, err := range c.SendStreamingMessage(ctx, ask("create_sales_order", orderArgs("s-2"))) {
		if err != nil {
			t.Fatal(err)
		}
		last = ev
	}
	if tk, ok := last.(*a2a.Task); !ok || tk.Status.State != a2a.TaskStateWorking || time.Since(start) > 5*time.Second {
		t.Fatalf("last event %#v after %s", last, time.Since(start))
	}

	// Another agent cannot follow it.
	other, _ := a2aClient(t, url, map[string]string{"X-Agent-Id": "mallory", "X-Agent-Roles": "integration-operator"})
	for _, err := range other.ResubscribeToTask(ctx, &a2a.TaskIDParams{ID: "create_sales_order/claude:s-2"}) {
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Fatalf("another agent: %v", err)
		}
	}
	// The owner resubscribes after the approval.
	w.mu.Lock()
	w.watch = []engine.AgentWriteStatus{committed()}
	w.mu.Unlock()
	var got []string
	for ev, err := range c.ResubscribeToTask(ctx, &a2a.TaskIDParams{ID: "create_sales_order/claude:s-2"}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(ev.(interface{ TaskInfo() a2a.TaskInfo }).TaskInfo().TaskID))
	}
	if len(got) != 3 { // the task, the artifact, the final status
		t.Fatalf("events %v", got)
	}
}

func TestPushConfigs(t *testing.T) {
	w := &fakeWrites{status: pending}
	url, _ := setupWith(t, "127.0.0.0/8", w, func(o *Options) {
		o.Push = PushGuard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.2/32")}}
	})
	c, _ := a2aClient(t, url, operator)
	ctx := context.Background()

	// A push URL in the message itself goes to the write's input.
	p := ask("create_sales_order", orderArgs("p-1"))
	p.Config = &a2a.MessageSendConfig{PushConfig: &a2a.PushConfig{URL: "http://127.0.0.2:9/hook", Token: "tok",
		Auth: &a2a.PushAuthInfo{Schemes: []string{"Bearer"}, Credentials: "secret-cred"}}}
	if _, err := c.SendMessage(ctx, p); err != nil {
		t.Fatal(err)
	}
	if got := w.inputs[0].Push; len(got) != 1 || got[0].ID != "default" || got[0].Credentials != "secret-cred" || got[0].Token != "tok" {
		t.Fatalf("input push %+v", got)
	}
	id := a2a.TaskID("create_sales_order/claude:p-1")

	for name, tc := range map[string]struct {
		url  string
		want string
	}{
		"metadata endpoint":   {"https://169.254.169.254/latest", "not a public address"},
		"private network":     {"https://10.0.0.5/hook", "not a public address"},
		"loopback by name":    {"https://localhost:8443/hook", "not a public address"},
		"plain http outside":  {"http://8.8.8.8/hook", "must use https"},
		"credentials in URL":  {"https://u:p@example.com/hook", "without credentials"},
		"not a URL":           {"mailto:agent@example.com", "must be https"},
		"unsupported auth":    {"https://8.8.8.8/hook#digest", "Bearer or Basic"},
		"private IPv6 (ULA)":  {"https://[fd00::1]/hook", "not a public address"},
		"IPv4-mapped private": {"https://[::ffff:192.168.1.1]/hook", "not a public address"},
	} {
		cfg := a2a.PushConfig{ID: "x", URL: tc.url}
		if name == "unsupported auth" {
			cfg.Auth = &a2a.PushAuthInfo{Schemes: []string{"Digest"}, Credentials: "c"}
		}
		_, err := c.SetTaskPushConfig(ctx, &a2a.TaskPushConfig{TaskID: id, Config: cfg})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}

	// A public https URL is accepted, and reported back without its
	// credentials.
	got, err := c.SetTaskPushConfig(ctx, &a2a.TaskPushConfig{TaskID: id, Config: a2a.PushConfig{ID: "pub", URL: "https://8.8.8.8/hook",
		Auth: &a2a.PushAuthInfo{Schemes: []string{"bearer"}, Credentials: "secret-cred"}}})
	if err != nil || got.Config.Auth == nil || got.Config.Auth.Credentials != "" {
		t.Fatalf("set: %+v, %v", got, err)
	}
	list, err := c.ListTaskPushConfig(ctx, &a2a.ListTaskPushConfigParams{TaskID: id})
	if err != nil || len(list) != 2 {
		t.Fatalf("list %+v, %v", list, err)
	}
	b, _ := json.Marshal(list)
	if strings.Contains(string(b), "secret-cred") {
		t.Fatalf("credentials reported back: %s", b)
	}
	if g, err := c.GetTaskPushConfig(ctx, &a2a.GetTaskPushConfigParams{TaskID: id, ConfigID: "pub"}); err != nil || g.Config.URL != "https://8.8.8.8/hook" {
		t.Fatalf("get %+v, %v", g, err)
	}
	if err := c.DeleteTaskPushConfig(ctx, &a2a.DeleteTaskPushConfigParams{TaskID: id, ConfigID: "pub"}); err != nil {
		t.Fatal(err)
	}

	// At most five per task.
	for i := range engine.MaxPushConfigs {
		_, err = c.SetTaskPushConfig(ctx, &a2a.TaskPushConfig{TaskID: id, Config: a2a.PushConfig{ID: string(rune('a' + i)), URL: "https://8.8.8.8/hook"}})
	}
	if err == nil || !strings.Contains(err.Error(), "at most 5") {
		t.Fatalf("sixth config: %v", err)
	}

	// Another agent's task is not found; an ended write takes no more.
	other, _ := a2aClient(t, url, map[string]string{"X-Agent-Id": "mallory", "X-Agent-Roles": "integration-operator"})
	if _, err := other.ListTaskPushConfig(ctx, &a2a.ListTaskPushConfigParams{TaskID: id}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("another agent: %v", err)
	}
	w.mu.Lock()
	w.ended = true
	w.mu.Unlock()
	if _, err := c.SetTaskPushConfig(ctx, &a2a.TaskPushConfig{TaskID: id, Config: a2a.PushConfig{ID: "a", URL: "https://8.8.8.8/x"}}); err == nil || !strings.Contains(err.Error(), "has ended") {
		t.Fatalf("ended write: %v", err)
	}
}
