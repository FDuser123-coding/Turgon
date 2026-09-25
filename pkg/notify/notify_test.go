package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type received struct {
	mu     sync.Mutex
	bodies []string
	header []http.Header
}

func (r *received) server(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.bodies = append(r.bodies, string(b))
		r.header = append(r.header, req.Header.Clone())
		r.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte("no_team"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

var sample = Notification{
	Kind: ApprovalPending, Workflow: "hubspot-won-deals-to-erp", RunID: "hubspot-won-deals-to-erp/17902983631", Step: "03-write",
	Title: "Approval needed: create-sales-order on erp-db", Text: "A run is waiting <@channel> & more",
	Facts: []Fact{{"Amount", "1500.00"}, {"Target", "erp-db"}}, At: time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC),
}

func TestHubSendsToEveryChannelWithAConsoleLink(t *testing.T) {
	var slack, teams, hook received
	h := &Hub{ConsoleURL: "https://turgon.example.com/", Channels: []Channel{
		&Slack{URL: slack.server(t, 200).URL}, &Teams{URL: teams.server(t, 202).URL},
		&Webhook{URL: hook.server(t, 204).URL, Secret: "s3cret", Now: func() time.Time { return sample.At }},
	}}
	if err := h.Notify(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	link := "https://turgon.example.com/runs/hubspot-won-deals-to-erp%2F17902983631"

	var s struct {
		Text   string           `json:"text"`
		Blocks []map[string]any `json:"blocks"`
	}
	// Values cannot become Slack mentions or links: <, > and & are escaped.
	escaped := "A run is waiting &lt;@channel&gt; &amp; more"
	if err := json.Unmarshal([]byte(slack.bodies[0]), &s); err != nil || len(s.Blocks) != 4 || !strings.Contains(slack.bodies[0], link) ||
		s.Blocks[1]["text"].(map[string]any)["text"] != escaped || !strings.HasSuffix(s.Text, escaped) {
		t.Fatalf("slack %s", slack.bodies[0])
	}
	if !strings.Contains(teams.bodies[0], `"contentType":"application/vnd.microsoft.card.adaptive"`) ||
		!strings.Contains(teams.bodies[0], `"type":"FactSet"`) || !strings.Contains(teams.bodies[0], link) {
		t.Fatalf("teams %s", teams.bodies[0])
	}
	var n Notification
	if err := json.Unmarshal([]byte(hook.bodies[0]), &n); err != nil || n.Link != link || n.RunID != sample.RunID {
		t.Fatalf("webhook %s", hook.bodies[0])
	}
	if got := hook.header[0].Get("Turgon-Signature"); got != Sign("s3cret", sample.At, []byte(hook.bodies[0])) || !strings.HasPrefix(got, "t=1790323200,v1=") {
		t.Fatalf("signature %q", got)
	}

	// The steward queue is one page, not a run.
	st := sample
	st.Kind = StewardNeeded
	if got := h.Link(st); got != "https://turgon.example.com/steward" {
		t.Fatal(got)
	}
	if got := (&Hub{}).Link(sample); got != "" {
		t.Fatalf("link without a console URL: %q", got)
	}
}

func TestFailuresAreReportedWithoutTheWebhookURL(t *testing.T) {
	var ok, bad received
	okSrv := ok.server(t, 200)
	secretURL := bad.server(t, 404).URL + "/services/T000/B000/XXXXSECRET"
	h := &Hub{Channels: []Channel{&Slack{URL: secretURL}, &Teams{URL: okSrv.URL}, &Webhook{URL: "http://127.0.0.1:1/hooks/SECRETPATH", Secret: "s"}}}
	err := h.Notify(context.Background(), sample)
	if err == nil || !strings.Contains(err.Error(), "slack: HTTP 404: no_team") || !strings.Contains(err.Error(), "webhook: post:") {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaks the webhook URL: %v", err)
	}
	if len(ok.bodies) != 1 {
		t.Fatal("one failing channel stopped the others")
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("TURGON_NOTIFY_SLACK_WEBHOOK_URL", "")
	t.Setenv("TURGON_NOTIFY_TEAMS_WEBHOOK_URL", "")
	t.Setenv("TURGON_NOTIFY_WEBHOOK_URL", "")
	if h, err := FromEnv("", nil); h != nil || err != nil {
		t.Fatalf("nothing configured: %v %v", h, err)
	}
	t.Setenv("TURGON_NOTIFY_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
	t.Setenv("TURGON_NOTIFY_TEAMS_WEBHOOK_URL", "https://prod-01.westeurope.logic.azure.com/workflows/x")
	h, err := FromEnv("https://turgon.example.com", nil)
	if err != nil || len(h.Channels) != 2 || h.ConsoleURL != "https://turgon.example.com" {
		t.Fatalf("%+v %v", h, err)
	}
	t.Setenv("TURGON_NOTIFY_WEBHOOK_URL", "https://ops.example.com/turgon")
	if _, err := FromEnv("", nil); err == nil || !strings.Contains(err.Error(), "TURGON_NOTIFY_WEBHOOK_SECRET") {
		t.Fatalf("unsigned webhook: %v", err)
	}
	t.Setenv("TURGON_NOTIFY_WEBHOOK_SECRET", "s")
	t.Setenv("TURGON_NOTIFY_SLACK_WEBHOOK_URL", "http://hooks.slack.com/services/T/B/X")
	if _, err := FromEnv("", nil); err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("plain http: %v", err)
	}
	t.Setenv("TURGON_NOTIFY_SLACK_WEBHOOK_URL", "http://127.0.0.1:9600/slack")
	if h, err := FromEnv("", nil); err != nil || len(h.Channels) != 3 {
		t.Fatalf("localhost: %+v %v", h, err)
	}
}

func TestLongTextIsClipped(t *testing.T) {
	n := sample
	n.Title = strings.Repeat("é", 400)
	b, _ := json.Marshal(SlackMessage(n))
	var s struct {
		Blocks []struct {
			Text struct {
				Text string `json:"text"`
			} `json:"text"`
		} `json:"blocks"`
	}
	_ = json.Unmarshal(b, &s)
	if got := []rune(s.Blocks[0].Text.Text); len(got) != 150 || got[149] != '…' {
		t.Fatalf("header is %d runes", len(got))
	}
}
