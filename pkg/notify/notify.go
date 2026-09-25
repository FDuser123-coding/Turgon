// Package notify tells people when a run needs them: a write waiting for
// approval, a record waiting for a data steward, a saga that could not undo
// its writes. Messages go to Slack or Microsoft Teams incoming webhooks, or
// to any HTTPS endpoint as JSON signed with HMAC-SHA256, and link to the
// console page where the person acts.
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Kinds of notification.
const (
	ApprovalPending    = "approval.pending"
	ApprovalTimedOut   = "approval.timed-out"
	StewardNeeded      = "steward.needed"
	CompensationFailed = "compensation.failed"
)

// Notification is one message about one run.
type Notification struct {
	Kind     string    `json:"kind"`
	Workflow string    `json:"workflow"`
	RunID    string    `json:"runId"`
	Step     string    `json:"step,omitempty"`
	Title    string    `json:"title"`
	Text     string    `json:"text"`
	Facts    []Fact    `json:"facts,omitempty"`
	Link     string    `json:"link,omitempty"`
	At       time.Time `json:"at"`
}

// Fact is a labelled value shown with the message.
type Fact struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Notifier delivers notifications.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

// Channel is one destination.
type Channel interface {
	Notifier
	Name() string
}

// Hub sends every notification to all its channels and adds console links.
type Hub struct {
	Channels []Channel
	// ConsoleURL is the console's base URL, e.g. https://turgon.example.com;
	// without it messages carry no link.
	ConsoleURL string
}

// Link returns the console page for a notification.
func (h *Hub) Link(n Notification) string {
	base := strings.TrimRight(h.ConsoleURL, "/")
	if base == "" {
		return ""
	}
	if n.Kind == StewardNeeded {
		return base + "/steward"
	}
	return base + "/runs/" + url.PathEscape(n.RunID)
}

// Notify implements Notifier. It tries every channel and reports those
// that failed.
func (h *Hub) Notify(ctx context.Context, n Notification) error {
	if n.Link == "" {
		n.Link = h.Link(n)
	}
	var errs []error
	for _, c := range h.Channels {
		if err := c.Notify(ctx, n); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", c.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// FromEnv builds a hub from environment variables, which hold the webhook
// URLs because the URLs themselves are credentials:
//
//	TURGON_NOTIFY_SLACK_WEBHOOK_URL   Slack incoming webhook
//	TURGON_NOTIFY_TEAMS_WEBHOOK_URL   Microsoft Teams workflow webhook
//	TURGON_NOTIFY_WEBHOOK_URL         any HTTPS endpoint, JSON body,
//	TURGON_NOTIFY_WEBHOOK_SECRET      signed with this secret
//
// It returns nil when none is set.
func FromEnv(consoleURL string, client *http.Client) (*Hub, error) {
	h := &Hub{ConsoleURL: consoleURL}
	if u := os.Getenv("TURGON_NOTIFY_SLACK_WEBHOOK_URL"); u != "" {
		h.Channels = append(h.Channels, &Slack{URL: u, Client: client})
	}
	if u := os.Getenv("TURGON_NOTIFY_TEAMS_WEBHOOK_URL"); u != "" {
		h.Channels = append(h.Channels, &Teams{URL: u, Client: client})
	}
	if u := os.Getenv("TURGON_NOTIFY_WEBHOOK_URL"); u != "" {
		secret := os.Getenv("TURGON_NOTIFY_WEBHOOK_SECRET")
		if secret == "" {
			return nil, errors.New("TURGON_NOTIFY_WEBHOOK_URL needs TURGON_NOTIFY_WEBHOOK_SECRET to sign its messages")
		}
		h.Channels = append(h.Channels, &Webhook{URL: u, Secret: secret, Client: client})
	}
	for _, c := range h.Channels {
		if err := checkURL(c); err != nil {
			return nil, err
		}
	}
	if len(h.Channels) == 0 {
		return nil, nil
	}
	return h, nil
}

func checkURL(c Channel) error {
	var raw string
	switch x := c.(type) {
	case *Slack:
		raw = x.URL
	case *Teams:
		raw = x.URL
	case *Webhook:
		raw = x.URL
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && !(u.Scheme == "http" && isLoopback(u.Hostname()))) {
		return fmt.Errorf("%s notification URL must be https (http only to localhost)", c.Name())
	}
	return nil
}

func isLoopback(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func post(ctx context.Context, client *http.Client, target string, header http.Header, body []byte) error {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		// The error names the URL, which is a credential: leave it out.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			return fmt.Errorf("post: %w", uerr.Err)
		}
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// Slack posts to an incoming webhook with Block Kit.
type Slack struct {
	URL    string
	Client *http.Client
}

func (*Slack) Name() string { return "slack" }

// SlackMessage renders a notification for Slack.
func SlackMessage(n Notification) map[string]any {
	blocks := []any{
		map[string]any{"type": "header", "text": map[string]any{"type": "plain_text", "text": clip(n.Title, 150)}}, // plain text: no escaping
		map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": clip(slackEscape(n.Text), 3000)}},
	}
	if len(n.Facts) > 0 {
		var fields []any
		for _, f := range n.Facts {
			if len(fields) == 10 { // Slack's limit per section
				break
			}
			fields = append(fields, map[string]any{"type": "mrkdwn", "text": clip("*"+slackEscape(f.Name)+"*\n"+slackEscape(f.Value), 2000)})
		}
		blocks = append(blocks, map[string]any{"type": "section", "fields": fields})
	}
	if n.Link != "" {
		blocks = append(blocks, map[string]any{"type": "actions", "elements": []any{map[string]any{
			"type": "button", "text": map[string]any{"type": "plain_text", "text": "Open in Turgon"}, "url": n.Link,
		}}})
	}
	// The fallback text shows in notifications and is mrkdwn too.
	return map[string]any{"text": slackEscape(n.Title + ": " + n.Text), "blocks": blocks}
}

func (s *Slack) Notify(ctx context.Context, n Notification) error {
	body, _ := json.Marshal(SlackMessage(n))
	return post(ctx, s.Client, s.URL, nil, body)
}

// slackEscape escapes the three characters Slack's mrkdwn gives meaning,
// so values from source systems cannot inject links or mentions.
func slackEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

// Teams posts an Adaptive Card to a Teams workflow webhook.
type Teams struct {
	URL    string
	Client *http.Client
}

func (*Teams) Name() string { return "teams" }

// TeamsMessage renders a notification as an Adaptive Card.
func TeamsMessage(n Notification) map[string]any {
	body := []any{
		map[string]any{"type": "TextBlock", "text": n.Title, "weight": "Bolder", "size": "Medium", "wrap": true},
		map[string]any{"type": "TextBlock", "text": n.Text, "wrap": true},
	}
	if len(n.Facts) > 0 {
		var facts []any
		for _, f := range n.Facts {
			facts = append(facts, map[string]any{"title": f.Name, "value": f.Value})
		}
		body = append(body, map[string]any{"type": "FactSet", "facts": facts})
	}
	card := map[string]any{
		"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
		"type":    "AdaptiveCard", "version": "1.4", "body": body,
	}
	if n.Link != "" {
		card["actions"] = []any{map[string]any{"type": "Action.OpenUrl", "title": "Open in Turgon", "url": n.Link}}
	}
	return map[string]any{"type": "message", "attachments": []any{map[string]any{
		"contentType": "application/vnd.microsoft.card.adaptive", "content": card,
	}}}
}

func (t *Teams) Notify(ctx context.Context, n Notification) error {
	body, _ := json.Marshal(TeamsMessage(n))
	return post(ctx, t.Client, t.URL, nil, body)
}

// Webhook posts the notification as JSON with
// Turgon-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "t.body">,
// which the receiver checks, and rejects when the time is old.
type Webhook struct {
	URL    string
	Secret string
	Client *http.Client
	Now    func() time.Time
}

func (*Webhook) Name() string { return "webhook" }

// Sign returns the Turgon-Signature header for body at time t.
func Sign(secret string, t time.Time, body []byte) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(ts + "."))
	m.Write(body)
	return "t=" + ts + ",v1=" + hex.EncodeToString(m.Sum(nil))
}

func (w *Webhook) Notify(ctx context.Context, n Notification) error {
	body, _ := json.Marshal(n)
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	return post(ctx, w.Client, w.URL, http.Header{"Turgon-Signature": {Sign(w.Secret, now(), body)}}, body)
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// SortedFacts turns a map into facts sorted by name.
func SortedFacts(m map[string]string) []Fact {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]Fact, 0, len(m))
	for _, k := range names {
		if m[k] != "" {
			out = append(out, Fact{Name: k, Value: m[k]})
		}
	}
	return out
}
