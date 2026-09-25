package rest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// WebhookConfig lets an event also arrive pushed by the API. The pushed
// item must look like a polled one (Stripe posts the same event objects
// its /v1/events list returns; Shopify posts the order), so a run sees the
// same payload however the event arrived and run IDs deduplicate the two.
type WebhookConfig struct {
	// Signature is how deliveries are signed: "stripe" (Stripe-Signature,
	// with a timestamp), "shopify" (X-Shopify-Hmac-Sha256) or "hmac-sha256"
	// (an HMAC of the body in Header).
	Signature string `json:"signature"`
	// SecretRef is the signing secret, which is not the API credential.
	SecretRef string `json:"secretRef"`
	// Header, Encoding ("hex", default, or "base64") and Prefix (such as
	// "sha256=") describe an hmac-sha256 signature.
	Header   string `json:"header,omitempty"`
	Encoding string `json:"encoding,omitempty"`
	Prefix   string `json:"prefix,omitempty"`
	// ToleranceSeconds bounds a signed timestamp's age, against replays
	// (stripe; default 300).
	ToleranceSeconds int `json:"toleranceSeconds,omitempty"`
	// Items is the dotted path of an item array in the body; by default
	// the body is one item.
	Items string `json:"items,omitempty"`
	// Filter keeps items whose dotted fields have these values, e.g.
	// {type: invoice.paid} when one endpoint receives several event types.
	Filter map[string]string `json:"filter,omitempty"`
}

func (w *WebhookConfig) validate() error {
	switch w.Signature {
	case "stripe", "shopify":
	case "hmac-sha256":
		if w.Header == "" {
			return errors.New("an hmac-sha256 signature needs its header")
		}
		switch w.Encoding {
		case "", "hex", "base64":
		default:
			return errors.New("encoding must be hex or base64")
		}
	default:
		return fmt.Errorf("signature must be stripe, shopify or hmac-sha256, not %q", w.Signature)
	}
	if w.SecretRef == "" {
		return errors.New("secretRef is required: deliveries are only accepted when signed")
	}
	for k := range w.Filter {
		if !fieldRE.MatchString(k) {
			return fmt.Errorf("invalid filter field %q", k)
		}
	}
	return nil
}

// webhook is one event's configured webhook with its resolved secret.
type webhook struct {
	event  string
	idPath string
	cfg    WebhookConfig
	secret []byte
	err    error // resolving the secret failed
}

// resolveWebhookSecrets reads each webhook's signing secret. A missing
// secret only matters to a worker that receives webhooks, so it is kept as
// the webhook's error rather than failing the connection.
func (c *Conn) resolveWebhookSecrets(ctx context.Context, secrets connector.SecretResolver) {
	for name, e := range c.cfg.Events {
		if e.Webhook == nil {
			continue
		}
		s, err := secrets.Resolve(ctx, e.Webhook.SecretRef)
		if err == nil && s == "" {
			err = errors.New("the signing secret is empty")
		}
		c.setWebhookSecret(name, s, err)
	}
}

func (c *Conn) setWebhookSecret(event, secret string, err error) {
	if c.webhooks == nil {
		c.webhooks = map[string]*webhook{}
	}
	e := c.cfg.Events[event]
	c.webhooks[event] = &webhook{event: event, idPath: or(e.ID, "id"), cfg: *e.Webhook, secret: []byte(secret), err: err}
}

var _ connector.WebhookSource = (*Conn)(nil)

// Webhook implements connector.WebhookSource.
func (c *Conn) Webhook(event string) (connector.Webhook, error) {
	w, ok := c.webhooks[event]
	if !ok {
		return nil, connector.ErrNoWebhook
	}
	if w.err != nil {
		return nil, fmt.Errorf("rest: webhook for %s: signing secret: %w", event, w.err)
	}
	return w, nil
}

// Receive implements connector.Webhook.
func (w *webhook) Receive(header http.Header, body []byte, now time.Time) ([]connector.Event, error) {
	if err := w.verify(header, body, now); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("rest: webhook for %s: body is not JSON: %w", w.event, err)
	}
	var items []any
	switch v := lookup(doc, w.cfg.Items).(type) {
	case []any:
		items = v
	case map[string]any:
		items = []any{v}
	default:
		return nil, fmt.Errorf("rest: webhook for %s: no item at %q", w.event, w.cfg.Items)
	}
	var out []connector.Event
	for _, it := range items {
		item, ok := it.(map[string]any)
		if !ok || !w.matches(item) {
			continue
		}
		id := scalar(lookup(item, w.idPath))
		if id == "" {
			return nil, fmt.Errorf("rest: webhook for %s: item without %s", w.event, w.idPath)
		}
		payload, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		out = append(out, connector.Event{ID: id, Name: w.event, Payload: payload})
	}
	return out, nil
}

func (w *webhook) matches(item map[string]any) bool {
	for k, want := range w.cfg.Filter {
		if scalar(lookup(item, k)) != want {
			return false
		}
	}
	return true
}

func (w *webhook) mac(parts ...[]byte) []byte {
	m := hmac.New(sha256.New, w.secret)
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

func (w *webhook) verify(header http.Header, body []byte, now time.Time) error {
	switch w.cfg.Signature {
	case "stripe":
		// t=1727251200,v1=<hex>[,v1=<hex>…]: an HMAC of "t.body", so the
		// timestamp is signed too and old deliveries cannot be replayed.
		var ts string
		var sigs []string
		for _, kv := range strings.Split(header.Get("Stripe-Signature"), ",") {
			k, v, _ := strings.Cut(strings.TrimSpace(kv), "=")
			switch k {
			case "t":
				ts = v
			case "v1":
				sigs = append(sigs, v)
			}
		}
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || len(sigs) == 0 {
			return connector.ErrUnauthenticated
		}
		tolerance := int64(w.cfg.ToleranceSeconds)
		if tolerance <= 0 {
			tolerance = 300
		}
		if d := now.Unix() - t; d > tolerance || d < -tolerance {
			return fmt.Errorf("%w: timestamp is %ds away from now", connector.ErrUnauthenticated, d)
		}
		want := w.mac([]byte(ts), []byte("."), body)
		for _, s := range sigs {
			if got, err := hex.DecodeString(s); err == nil && hmac.Equal(got, want) {
				return nil
			}
		}
		return connector.ErrUnauthenticated
	case "shopify":
		got, err := base64.StdEncoding.DecodeString(header.Get("X-Shopify-Hmac-Sha256"))
		if err != nil || !hmac.Equal(got, w.mac(body)) {
			return connector.ErrUnauthenticated
		}
		return nil
	default: // hmac-sha256
		v, ok := strings.CutPrefix(header.Get(w.cfg.Header), w.cfg.Prefix)
		if !ok || v == "" {
			return connector.ErrUnauthenticated
		}
		var got []byte
		var err error
		if w.cfg.Encoding == "base64" {
			got, err = base64.StdEncoding.DecodeString(v)
		} else {
			got, err = hex.DecodeString(v)
		}
		if err != nil || !hmac.Equal(got, w.mac(body)) {
			return connector.ErrUnauthenticated
		}
		return nil
	}
}
