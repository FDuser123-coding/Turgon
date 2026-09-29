package sap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
)

// InterfaceEventMesh is the event interface of events taken from Event
// Mesh; the Event Mesh client is set up only for specs that use it.
const InterfaceEventMesh = "event-mesh"

// EventMesh is the SAP Event Mesh instance S/4HANA publishes its business
// events to (in S/4HANA Cloud through communication scenario SAP_COM_0092,
// on premise through an event channel, transaction /IWXBE/CONFIG). Turgon
// takes them from queues over the instance's REST messaging API: it only
// calls out, so no port is opened to receive events.
type EventMesh struct {
	// URL is the REST messaging endpoint: the service key's messaging
	// entry for protocol "httprest", its "uri".
	URL string `json:"url"`
	// TokenURL is the service key's oa2.tokenendpoint.
	TokenURL string `json:"tokenURL"`
	// SecretRef holds the service key's OAuth client, as JSON: its oa2
	// section ({"clientid", "clientsecret"}) as it stands.
	SecretRef string `json:"secretRef"`
}

// Subscription is an event taken from an Event Mesh queue: the CloudEvents
// S/4HANA publishes (sap.s4.beh.salesorder.v1.SalesOrder.Created.v1),
// which carry the business object's key and a few fields. Each message is
// acknowledged only after the event is stored in Turgon's inbox; one that
// was not (the worker stopped) is delivered again, and the inbox drops it
// by its CloudEvent ID. Give the queue a dead message queue and a
// redelivery limit, so a message that can never be read is set aside.
type Subscription struct {
	// Queue is the queue's full name, e.g. acme/s4/turgon/salesorders. One
	// subscription per queue: two would take each other's messages.
	Queue string `json:"queue"`
	// Types keeps only these CloudEvent types; one ending in * matches the
	// types it prefixes. Other messages on the queue are acknowledged and
	// dropped.
	Types []string `json:"types"`
	// Match keeps only events whose data has these field values, e.g.
	// {SalesOrderType: OR}.
	Match map[string]any `json:"match,omitempty"`
	// Read, if set, reads the business object's current state through
	// OData and makes it the payload: an event carries only its key and a
	// few fields, and a flow usually needs more.
	Read *Read `json:"read,omitempty"`
	// Idle is how long to wait after finding the queue empty (default 1s).
	Idle string `json:"idle,omitempty"`
}

// Read reads the entity an event is about.
type Read struct {
	Service   string `json:"service"`
	EntitySet string `json:"entitySet"`
	// Key is the entity's key property.
	Key string `json:"key"`
	// KeyFrom is the event data field holding the key (default Key: S/4HANA
	// events name it as the API does, SalesOrder or BusinessPartner).
	KeyFrom string   `json:"keyFrom,omitempty"`
	Select  []string `json:"select,omitempty"`
	Expand  []string `json:"expand,omitempty"`
}

var (
	queueRE     = regexp.MustCompile(`^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$`)
	eventTypeRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+\*?$`)
)

func (c Config) validateSubscriptions() error {
	if len(c.Subscriptions) == 0 {
		return nil
	}
	m := c.EventMesh
	if m == nil {
		return errors.New("subscriptions need eventMesh: the instance's url, tokenURL and secretRef")
	}
	if err := checkURL("eventMesh.url", m.URL); err != nil {
		return err
	}
	if err := checkURL("eventMesh.tokenURL", m.TokenURL); err != nil {
		return err
	}
	if m.SecretRef == "" {
		return errors.New("eventMesh.secretRef is required: the service key's OAuth client")
	}
	queues := map[string]string{}
	for name, s := range c.Subscriptions {
		where := "subscription " + name
		if _, ok := c.Events[name]; ok {
			return fmt.Errorf("%s: %s is also a polled event; name it once", where, name)
		}
		if !queueRE.MatchString(s.Queue) || len(s.Queue) > 200 {
			return fmt.Errorf("%s: invalid queue %q", where, s.Queue)
		}
		if other, ok := queues[s.Queue]; ok {
			return fmt.Errorf("%s: queue %s is also subscription %s's; consumers of one queue take each other's messages", where, s.Queue, other)
		}
		queues[s.Queue] = name
		if len(s.Types) == 0 {
			return fmt.Errorf("%s: types are required (the CloudEvent types to take, e.g. sap.s4.beh.salesorder.v1.SalesOrder.Created.v1)", where)
		}
		for _, t := range s.Types {
			if !eventTypeRE.MatchString(t) {
				return fmt.Errorf("%s: invalid type %q", where, t)
			}
		}
		for f := range s.Match {
			if !identRE.MatchString(f) {
				return fmt.Errorf("%s: invalid match field %q", where, f)
			}
		}
		if s.Idle != "" {
			d, err := time.ParseDuration(s.Idle)
			if err != nil || d < 100*time.Millisecond || d > time.Minute {
				return fmt.Errorf("%s: idle must be a duration from 100ms to 1m", where)
			}
		}
		if r := s.Read; r != nil {
			if !serviceRE.MatchString(r.Service) || !identRE.MatchString(r.EntitySet) || !identRE.MatchString(r.Key) {
				return fmt.Errorf("%s: read needs service, entitySet and key", where)
			}
			if r.KeyFrom != "" && !identRE.MatchString(r.KeyFrom) {
				return fmt.Errorf("%s: invalid keyFrom %q", where, r.KeyFrom)
			}
			for _, p := range append(append([]string{}, r.Select...), r.Expand...) {
				if !pathRE.MatchString(strings.ReplaceAll(p, "/", ".")) {
					return fmt.Errorf("%s: invalid property %q", where, p)
				}
			}
		}
	}
	return nil
}

// checkURL accepts https, and http only to a loopback address (a test
// fake): the requests carry bearer tokens.
func checkURL(field, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%s must be an https URL", field)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if ip := net.ParseIP(u.Hostname()); u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback()) {
			return nil
		}
	}
	return fmt.Errorf("%s must be an https URL", field)
}

// meshClient calls an Event Mesh instance's REST messaging API.
type meshClient struct {
	base string
	auth *rest.Authenticator
	http *http.Client
	// liveness is how often an idle subscription reports that it is open.
	liveness time.Duration
}

// UseEventMesh sets the Event Mesh instance's OAuth client (the service
// key's oa2 section, as JSON).
func (c *Conn) UseEventMesh(secret string, client *http.Client) error {
	m := c.cfg.EventMesh
	if m == nil {
		return errors.New("the connection configures no eventMesh")
	}
	auth, err := rest.NewAuthenticator(rest.Auth{Type: "oauth2", TokenURL: m.TokenURL, ClientAuth: "body"}, secret, client)
	if err != nil {
		return err
	}
	c.meshMu.Lock()
	defer c.meshMu.Unlock()
	c.mesh = &meshClient{base: strings.TrimRight(m.URL, "/") + "/messagingrest/v1", auth: auth, http: client, liveness: 30 * time.Second}
	return nil
}

// meshClient returns the Event Mesh client.
func (c *Conn) meshClient() (*meshClient, error) {
	c.meshMu.Lock()
	defer c.meshMu.Unlock()
	if c.mesh == nil {
		return nil, errors.New("event mesh: no credentials")
	}
	return c.mesh, nil
}

var _ connector.Streamer = (*Conn)(nil)

// Streams reports whether event is taken from an Event Mesh queue.
func (c *Conn) Streams(event string) bool {
	_, ok := c.cfg.Subscriptions[event]
	return ok
}

// message is a message consumed from a queue.
type message struct {
	id     string
	header http.Header
	body   []byte
}

// meshError is an error response from the messaging API.
type meshError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *meshError) Error() string {
	return fmt.Sprintf("event mesh %s %s: %d %s", e.Method, e.Path, e.Status, e.Body)
}

// call sends a request, fetching the token again once if it was rejected.
func (m *meshClient) call(ctx context.Context, path string, header http.Header) (*http.Response, []byte, error) {
	for retried := false; ; retried = true {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.base+path, nil)
		if err != nil {
			return nil, nil, err
		}
		for k, v := range header {
			req.Header[k] = v
		}
		if err := m.auth.Apply(ctx, req); err != nil {
			return nil, nil, fmt.Errorf("event mesh: %w", err)
		}
		resp, err := m.http.Do(req)
		if err != nil {
			return nil, nil, fmt.Errorf("event mesh: %w", err)
		}
		// Event Mesh takes messages of at most 1 MB.
		body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("event mesh: %w", err)
		}
		if resp.StatusCode == http.StatusUnauthorized && !retried {
			m.auth.Invalidate()
			continue
		}
		if resp.StatusCode >= 300 {
			msg := strings.TrimSpace(string(body))
			if len(msg) > 300 {
				msg = msg[:300] + "…"
			}
			return nil, nil, &meshError{Method: http.MethodPost, Path: path, Status: resp.StatusCode, Body: msg}
		}
		return resp, body, nil
	}
}

func queuePath(queue string) string { return "/queues/" + url.PathEscape(queue) }

// consume takes the next message from a queue with QoS 1: it stays on the
// queue until acknowledged. nil when the queue is empty.
func (m *meshClient) consume(ctx context.Context, queue string) (*message, error) {
	resp, body, err := m.call(ctx, queuePath(queue)+"/messages/consumption", http.Header{"X-Qos": {"1"}})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	id := resp.Header.Get("X-Message-Id")
	if id == "" {
		return nil, errors.New("event mesh: a consumed message came without x-message-id, so it cannot be acknowledged")
	}
	return &message{id: id, header: resp.Header, body: body}, nil
}

func (m *meshClient) ack(ctx context.Context, queue, id string) error {
	_, _, err := m.call(ctx, queuePath(queue)+"/messages/"+url.PathEscape(id)+"/acknowledgement", nil)
	return err
}

// Stream takes event's messages from its queue until ctx ends or a call
// fails. Queues keep their own place (unacknowledged messages come back),
// so there is no resume position.
func (c *Conn) Stream(ctx context.Context, event string, _ []byte, deliver func([]connector.Event, []byte) error) error {
	sub, ok := c.cfg.Subscriptions[event]
	if !ok {
		return fmt.Errorf("sap: event %q is not a subscription on this connection", event)
	}
	mesh, err := c.meshClient()
	if err != nil {
		return fmt.Errorf("sap: %w", err)
	}
	idle := time.Second
	if sub.Idle != "" {
		idle, _ = time.ParseDuration(sub.Idle)
	}
	lastReport := time.Time{}
	for {
		msg, err := mesh.consume(ctx, sub.Queue)
		if err != nil {
			return c.meshErr(err, sub.Queue)
		}
		if msg == nil {
			// Say the subscription is open now and then, so a quiet queue
			// is not taken for a stalled one.
			if time.Since(lastReport) >= mesh.liveness {
				if err := deliver(nil, nil); err != nil {
					return err
				}
				lastReport = time.Now()
			}
			t := time.NewTimer(idle)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			continue
		}
		ev, keep, err := c.convertMessage(ctx, event, sub, msg)
		if err != nil {
			// Not acknowledged: the queue delivers it again later, and after
			// its redelivery limit moves it to the dead message queue.
			return fmt.Errorf("sap event mesh %s: message %s: %w", sub.Queue, msg.id, err)
		}
		if keep {
			if err := deliver([]connector.Event{ev}, nil); err != nil {
				return err
			}
			lastReport = time.Now()
		}
		if err := mesh.ack(ctx, sub.Queue, msg.id); err != nil {
			return c.meshErr(err, sub.Queue)
		}
	}
}

func (c *Conn) meshErr(err error, queue string) error {
	var me *meshError
	if errors.As(err, &me) && me.Status == http.StatusNotFound {
		return fmt.Errorf("%w (is there a queue %s on the instance?)", err, queue)
	}
	return err
}

// cloudEvent is a CloudEvents 1.0 event in structured mode.
type cloudEvent struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	Subject     string          `json:"subject,omitempty"`
	Time        string          `json:"time,omitempty"`
	Data        json.RawMessage `json:"data,omitempty"`
	DataBase64  string          `json:"data_base64,omitempty"`
}

// parseCloudEvent reads a message in structured mode (the event is the
// JSON body) or binary mode (ce- headers, the data is the body).
func parseCloudEvent(msg *message) (cloudEvent, error) {
	var ce cloudEvent
	if id := msg.header.Get("Ce-Id"); id != "" {
		ce = cloudEvent{SpecVersion: msg.header.Get("Ce-Specversion"), ID: id, Type: msg.header.Get("Ce-Type"),
			Source: msg.header.Get("Ce-Source"), Subject: msg.header.Get("Ce-Subject"), Time: msg.header.Get("Ce-Time"), Data: msg.body}
	} else if err := json.Unmarshal(msg.body, &ce); err != nil {
		return ce, fmt.Errorf("not a CloudEvent: %w", err)
	}
	if ce.SpecVersion == "" || ce.ID == "" || ce.Type == "" || ce.Source == "" {
		return ce, errors.New("not a CloudEvent: specversion, id, type and source are required")
	}
	if len(ce.Data) == 0 && ce.DataBase64 != "" {
		data, err := base64.StdEncoding.DecodeString(ce.DataBase64)
		if err != nil {
			return ce, fmt.Errorf("data_base64: %w", err)
		}
		ce.Data = data
	}
	return ce, nil
}

func typeMatches(types []string, t string) bool {
	for _, want := range types {
		if p, ok := strings.CutSuffix(want, "*"); (ok && strings.HasPrefix(t, p)) || want == t {
			return true
		}
	}
	return false
}

// convertMessage makes a message an event; keep is false for messages the
// subscription does not take.
func (c *Conn) convertMessage(ctx context.Context, event string, sub Subscription, msg *message) (connector.Event, bool, error) {
	ce, err := parseCloudEvent(msg)
	if err != nil {
		return connector.Event{}, false, err
	}
	if !typeMatches(sub.Types, ce.Type) {
		return connector.Event{}, false, nil
	}
	data := map[string]any{}
	if len(bytes.TrimSpace(ce.Data)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(ce.Data))
		dec.UseNumber()
		if err := dec.Decode(&data); err != nil {
			return connector.Event{}, false, fmt.Errorf("event %s: data is not a JSON object: %w", ce.ID, err)
		}
	}
	if !matchesData(data, sub.Match) {
		return connector.Event{}, false, nil
	}
	payload := data
	if r := sub.Read; r != nil {
		from := r.KeyFrom
		if from == "" {
			from = r.Key
		}
		key, ok := data[from].(string)
		if !ok {
			if n, isNum := data[from].(json.Number); isNum {
				key, ok = n.String(), true
			}
		}
		if !ok || key == "" {
			return connector.Event{}, false, fmt.Errorf("event %s: data has no %s to read the %s by", ce.ID, from, r.EntitySet)
		}
		current, err := c.readEntity(ctx, r, key)
		if err != nil {
			return connector.Event{}, false, fmt.Errorf("event %s: %w", ce.ID, err)
		}
		// A deleted object keeps the event's data.
		if current != nil {
			payload = current
		}
	}
	payload["cloudEvent"] = map[string]any{"id": ce.ID, "type": ce.Type, "source": ce.Source, "subject": ce.Subject, "time": ce.Time}
	b, err := json.Marshal(payload)
	if err != nil {
		return connector.Event{}, false, err
	}
	return connector.Event{ID: ce.ID, Name: event, Payload: b}, true, nil
}

// readEntity reads one entity; nil if it no longer exists.
func (c *Conn) readEntity(ctx context.Context, r *Read, key string) (map[string]any, error) {
	path, err := entityPath(r.EntitySet, key)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if len(r.Select) > 0 {
		sel := append([]string{}, r.Select...)
		if !contains(sel, r.Key) {
			sel = append(sel, r.Key)
		}
		q.Set("$select", strings.Join(sel, ","))
	}
	if len(r.Expand) > 0 {
		q.Set("$expand", strings.Join(r.Expand, ","))
	}
	resp, err := c.do(ctx, http.MethodGet, c.url(r.Service, path, q), nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.Status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec, err := entity(resp)
	if err != nil {
		return nil, err
	}
	out, _ := clean(rec).(map[string]any)
	return out, nil
}

func matchesData(data, want map[string]any) bool {
	for k, v := range want {
		got, ok := data[k]
		if !ok {
			return false
		}
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(v)
		if string(a) != string(b) {
			return false
		}
	}
	return true
}

// checkEventMesh signs in to the Event Mesh instance. Queues cannot be
// looked at without taking their messages, so the first subscription to
// open shows whether they exist.
func (c *Conn) checkEventMesh(ctx context.Context) connector.CheckResult {
	const name = "event mesh"
	mesh, err := c.meshClient()
	if err != nil {
		return connector.Fail(name, strings.TrimPrefix(err.Error(), "event mesh: "), fmt.Sprintf("Put the Event Mesh service key's oa2 section in %s.", c.cfg.EventMesh.SecretRef))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mesh.base, nil)
	if err != nil {
		return connector.Fail(name, err.Error(), "")
	}
	if err := mesh.auth.Apply(ctx, req); err != nil {
		var api *rest.APIError
		if errors.As(err, &api) {
			return connector.Fail(name, err.Error(), "The instance's token endpoint rejected the client. Put the service key's oa2 clientid and clientsecret in the secret, and oa2.tokenendpoint in eventMesh.tokenURL.")
		}
		return connector.Fail(name, err.Error(), connector.NetworkFix(err, c.cfg.EventMesh.TokenURL))
	}
	return connector.Pass(name, fmt.Sprintf("signed in; %d queue(s) are read when the subscriptions open", len(c.cfg.Subscriptions)))
}
