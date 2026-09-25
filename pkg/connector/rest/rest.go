// Package rest is Turgon's generic connector for HTTP JSON APIs: SaaS such
// as Shopify, Stripe or HubSpot, and in-house services. Like the Postgres
// connector it is configured entirely by the Connection (architecture
// §7.1, AD-04): events, reads and writes are declared as HTTP requests, so
// a new API needs configuration, not code.
//
//   - Events are polled with a cursor the API understands (since_id,
//     created[gt], updated_at_min), in position order.
//   - Reads fetch one record by ID and rename its fields to business names.
//   - Writes send one request per operation: a method, a path templated
//     from the payload, and a body built from it. An update can capture
//     the fields it changes first, which previews the change before
//     approval, confirms it afterwards, and lets a restore operation undo
//     it exactly in a saga. A create is undone by a request templated from
//     its own result, such as DELETE /orders/{{id}}.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Name is the connector name this package implements.
const Name = "rest"

// Config is the connection-specific configuration.
type Config struct {
	// BaseURL prefixes every path, e.g. https://shop.example.com/admin/api/2026-07.
	BaseURL string `json:"baseURL"`
	Auth    Auth   `json:"auth"`
	// Headers are sent with every request. They must not hold secrets:
	// secrets come from the connection's secret reference.
	Headers map[string]string `json:"headers,omitempty"`
	// CheckPath is fetched by `turgon check` to prove access, e.g. /shop.json.
	CheckPath  string               `json:"checkPath,omitempty"`
	Events     map[string]Event     `json:"events,omitempty"`
	Operations map[string]Operation `json:"operations,omitempty"`
}

// Event defines an event as the items a list request returns.
type Event struct {
	Path string `json:"path"`
	// Params are query parameters. Values may use {{position}}, the last
	// position read (formatted as PositionFormat), and {{limit}}. The API
	// must return items after the position, oldest first.
	Params map[string]string `json:"params,omitempty"`
	// Items is the dotted path of the item array in the response; empty
	// when the response is the array.
	Items string `json:"items,omitempty"`
	// ID is the item field that identifies it (default "id").
	ID string `json:"id,omitempty"`
	// Position is the item field ordering items (default: the ID field).
	Position string `json:"position,omitempty"`
	// PositionFormat is "number" (default), "rfc3339" or "unix" (seconds).
	PositionFormat string `json:"positionFormat,omitempty"`
}

// Operation is one request.
type Operation struct {
	// Method is GET for reads; POST, PUT, PATCH or DELETE for writes.
	Method string `json:"method"`
	// Path may use {{field}} from the payload (a read's is {{id}});
	// values are escaped as path segments.
	Path string `json:"path"`
	// Fields maps API fields to payload fields (writes) or to the names a
	// read returns them under. Empty sends the whole payload / returns the
	// whole record.
	Fields map[string]string `json:"fields,omitempty"`
	// Wrap nests the body under a key, e.g. {"order": {...}}.
	Wrap string `json:"wrap,omitempty"`
	// Result is the dotted path of the record in the response.
	Result string `json:"result,omitempty"`
	// IdempotencyHeader sends the write's idempotency key, e.g. Idempotency-Key.
	IdempotencyHeader string `json:"idempotencyHeader,omitempty"`
	// Capture reads the record before an update: its current values of
	// the fields being set are kept, so the write can be previewed,
	// confirmed and restored.
	Capture *Capture `json:"capture,omitempty"`
	// Restore undoes a captured update: it sends the previous values from
	// the update's result, to the path templated from the update's params.
	Restore bool `json:"restore,omitempty"`
}

// Capture locates the record an update changes.
type Capture struct {
	Path   string `json:"path"`
	Result string `json:"result,omitempty"`
}

// UpdateResult is what a captured update returns and restore consumes.
type UpdateResult struct {
	Params   map[string]any `json:"params"`
	Previous map[string]any `json:"previous"`
	Applied  map[string]any `json:"applied"`
	Response any            `json:"response,omitempty"`
}

var (
	varRE   = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_.]*)\s*\}\}`)
	fieldRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)
)

func (c Config) validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("baseURL %q must be an http(s) URL", c.BaseURL)
	}
	if err := c.Auth.validate(); err != nil {
		return err
	}
	for k := range c.Headers {
		if strings.EqualFold(k, "Authorization") || strings.EqualFold(k, c.Auth.Header) {
			return fmt.Errorf("header %s carries a credential; use the connection's secret", k)
		}
	}
	for name, e := range c.Events {
		if !strings.HasPrefix(e.Path, "/") {
			return fmt.Errorf("event %s: path must start with /", name)
		}
		switch e.PositionFormat {
		case "", "number", "rfc3339", "unix":
		default:
			return fmt.Errorf("event %s: positionFormat must be number, rfc3339 or unix", name)
		}
	}
	for name, op := range c.Operations {
		if !strings.HasPrefix(op.Path, "/") {
			return fmt.Errorf("operation %s: path must start with /", name)
		}
		switch op.Method {
		case http.MethodGet:
			if !strings.Contains(op.Path, "{{id}}") && !strings.Contains(op.Path, "{{ id }}") {
				return fmt.Errorf("operation %s: a read's path must contain {{id}}", name)
			}
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			return fmt.Errorf("operation %s: method must be GET, POST, PUT, PATCH or DELETE", name)
		}
		for api, field := range op.Fields {
			if !fieldRE.MatchString(api) || !fieldRE.MatchString(field) {
				return fmt.Errorf("operation %s: invalid field mapping %q: %q", name, api, field)
			}
		}
		if op.Capture != nil && (len(op.Fields) == 0 || !strings.HasPrefix(op.Capture.Path, "/")) {
			return fmt.Errorf("operation %s: capture needs fields and a path", name)
		}
		if op.Restore && op.Method == http.MethodGet {
			return fmt.Errorf("operation %s: restore must write", name)
		}
	}
	return nil
}

// Factory builds a connector. The secret depends on the auth type.
func Factory(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	var c Config
	if err := json.Unmarshal(cfg.Config, &c); err != nil {
		return nil, fmt.Errorf("rest %s: config: %w", cfg.Endpoint, err)
	}
	secret := ""
	if c.Auth.Type != "none" {
		s, err := secrets.Resolve(ctx, cfg.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("rest %s: %w", cfg.Endpoint, err)
		}
		secret = s
	}
	conn, err := New(c, secret, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("rest %s: %w", cfg.Endpoint, err)
	}
	return conn, nil
}

// Conn is a connector instance.
type Conn struct {
	cfg  Config
	auth *authenticator
	http *http.Client
}

var (
	_ connector.Instance   = (*Conn)(nil)
	_ connector.Source     = (*Conn)(nil)
	_ writeguard.Confirmer = (*Conn)(nil)
	_ writeguard.Reader    = (*Conn)(nil)
)

// New validates the configuration and returns a connector.
func New(cfg Config, secret string, client *http.Client) (*Conn, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	a, err := newAuthenticator(cfg.Auth, secret, client)
	if err != nil {
		return nil, err
	}
	return &Conn{cfg: cfg, auth: a, http: client}, nil
}

func (c *Conn) Close() {}

// APIError is a non-success response.
type APIError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// permanent reports responses that retrying cannot fix: client errors,
// except timeouts, conflicts on concurrent changes and rate limits.
func (e *APIError) permanent() bool {
	switch e.Status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	}
	return e.Status >= 400 && e.Status < 500
}

// do sends a request and decodes a JSON response into out, if any.
func (c *Conn) do(ctx context.Context, method, path string, query url.Values, header http.Header, body any, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	target := c.cfg.BaseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range c.cfg.Headers {
			req.Header.Set(k, v)
		}
		for k, v := range header {
			req.Header[k] = v
		}
		if err := c.auth.apply(ctx, req); err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("%s %s: %w", method, path, err) // transient: retried
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 && c.auth.refreshable() {
			c.auth.invalidate()
			continue
		}
		if resp.StatusCode >= 300 {
			msg := strings.TrimSpace(string(data))
			if len(msg) > 300 {
				msg = msg[:300] + "…"
			}
			return &APIError{Method: method, Path: path, Status: resp.StatusCode, Body: msg}
		}
		if out != nil && len(bytes.TrimSpace(data)) > 0 {
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.UseNumber()
			return dec.Decode(out)
		}
		return nil
	}
}

// classify turns permanent API errors into writeguard.ErrInvalid.
func classify(err error) error {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.permanent() {
		return fmt.Errorf("%w: %v", writeguard.ErrInvalid, apiErr)
	}
	return err
}

// Poll returns up to limit items after the position, in position order.
// When a page is full and positions are timestamps, the trailing group of
// items sharing the last timestamp is left for the next poll, so a strict
// "after" cursor cannot skip items.
func (c *Conn) Poll(ctx context.Context, event string, after int64, limit int) ([]connector.Event, error) {
	e, ok := c.cfg.Events[event]
	if !ok {
		return nil, fmt.Errorf("rest: event %q is not configured on this connection", event)
	}
	idField := or(e.ID, "id")
	posField := or(e.Position, idField)
	q := url.Values{}
	for k, v := range e.Params {
		q.Set(k, varRE.ReplaceAllStringFunc(v, func(m string) string {
			switch varRE.FindStringSubmatch(m)[1] {
			case "position":
				return formatPosition(after, e.PositionFormat)
			case "limit":
				return strconv.Itoa(limit)
			}
			return m
		}))
	}
	var resp any
	if err := c.do(ctx, http.MethodGet, e.Path, q, nil, nil, &resp); err != nil {
		return nil, err
	}
	list, ok := lookup(resp, e.Items).([]any)
	if !ok {
		return nil, fmt.Errorf("rest: %s: response has no item array at %q", e.Path, e.Items)
	}
	var events []connector.Event
	for _, it := range list {
		item, ok := it.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("rest: %s: items must be objects", e.Path)
		}
		id := scalar(lookup(item, idField))
		pos, err := parsePosition(lookup(item, posField), e.PositionFormat)
		if id == "" || err != nil {
			return nil, fmt.Errorf("rest: %s: item without a usable %s/%s: %v", e.Path, idField, posField, err)
		}
		if pos <= after {
			continue
		}
		payload, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		events = append(events, connector.Event{ID: id, Position: pos, Name: event, Payload: payload})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Position < events[j].Position })
	full := len(list) >= limit
	if len(events) > limit {
		events, full = events[:limit], true
	}
	if full && e.PositionFormat != "" && e.PositionFormat != "number" {
		cut := len(events)
		for cut > 0 && events[cut-1].Position == events[len(events)-1].Position {
			cut--
		}
		if cut > 0 {
			events = events[:cut]
		}
	}
	return events, nil
}

func formatPosition(pos int64, format string) string {
	switch format {
	case "rfc3339":
		return time.UnixMilli(pos).UTC().Format("2006-01-02T15:04:05.000Z07:00")
	case "unix":
		return strconv.FormatInt(pos/1000, 10)
	}
	return strconv.FormatInt(pos, 10)
}

// parsePosition returns a number, or a time in Unix milliseconds.
func parsePosition(v any, format string) (int64, error) {
	s := scalar(v)
	switch format {
	case "rfc3339":
		t, err := time.Parse(time.RFC3339Nano, s)
		return t.UnixMilli(), err
	case "unix":
		n, err := strconv.ParseInt(s, 10, 64)
		return n * 1000, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func (c *Conn) operation(name string) (Operation, error) {
	op, ok := c.cfg.Operations[name]
	if !ok {
		return op, fmt.Errorf("%w: operation %q is not configured on this connection", writeguard.ErrInvalid, name)
	}
	return op, nil
}

func decodePayload(payload json.RawMessage) (map[string]any, error) {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%w: payload is not a JSON object: %v", writeguard.ErrInvalid, err)
	}
	return doc, nil
}

// render fills a path template from vars, escaping each value as a path
// segment, and returns the values it used.
func render(tmpl string, vars map[string]any) (string, map[string]any, error) {
	used := map[string]any{}
	var missing []string
	out := varRE.ReplaceAllStringFunc(tmpl, func(m string) string {
		name := varRE.FindStringSubmatch(m)[1]
		v := lookup(vars, name)
		s := scalar(v)
		if s == "" {
			missing = append(missing, name)
			return ""
		}
		used[name] = v
		return url.PathEscape(s)
	})
	if len(missing) > 0 {
		return "", nil, fmt.Errorf("%w: path %s needs %s", writeguard.ErrInvalid, tmpl, strings.Join(missing, ", "))
	}
	return out, used, nil
}

// body builds a write's request body from the payload.
func (op Operation) body(doc map[string]any) (map[string]any, error) {
	if len(op.Fields) == 0 {
		return doc, nil
	}
	b := map[string]any{}
	for api, field := range op.Fields {
		v := lookup(doc, field)
		if v == nil {
			return nil, fmt.Errorf("%w: payload has no %s for %s", writeguard.ErrInvalid, field, api)
		}
		b[api] = v
	}
	return b, nil
}

func (op Operation) wrap(b any) any {
	if op.Wrap == "" {
		return b
	}
	return map[string]any{op.Wrap: b}
}

// current reads the record a captured update changes.
func (c *Conn) current(ctx context.Context, op Operation, vars map[string]any) (map[string]any, error) {
	path, _, err := render(op.Capture.Path, vars)
	if err != nil {
		return nil, err
	}
	var resp any
	if err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &resp); err != nil {
		return nil, classify(err)
	}
	rec, ok := lookup(resp, op.Capture.Result).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rest: %s: no record at %q", path, op.Capture.Result)
	}
	return rec, nil
}

// Simulate previews a captured update: the record's current values and
// the values the write would set. Other writes cannot be dry-run through
// a generic API.
func (c *Conn) Simulate(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	if op.Capture == nil {
		return nil, writeguard.ErrSimulationUnsupported
	}
	doc, err := decodePayload(payload)
	if err != nil {
		return nil, err
	}
	b, err := op.body(doc)
	if err != nil {
		return nil, err
	}
	rec, err := c.current(ctx, op, doc)
	if err != nil {
		return nil, err
	}
	path, _, err := render(op.Path, doc)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"mode": "preview", "request": op.Method + " " + path, "current": previous(rec, b), "proposed": b})
}

func previous(rec, body map[string]any) map[string]any {
	prev := map[string]any{}
	for k := range body {
		prev[k] = rec[k]
	}
	return prev
}

// Commit performs a write.
func (c *Conn) Commit(ctx context.Context, name, key string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	if op.Method == http.MethodGet {
		return nil, fmt.Errorf("%w: %s is a read", writeguard.ErrInvalid, name)
	}
	doc, err := decodePayload(payload)
	if err != nil {
		return nil, err
	}
	vars, body := doc, any(nil)
	var captured map[string]any
	switch {
	case op.Restore:
		var r UpdateResult
		if err := json.Unmarshal(payload, &r); err != nil || r.Params == nil || r.Previous == nil {
			return nil, fmt.Errorf("%w: restore needs a captured update's result", writeguard.ErrInvalid)
		}
		vars, body = r.Params, op.wrap(r.Previous)
	case op.Method != http.MethodDelete:
		b, err := op.body(doc)
		if err != nil {
			return nil, err
		}
		body = op.wrap(b)
		if op.Capture != nil {
			rec, err := c.current(ctx, op, doc)
			if err != nil {
				return nil, err
			}
			captured = previous(rec, b)
		}
	}
	path, params, err := render(op.Path, vars)
	if err != nil {
		return nil, err
	}
	var header http.Header
	if op.IdempotencyHeader != "" && key != "" {
		header = http.Header{http.CanonicalHeaderKey(op.IdempotencyHeader): {key}}
	}
	var resp any
	if err := c.do(ctx, op.Method, path, nil, header, body, &resp); err != nil {
		return nil, classify(err)
	}
	result := lookup(resp, op.Result)
	switch {
	case captured != nil:
		b, _ := op.body(doc)
		return json.Marshal(UpdateResult{Params: params, Previous: captured, Applied: b, Response: result})
	case op.Restore:
		return json.Marshal(map[string]any{"restored": params, "response": result})
	case result == nil:
		return json.Marshal(map[string]any{"request": op.Method + " " + path, "params": params})
	}
	return json.Marshal(result)
}

// Confirm reads a captured update back and checks the values it set.
func (c *Conn) Confirm(ctx context.Context, name string, result json.RawMessage) error {
	op, ok := c.cfg.Operations[name]
	if !ok || op.Capture == nil || op.Restore {
		return nil
	}
	var r UpdateResult
	if err := json.Unmarshal(result, &r); err != nil {
		return err
	}
	rec, err := c.current(ctx, op, r.Params)
	if err != nil {
		return err
	}
	for k, want := range r.Applied {
		if !sameJSON(rec[k], want) {
			return fmt.Errorf("rest: %s reads back %s = %v, want %v", name, k, rec[k], want)
		}
	}
	return nil
}

// Read fetches one record by ID.
func (c *Conn) Read(ctx context.Context, name, id string) (json.RawMessage, error) {
	op, ok := c.cfg.Operations[name]
	if !ok || op.Method != http.MethodGet {
		return nil, fmt.Errorf("rest: %s is not a read on this connection", name)
	}
	path, _, err := render(op.Path, map[string]any{"id": id})
	if err != nil {
		return nil, writeguard.ErrNotFound
	}
	var resp any
	err = c.do(ctx, http.MethodGet, path, nil, nil, nil, &resp)
	var apiErr *APIError
	if errors.As(err, &apiErr) && (apiErr.Status == http.StatusNotFound || apiErr.Status == http.StatusGone) {
		return nil, writeguard.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rec, ok := lookup(resp, op.Result).(map[string]any)
	if !ok {
		return nil, writeguard.ErrNotFound
	}
	if len(op.Fields) == 0 {
		return json.Marshal(rec)
	}
	out := map[string]any{"id": id}
	for api, as := range op.Fields {
		out[as] = lookup(rec, api)
	}
	return json.Marshal(out)
}

// lookup follows a dotted path through JSON objects; "" is v itself.
func lookup(v any, path string) any {
	if path == "" {
		return v
	}
	for _, part := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[part]
	}
	return v
}

// scalar formats a JSON scalar for a path, query or ID.
func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return ""
}

func sameJSON(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	var x, y any
	_ = json.Unmarshal(ja, &x)
	_ = json.Unmarshal(jb, &y)
	return reflect.DeepEqual(x, y)
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
