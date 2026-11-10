// Package rest is Turgon's generic connector for HTTP JSON APIs: SaaS such
// as Shopify, Stripe or HubSpot, and in-house services. Like the Postgres
// connector it is configured entirely by the Connection (architecture
// §7.1, AD-04): events, reads and writes are declared as HTTP requests, so
// a new API needs configuration, not code.
//
//   - Events are polled with a cursor the API understands (since_id,
//     created[gt], updated_at_min, or a search body), in position order.
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
	CheckPath string `json:"checkPath,omitempty"`
	// OpenAPI is the URL of the API's OpenAPI 3 description (JSON or
	// YAML). Discovery then reads from it what each operation sends and
	// receives and what each event's list returns, instead of sampling.
	OpenAPI    string               `json:"openapi,omitempty"`
	Events     map[string]Event     `json:"events,omitempty"`
	Operations map[string]Operation `json:"operations,omitempty"`
}

// Event defines an event as the items a list request returns.
type Event struct {
	// Method is GET (default) or POST, for search APIs such as HubSpot's
	// that take the query as a JSON body.
	Method string `json:"method,omitempty"`
	Path   string `json:"path"`
	// Params are query parameters. Values may use {{position}}, the last
	// position read (formatted as PositionFormat), {{positionMillis}}, a
	// time position as Unix milliseconds, and {{limit}}. The API must
	// return items after the position, oldest first.
	Params map[string]string `json:"params,omitempty"`
	// Body is a POST request's JSON body. Its strings may use the same
	// variables; a string that is only {{limit}} becomes a number.
	Body any `json:"body,omitempty"`
	// Items is the dotted path of the item array in the response; empty
	// when the response is the array.
	Items string `json:"items,omitempty"`
	// ID is the item field that identifies it (default "id").
	ID string `json:"id,omitempty"`
	// Position is the item field ordering items (default: the ID field).
	Position string `json:"position,omitempty"`
	// Versioned names each event by its item's ID and position
	// (connector.VersionID), so a record changed again starts a new run.
	// Default: true when Position is another field than ID, as in a feed of
	// changed records; false keeps the ID alone.
	Versioned *bool `json:"versioned,omitempty"`
	// PositionFormat is "number" (default), "rfc3339", "unix" (seconds) or
	// "msjson" (Microsoft's JSON date, "/Date(1730803200000+0000)/", as
	// Xero answers). {{position}} renders in it; {{positionTime}} is always
	// the UTC time without a zone ("2009-11-12T00:00:00"), for a request that
	// takes a time where the answer holds another format (Xero's
	// If-Modified-Since, which reads it as UTC). {{positionTimeMillis}} is
	// the same with milliseconds ("2009-11-12T00:00:00.000"), for an API
	// that keeps them (Exact Online): a cursor cut to the second would
	// re-read the rest of its second on every poll.
	PositionFormat string `json:"positionFormat,omitempty"`
	// Order is the API's list order: "asc" (default, oldest first) or
	// "desc" (newest first, as Stripe lists). A descending list is paged
	// back to the cursor, so a burst of new items is never skipped.
	Order string `json:"order,omitempty"`
	// Pagination pages a list by the last item's ID.
	Pagination *Pagination `json:"pagination,omitempty"`
	// MaxItems is the most items the API answers at once, whatever it is
	// asked for (Exact Online: 60). A poll asks for no more, so that a full
	// answer is known to be cut short, and a timestamp it ends in is never
	// split.
	MaxItems int `json:"maxItems,omitempty"`
	// Headers are sent with this event's requests only, e.g. NetSuite's
	// "Prefer: transient" for SuiteQL.
	Headers map[string]string `json:"headers,omitempty"`
	// Webhook lets the event also arrive pushed by the API, to a worker
	// that listens for webhooks; the list request then reconciles what was
	// not delivered.
	Webhook *WebhookConfig `json:"webhook,omitempty"`
}

// versioned reports whether the event's IDs name the item's version.
func (e Event) versioned() bool {
	if e.Versioned != nil {
		return *e.Versioned
	}
	return e.Position != "" && e.Position != or(e.ID, "id")
}

// Pagination is cursor paging by the last item: the next page is
// requested with Param set to the last item's Cursor field, while the
// response's More field is true.
type Pagination struct {
	Param  string `json:"param"`            // e.g. starting_after
	Cursor string `json:"cursor,omitempty"` // item field, default the event's ID field
	More   string `json:"more"`             // response field, e.g. has_more
	// MaxPages bounds one poll (default 20).
	MaxPages int `json:"maxPages,omitempty"`
}

// Operation is one request.
type Operation struct {
	// Method is GET for reads; POST, PUT, PATCH or DELETE for writes.
	Method string `json:"method"`
	// Path may use {{field}} from the payload (a read's is {{id}});
	// values are escaped as path segments, and a value quoted in the
	// template ('{{field}}', as OData keys are) has its quotes doubled.
	// {{idempotencyKey}} is the write's idempotency key, for APIs that
	// upsert by an external ID (NetSuite eid:…, Dataverse alternate keys).
	Path string `json:"path"`
	// Headers are sent with this operation's requests only, e.g.
	// Dataverse's "Prefer: return=representation".
	Headers map[string]string `json:"headers,omitempty"`
	// IDFrom names a response header holding the written record's URL,
	// for APIs that answer a write with 204 and no body (NetSuite's
	// Location, Dataverse's OData-EntityId). The result is then
	// {"id": <last path segment, or the key in its parentheses>, "url": ...}.
	IDFrom string `json:"idFrom,omitempty"`
	// Fields maps API fields to payload fields (writes) or to the names a
	// read returns them under. Empty sends the whole payload / returns the
	// whole record. A dot in an API field nests it (metadata.key); "\."
	// is a literal dot, as in OData's customerid_account@odata\.bind.
	Fields map[string]string `json:"fields,omitempty"`
	// Wrap nests the body under a key, e.g. {"order": {...}}.
	Wrap string `json:"wrap,omitempty"`
	// BodyFormat is "json" (default), "none" (no body: an action such as
	// Business Central's or SAP Business One's POST .../Cancel, whose
	// payload only fills the path) or "form": form-encoded, with nested
	// fields as metadata[key] and a null sent as "" (which Stripe reads
	// as "unset").
	BodyFormat string `json:"bodyFormat,omitempty"`
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
	// NullValue is sent in a JSON body in place of null, for APIs that
	// clear a field with another value (HubSpot clears a property with "").
	NullValue *string `json:"nullValue,omitempty"`
	// Constants are fixed API field values sent with the mapped ones, such
	// as Xero's Status: VOIDED for the request that voids an invoice. They
	// are not part of what an update captures, confirms or restores.
	Constants map[string]any `json:"constants,omitempty"`
	// KeyField is the API field a create stores the write's idempotency key
	// in (Business Central's externalDocumentNumber, SAP Business One's
	// NumAtCard), so the existing lookup finds a retry's record by the key
	// itself.
	KeyField string `json:"keyField,omitempty"`
	// Existing finds the record a retried create already made, for APIs
	// that can neither upsert by an external ID nor take an idempotency
	// key (Business Central): the create is sent only when it lists none.
	Existing *Existing `json:"existing,omitempty"`
}

// Existing is a list request whose path and params may use the payload's
// fields and {{idempotencyKey}}, such as an OData filter on the field the
// create stores the key in. A value quoted in the template ('{{field}}')
// has its quotes doubled, in the params as in the path.
type Existing struct {
	Path   string            `json:"path"`
	Params map[string]string `json:"params,omitempty"`
	// Items is the dotted path of the list in the response (value for OData).
	Items string `json:"items"`
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
	// apiFieldRE also allows OData annotations (@odata\.bind).
	apiFieldRE = regexp.MustCompile(`^[A-Za-z_@][A-Za-z0-9_.@\\]*$`)
)

// checkHeaders refuses headers that would carry a credential.
func (c Config) checkHeaders(where string, h map[string]string) error {
	for k := range h {
		if strings.EqualFold(k, "Authorization") || (c.Auth.Header != "" && strings.EqualFold(k, c.Auth.Header)) {
			return fmt.Errorf("%sheader %s carries a credential; use the connection's secret", where, k)
		}
	}
	return nil
}

func (c Config) validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("baseURL %q must be an http(s) URL", c.BaseURL)
	}
	if err := c.Auth.validate(); err != nil {
		return err
	}
	if err := c.Auth.SameOrigin(u); err != nil {
		return err
	}
	if c.OpenAPI != "" {
		o, err := url.Parse(c.OpenAPI)
		if err != nil || (o.Scheme != "https" && o.Scheme != "http") || o.Host == "" {
			return fmt.Errorf("openapi %q must be an http(s) URL", c.OpenAPI)
		}
	}
	if err := c.checkHeaders("", c.Headers); err != nil {
		return err
	}
	for name, e := range c.Events {
		if !strings.HasPrefix(e.Path, "/") {
			return fmt.Errorf("event %s: path must start with /", name)
		}
		if e.MaxItems < 0 {
			return fmt.Errorf("event %s: maxItems must not be negative", name)
		}
		switch e.Method {
		case "", http.MethodGet:
			if e.Body != nil {
				return fmt.Errorf("event %s: a GET list has no body", name)
			}
		case http.MethodPost:
		default:
			return fmt.Errorf("event %s: method must be GET or POST", name)
		}
		switch e.PositionFormat {
		case "", "number", "rfc3339", "unix", "msjson":
		default:
			return fmt.Errorf("event %s: positionFormat must be number, rfc3339, unix or msjson", name)
		}
		switch e.Order {
		case "", "asc":
		case "desc":
			if e.Pagination == nil {
				return fmt.Errorf("event %s: a descending list needs pagination to reach the cursor", name)
			}
		default:
			return fmt.Errorf("event %s: order must be asc or desc", name)
		}
		if p := e.Pagination; p != nil && (p.Param == "" || p.More == "") {
			return fmt.Errorf("event %s: pagination needs param and more", name)
		}
		if err := c.checkHeaders("event "+name+": ", e.Headers); err != nil {
			return err
		}
		if e.Webhook != nil {
			if err := e.Webhook.validate(); err != nil {
				return fmt.Errorf("event %s: webhook: %w", name, err)
			}
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
		if err := c.checkHeaders("operation "+name+": ", op.Headers); err != nil {
			return err
		}
		switch op.IDFrom {
		case "", "Location", "OData-EntityId":
		default:
			return fmt.Errorf("operation %s: idFrom must be Location or OData-EntityId", name)
		}
		for api, field := range op.Fields {
			if !apiFieldRE.MatchString(api) || !fieldRE.MatchString(field) {
				return fmt.Errorf("operation %s: invalid field mapping %q: %q", name, api, field)
			}
		}
		for api := range op.Constants {
			if !apiFieldRE.MatchString(api) {
				return fmt.Errorf("operation %s: invalid constant field %q", name, api)
			}
			for f := range op.Fields {
				if f == api || strings.HasPrefix(f, api+".") || strings.HasPrefix(api, f+".") {
					return fmt.Errorf("operation %s: constant %s overlaps the mapped field %s", name, api, f)
				}
			}
		}
		if k := op.KeyField; k != "" {
			if !apiFieldRE.MatchString(k) || (op.Method != http.MethodPost && op.Method != http.MethodPut && op.Method != http.MethodPatch) {
				return fmt.Errorf("operation %s: keyField must be an API field of a POST, PUT or PATCH", name)
			}
			if _, mapped := op.Fields[k]; mapped {
				return fmt.Errorf("operation %s: keyField %s is also mapped", name, k)
			}
			if _, fixed := op.Constants[k]; fixed {
				return fmt.Errorf("operation %s: keyField %s is also a constant", name, k)
			}
		}
		if op.Capture != nil && (len(op.Fields) == 0 || !strings.HasPrefix(op.Capture.Path, "/")) {
			return fmt.Errorf("operation %s: capture needs fields and a path", name)
		}
		if e := op.Existing; e != nil {
			if !strings.HasPrefix(e.Path, "/") || e.Items == "" {
				return fmt.Errorf("operation %s: existing needs a path and items", name)
			}
			if op.Method != http.MethodPost && op.Method != http.MethodPut {
				return fmt.Errorf("operation %s: existing finds a created record, so the operation must POST or PUT", name)
			}
		}
		if op.Restore && op.Method == http.MethodGet {
			return fmt.Errorf("operation %s: restore must write", name)
		}
		switch op.BodyFormat {
		case "", "json":
		case "none":
			if len(op.Fields) > 0 || len(op.Constants) > 0 || op.KeyField != "" || op.Capture != nil || op.Restore {
				return fmt.Errorf("operation %s: a request without a body has no fields, constants or keyField", name)
			}
		case "form":
			if op.NullValue != nil {
				return fmt.Errorf("operation %s: a form body sends null as \"\" already; nullValue is for JSON bodies", name)
			}
		default:
			return fmt.Errorf("operation %s: bodyFormat must be json, form or none", name)
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
	conn.endpoint = cfg.Endpoint
	if ts, ok := connector.Tokens(secrets); ok {
		conn.UseTokens(ts)
	}
	conn.resolveWebhookSecrets(ctx, secrets)
	return conn, nil
}

// UseTokens gives an oauth2-refresh connection the store for the refresh
// tokens its server rotates (Turgon's database), keyed by the endpoint.
func (c *Conn) UseTokens(store connector.TokenStore) {
	key := c.endpoint
	if key == "" {
		key = c.cfg.BaseURL
	}
	c.auth.useTokens(store, "rest/"+key)
}

// Conn is a connector instance.
type Conn struct {
	cfg      Config
	auth     *authenticator
	http     *http.Client
	endpoint string
	webhooks map[string]*webhook
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
	_, err := c.doH(ctx, method, path, query, header, body, out)
	return err
}

// doH is do, returning the response's headers.
func (c *Conn) doH(ctx context.Context, method, path string, query url.Values, header http.Header, body any, out any) (http.Header, error) {
	var payload []byte
	contentType := "application/json"
	switch b := body.(type) {
	case nil:
	case formBody:
		payload, contentType = []byte(encodeForm(b.v).Encode()), "application/x-www-form-urlencoded"
	default:
		var err error
		if payload, err = json.Marshal(b); err != nil {
			return nil, err
		}
	}
	target := c.cfg.BaseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", contentType)
		}
		for k, v := range c.cfg.Headers {
			req.Header.Set(k, v)
		}
		for k, v := range header {
			req.Header[k] = v
		}
		if err := c.auth.apply(ctx, req); err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", method, path, err) // transient: retried
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 && c.auth.refreshable() {
			c.auth.invalidate(req)
			continue
		}
		if resp.StatusCode >= 300 {
			msg := strings.TrimSpace(string(data))
			if len(msg) > 300 {
				msg = msg[:300] + "…"
			}
			return nil, &APIError{Method: method, Path: path, Status: resp.StatusCode, Body: msg}
		}
		if out != nil && len(bytes.TrimSpace(data)) > 0 {
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.UseNumber()
			return resp.Header, dec.Decode(out)
		}
		return resp.Header, nil
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
// An ascending list is read one page at a time. A descending list is paged
// back until it reaches the position, then its oldest items are taken, so
// the cursor never jumps over items it has not returned. When a batch is
// cut short and positions are timestamps, the trailing group of items
// sharing the last timestamp is left for the next poll, so a strict
// "after" cursor cannot skip items.
func (c *Conn) Poll(ctx context.Context, event string, after int64, limit int) ([]connector.Event, error) {
	e, ok := c.cfg.Events[event]
	if !ok {
		return nil, fmt.Errorf("rest: event %q is not configured on this connection", event)
	}
	idField := or(e.ID, "id")
	posField := or(e.Position, idField)
	if e.MaxItems > 0 && limit > e.MaxItems {
		limit = e.MaxItems
	}
	method, q, body := e.listRequest(after, limit)
	desc := e.Order == "desc"
	maxPages := 1
	if e.Pagination != nil && desc {
		maxPages = e.Pagination.MaxPages
		if maxPages <= 0 {
			maxPages = 20
		}
	}
	var events []connector.Event
	truncated := false
	for page := 1; ; page++ {
		var resp any
		if err := c.do(ctx, method, e.Path, q, e.headers(after, limit), body, &resp); err != nil {
			return nil, err
		}
		list, ok := lookup(resp, e.Items).([]any)
		if !ok && !emptyList(resp, e.Items) {
			return nil, fmt.Errorf("rest: %s: response has no item array at %q", e.Path, e.Items)
		}
		reached := false
		var last map[string]any
		for _, it := range list {
			item, ok := it.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("rest: %s: items must be objects", e.Path)
			}
			last = item
			id := scalar(lookup(item, idField))
			pos, err := parsePosition(lookup(item, posField), e.PositionFormat)
			if id == "" || err != nil {
				return nil, fmt.Errorf("rest: %s: item without a usable %s/%s: %v", e.Path, idField, posField, err)
			}
			if pos <= after {
				reached = true
				continue
			}
			payload, err := json.Marshal(item)
			if err != nil {
				return nil, err
			}
			if e.versioned() {
				id = connector.VersionID(id, pos)
			}
			events = append(events, connector.Event{ID: id, Position: pos, Name: event, Payload: payload})
		}
		if !desc {
			truncated = len(list) >= limit
			break
		}
		more, _ := lookup(resp, e.Pagination.More).(bool)
		if reached || !more || last == nil {
			break
		}
		if page >= maxPages {
			// Too many new items to reach the cursor in one poll: the
			// newest pages were read, but older unread items remain, so
			// report none rather than move the cursor past them.
			return nil, fmt.Errorf("rest: %s: more than %d pages of new items; raise pagination.maxPages or poll more often", e.Path, maxPages)
		}
		q.Set(e.Pagination.Param, scalar(lookup(last, or(e.Pagination.Cursor, idField))))
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Position < events[j].Position })
	if len(events) > limit {
		events, truncated = events[:limit], true
	}
	if truncated && e.PositionFormat != "" && e.PositionFormat != "number" {
		cut := len(events)
		for cut > 0 && events[cut-1].Position == events[len(events)-1].Position {
			cut--
		}
		switch {
		case cut > 0:
			events = events[:cut]
		case e.MaxItems > 0 && limit == e.MaxItems:
			// The API answers no more than this, so the next poll would
			// read the same page: moving past a timestamp it fills would
			// skip the rest of it, staying would never move.
			return nil, fmt.Errorf("rest: %s: all %d items of a full answer share one %s; the cursor cannot pass it without skipping items", e.Path, len(events), posField)
		}
	}
	return events, nil
}

// listRequest is the first list request for items after a position.
// headers returns the event's request headers, with {{position}},
// {{positionTime}} and {{limit}} filled in (Xero's If-Modified-Since).
func (e Event) headers(after int64, limit int) http.Header {
	if len(e.Headers) == 0 {
		return nil
	}
	tmp := e
	tmp.Params = e.Headers
	_, q, _ := tmp.listRequest(after, limit)
	h := http.Header{}
	for k := range e.Headers {
		h.Set(k, q.Get(k))
	}
	return h
}

func (e Event) listRequest(after int64, limit int) (string, url.Values, any) {
	fill := func(v string) string {
		return varRE.ReplaceAllStringFunc(v, func(m string) string {
			switch varRE.FindStringSubmatch(m)[1] {
			case "position":
				return formatPosition(after, e.PositionFormat)
			case "positionMillis":
				return strconv.FormatInt(after, 10)
			case "positionTime":
				return time.UnixMilli(after).UTC().Format("2006-01-02T15:04:05")
			case "positionTimeMillis":
				return time.UnixMilli(after).UTC().Format("2006-01-02T15:04:05.000")
			case "limit":
				return strconv.Itoa(limit)
			}
			return m
		})
	}
	q := url.Values{}
	for k, v := range e.Params {
		q.Set(k, fill(v))
	}
	method, body := http.MethodGet, any(nil)
	if e.Method == http.MethodPost {
		method, body = http.MethodPost, fillBody(e.Body, fill, limit)
		if body == nil {
			body = map[string]any{}
		}
	}
	return method, q, body
}

// fillBody copies a list request's body template with its variables set.
func fillBody(v any, fill func(string) string, limit int) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, child := range x {
			out[k] = fillBody(child, fill, limit)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			out[i] = fillBody(child, fill, limit)
		}
		return out
	case string:
		if m := varRE.FindStringSubmatch(x); m != nil && m[0] == x && m[1] == "limit" {
			return limit
		}
		return fill(x)
	}
	return v
}

func formatPosition(pos int64, format string) string {
	switch format {
	case "msjson":
		return "/Date(" + strconv.FormatInt(pos, 10) + "+0000)/"
	case "rfc3339":
		return time.UnixMilli(pos).UTC().Format("2006-01-02T15:04:05.000Z07:00")
	case "unix":
		return strconv.FormatInt(pos/1000, 10)
	}
	return strconv.FormatInt(pos, 10)
}

// parsePosition returns a number, or a time in Unix milliseconds.
var msjsonRE = regexp.MustCompile(`^/Date\((-?\d+)([+-]\d{4})?\)/$`)

func parsePosition(v any, format string) (int64, error) {
	s := scalar(v)
	switch format {
	case "msjson":
		m := msjsonRE.FindStringSubmatch(s)
		if m == nil {
			return 0, fmt.Errorf("%q is not a /Date(milliseconds)/ value", s)
		}
		return strconv.ParseInt(m[1], 10, 64) // milliseconds since the epoch, UTC; the offset is for display
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
// segment, and returns the values it used. A value quoted in the template
// ('{{key}}', as in OData key predicates) has its quotes doubled, so it
// cannot end the literal.
// emptyList reports an answer that leaves out an empty list: the list's
// key is absent but the object holding it is there, as QuickBooks Online
// answers a query that finds nothing ({"QueryResponse": {}}). A path that
// does not lead anywhere, and a list absent from the answer's top level
// (an error body, a misspelled items path), are still errors.
func emptyList(resp any, path string) bool {
	if lookup(resp, path) != nil {
		return false
	}
	// Only a list nested in an object the answer has: at the top level, an
	// absent list is an error body or a misspelled path, not "nothing".
	i := strings.LastIndexByte(path, '.')
	if i < 0 {
		return false
	}
	_, ok := lookup(resp, path[:i]).(map[string]any)
	return ok
}

// existing returns the first record the lookup lists, or nil.
func (c *Conn) existing(ctx context.Context, e *Existing, vars map[string]any) (map[string]any, error) {
	path, _, err := render(e.Path, vars)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	for k, tmpl := range e.Params {
		v, _, err := renderWith(tmpl, vars, func(s string) string { return s })
		if err != nil {
			return nil, err
		}
		q.Set(k, v)
	}
	var resp any
	if _, err := c.doH(ctx, http.MethodGet, path, q, nil, nil, &resp); err != nil {
		return nil, classify(err)
	}
	list, ok := lookup(resp, e.Items).([]any)
	if !ok && !emptyList(resp, e.Items) {
		return nil, fmt.Errorf("rest: the lookup for an existing record answered without a list at %q", e.Items)
	}
	if len(list) == 0 {
		return nil, nil
	}
	rec, ok := list[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rest: the lookup for an existing record listed a non-object")
	}
	return rec, nil
}

func render(tmpl string, vars map[string]any) (string, map[string]any, error) {
	return renderWith(tmpl, vars, url.PathEscape)
}

// renderWith fills a template's {{field}}s, each escaped with escape.
func renderWith(tmpl string, vars map[string]any, escape func(string) string) (string, map[string]any, error) {
	used := map[string]any{}
	var missing []string
	var b strings.Builder
	last := 0
	for _, loc := range varRE.FindAllStringSubmatchIndex(tmpl, -1) {
		b.WriteString(tmpl[last:loc[0]])
		last = loc[1]
		name := tmpl[loc[2]:loc[3]]
		v := lookup(vars, name)
		s := scalar(v)
		if s == "" || s == "." || s == ".." {
			// Escaping leaves dot segments as they are, and servers resolve
			// them: an ID of ".." would address the parent resource.
			missing = append(missing, name)
			continue
		}
		used[name] = v
		// Inside a quoted literal ('X-{{key}}' as well as '{{key}}'), a quote
		// in the value is doubled so it cannot end the literal.
		if strings.Count(tmpl[:loc[0]], "'")%2 == 1 {
			s = strings.ReplaceAll(s, "'", "''")
		}
		b.WriteString(escape(s))
	}
	b.WriteString(tmpl[last:])
	if len(missing) > 0 {
		return "", nil, fmt.Errorf("%w: path %s needs %s (not empty, \".\" or \"..\")", writeguard.ErrInvalid, tmpl, strings.Join(missing, ", "))
	}
	return b.String(), used, nil
}

// headerOf turns configured headers into request headers.
func headerOf(h map[string]string) http.Header {
	if len(h) == 0 {
		return nil
	}
	out := http.Header{}
	for k, v := range h {
		out.Set(k, v)
	}
	return out
}

// idFrom reads a record's ID from the URL a write answered with:
// .../salesOrder/123 or .../salesorders(3f2c...).
func idFrom(u string) string {
	u = strings.TrimRight(u, "/")
	if strings.HasSuffix(u, ")") {
		if i := strings.LastIndex(u, "("); i >= 0 {
			return strings.Trim(u[i+1:len(u)-1], "'")
		}
	}
	return u[strings.LastIndex(u, "/")+1:]
}

// values maps a write's API fields (dotted for nested ones, such as
// metadata.erp_payment_id) to their values from the payload. Without
// Fields, the payload is sent as it is.
func (op Operation) values(doc map[string]any) (map[string]any, error) {
	if len(op.Fields) == 0 {
		if len(op.Constants) > 0 {
			return map[string]any{}, nil // the payload only fills the path
		}
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

// request builds the request body from API field values.
func (op Operation) request(values map[string]any) any {
	if op.BodyFormat == "none" {
		return nil
	}
	if len(op.Constants) > 0 && !op.Restore {
		merged := make(map[string]any, len(values)+len(op.Constants))
		for k, v := range values {
			merged[k] = v
		}
		// A copy: nest writes into the maps it builds the body from, and the
		// configuration's own constants must stay as configured.
		raw, _ := json.Marshal(op.Constants)
		var fixed map[string]any
		_ = json.Unmarshal(raw, &fixed)
		for k, v := range fixed {
			merged[k] = v
		}
		values = merged
	}
	var body any = values
	if len(op.Fields) > 0 || len(op.Constants) > 0 || op.Restore {
		body = nest(values)
	}
	if op.Wrap != "" {
		body = map[string]any{op.Wrap: body}
	}
	if op.BodyFormat == "form" {
		return formBody{body}
	}
	if op.NullValue != nil {
		body = replaceNull(body, *op.NullValue)
	}
	return body
}

// replaceNull replaces nulls in objects and arrays with a value.
func replaceNull(v any, with string) any {
	switch x := v.(type) {
	case nil:
		return with
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, child := range x {
			out[k] = replaceNull(child, with)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			out[i] = replaceNull(child, with)
		}
		return out
	}
	return v
}

// nest turns dotted keys into nested objects; `\.` is a literal dot.
func nest(flat map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range flat {
		parts := strings.Split(strings.ReplaceAll(k, `\.`, "\x00"), ".")
		for i := range parts {
			parts[i] = strings.ReplaceAll(parts[i], "\x00", ".")
		}
		m := out
		for _, p := range parts[:len(parts)-1] {
			child, ok := m[p].(map[string]any)
			if !ok {
				child = map[string]any{}
				m[p] = child
			}
			m = child
		}
		m[parts[len(parts)-1]] = v
	}
	return out
}

// formBody marks a body to be sent form-encoded.
type formBody struct{ v any }

// encodeForm encodes nested objects and arrays the way Stripe and Rails
// read them: metadata[key]=v, items[0][price]=v; null is sent as "".
func encodeForm(v any) url.Values {
	out := url.Values{}
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, child := range x {
				key := k
				if prefix != "" {
					key = prefix + "[" + k + "]"
				}
				walk(key, child)
			}
		case []any:
			for i, child := range x {
				walk(fmt.Sprintf("%s[%d]", prefix, i), child)
			}
		case nil:
			out.Set(prefix, "")
		default:
			out.Set(prefix, scalar(x))
		}
	}
	walk("", v)
	return out
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
// the values the write would set. A generic API cannot dry-run other
// writes; a create is previewed as the request it would send, which also
// checks the payload has every field the API needs.
func (c *Conn) Simulate(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	if op.Capture == nil {
		if op.Restore || op.Method == http.MethodDelete || op.Method == http.MethodGet {
			return nil, writeguard.ErrSimulationUnsupported
		}
		doc, err := decodePayload(payload)
		if err != nil {
			return nil, err
		}
		vals, err := op.values(doc)
		if err != nil {
			return nil, err
		}
		body := op.request(vals)
		if f, ok := body.(formBody); ok {
			body = f.v
		}
		return json.Marshal(map[string]any{"mode": "request", "request": op.Method + " " + op.Path, "body": body})
	}
	doc, err := decodePayload(payload)
	if err != nil {
		return nil, err
	}
	vals, err := op.values(doc)
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
	return json.Marshal(map[string]any{"mode": "preview", "request": op.Method + " " + path, "current": previous(rec, vals), "proposed": vals})
}

// previous returns the record's current value of each field.
func previous(rec, values map[string]any) map[string]any {
	prev := map[string]any{}
	for k := range values {
		prev[k] = lookup(rec, k)
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
	var vals, captured map[string]any
	switch {
	case op.Restore:
		var r UpdateResult
		if err := json.Unmarshal(payload, &r); err != nil || r.Params == nil || r.Previous == nil {
			return nil, fmt.Errorf("%w: restore needs a captured update's result", writeguard.ErrInvalid)
		}
		vars, body = r.Params, op.request(r.Previous)
	case op.Method != http.MethodDelete:
		if vals, err = op.values(doc); err != nil {
			return nil, err
		}
		sent := vals
		if op.KeyField != "" {
			if key == "" {
				return nil, fmt.Errorf("%w: %s stores the idempotency key in %s, and the write has none", writeguard.ErrInvalid, name, op.KeyField)
			}
			sent = make(map[string]any, len(vals)+1)
			for k, v := range vals {
				sent[k] = v
			}
			sent[op.KeyField] = key
		}
		body = op.request(sent)
		if op.Capture != nil {
			rec, err := c.current(ctx, op, doc)
			if err != nil {
				return nil, err
			}
			captured = previous(rec, vals)
		}
	}
	if key != "" && (strings.Contains(op.Path, "idempotencyKey") || op.Existing != nil) {
		withKey := make(map[string]any, len(vars)+1)
		for k, v := range vars {
			withKey[k] = v
		}
		withKey["idempotencyKey"] = key
		vars = withKey
	}
	path, params, err := render(op.Path, vars)
	if err != nil {
		return nil, err
	}
	if op.Existing != nil {
		found, err := c.existing(ctx, op.Existing, vars)
		if err != nil {
			return nil, err
		}
		if found != nil {
			found["existing"] = true
			return json.Marshal(found)
		}
	}
	header := headerOf(op.Headers)
	if op.IdempotencyHeader != "" && key != "" {
		if header == nil {
			header = http.Header{}
		}
		header.Set(op.IdempotencyHeader, key)
	}
	var resp any
	respHeader, err := c.doH(ctx, op.Method, path, nil, header, body, &resp)
	if err != nil {
		return nil, classify(err)
	}
	result := lookup(resp, op.Result)
	if op.IDFrom != "" && result == nil {
		u := respHeader.Get(op.IDFrom)
		if u == "" {
			return nil, fmt.Errorf("rest: %s answered without a body or a %s header", name, op.IDFrom)
		}
		result = map[string]any{"id": idFrom(u), "url": u}
	}
	switch {
	case captured != nil:
		return json.Marshal(UpdateResult{Params: params, Previous: captured, Applied: vals, Response: result})
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
		if got := lookup(rec, k); !sameJSON(got, want) && !(op.BodyFormat == "form" && scalar(got) == scalar(want)) {
			return fmt.Errorf("rest: %s reads back %s = %v, want %v", name, k, got, want)
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
	err = c.do(ctx, http.MethodGet, path, nil, headerOf(op.Headers), nil, &resp)
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
		switch x := v.(type) {
		case map[string]any:
			v = x[part]
		case []any:
			// A number indexes a list: Invoices.0 is the first invoice of
			// an API that always answers a list (Xero).
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(x) {
				return nil
			}
			v = x[i]
		default:
			return nil
		}
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
