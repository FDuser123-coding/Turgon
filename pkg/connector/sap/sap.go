// Package sap is Turgon's connector for SAP S/4HANA (Cloud and on-premise)
// through its released OData v2 APIs, such as API_SALES_ORDER_SRV and
// API_BUSINESS_PARTNER (architecture §13: SAP only through sanctioned
// interfaces, never its database).
//
// Like the REST connector it is configured by the Connection:
//
//   - Events are polled on a change timestamp (LastChangeDateTime), with
//     $filter and $orderby, in position order.
//   - Reads fetch one entity by key and rename its properties.
//   - A create sends one deep insert (header and items). It first looks
//     for an entity already carrying the write's idempotency key in a
//     reference property (PurchaseOrderByCustomer), so a retry after a
//     lost response finds the order instead of creating a second one.
//   - A create can be dry-run by the API's simulation service
//     (API_SALES_ORDER_SIMULATION_SRV), which prices and checks the order
//     without saving it: the preview people approve.
//   - A delete undoes a create, as a saga's compensation.
//
// Writes fetch and send SAP's CSRF token, as every modifying OData request
// requires.
package sap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Name is the connector name this package implements.
const Name = "sap-odata"

// Config is the connection-specific configuration.
type Config struct {
	// BaseURL is the system's address, e.g. https://my300000-api.s4hana.cloud.sap.
	BaseURL string `json:"baseURL"`
	// ServicePath prefixes service names (default /sap/opu/odata/sap).
	ServicePath string `json:"servicePath,omitempty"`
	// Client is the SAP client (sap-client), for on-premise systems.
	Client string `json:"client,omitempty"`
	// Auth is basic (a communication user, "user:password") or oauth2
	// (client credentials); the credential is the connection's secret.
	Auth       rest.Auth            `json:"auth"`
	Events     map[string]Event     `json:"events,omitempty"`
	Operations map[string]Operation `json:"operations,omitempty"`
}

// Event defines an event as the entities changed after the cursor.
type Event struct {
	Service   string `json:"service"`
	EntitySet string `json:"entitySet"`
	// Key is the entity's key property; its value is the event ID.
	Key string `json:"key"`
	// Changed is the Edm.DateTimeOffset property ordering changes
	// (default LastChangeDateTime).
	Changed string `json:"changed,omitempty"`
	// Filter narrows the entities, in OData $filter syntax.
	Filter string   `json:"filter,omitempty"`
	Select []string `json:"select,omitempty"`
	Expand []string `json:"expand,omitempty"`
}

// Operation binds a manifest operation to an entity set.
type Operation struct {
	// Action is create, delete (undo a create, given its result) or get.
	Action    string `json:"action"`
	Service   string `json:"service"`
	EntitySet string `json:"entitySet"`
	// Key is the entity's key property.
	Key string `json:"key"`
	// Fields maps SAP properties to payload fields (create; dotted paths
	// allowed), or to the names a get returns them under.
	Fields map[string]string `json:"fields,omitempty"`
	// Constants are fixed property values, e.g. SalesOrderType: OR.
	Constants map[string]any `json:"constants,omitempty"`
	// Types converts values the payload holds as JSON into OData v2's
	// representation: date (Edm.DateTime from an ISO date), decimal
	// (Edm.Decimal, sent as a string) or string.
	Types map[string]string `json:"types,omitempty"`
	// Items is a deep insert's item list.
	Items *Items `json:"items,omitempty"`
	// Reference is the property that carries the write's idempotency key
	// (create), looked up before creating.
	Reference string `json:"reference,omitempty"`
	// Simulate is the service and entity set that dry-run a create.
	Simulate *Simulation `json:"simulate,omitempty"`
}

// Items builds a navigation property's entities from a payload array.
type Items struct {
	Navigation string            `json:"navigation"`
	From       string            `json:"from"`
	Fields     map[string]string `json:"fields"`
	Constants  map[string]any    `json:"constants,omitempty"`
	Types      map[string]string `json:"types,omitempty"`
}

// Simulation names a simulation service's entity set.
type Simulation struct {
	Service   string `json:"service"`
	EntitySet string `json:"entitySet"`
}

// CreateResult is what a create returns and a delete consumes.
type CreateResult struct {
	Service   string `json:"service"`
	EntitySet string `json:"entitySet"`
	ID        string `json:"id"`
	// Reference is the idempotency key stored in the reference property.
	Reference string `json:"reference,omitempty"`
	// Existing is set when the entity was found, not created: an earlier
	// attempt created it.
	Existing bool           `json:"existing,omitempty"`
	Record   map[string]any `json:"record,omitempty"`
}

var (
	identRE   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,79}$`)
	pathRE    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	keyRE     = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,80}$`)
	serviceRE = regexp.MustCompile(`^[A-Za-z0-9_]+(;v=\d{4})?$`)
)

func checkTypes(where string, types map[string]string) error {
	for p, t := range types {
		if !identRE.MatchString(p) {
			return fmt.Errorf("%s: invalid property %q", where, p)
		}
		switch t {
		case "date", "decimal", "string":
		default:
			return fmt.Errorf("%s: type of %s must be date, decimal or string, got %q", where, p, t)
		}
	}
	return nil
}

func (c Config) validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("baseURL must be an http(s) URL")
	}
	if c.Client != "" && !regexp.MustCompile(`^\d{3}$`).MatchString(c.Client) {
		return fmt.Errorf("client must be three digits, got %q", c.Client)
	}
	for name, e := range c.Events {
		if !serviceRE.MatchString(e.Service) || !identRE.MatchString(e.EntitySet) || !identRE.MatchString(e.Key) {
			return fmt.Errorf("event %s: service, entitySet and key are required", name)
		}
		if e.Changed != "" && !identRE.MatchString(e.Changed) {
			return fmt.Errorf("event %s: invalid changed property %q", name, e.Changed)
		}
		for _, p := range append(append([]string{}, e.Select...), e.Expand...) {
			if !pathRE.MatchString(strings.ReplaceAll(p, "/", ".")) {
				return fmt.Errorf("event %s: invalid property %q", name, p)
			}
		}
		// The filter is ANDed with the cursor condition inside parentheses;
		// a filter cannot close them early or add query parameters.
		if strings.ContainsAny(e.Filter, "&#") || !oneExpression(e.Filter) {
			return fmt.Errorf("event %s: filter must be one OData expression", name)
		}
	}
	for name, op := range c.Operations {
		where := "operation " + name
		if !serviceRE.MatchString(op.Service) || !identRE.MatchString(op.EntitySet) || !identRE.MatchString(op.Key) {
			return fmt.Errorf("%s: service, entitySet and key are required", where)
		}
		switch op.Action {
		case "create":
			if len(op.Fields) == 0 {
				return fmt.Errorf("%s: create needs fields", where)
			}
			for p, f := range op.Fields {
				if !identRE.MatchString(p) || !pathRE.MatchString(f) {
					return fmt.Errorf("%s: invalid field mapping %q: %q", where, p, f)
				}
			}
			for p := range op.Constants {
				if !identRE.MatchString(p) {
					return fmt.Errorf("%s: invalid constant %q", where, p)
				}
			}
			if err := checkTypes(where, op.Types); err != nil {
				return err
			}
			if op.Reference != "" && !identRE.MatchString(op.Reference) {
				return fmt.Errorf("%s: invalid reference property %q", where, op.Reference)
			}
			if it := op.Items; it != nil {
				if !identRE.MatchString(it.Navigation) || !pathRE.MatchString(it.From) || len(it.Fields) == 0 {
					return fmt.Errorf("%s: items need navigation, from and fields", where)
				}
				for p, f := range it.Fields {
					if !identRE.MatchString(p) || !pathRE.MatchString(f) {
						return fmt.Errorf("%s: invalid item field mapping %q: %q", where, p, f)
					}
				}
				if err := checkTypes(where+" items", it.Types); err != nil {
					return err
				}
			}
			if s := op.Simulate; s != nil && (!serviceRE.MatchString(s.Service) || !identRE.MatchString(s.EntitySet)) {
				return fmt.Errorf("%s: simulate needs service and entitySet", where)
			}
		case "delete":
		case "get":
			if len(op.Fields) == 0 {
				return fmt.Errorf("%s: get needs fields", where)
			}
			for p, as := range op.Fields {
				if !identRE.MatchString(p) || !identRE.MatchString(as) {
					return fmt.Errorf("%s: invalid field mapping %q: %q", where, p, as)
				}
			}
		default:
			return fmt.Errorf("%s: action must be create, delete or get, got %q", where, op.Action)
		}
	}
	return nil
}

// oneExpression reports whether parentheses outside string literals
// balance without ever closing more than were opened.
func oneExpression(f string) bool {
	depth, quoted := 0, false
	for _, r := range f {
		switch {
		case r == '\'':
			quoted = !quoted // '' inside a literal toggles twice
		case quoted:
		case r == '(':
			depth++
		case r == ')':
			if depth--; depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && !quoted
}

// Factory builds a connector from its compiled configuration.
func Factory(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	var c Config
	if len(cfg.Config) > 0 {
		if err := json.Unmarshal(cfg.Config, &c); err != nil {
			return nil, fmt.Errorf("sap %s: config: %w", cfg.Endpoint, err)
		}
	}
	secret, err := secrets.Resolve(ctx, cfg.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("sap %s: %w", cfg.Endpoint, err)
	}
	conn, err := New(c, secret, &http.Client{Timeout: 60 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("sap %s: %w", cfg.Endpoint, err)
	}
	return conn, nil
}

// Conn is a connector instance.
type Conn struct {
	cfg  Config
	auth *rest.Authenticator
	http *http.Client
	// PageSize is the $top of each page read. Default 100.
	PageSize int
	// MaxPages bounds one poll. Default 20.
	MaxPages int

	mu   sync.Mutex
	csrf string
}

var (
	_ connector.Instance   = (*Conn)(nil)
	_ connector.Source     = (*Conn)(nil)
	_ writeguard.Confirmer = (*Conn)(nil)
	_ writeguard.Reader    = (*Conn)(nil)
)

// New returns a connector. The client gets a cookie jar of its own: SAP
// binds the CSRF token to the session cookie it sets.
func New(cfg Config, secret string, client *http.Client) (*Conn, error) {
	if cfg.ServicePath == "" {
		cfg.ServicePath = "/sap/opu/odata/sap"
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.Auth.Type == "" {
		cfg.Auth.Type = "basic"
	}
	for name, e := range cfg.Events {
		if e.Changed == "" {
			e.Changed = "LastChangeDateTime"
			cfg.Events[name] = e
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	auth, err := rest.NewAuthenticator(cfg.Auth, secret, client)
	if err != nil {
		return nil, err
	}
	hc := *client
	hc.Jar, _ = cookiejar.New(nil)
	return &Conn{cfg: cfg, auth: auth, http: &hc, PageSize: 100, MaxPages: 20}, nil
}

func (c *Conn) Close() {}

// APIError is an error response from an OData service.
type APIError struct {
	Method, URL string
	Status      int
	Code        string
	Message     string
}

func (e *APIError) Error() string {
	msg := e.Message
	if e.Code != "" {
		msg = e.Code + ": " + msg
	}
	return fmt.Sprintf("sap %s %s: %d %s", e.Method, e.URL, e.Status, msg)
}

// permanent reports errors that retrying cannot fix. Throttling, lock
// contention (409, 412, 423) and server errors are retryable.
func (e *APIError) permanent() bool {
	switch e.Status {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusUnprocessableEntity:
		return true
	}
	return false
}

// url returns an entity set's (or a service's, with set "") address.
func (c *Conn) url(service, set string, q url.Values) string {
	u := c.cfg.BaseURL + c.cfg.ServicePath + "/" + service + "/" + set
	if q == nil {
		q = url.Values{}
	}
	q.Set("$format", "json")
	if c.cfg.Client != "" {
		q.Set("sap-client", c.cfg.Client)
	}
	return u + "?" + strings.ReplaceAll(q.Encode(), "+", "%20")
}

// entityPath addresses one entity by a string key.
func entityPath(set, id string) (string, error) {
	if !keyRE.MatchString(id) {
		return "", fmt.Errorf("%w: %q is not an SAP key", writeguard.ErrInvalid, id)
	}
	return set + "('" + id + "')", nil
}

// literal quotes a string for $filter.
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

type response struct {
	status int
	header http.Header
	body   []byte
}

// do sends a request. Modifying requests carry the CSRF token, fetched
// once and again when SAP says it expired; a rejected OAuth token is
// fetched again once.
func (c *Conn) do(ctx context.Context, method, target string, body any, header http.Header) (response, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return response{}, err
		}
	}
	modifying := method != http.MethodGet && method != http.MethodHead
	retriedAuth, retriedCSRF := false, false
	for {
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
		if err != nil {
			return response{}, err
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range header {
			req.Header[k] = v
		}
		if modifying {
			token, err := c.token(ctx, target)
			if err != nil {
				return response{}, err
			}
			req.Header.Set("X-CSRF-Token", token)
		}
		if err := c.auth.Apply(ctx, req); err != nil {
			return response{}, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return response{}, fmt.Errorf("sap %s %s: %w", method, redact(target), err)
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		r := response{status: resp.StatusCode, header: resp.Header, body: data}
		if resp.StatusCode == http.StatusUnauthorized && c.auth.Refreshable() && !retriedAuth {
			retriedAuth = true
			c.auth.Invalidate()
			continue
		}
		if resp.StatusCode == http.StatusForbidden && modifying && strings.EqualFold(resp.Header.Get("X-CSRF-Token"), "Required") && !retriedCSRF {
			retriedCSRF = true
			c.dropToken()
			continue
		}
		if resp.StatusCode >= 300 {
			return r, apiError(method, target, r)
		}
		return r, nil
	}
}

func apiError(method, target string, r response) error {
	e := &APIError{Method: method, URL: redact(target), Status: r.status, Message: strings.TrimSpace(string(r.body))}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message struct {
				Value string `json:"value"`
			} `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(r.body, &body) == nil && body.Error.Message.Value != "" {
		e.Code, e.Message = body.Error.Code, body.Error.Message.Value
	}
	if len(e.Message) > 500 {
		e.Message = e.Message[:500] + "…"
	}
	if e.permanent() {
		return fmt.Errorf("%w: %w", writeguard.ErrInvalid, e)
	}
	return e
}

// redact drops the query, which can quote record values ($filter).
func redact(target string) string {
	if i := strings.IndexByte(target, '?'); i >= 0 {
		return target[:i]
	}
	return target
}

func (c *Conn) token(ctx context.Context, target string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.csrf != "" {
		return c.csrf, nil
	}
	// Any GET within the service returns a token for the session; the
	// service document is the cheapest.
	u, err := url.Parse(target)
	if err != nil {
		return "", err
	}
	root := u.Scheme + "://" + u.Host + u.Path[:strings.LastIndex(u.Path, "/")+1]
	if c.cfg.Client != "" {
		root += "?sap-client=" + c.cfg.Client
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, root, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-CSRF-Token", "Fetch")
	req.Header.Set("Accept", "application/json")
	if err := c.auth.Apply(ctx, req); err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("sap csrf token: %w", err)
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", apiError(http.MethodGet, root, response{status: resp.StatusCode, body: data})
	}
	c.csrf = resp.Header.Get("X-CSRF-Token")
	if c.csrf == "" {
		return "", errors.New("sap csrf token: the service returned none")
	}
	return c.csrf, nil
}

func (c *Conn) dropToken() {
	c.mu.Lock()
	c.csrf = ""
	c.mu.Unlock()
}

// entity decodes {"d": {...}}.
func entity(r response) (map[string]any, error) {
	var body struct {
		D map[string]any `json:"d"`
	}
	dec := json.NewDecoder(bytes.NewReader(r.body))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil || body.D == nil {
		return nil, fmt.Errorf("sap: response is not an OData v2 entity")
	}
	return body.D, nil
}

// collection decodes {"d": {"results": [...]}} (or {"d": [...]}).
func collection(r response) ([]map[string]any, error) {
	var body struct {
		D json.RawMessage `json:"d"`
	}
	if err := json.Unmarshal(r.body, &body); err != nil || len(body.D) == 0 {
		return nil, fmt.Errorf("sap: response is not an OData v2 collection")
	}
	var list []map[string]any
	dec := json.NewDecoder(bytes.NewReader(body.D))
	dec.UseNumber()
	if bytes.HasPrefix(bytes.TrimSpace(body.D), []byte("[")) {
		err := dec.Decode(&list)
		return list, err
	}
	var wrapped struct {
		Results []map[string]any `json:"results"`
	}
	err := dec.Decode(&wrapped)
	return wrapped.Results, err
}

var dateRE = regexp.MustCompile(`^/Date\((-?\d+)([+-]\d{4})?\)/$`)

// parseDate reads an OData v2 date, or an ISO 8601 one.
func parseDate(v any) (time.Time, bool) {
	s, _ := v.(string)
	if m := dateRE.FindStringSubmatch(s); m != nil {
		ms, err := strconv.ParseInt(m[1], 10, 64)
		return time.UnixMilli(ms).UTC(), err == nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// clean turns an OData v2 record into plain JSON: no __metadata, no
// deferred navigation properties, expanded lists as arrays, and dates as
// ISO 8601 (a date alone when it is midnight UTC, as Edm.DateTime dates are).
func clean(v any) any {
	switch x := v.(type) {
	case map[string]any:
		if res, ok := x["results"].([]any); ok && len(x) <= 2 {
			return clean(res)
		}
		out := make(map[string]any, len(x))
		for k, child := range x {
			if k == "__metadata" {
				continue
			}
			if m, ok := child.(map[string]any); ok {
				if _, deferred := m["__deferred"]; deferred {
					continue
				}
			}
			out[k] = clean(child)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			out[i] = clean(child)
		}
		return out
	case string:
		if dateRE.MatchString(x) {
			t, _ := parseDate(x)
			if t.Equal(t.Truncate(24 * time.Hour)) {
				return t.Format("2006-01-02")
			}
			return t.Format(time.RFC3339)
		}
	}
	return v
}

// Poll returns entities changed after the position (Unix milliseconds),
// in change order. A poll never ends inside a group of entities sharing a
// timestamp, so the strict "greater than" cursor cannot skip any.
func (c *Conn) Poll(ctx context.Context, name string, after int64, limit int) ([]connector.Event, error) {
	e, ok := c.cfg.Events[name]
	if !ok {
		return nil, fmt.Errorf("sap: event %q is not configured on this connection", name)
	}
	if limit <= 0 || limit > c.PageSize {
		limit = c.PageSize
	}
	filter := fmt.Sprintf("%s gt datetimeoffset'%s'", e.Changed, time.UnixMilli(after).UTC().Format("2006-01-02T15:04:05.000Z"))
	if e.Filter != "" {
		filter = "(" + e.Filter + ") and " + filter
	}
	q := url.Values{"$filter": {filter}, "$orderby": {e.Changed + " asc," + e.Key + " asc"}, "$top": {strconv.Itoa(limit)}}
	if len(e.Select) > 0 {
		sel := append([]string{}, e.Select...)
		for _, must := range []string{e.Key, e.Changed} {
			if !contains(sel, must) {
				sel = append(sel, must)
			}
		}
		q.Set("$select", strings.Join(sel, ","))
	}
	if len(e.Expand) > 0 {
		q.Set("$expand", strings.Join(e.Expand, ","))
	}
	var events []connector.Event
	for page := 0; page < c.MaxPages; page++ {
		q.Set("$skip", strconv.Itoa(page*limit))
		r, err := c.do(ctx, http.MethodGet, c.url(e.Service, e.EntitySet, q), nil, nil)
		if err != nil {
			return nil, err
		}
		recs, err := collection(r)
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			ev, err := toEvent(name, e, rec)
			if err != nil {
				return nil, err
			}
			events = append(events, ev)
		}
		if len(recs) < limit {
			return events, nil
		}
		// A full page: more may share the last timestamp. End the batch
		// before that group, unless the batch is nothing but that group.
		if cut := boundary(events); cut > 0 {
			return events[:cut], nil
		}
	}
	return nil, fmt.Errorf("sap: more than %d entities of %s share one %s", c.MaxPages*limit, e.EntitySet, e.Changed)
}

func boundary(events []connector.Event) int {
	cut := len(events)
	for cut > 0 && events[cut-1].Position == events[len(events)-1].Position {
		cut--
	}
	return cut
}

func toEvent(name string, e Event, rec map[string]any) (connector.Event, error) {
	id := fmt.Sprint(rec[e.Key])
	t, ok := parseDate(rec[e.Changed])
	if !ok || rec[e.Key] == nil {
		return connector.Event{}, fmt.Errorf("sap: %s entity %q has no valid %s", e.EntitySet, id, e.Changed)
	}
	payload, err := json.Marshal(clean(rec))
	if err != nil {
		return connector.Event{}, err
	}
	return connector.Event{ID: id, Position: t.UnixMilli(), Name: name, Payload: payload}, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func (c *Conn) operation(name, action string) (Operation, error) {
	op, ok := c.cfg.Operations[name]
	if !ok {
		return Operation{}, fmt.Errorf("sap: operation %q is not configured on this connection", name)
	}
	if action != "" && op.Action != action {
		return Operation{}, fmt.Errorf("sap: operation %q is a %s, not a %s", name, op.Action, action)
	}
	return op, nil
}

// lookup follows a dotted path through a document.
func lookup(doc map[string]any, path string) any {
	var v any = doc
	for _, part := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[part]
	}
	return v
}

func convert(prop string, v any, typ string) (any, error) {
	if v == nil || typ == "" {
		return v, nil
	}
	switch typ {
	case "date":
		t, ok := parseDate(v)
		if !ok {
			return nil, fmt.Errorf("%w: %s must be a date, got %v", writeguard.ErrInvalid, prop, v)
		}
		return fmt.Sprintf("/Date(%d)/", t.UnixMilli()), nil
	case "decimal":
		switch n := v.(type) {
		case json.Number:
			return n.String(), nil
		case float64:
			return strconv.FormatFloat(n, 'f', -1, 64), nil
		case string:
			if _, err := strconv.ParseFloat(n, 64); err == nil {
				return n, nil
			}
		}
		return nil, fmt.Errorf("%w: %s must be a number, got %v", writeguard.ErrInvalid, prop, v)
	case "string":
		return fmt.Sprint(v), nil
	}
	return v, nil
}

func build(doc map[string]any, fields map[string]string, constants map[string]any, types map[string]string) (map[string]any, error) {
	out := make(map[string]any, len(fields)+len(constants))
	for p, v := range constants {
		out[p] = v
	}
	for p, f := range fields {
		v, err := convert(p, lookup(doc, f), types[p])
		if err != nil {
			return nil, err
		}
		if v != nil {
			out[p] = v
		}
	}
	return out, nil
}

// body builds a create's request: header properties and deep-insert items.
func body(op Operation, payload json.RawMessage) (map[string]any, error) {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%w: payload must be a JSON object", writeguard.ErrInvalid)
	}
	out, err := build(doc, op.Fields, op.Constants, op.Types)
	if err != nil {
		return nil, err
	}
	if it := op.Items; it != nil {
		list, _ := lookup(doc, it.From).([]any)
		if len(list) == 0 {
			return nil, fmt.Errorf("%w: %s has no items", writeguard.ErrInvalid, it.From)
		}
		items := make([]any, 0, len(list))
		for i, x := range list {
			m, ok := x.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: %s[%d] is not an object", writeguard.ErrInvalid, it.From, i)
			}
			item, err := build(m, it.Fields, it.Constants, it.Types)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		out[it.Navigation] = items
	}
	return out, nil
}

// Simulate dry-runs a create with the API's simulation service, which
// determines prices, partners and checks without saving anything.
func (c *Conn) Simulate(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name, "")
	if err != nil {
		return nil, err
	}
	if op.Action != "create" || op.Simulate == nil {
		return nil, writeguard.ErrSimulationUnsupported
	}
	req, err := body(op, payload)
	if err != nil {
		return nil, err
	}
	r, err := c.do(ctx, http.MethodPost, c.url(op.Simulate.Service, op.Simulate.EntitySet, nil), req, nil)
	if err != nil {
		return nil, err
	}
	rec, err := entity(r)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"mode": "testrun", "entitySet": op.EntitySet, "record": clean(rec)})
}

// existing finds the entity an earlier attempt created with this key.
func (c *Conn) existing(ctx context.Context, op Operation, key string) (map[string]any, error) {
	q := url.Values{"$filter": {op.Reference + " eq " + literal(key)}, "$top": {"2"}}
	r, err := c.do(ctx, http.MethodGet, c.url(op.Service, op.EntitySet, q), nil, nil)
	if err != nil {
		return nil, err
	}
	recs, err := collection(r)
	if err != nil {
		return nil, err
	}
	switch len(recs) {
	case 0:
		return nil, nil
	case 1:
		return recs[0], nil
	}
	return nil, fmt.Errorf("%w: more than one %s has %s %s; the reference must be unique", writeguard.ErrInvalid, op.EntitySet, op.Reference, key)
}

// Commit creates the entity, or deletes the one a create's result names.
func (c *Conn) Commit(ctx context.Context, name, key string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name, "")
	if err != nil {
		return nil, err
	}
	switch op.Action {
	case "create":
		return c.create(ctx, op, key, payload)
	case "delete":
		return c.delete(ctx, op, payload)
	}
	return nil, fmt.Errorf("sap: operation %q is a %s, not a write", name, op.Action)
}

func (c *Conn) create(ctx context.Context, op Operation, key string, payload json.RawMessage) (json.RawMessage, error) {
	req, err := body(op, payload)
	if err != nil {
		return nil, err
	}
	res := CreateResult{Service: op.Service, EntitySet: op.EntitySet}
	if op.Reference != "" {
		if key == "" || len(key) > 35 {
			return nil, fmt.Errorf("%w: the idempotency key must be 1 to 35 characters to fit %s, got %q", writeguard.ErrInvalid, op.Reference, key)
		}
		req[op.Reference], res.Reference = key, key
		found, err := c.existing(ctx, op, key)
		if err != nil {
			return nil, err
		}
		if found != nil {
			res.ID, res.Existing = fmt.Sprint(found[op.Key]), true
			res.Record, _ = clean(found).(map[string]any)
			return json.Marshal(res)
		}
	}
	r, err := c.do(ctx, http.MethodPost, c.url(op.Service, op.EntitySet, nil), req, nil)
	if err != nil {
		return nil, err
	}
	rec, err := entity(r)
	if err != nil {
		return nil, err
	}
	res.ID = fmt.Sprint(rec[op.Key])
	if rec[op.Key] == nil {
		return nil, fmt.Errorf("sap: the created %s has no %s", op.EntitySet, op.Key)
	}
	res.Record, _ = clean(rec).(map[string]any)
	return json.Marshal(res)
}

func (c *Conn) delete(ctx context.Context, op Operation, payload json.RawMessage) (json.RawMessage, error) {
	var prior CreateResult
	if err := json.Unmarshal(payload, &prior); err != nil || prior.ID == "" {
		return nil, fmt.Errorf("%w: delete needs the result of a create", writeguard.ErrInvalid)
	}
	if prior.EntitySet != op.EntitySet || prior.Service != op.Service {
		return nil, fmt.Errorf("%w: cannot delete a %s with an operation on %s", writeguard.ErrInvalid, prior.EntitySet, op.EntitySet)
	}
	path, err := entityPath(op.EntitySet, prior.ID)
	if err != nil {
		return nil, err
	}
	// Deleting needs the entity's current ETag.
	target := c.url(op.Service, path, nil)
	r, err := c.do(ctx, http.MethodGet, target, nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.Status == http.StatusNotFound {
		return json.Marshal(map[string]any{"entitySet": op.EntitySet, "id": prior.ID, "deleted": true, "alreadyGone": true})
	}
	if err != nil {
		return nil, err
	}
	etag := r.header.Get("ETag")
	if etag == "" {
		if rec, err := entity(r); err == nil {
			if md, ok := rec["__metadata"].(map[string]any); ok {
				etag, _ = md["etag"].(string)
			}
		}
	}
	if etag == "" {
		etag = "*"
	}
	if _, err := c.do(ctx, http.MethodDelete, target, nil, http.Header{"If-Match": {etag}}); err != nil {
		if errors.As(err, &api) && api.Status == http.StatusNotFound {
			return json.Marshal(map[string]any{"entitySet": op.EntitySet, "id": prior.ID, "deleted": true, "alreadyGone": true})
		}
		return nil, err
	}
	return json.Marshal(map[string]any{"entitySet": op.EntitySet, "id": prior.ID, "deleted": true})
}

// Confirm reads a created entity back (and checks it carries the write's
// reference), or checks a deleted one is gone.
func (c *Conn) Confirm(ctx context.Context, name string, result json.RawMessage) error {
	op, err := c.operation(name, "")
	if err != nil {
		return err
	}
	var res struct {
		ID        string `json:"id"`
		Reference string `json:"reference"`
	}
	if err := json.Unmarshal(result, &res); err != nil {
		return err
	}
	path, err := entityPath(op.EntitySet, res.ID)
	if err != nil {
		return err
	}
	r, err := c.do(ctx, http.MethodGet, c.url(op.Service, path, nil), nil, nil)
	var api *APIError
	notFound := errors.As(err, &api) && api.Status == http.StatusNotFound
	switch {
	case op.Action == "delete" && notFound:
		return nil
	case op.Action == "delete" && err == nil:
		return fmt.Errorf("%s %s still exists after deletion", op.EntitySet, res.ID)
	case err != nil:
		return err
	}
	if op.Reference == "" {
		return nil
	}
	rec, err := entity(r)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rec[op.Reference]); got != res.Reference {
		return fmt.Errorf("%s %s reads back %s %q, want %q", op.EntitySet, res.ID, op.Reference, got, res.Reference)
	}
	return nil
}

// Read returns an entity's configured properties under their business names.
func (c *Conn) Read(ctx context.Context, name, id string) (json.RawMessage, error) {
	op, err := c.operation(name, "get")
	if err != nil {
		return nil, err
	}
	path, err := entityPath(op.EntitySet, id)
	if err != nil {
		return nil, writeguard.ErrNotFound // not a key, so no such entity
	}
	props := make([]string, 0, len(op.Fields))
	for p := range op.Fields {
		props = append(props, p)
	}
	sort.Strings(props)
	r, err := c.do(ctx, http.MethodGet, c.url(op.Service, path, url.Values{"$select": {strings.Join(props, ",")}}), nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.Status == http.StatusNotFound {
		return nil, writeguard.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rec, err := entity(r)
	if err != nil {
		return nil, err
	}
	rec, _ = clean(rec).(map[string]any)
	out := map[string]any{"id": id}
	for p, as := range op.Fields {
		out[as] = rec[p]
	}
	return json.Marshal(out)
}
