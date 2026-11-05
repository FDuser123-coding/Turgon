// Package odoo is Turgon's connector for Odoo (19 and later) through its
// external JSON-2 API: every call is POST /json/2/<model>/<method> with the
// method's arguments by name, authenticated by an API key as a bearer
// token. It never touches Odoo's database: writes go through the ORM, so
// Odoo's constraints, computed fields and access rights apply.
//
// Like the REST connector it is configured by the Connection:
//
//   - Events are records changed after the cursor, read with search_read
//     on write_date, oldest first.
//   - Reads fetch one record by ID (read) and rename its fields.
//   - A create sends one create with the record and its lines (one2many
//     commands). It first looks for a record already carrying the write's
//     idempotency key in a reference field (client_order_ref), so a retry
//     after a lost response finds the order instead of creating a second.
//   - A create is dry-run with onchange, as Odoo's web client computes a
//     form before saving it: Odoo prices the lines and totals the order
//     without storing anything. The preview carries amount_untaxed.
//   - A cancel undoes a create, as a saga's compensation: action_cancel
//     (the order stays, cancelled) or unlink.
//
// Values are converted by the fields' types, which fields_get declares:
// a many2one or integer field takes an integer, a float or monetary field
// a number, so a mapping may hand over IDs as strings.
package odoo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Name is the connector name this package implements.
const Name = "odoo"

// Config is the connection-specific configuration.
type Config struct {
	// BaseURL is the instance's address, e.g. https://acme.odoo.com.
	BaseURL string `json:"baseURL"`
	// Database is sent as X-Odoo-Database, for a server hosting several
	// databases without a dbfilter.
	Database   string               `json:"database,omitempty"`
	Events     map[string]Event     `json:"events,omitempty"`
	Operations map[string]Operation `json:"operations,omitempty"`
}

// Event defines an event as the records of a model changed after the cursor.
type Event struct {
	Model string `json:"model"`
	// Domain narrows the records, in Odoo's domain syntax
	// ([["state", "=", "sale"]]); it is ANDed with the cursor.
	Domain []any `json:"domain,omitempty"`
	// Fields are the fields read (id and write_date always are).
	Fields []string `json:"fields,omitempty"`
	// SettleSeconds is how long a record waits before it is read (default
	// 60): Odoo stamps write_date with its transaction's start, so a long
	// transaction commits records stamped before others already read.
	SettleSeconds *int `json:"settleSeconds,omitempty"`
}

// Operation binds a manifest operation to a model.
type Operation struct {
	// Action is create, cancel (undo a create, given its result) or get.
	Action string `json:"action"`
	Model  string `json:"model"`
	// Fields maps Odoo fields to payload fields (create; dotted paths
	// allowed), or to the names a get returns them under.
	Fields map[string]string `json:"fields,omitempty"`
	// Constants are fixed field values.
	Constants map[string]any `json:"constants,omitempty"`
	// Lines builds a one2many field's records from a payload array.
	Lines *Lines `json:"lines,omitempty"`
	// Reference is the field that carries the write's idempotency key
	// (create), looked up before creating.
	Reference string `json:"reference,omitempty"`
	// Preview lists the computed fields the dry run returns (create), such
	// as amount_untaxed; empty means the create is not dry-run.
	Preview []string `json:"preview,omitempty"`
	// Method is how a cancel undoes the create: action_cancel (default) or unlink.
	Method string `json:"method,omitempty"`
}

// Lines builds the records of a one2many field (order_line).
type Lines struct {
	Field  string            `json:"field"`
	From   string            `json:"from"`
	Fields map[string]string `json:"fields"`
	// Preview lists the line fields the dry run returns, such as price_subtotal.
	Preview []string `json:"preview,omitempty"`
}

// CreateResult is what a create returns and a cancel consumes.
type CreateResult struct {
	Model string `json:"model"`
	ID    int64  `json:"id"`
	// Number is the record's display name (S00042), for notes and messages.
	Number string `json:"number,omitempty"`
	// Reference is the idempotency key stored in the reference field.
	Reference string `json:"reference,omitempty"`
	// Existing is set when the record was found, not created.
	Existing bool           `json:"existing,omitempty"`
	Record   map[string]any `json:"record,omitempty"`
}

var (
	modelRE = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$`)
	fieldRE = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	pathRE  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

func (c Config) validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("baseURL must be the instance's http(s) address, without a path")
	}
	for name, e := range c.Events {
		if !modelRE.MatchString(e.Model) {
			return fmt.Errorf("event %s: model is required", name)
		}
		for _, f := range e.Fields {
			if !fieldRE.MatchString(f) {
				return fmt.Errorf("event %s: invalid field %q", name, f)
			}
		}
		if err := checkDomain(e.Domain); err != nil {
			return fmt.Errorf("event %s: %w", name, err)
		}
	}
	for name, op := range c.Operations {
		where := "operation " + name
		if !modelRE.MatchString(op.Model) {
			return fmt.Errorf("%s: model is required", where)
		}
		switch op.Action {
		case "create":
			if len(op.Fields) == 0 {
				return fmt.Errorf("%s: create needs fields", where)
			}
			for f, p := range op.Fields {
				if !fieldRE.MatchString(f) || !pathRE.MatchString(p) {
					return fmt.Errorf("%s: invalid field mapping %q: %q", where, f, p)
				}
			}
			for f := range op.Constants {
				if !fieldRE.MatchString(f) {
					return fmt.Errorf("%s: invalid constant %q", where, f)
				}
			}
			if op.Reference != "" && !fieldRE.MatchString(op.Reference) {
				return fmt.Errorf("%s: invalid reference field %q", where, op.Reference)
			}
			for _, f := range op.Preview {
				if !fieldRE.MatchString(f) {
					return fmt.Errorf("%s: invalid preview field %q", where, f)
				}
			}
			if l := op.Lines; l != nil {
				if !fieldRE.MatchString(l.Field) || !pathRE.MatchString(l.From) || len(l.Fields) == 0 {
					return fmt.Errorf("%s: lines need field, from and fields", where)
				}
				for f, p := range l.Fields {
					if !fieldRE.MatchString(f) || !pathRE.MatchString(p) {
						return fmt.Errorf("%s: invalid line field mapping %q: %q", where, f, p)
					}
				}
				for _, f := range l.Preview {
					if !fieldRE.MatchString(f) {
						return fmt.Errorf("%s: invalid line preview field %q", where, f)
					}
				}
			}
		case "cancel":
			switch op.Method {
			case "", "action_cancel", "unlink":
			default:
				return fmt.Errorf("%s: method must be action_cancel or unlink, got %q", where, op.Method)
			}
		case "get":
			if len(op.Fields) == 0 {
				return fmt.Errorf("%s: get needs fields", where)
			}
			for f, as := range op.Fields {
				if !fieldRE.MatchString(f) || !pathRE.MatchString(as) {
					return fmt.Errorf("%s: invalid field mapping %q: %q", where, f, as)
				}
			}
		default:
			return fmt.Errorf("%s: action must be create, cancel or get, got %q", where, op.Action)
		}
	}
	return nil
}

// checkDomain accepts a list of [field, operator, value] terms and the
// prefix operators "&" and "|" (two operands) and "!" (one), each with its
// operands; consecutive expressions are ANDed, as Odoo reads them.
func checkDomain(d []any) error {
	var expr func(i int) (int, error)
	expr = func(i int) (int, error) {
		if i >= len(d) {
			return 0, fmt.Errorf("domain: an operator lacks an operand")
		}
		switch x := d[i].(type) {
		case string:
			n := 2
			switch x {
			case "!":
				n = 1
			case "&", "|":
			default:
				return 0, fmt.Errorf("domain: %q is not an operator", x)
			}
			j := i + 1
			for k := 0; k < n; k++ {
				var err error
				if j, err = expr(j); err != nil {
					return 0, err
				}
			}
			return j, nil
		case []any:
			if len(x) != 3 {
				return 0, fmt.Errorf("domain: a term is [field, operator, value]")
			}
			if f, ok := x[0].(string); !ok || !pathRE.MatchString(f) {
				return 0, fmt.Errorf("domain: invalid field %v", x[0])
			}
			if _, ok := x[1].(string); !ok {
				return 0, fmt.Errorf("domain: invalid operator %v", x[1])
			}
			return i + 1, nil
		}
		return 0, fmt.Errorf("domain: a term is [field, operator, value]")
	}
	for i := 0; i < len(d); {
		var err error
		if i, err = expr(i); err != nil {
			return err
		}
	}
	return nil
}

// Factory builds a connector from its compiled configuration.
func Factory(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	var c Config
	if len(cfg.Config) > 0 {
		if err := json.Unmarshal(cfg.Config, &c); err != nil {
			return nil, fmt.Errorf("odoo %s: config: %w", cfg.Endpoint, err)
		}
	}
	secret, err := secrets.Resolve(ctx, cfg.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("odoo %s: %w", cfg.Endpoint, err)
	}
	conn, err := New(c, secret, &http.Client{Timeout: 60 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("odoo %s: %w", cfg.Endpoint, err)
	}
	return conn, nil
}

// Conn is a connector instance.
type Conn struct {
	cfg  Config
	key  string
	http *http.Client
	// PageSize is the limit of each poll. Default 100.
	PageSize int
	// now is the clock settling is measured with (tests set it).
	now func() time.Time

	mu     sync.Mutex
	fields map[string]map[string]FieldInfo // fields_get, by model
}

// FieldInfo is what fields_get says about a field.
type FieldInfo struct {
	Type     string `json:"type"`
	String   string `json:"string"`
	Required bool   `json:"required"`
	Readonly bool   `json:"readonly"`
	Relation string `json:"relation"`
	Size     int    `json:"size"`
}

var (
	_ connector.Instance   = (*Conn)(nil)
	_ connector.Source     = (*Conn)(nil)
	_ connector.Checker    = (*Conn)(nil)
	_ connector.Discoverer = (*Conn)(nil)
	_ writeguard.Confirmer = (*Conn)(nil)
	_ writeguard.Reader    = (*Conn)(nil)
)

// New returns a connector. The secret is the API key.
func New(cfg Config, secret string, client *http.Client) (*Conn, error) {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	key := strings.TrimSpace(secret)
	if key == "" {
		return nil, fmt.Errorf("the secret must be an Odoo API key")
	}
	return &Conn{cfg: cfg, key: key, http: client, PageSize: 100, now: time.Now, fields: map[string]map[string]FieldInfo{}}, nil
}

func (c *Conn) Close() {}

// SetClock sets the clock events' settling is measured with (tests).
func (c *Conn) SetClock(now func() time.Time) { c.now = now }

// APIError is an error response from Odoo.
type APIError struct {
	Model, Method string
	Status        int
	// Name is the exception's class, such as odoo.exceptions.ValidationError.
	Name    string
	Message string
}

func (e *APIError) Error() string {
	name := e.Name
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	if name != "" {
		name += ": "
	}
	return fmt.Sprintf("odoo %s/%s: %d %s%s", e.Model, e.Method, e.Status, name, e.Message)
}

// permanent reports errors that retrying cannot fix: Odoo refused the
// request (a UserError, ValidationError, AccessError or MissingError), or
// the key is not valid. Concurrent updates, throttling and server errors
// are retryable.
func (e *APIError) permanent() bool {
	switch e.Status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity:
		return true
	}
	return false
}

// call invokes a model's method with named arguments.
func (c *Conn) call(ctx context.Context, model, method string, args map[string]any, out any) error {
	if args == nil {
		args = map[string]any{}
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	target := c.cfg.BaseURL + "/json/2/" + model + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "bearer "+c.key)
	if c.cfg.Database != "" {
		req.Header.Set("X-Odoo-Database", c.cfg.Database)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("odoo %s/%s: %w", model, method, err)
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		e := &APIError{Model: model, Method: method, Status: resp.StatusCode, Message: strings.TrimSpace(string(data))}
		var b struct {
			Name    string `json:"name"`
			Message string `json:"message"`
		}
		if json.Unmarshal(data, &b) == nil && b.Message != "" {
			e.Name, e.Message = b.Name, b.Message
		}
		if len(e.Message) > 500 {
			e.Message = e.Message[:500] + "…"
		}
		if e.permanent() {
			return fmt.Errorf("%w: %w", writeguard.ErrInvalid, e)
		}
		return e
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("odoo %s/%s: the answer is not JSON: %w", model, method, err)
	}
	return nil
}

// fieldsOf returns a model's fields, as fields_get describes them (cached).
func (c *Conn) fieldsOf(ctx context.Context, model string) (map[string]FieldInfo, error) {
	c.mu.Lock()
	f, ok := c.fields[model]
	c.mu.Unlock()
	if ok {
		return f, nil
	}
	if err := c.call(ctx, model, "fields_get", map[string]any{
		"attributes": []string{"type", "string", "required", "readonly", "relation", "size"},
	}, &f); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.fields[model] = f
	c.mu.Unlock()
	return f, nil
}

// A position is a record's place in change order: its write_date (whole
// seconds, as Odoo stamps it) and its ID, as (seconds << 31) | id. The
// cursor is both, so records written in one second are read in ID order and
// none is skipped, however many share it. Positions of earlier versions
// were Unix milliseconds of a second already read; they still work.
const idBits = 31

func position(t time.Time, id int64) int64 { return t.Unix()<<idBits | id }

func cursor(after int64) (time.Time, int64) {
	if after < 1<<40 { // milliseconds: that second was read in full
		return time.UnixMilli(after).UTC().Truncate(time.Second), 1<<idBits - 1
	}
	return time.Unix(after>>idBits, 0).UTC(), after & (1<<idBits - 1)
}

// Poll returns records changed after the position, in change order: after
// its write_date, or in its second with a higher ID. Records younger than
// the event's settle time are left for a later poll, because Odoo stamps
// write_date when the transaction starts, not when it commits.
func (c *Conn) Poll(ctx context.Context, name string, after int64, limit int) ([]connector.Event, error) {
	e, ok := c.cfg.Events[name]
	if !ok {
		return nil, fmt.Errorf("odoo: event %q is not configured on this connection", name)
	}
	if limit <= 0 || limit > c.PageSize {
		limit = c.PageSize
	}
	settle := 60
	if e.SettleSeconds != nil {
		settle = *e.SettleSeconds
	}
	since, lastID := cursor(after)
	stamp := since.Format("2006-01-02 15:04:05")
	// Consecutive expressions are ANDed: the event's domain, the cursor and
	// the settle limit.
	domain := append([]any{}, e.Domain...)
	domain = append(domain, "|", []any{"write_date", ">", stamp}, "&", []any{"write_date", "=", stamp}, []any{"id", ">", lastID},
		[]any{"write_date", "<=", c.now().UTC().Add(-time.Duration(settle) * time.Second).Format("2006-01-02 15:04:05")})
	fields := append([]string{"id", "write_date"}, e.Fields...)
	var recs []map[string]any
	if err := c.call(ctx, e.Model, "search_read", map[string]any{
		"domain": domain, "fields": fields, "order": "write_date asc, id asc", "limit": limit,
	}, &recs); err != nil {
		return nil, err
	}
	events := make([]connector.Event, 0, len(recs))
	for _, rec := range recs {
		ev, err := toEvent(name, e, rec)
		if err != nil {
			return nil, err
		}
		if ev.Position > after {
			events = append(events, ev)
		}
	}
	return events, nil
}

func toEvent(name string, e Event, rec map[string]any) (connector.Event, error) {
	id := fmt.Sprint(rec["id"])
	s, _ := rec["write_date"].(string)
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		return connector.Event{}, fmt.Errorf("odoo: %s record %s has no valid write_date (%q)", e.Model, id, s)
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return connector.Event{}, err
	}
	n := id64(rec["id"])
	if n <= 0 || n >= 1<<idBits {
		return connector.Event{}, fmt.Errorf("odoo: %s record ID %s is outside the positions' range", e.Model, id)
	}
	return connector.Event{ID: id, Position: position(t, n), Name: name, Payload: payload}, nil
}

func (c *Conn) operation(name, action string) (Operation, error) {
	op, ok := c.cfg.Operations[name]
	if !ok {
		return Operation{}, fmt.Errorf("odoo: operation %q is not configured on this connection", name)
	}
	if action != "" && op.Action != action {
		return Operation{}, fmt.Errorf("odoo: operation %q is a %s, not a %s", name, op.Action, action)
	}
	return op, nil
}

func lookup(doc map[string]any, path string) any {
	var v any = doc
	for _, k := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}

// convert turns a payload value into what a field of this type takes.
func convert(model, field string, v any, f FieldInfo) (any, error) {
	if v == nil {
		return false, nil // Odoo's empty value
	}
	bad := func() error {
		return fmt.Errorf("%w: %s.%s is a %s field, and %v is not one", writeguard.ErrInvalid, model, field, f.Type, v)
	}
	switch f.Type {
	case "many2one", "integer":
		switch x := v.(type) {
		case json.Number:
			n, err := x.Int64()
			if err != nil {
				return nil, bad()
			}
			return n, nil
		case float64:
			if x != float64(int64(x)) {
				return nil, bad()
			}
			return int64(x), nil
		case string:
			n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
			if err != nil {
				return nil, bad()
			}
			return n, nil
		}
		return nil, bad()
	case "float", "monetary":
		switch x := v.(type) {
		case json.Number:
			n, err := x.Float64()
			if err != nil {
				return nil, bad()
			}
			return n, nil
		case float64:
			return x, nil
		case string:
			n, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if err != nil {
				return nil, bad()
			}
			return n, nil
		}
		return nil, bad()
	case "boolean":
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, bad()
	case "char", "text", "html", "selection", "date", "datetime":
		switch x := v.(type) {
		case string:
			if f.Size > 0 && len([]rune(x)) > f.Size {
				return nil, fmt.Errorf("%w: %s.%s holds at most %d characters", writeguard.ErrInvalid, model, field, f.Size)
			}
			return x, nil
		case json.Number:
			return x.String(), nil
		}
		return nil, bad()
	}
	return v, nil
}

// values builds a record's values from the payload.
func values(model string, fields map[string]FieldInfo, doc map[string]any, mapping map[string]string, constants map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for f, v := range constants {
		out[f] = v
	}
	for f, p := range mapping {
		info, ok := fields[f]
		if !ok {
			return nil, fmt.Errorf("%w: %s has no field %s", writeguard.ErrInvalid, model, f)
		}
		raw := lookup(doc, p)
		if raw == nil {
			continue // not in the payload: Odoo's default applies
		}
		v, err := convert(model, f, raw, info)
		if err != nil {
			return nil, err
		}
		out[f] = v
	}
	return out, nil
}

func decode(payload json.RawMessage) (map[string]any, error) {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil || doc == nil {
		return nil, fmt.Errorf("%w: the payload must be a JSON object", writeguard.ErrInvalid)
	}
	return doc, nil
}

// record builds a create's values, with its lines as one2many create
// commands ([0, 0, {...}]).
func (c *Conn) record(ctx context.Context, op Operation, payload json.RawMessage) (map[string]any, error) {
	doc, err := decode(payload)
	if err != nil {
		return nil, err
	}
	fields, err := c.fieldsOf(ctx, op.Model)
	if err != nil {
		return nil, err
	}
	vals, err := values(op.Model, fields, doc, op.Fields, op.Constants)
	if err != nil {
		return nil, err
	}
	if l := op.Lines; l != nil {
		info, ok := fields[l.Field]
		if !ok || info.Type != "one2many" || info.Relation == "" {
			return nil, fmt.Errorf("%w: %s.%s is not a one2many field", writeguard.ErrInvalid, op.Model, l.Field)
		}
		lineFields, err := c.fieldsOf(ctx, info.Relation)
		if err != nil {
			return nil, err
		}
		list, ok := lookup(doc, l.From).([]any)
		if !ok || len(list) == 0 {
			return nil, fmt.Errorf("%w: %s must list at least one line", writeguard.ErrInvalid, l.From)
		}
		var cmds []any
		for i, x := range list {
			line, ok := x.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: %s[%d] is not an object", writeguard.ErrInvalid, l.From, i)
			}
			lv, err := values(info.Relation, lineFields, line, l.Fields, nil)
			if err != nil {
				return nil, fmt.Errorf("%s[%d]: %w", l.From, i, err)
			}
			// A line without a price is priced by Odoo (its pricelist): a
			// field missing from the payload is not sent.
			cmds = append(cmds, []any{0, 0, lv})
		}
		vals[l.Field] = cmds
	}
	return vals, nil
}

// Simulate dry-runs a create with onchange, as Odoo's web client computes
// a form before saving: Odoo prices the lines and totals the record, and
// stores nothing. The preview has the configured computed fields.
func (c *Conn) Simulate(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name, "")
	if err != nil {
		return nil, err
	}
	if op.Action != "create" || len(op.Preview) == 0 {
		return nil, writeguard.ErrSimulationUnsupported
	}
	vals, err := c.record(ctx, op, payload)
	if err != nil {
		return nil, err
	}
	spec := map[string]any{}
	for _, f := range op.Preview {
		spec[f] = map[string]any{}
	}
	changed := make([]string, 0, len(vals))
	for f := range vals {
		changed = append(changed, f)
		if _, ok := spec[f]; !ok {
			spec[f] = map[string]any{}
		}
	}
	sort.Strings(changed)
	if l := op.Lines; l != nil && len(l.Preview) > 0 {
		lineSpec := map[string]any{}
		for _, f := range l.Preview {
			lineSpec[f] = map[string]any{}
		}
		for f := range l.Fields {
			lineSpec[f] = map[string]any{}
		}
		spec[l.Field] = map[string]any{"fields": lineSpec}
	}
	var res struct {
		Value   map[string]any `json:"value"`
		Warning any            `json:"warning"`
	}
	if err := c.call(ctx, op.Model, "onchange", map[string]any{"values": vals, "field_names": changed, "fields_spec": spec}, &res); err != nil {
		return nil, err
	}
	record := map[string]any{}
	for _, f := range op.Preview {
		record[f] = res.Value[f]
	}
	if l := op.Lines; l != nil && len(l.Preview) > 0 {
		record[l.Field] = linesOf(res.Value[l.Field])
	}
	out := map[string]any{"mode": "onchange", "model": op.Model, "record": record, "values": vals}
	if res.Warning != nil && res.Warning != false {
		out["warning"] = res.Warning
	}
	return json.Marshal(out)
}

// linesOf takes the line values out of onchange's one2many commands.
func linesOf(v any) []any {
	cmds, _ := v.([]any)
	var out []any
	for _, x := range cmds {
		cmd, ok := x.([]any)
		if ok && len(cmd) == 3 {
			if vals, ok := cmd[2].(map[string]any); ok {
				out = append(out, vals)
			}
		}
	}
	return out
}

// existing finds the record an earlier attempt created with this key.
func (c *Conn) existing(ctx context.Context, op Operation, key string) (map[string]any, error) {
	var recs []map[string]any
	if err := c.call(ctx, op.Model, "search_read", map[string]any{
		"domain": []any{[]any{op.Reference, "=", key}}, "fields": []string{"id", "display_name", op.Reference}, "limit": 2,
	}, &recs); err != nil {
		return nil, err
	}
	switch len(recs) {
	case 0:
		return nil, nil
	case 1:
		return recs[0], nil
	}
	return nil, fmt.Errorf("%w: more than one %s has %s %s; the reference must be unique", writeguard.ErrInvalid, op.Model, op.Reference, key)
}

// Commit creates the record, or cancels the one a create's result names.
func (c *Conn) Commit(ctx context.Context, name, key string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name, "")
	if err != nil {
		return nil, err
	}
	switch op.Action {
	case "create":
		return c.create(ctx, op, key, payload)
	case "cancel":
		return c.cancel(ctx, op, payload)
	}
	return nil, fmt.Errorf("odoo: operation %q is a %s, not a write", name, op.Action)
}

func id64(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		n, _ := x.Int64()
		return n
	case float64:
		return int64(x)
	}
	return 0
}

func (c *Conn) create(ctx context.Context, op Operation, key string, payload json.RawMessage) (json.RawMessage, error) {
	vals, err := c.record(ctx, op, payload)
	if err != nil {
		return nil, err
	}
	res := CreateResult{Model: op.Model}
	if op.Reference != "" {
		if key == "" {
			return nil, fmt.Errorf("%w: the write has no idempotency key for %s", writeguard.ErrInvalid, op.Reference)
		}
		// The key must fit the reference field whole: Odoo would cut a
		// longer one, and a retry's lookup of the whole key would then miss.
		fields, err := c.fieldsOf(ctx, op.Model)
		if err != nil {
			return nil, err
		}
		ref := fields[op.Reference]
		if ref.Type != "char" {
			return nil, fmt.Errorf("%w: %s.%s must be a char field to hold the idempotency key, not %q", writeguard.ErrInvalid, op.Model, op.Reference, ref.Type)
		}
		if ref.Size > 0 && len([]rune(key)) > ref.Size {
			return nil, fmt.Errorf("%w: the idempotency key %q is longer than %s.%s holds (%d)", writeguard.ErrInvalid, key, op.Model, op.Reference, ref.Size)
		}
		vals[op.Reference], res.Reference = key, key
		found, err := c.existing(ctx, op, key)
		if err != nil {
			return nil, err
		}
		if found != nil {
			res.ID, res.Existing, res.Record = id64(found["id"]), true, found
			res.Number, _ = found["display_name"].(string)
			return json.Marshal(res)
		}
	}
	var ids []any
	if err := c.call(ctx, op.Model, "create", map[string]any{"vals_list": []any{vals}}, &ids); err != nil {
		return nil, err
	}
	if len(ids) != 1 || id64(ids[0]) == 0 {
		return nil, fmt.Errorf("odoo %s/create: answered %v, not one record ID", op.Model, ids)
	}
	res.ID = id64(ids[0])
	read := append([]string{"display_name"}, op.Preview...)
	var recs []map[string]any
	if err := c.call(ctx, op.Model, "read", map[string]any{"ids": []int64{res.ID}, "fields": read}, &recs); err == nil && len(recs) == 1 {
		res.Record = recs[0]
		res.Number, _ = recs[0]["display_name"].(string)
	}
	return json.Marshal(res)
}

func (c *Conn) cancel(ctx context.Context, op Operation, payload json.RawMessage) (json.RawMessage, error) {
	var prior CreateResult
	if err := json.Unmarshal(payload, &prior); err != nil || prior.ID == 0 {
		return nil, fmt.Errorf("%w: cancel needs the result of a create", writeguard.ErrInvalid)
	}
	if prior.Model != op.Model {
		return nil, fmt.Errorf("%w: cannot cancel a %s with an operation on %s", writeguard.ErrInvalid, prior.Model, op.Model)
	}
	method := op.Method
	if method == "" {
		method = "action_cancel"
	}
	out := map[string]any{"model": op.Model, "id": prior.ID, "method": method}
	var gone []any
	if err := c.call(ctx, op.Model, "search", map[string]any{"domain": []any{[]any{"id", "=", prior.ID}}}, &gone); err != nil {
		return nil, err
	}
	if len(gone) == 0 {
		out["alreadyGone"] = true
		return json.Marshal(out)
	}
	args := map[string]any{"ids": []int64{prior.ID}}
	if method == "action_cancel" {
		// Since Odoo 16, cancelling an order that was sent or confirmed opens
		// a confirmation wizard instead, unless the warning is disabled.
		args["context"] = map[string]any{"disable_cancel_warning": true}
	}
	var answer any
	err := c.call(ctx, op.Model, method, args, &answer)
	var api *APIError
	if errors.As(err, &api) && api.Status == http.StatusNotFound {
		out["alreadyGone"] = true // deleted between the search and the call
		return json.Marshal(out)
	}
	if err != nil {
		return nil, err
	}
	if action, ok := answer.(map[string]any); ok {
		return nil, fmt.Errorf("%w: Odoo answered %s of %s %d with an action (%v) instead of cancelling it; cancel it in Odoo", writeguard.ErrInvalid, method, op.Model, prior.ID, action["res_model"])
	}
	out["done"] = true
	return json.Marshal(out)
}

// Confirm reads a created record back (and checks it carries the write's
// reference), or checks a cancelled one is cancelled or gone.
func (c *Conn) Confirm(ctx context.Context, name string, result json.RawMessage) error {
	op, err := c.operation(name, "")
	if err != nil {
		return err
	}
	var res struct {
		ID        int64  `json:"id"`
		Reference string `json:"reference"`
	}
	if err := json.Unmarshal(result, &res); err != nil || res.ID == 0 {
		return fmt.Errorf("odoo: %s's result names no record", name)
	}
	fields := []string{"id"}
	switch {
	case op.Action == "create" && op.Reference != "":
		fields = append(fields, op.Reference)
	case op.Action == "cancel" && op.Method != "unlink":
		fields = append(fields, "state")
	}
	var recs []map[string]any
	if err := c.call(ctx, op.Model, "search_read", map[string]any{
		"domain": []any{[]any{"id", "=", res.ID}}, "fields": fields,
	}, &recs); err != nil {
		return err
	}
	switch {
	case op.Action == "cancel" && len(recs) == 0:
		return nil
	case op.Action == "cancel" && op.Method == "unlink":
		return fmt.Errorf("%s %d still exists after unlink", op.Model, res.ID)
	case op.Action == "cancel":
		if s := recs[0]["state"]; s != "cancel" {
			return fmt.Errorf("%s %d reads back state %v after action_cancel, want cancel", op.Model, res.ID, s)
		}
		return nil
	case len(recs) == 0:
		return fmt.Errorf("%s %d is not in Odoo after the create", op.Model, res.ID)
	}
	if op.Reference != "" && fmt.Sprint(recs[0][op.Reference]) != res.Reference {
		return fmt.Errorf("%s %d reads back %s %v, want %q", op.Model, res.ID, op.Reference, recs[0][op.Reference], res.Reference)
	}
	return nil
}

// Read returns a record's configured fields under their business names. A
// many2one reads as its ID.
func (c *Conn) Read(ctx context.Context, name, id string) (json.RawMessage, error) {
	op, err := c.operation(name, "get")
	if err != nil {
		return nil, err
	}
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return nil, writeguard.ErrNotFound
	}
	fields := make([]string, 0, len(op.Fields))
	for f := range op.Fields {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	var recs []map[string]any
	err = c.call(ctx, op.Model, "search_read", map[string]any{"domain": []any{[]any{"id", "=", n}}, "fields": fields}, &recs)
	var api *APIError
	if errors.As(err, &api) && api.Status == http.StatusNotFound {
		return nil, writeguard.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, writeguard.ErrNotFound
	}
	types, err := c.fieldsOf(ctx, op.Model)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for f, as := range op.Fields {
		v := recs[0][f]
		if pair, ok := v.([]any); ok && len(pair) == 2 && types[f].Type == "many2one" { // [id, name]
			v = pair[0]
		}
		out[as] = v
	}
	return json.Marshal(out)
}
