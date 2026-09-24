// Package salesforce is the prototype's native Salesforce connector
// (architecture §13: REST API reads and writes, OAuth with a dedicated
// integration user, API limits declared in the manifest).
//
// Events are read by polling SOQL on SystemModstamp, a sanctioned REST read.
// A Pub/Sub API change-data-capture subscriber replaces polling later.
// Updates capture the previous values of the fields they change, so the
// restore action can undo them exactly.
package salesforce

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
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Name is the connector name this package implements.
const Name = "salesforce"

// DefaultAPIVersion is the REST API version used unless configured.
const DefaultAPIVersion = "61.0"

// Config is the connection-specific configuration.
type Config struct {
	APIVersion string                `json:"apiVersion,omitempty"`
	Events     map[string]EventQuery `json:"events,omitempty"`
	Operations map[string]Operation  `json:"operations,omitempty"`
}

// EventQuery defines an event as the records matching a SOQL condition,
// ordered by SystemModstamp. Fields may include relationship subqueries.
type EventQuery struct {
	SObject string   `json:"sobject"`
	Where   string   `json:"where,omitempty"`
	Fields  []string `json:"fields"`
}

// Operation binds a manifest operation to an sObject.
type Operation struct {
	SObject string `json:"sobject"`
	// Action is update (set fields on a record), restore (undo an update,
	// given the update's result), or get (read a record by ID).
	Action string `json:"action"`
	// IDField is the payload field holding the record ID (update only).
	IDField string `json:"idField,omitempty"`
	// Fields maps Salesforce field names to payload fields (update), or to
	// the names a get returns them under.
	Fields map[string]string `json:"fields,omitempty"`
}

// UpdateResult is what an update returns and what restore consumes.
type UpdateResult struct {
	SObject  string         `json:"sobject"`
	ID       string         `json:"id"`
	Previous map[string]any `json:"previous"`
	Applied  map[string]any `json:"applied"`
}

var (
	identRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
	idRE    = regexp.MustCompile(`^[A-Za-z0-9]{15}([A-Za-z0-9]{3})?$`)
)

func (c Config) validate() error {
	for name, q := range c.Events {
		if !identRE.MatchString(q.SObject) || len(q.Fields) == 0 {
			return fmt.Errorf("event %s: sobject and fields are required", name)
		}
		for _, part := range append(append([]string{}, q.Fields...), q.Where) {
			if strings.ContainsAny(part, ";") {
				return fmt.Errorf("event %s: SOQL fragments must not contain ';'", name)
			}
		}
	}
	for name, op := range c.Operations {
		if !identRE.MatchString(op.SObject) {
			return fmt.Errorf("operation %s: invalid sobject %q", name, op.SObject)
		}
		switch op.Action {
		case "update":
			if op.IDField == "" || len(op.Fields) == 0 {
				return fmt.Errorf("operation %s: update needs idField and fields", name)
			}
			for f := range op.Fields {
				if !identRE.MatchString(f) {
					return fmt.Errorf("operation %s: invalid field %q", name, f)
				}
			}
		case "restore":
		case "get":
			if len(op.Fields) == 0 {
				return fmt.Errorf("operation %s: get needs fields", name)
			}
			for f, as := range op.Fields {
				if !identRE.MatchString(f) || !identRE.MatchString(as) {
					return fmt.Errorf("operation %s: invalid field mapping %q: %q", name, f, as)
				}
			}
		default:
			return fmt.Errorf("operation %s: unknown action %q", name, op.Action)
		}
	}
	return nil
}

// Factory builds a connector. The secret is Credentials as JSON.
func Factory(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	var c Config
	if len(cfg.Config) > 0 {
		if err := json.Unmarshal(cfg.Config, &c); err != nil {
			return nil, fmt.Errorf("salesforce %s: config: %w", cfg.Endpoint, err)
		}
	}
	secret, err := secrets.Resolve(ctx, cfg.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("salesforce %s: %w", cfg.Endpoint, err)
	}
	conn, err := New(secret, c, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("salesforce %s: %w", cfg.Endpoint, err)
	}
	return conn, nil
}

// Conn is a connector instance.
type Conn struct {
	cfg  Config
	sess *session
	// PageLimit caps pages read per poll. Default 50.
	PageLimit int
}

var (
	_ connector.Instance   = (*Conn)(nil)
	_ connector.Source     = (*Conn)(nil)
	_ writeguard.Confirmer = (*Conn)(nil)
)

// New returns a connector for the given credentials JSON.
func New(secret string, cfg Config, client *http.Client) (*Conn, error) {
	if cfg.APIVersion == "" {
		cfg.APIVersion = DefaultAPIVersion
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	creds, key, err := parseCredentials(secret)
	if err != nil {
		return nil, err
	}
	return &Conn{cfg: cfg, sess: &session{creds: creds, key: key, http: client, now: time.Now}, PageLimit: 50}, nil
}

func (c *Conn) Close() {}

// APIError is an error response from the Salesforce API.
type APIError struct {
	Status int
	Code   string
	Msg    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("salesforce %d %s: %s", e.Status, e.Code, e.Msg)
}

// permanent reports errors that retrying cannot fix. Limits, lock
// contention and server errors are retryable.
func (e *APIError) permanent() bool {
	switch e.Code {
	case "REQUEST_LIMIT_EXCEEDED", "UNABLE_TO_LOCK_ROW", "SERVER_UNAVAILABLE", "INVALID_SESSION_ID":
		return false
	}
	return e.Status == http.StatusBadRequest || e.Status == http.StatusNotFound || e.Status == http.StatusForbidden
}

// do sends an API request, logging in or refreshing the token as needed.
func (c *Conn) do(ctx context.Context, method, path string, body any, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	for attempt := 0; ; attempt++ {
		token, instance, err := c.sess.current(ctx)
		if err != nil {
			return err
		}
		target := path
		if !strings.HasPrefix(path, "http") {
			target = instance + path
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.sess.http.Do(req)
		if err != nil {
			return fmt.Errorf("salesforce %s %s: %w", method, path, err)
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			c.sess.invalidate(token)
			continue
		}
		if resp.StatusCode >= 300 {
			apiErr := &APIError{Status: resp.StatusCode, Msg: strings.TrimSpace(string(data))}
			var errs []struct {
				Message   string `json:"message"`
				ErrorCode string `json:"errorCode"`
			}
			if json.Unmarshal(data, &errs) == nil && len(errs) > 0 {
				apiErr.Code, apiErr.Msg = errs[0].ErrorCode, errs[0].Message
			}
			if apiErr.permanent() {
				return fmt.Errorf("%w: %v", writeguard.ErrInvalid, apiErr)
			}
			return apiErr
		}
		if out != nil && len(data) > 0 {
			return json.Unmarshal(data, out)
		}
		return nil
	}
}

func (c *Conn) base() string { return "/services/data/v" + c.cfg.APIVersion }

// Poll returns records matching the event query modified after the
// position (Unix milliseconds of SystemModstamp). A poll never ends in the
// middle of a group of records sharing one timestamp, so a strict
// "greater than" cursor cannot skip records.
func (c *Conn) Poll(ctx context.Context, event string, after int64, limit int) ([]connector.Event, error) {
	q, ok := c.cfg.Events[event]
	if !ok {
		return nil, fmt.Errorf("salesforce: event %q is not configured on this connection", event)
	}
	fields := append([]string{}, q.Fields...)
	if !containsFold(fields, "Id") {
		fields = append(fields, "Id")
	}
	if !containsFold(fields, "SystemModstamp") {
		fields = append(fields, "SystemModstamp")
	}
	where := "SystemModstamp > " + time.UnixMilli(after).UTC().Format("2006-01-02T15:04:05.000Z")
	if q.Where != "" {
		where = "(" + q.Where + ") AND " + where
	}
	soql := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY SystemModstamp, Id", strings.Join(fields, ", "), q.SObject, where)

	var events []connector.Event
	next := c.base() + "/query?q=" + url.QueryEscape(soql)
	for pages := 1; next != ""; pages++ {
		var res struct {
			Done           bool             `json:"done"`
			NextRecordsURL string           `json:"nextRecordsUrl"`
			Records        []map[string]any `json:"records"`
		}
		if err := c.do(ctx, http.MethodGet, next, nil, &res); err != nil {
			return nil, err
		}
		for _, rec := range res.Records {
			ev, err := toEvent(event, rec)
			if err != nil {
				return nil, err
			}
			events = append(events, ev)
		}
		next = ""
		if !res.Done {
			next = res.NextRecordsURL
		}
		if next != "" && (len(events) >= limit || pages >= c.PageLimit) {
			// The batch is full: end it at a timestamp boundary, leaving the
			// trailing group for the next poll. If every record shares one
			// timestamp, keep reading until it changes.
			if cut := boundary(events); cut > 0 {
				return events[:cut], nil
			}
			if pages >= 10*c.PageLimit {
				return nil, fmt.Errorf("salesforce: more than %d pages of %s share one SystemModstamp", pages, q.SObject)
			}
		}
	}
	return events, nil
}

// boundary returns the length of events without its trailing group of
// records sharing the last position.
func boundary(events []connector.Event) int {
	cut := len(events)
	for cut > 0 && events[cut-1].Position == events[len(events)-1].Position {
		cut--
	}
	return cut
}

func toEvent(name string, rec map[string]any) (connector.Event, error) {
	id, _ := rec["Id"].(string)
	stamp, _ := rec["SystemModstamp"].(string)
	t, err := time.Parse("2006-01-02T15:04:05.000-0700", stamp)
	if err != nil {
		return connector.Event{}, fmt.Errorf("salesforce: record %s has invalid SystemModstamp %q", id, stamp)
	}
	stripAttributes(rec)
	payload, err := json.Marshal(rec)
	if err != nil {
		return connector.Event{}, err
	}
	return connector.Event{ID: id, Position: t.UnixMilli(), Name: name, Payload: payload}, nil
}

// stripAttributes removes the API's per-record metadata, recursively.
func stripAttributes(v any) {
	switch x := v.(type) {
	case map[string]any:
		delete(x, "attributes")
		for _, child := range x {
			stripAttributes(child)
		}
	case []any:
		for _, child := range x {
			stripAttributes(child)
		}
	}
}

func containsFold(list []string, s string) bool {
	for _, x := range list {
		if strings.EqualFold(strings.TrimSpace(x), s) {
			return true
		}
	}
	return false
}

func (c *Conn) operation(name string) (Operation, error) {
	op, ok := c.cfg.Operations[name]
	if !ok {
		return Operation{}, fmt.Errorf("salesforce: operation %q is not configured on this connection", name)
	}
	return op, nil
}

// plan works out the record and field values a write will set.
func (c *Conn) plan(op Operation, payload json.RawMessage) (UpdateResult, error) {
	var doc map[string]any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return UpdateResult{}, fmt.Errorf("%w: payload must be a JSON object", writeguard.ErrInvalid)
	}
	switch op.Action {
	case "update":
		id, _ := doc[op.IDField].(string)
		if !idRE.MatchString(id) {
			return UpdateResult{}, fmt.Errorf("%w: %s is not a Salesforce ID: %q", writeguard.ErrInvalid, op.IDField, id)
		}
		applied := map[string]any{}
		for sf, field := range op.Fields {
			applied[sf] = doc[field]
		}
		return UpdateResult{SObject: op.SObject, ID: id, Applied: applied}, nil
	case "restore":
		var prior UpdateResult
		if err := json.Unmarshal(payload, &prior); err != nil || !idRE.MatchString(prior.ID) || prior.Previous == nil {
			return UpdateResult{}, fmt.Errorf("%w: restore needs the result of an update", writeguard.ErrInvalid)
		}
		if prior.SObject != op.SObject {
			return UpdateResult{}, fmt.Errorf("%w: cannot restore a %s with a %s operation", writeguard.ErrInvalid, prior.SObject, op.SObject)
		}
		return UpdateResult{SObject: op.SObject, ID: prior.ID, Applied: prior.Previous}, nil
	}
	return UpdateResult{}, fmt.Errorf("salesforce: unknown action %q", op.Action)
}

func (c *Conn) record(ctx context.Context, sobject, id string, fields map[string]any) (map[string]any, error) {
	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)
	var rec map[string]any
	path := fmt.Sprintf("%s/sobjects/%s/%s?fields=%s", c.base(), sobject, id, url.QueryEscape(strings.Join(names, ",")))
	if err := c.do(ctx, http.MethodGet, path, nil, &rec); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(names))
	for _, f := range names {
		out[f] = rec[f]
	}
	return out, nil
}

// Simulate previews an update: the record's current values next to the
// ones the write would set. Salesforce has no server-side dry run for REST
// writes, so validation rules run only at commit.
func (c *Conn) Simulate(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	plan, err := c.plan(op, payload)
	if err != nil {
		return nil, err
	}
	current, err := c.record(ctx, plan.SObject, plan.ID, plan.Applied)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"mode": "preview", "sobject": plan.SObject, "id": plan.ID, "current": current, "proposed": plan.Applied})
}

// Commit applies the update, returning the values it replaced.
func (c *Conn) Commit(ctx context.Context, name, _ string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	plan, err := c.plan(op, payload)
	if err != nil {
		return nil, err
	}
	plan.Previous, err = c.record(ctx, plan.SObject, plan.ID, plan.Applied)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("%s/sobjects/%s/%s", c.base(), plan.SObject, plan.ID)
	if err := c.do(ctx, http.MethodPatch, path, plan.Applied, nil); err != nil {
		return nil, err
	}
	return json.Marshal(plan)
}

// Confirm reads the record back and checks the applied values.
func (c *Conn) Confirm(ctx context.Context, _ string, result json.RawMessage) error {
	var r UpdateResult
	if err := json.Unmarshal(result, &r); err != nil {
		return err
	}
	now, err := c.record(ctx, r.SObject, r.ID, r.Applied)
	if err != nil {
		return err
	}
	for f, want := range r.Applied {
		if fmt.Sprint(now[f]) != fmt.Sprint(want) {
			return fmt.Errorf("%s %s.%s reads back %v, want %v", r.SObject, r.ID, f, now[f], want)
		}
	}
	return nil
}

var _ writeguard.Reader = (*Conn)(nil)

// Read returns the configured fields of a record, under their business names.
func (c *Conn) Read(ctx context.Context, name, id string) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	if op.Action != "get" {
		return nil, fmt.Errorf("salesforce: operation %q is a %s, not a read", name, op.Action)
	}
	if !idRE.MatchString(id) {
		return nil, writeguard.ErrNotFound // not an ID, so no such record
	}
	fields := make(map[string]any, len(op.Fields))
	for f := range op.Fields {
		fields[f] = nil
	}
	rec, err := c.record(ctx, op.SObject, id, fields)
	var api *APIError
	if errors.As(err, &api) && api.Status == http.StatusNotFound {
		return nil, writeguard.ErrNotFound
	}
	if err != nil {
		if strings.Contains(err.Error(), "NOT_FOUND") {
			return nil, writeguard.ErrNotFound
		}
		return nil, err
	}
	out := map[string]any{"id": id}
	for f, as := range op.Fields {
		out[as] = rec[f]
	}
	return json.Marshal(out)
}
