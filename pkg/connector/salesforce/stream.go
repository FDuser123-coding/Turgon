package salesforce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/pubsub"
)

// DefaultPubSubEndpoint is Salesforce's Pub/Sub API.
const DefaultPubSubEndpoint = "api.pubsub.salesforce.com:7443"

// Subscription is an event received over the Pub/Sub API: change data
// capture (/data/AccountChangeEvent, /data/ChangeEvents) or a platform
// event (/event/Order_Placed__e). Salesforce keeps events for three days
// (CDC and high-volume platform events); a subscription resumes after the
// last event Turgon stored, so a worker down for less than that misses
// nothing.
type Subscription struct {
	Topic string `json:"topic"`
	// ChangeTypes keeps only these change events: CREATE, UPDATE, DELETE,
	// UNDELETE, or a GAP_ or overflow type. Default all.
	ChangeTypes []string `json:"changeTypes,omitempty"`
	// Match keeps only events whose payload has these field values. An
	// update carries only the fields it changed, so {StageName: Closed Won}
	// matches the update that closed a deal, not later edits to it.
	Match map[string]any `json:"match,omitempty"`
	// Start is where a subscription without a stored position begins:
	// latest (default: what is published from now on) or earliest (all
	// events Salesforce still keeps).
	Start string `json:"start,omitempty"`
	// Batch is how many events are requested at a time. Default 100.
	Batch int `json:"batch,omitempty"`
	// Fields, if set, reads each changed record's current values with SOQL
	// (subqueries allowed) and makes them the payload, next to the
	// ChangeEventHeader: a change event carries only the fields that
	// changed, and a flow usually needs more. Deleted records keep the
	// change event's payload.
	Fields []string `json:"fields,omitempty"`
}

var topicRE = regexp.MustCompile(`^/(data|event)/[A-Za-z0-9_]+$`)

func (s Subscription) validate(name string) error {
	if !topicRE.MatchString(s.Topic) {
		return fmt.Errorf("subscription %s: topic %q must be /data/<channel> or /event/<platform event>", name, s.Topic)
	}
	switch s.Start {
	case "", "latest", "earliest":
	default:
		return fmt.Errorf("subscription %s: start must be latest or earliest", name)
	}
	for _, ct := range s.ChangeTypes {
		if !identRE.MatchString(ct) {
			return fmt.Errorf("subscription %s: invalid change type %q", name, ct)
		}
	}
	for _, f := range s.Fields {
		if strings.ContainsAny(f, ";") {
			return fmt.Errorf("subscription %s: SOQL fragments must not contain ';'", name)
		}
	}
	if s.Batch < 0 || s.Batch > 1000 {
		return fmt.Errorf("subscription %s: batch must be between 1 and 1000", name)
	}
	return nil
}

var _ connector.Streamer = (*Conn)(nil)

// Streams reports whether event arrives over the Pub/Sub API.
func (c *Conn) Streams(event string) bool {
	_, ok := c.cfg.Subscriptions[event]
	return ok
}

// pubsubConn dials the Pub/Sub API once. TLS is always used, except to a
// loopback address (the test fake).
func (c *Conn) pubsubConn() (*grpc.ClientConn, error) {
	c.psMu.Lock()
	defer c.psMu.Unlock()
	if c.ps != nil {
		return c.ps, nil
	}
	endpoint := c.cfg.PubSubEndpoint
	if endpoint == "" {
		endpoint = DefaultPubSubEndpoint
	}
	creds := credentials.NewTLS(nil)
	if host, _, err := net.SplitHostPort(endpoint); err == nil {
		if ip := net.ParseIP(host); host == "localhost" || (ip != nil && ip.IsLoopback()) {
			creds = insecure.NewCredentials()
		}
	}
	cc, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds), grpc.WithDefaultCallOptions(pubsub.CallOption()))
	if err != nil {
		return nil, fmt.Errorf("salesforce pub/sub %s: %w", endpoint, err)
	}
	c.ps = cc
	return cc, nil
}

func (c *Conn) closePubSub() {
	c.psMu.Lock()
	defer c.psMu.Unlock()
	if c.ps != nil {
		_ = c.ps.Close()
		c.ps = nil
	}
}

// Stream subscribes to an event's topic and delivers what arrives.
func (c *Conn) Stream(ctx context.Context, event string, resume []byte, deliver func([]connector.Event, []byte) error) error {
	sub, ok := c.cfg.Subscriptions[event]
	if !ok {
		return fmt.Errorf("salesforce: event %q is not a subscription on this connection", event)
	}
	err := c.stream(ctx, event, sub, resume, deliver)
	if resume != nil && isReplayRejected(err) {
		// The stored position is past Salesforce's retention (the worker was
		// down for days) or unusable: take every event still kept. The inbox
		// drops those it already holds.
		err = c.stream(ctx, event, Subscription{Topic: sub.Topic, ChangeTypes: sub.ChangeTypes, Match: sub.Match, Start: "earliest", Batch: sub.Batch}, nil, deliver)
	}
	return err
}

func isReplayRejected(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.InvalidArgument && strings.Contains(strings.ToLower(st.Message()), "replay")
}

func (c *Conn) stream(ctx context.Context, event string, sub Subscription, resume []byte, deliver func([]connector.Event, []byte) error) error {
	cc, err := c.pubsubConn()
	if err != nil {
		return err
	}
	token, instance, org, err := c.sess.identity(ctx)
	if err != nil {
		return err
	}
	if org == "" {
		return errors.New("salesforce pub/sub: the token response named no org (identity URL missing)")
	}
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(ctx,
		metadata.Pairs("accesstoken", token, "instanceurl", instance, "tenantid", org)))
	defer cancel()
	st, err := cc.NewStream(ctx, pubsub.SubscribeStream, pubsub.Subscribe)
	if err != nil {
		return c.rpcErr(token, err)
	}
	batch := int32(sub.Batch)
	if batch <= 0 {
		batch = 100
	}
	req := pubsub.FetchRequest{TopicName: sub.Topic, NumRequested: batch}
	switch {
	case resume != nil:
		req.ReplayPreset, req.ReplayID = pubsub.Custom, resume
	case sub.Start == "earliest":
		req.ReplayPreset = pubsub.Earliest
	}
	frame := req.Encode()
	if err := st.SendMsg(&frame); err != nil {
		return c.rpcErr(token, err)
	}
	pending := batch
	for {
		var in pubsub.Frame
		if err := st.RecvMsg(&in); err != nil {
			return c.rpcErr(token, err)
		}
		resp, err := pubsub.DecodeFetchResponse(in)
		if err != nil {
			return err
		}
		var events []connector.Event
		for _, ce := range resp.Events {
			evs, err := c.convert(ctx, cc, event, sub, ce)
			if err != nil {
				return fmt.Errorf("salesforce pub/sub %s: event %s: %w", sub.Topic, ce.Event.ID, err)
			}
			events = append(events, evs...)
		}
		next := resp.LatestReplayID
		if n := len(resp.Events); n > 0 && len(resp.Events[n-1].ReplayID) > 0 {
			next = resp.Events[n-1].ReplayID
		}
		if len(resp.Events) > 0 || len(next) > 0 {
			if err := deliver(events, next); err != nil {
				return err
			}
		}
		// Keep events flowing: ask for more once half the batch arrived.
		pending -= int32(len(resp.Events))
		if pending <= batch/2 {
			more := pubsub.FetchRequest{TopicName: sub.Topic, NumRequested: batch - pending}.Encode()
			if err := st.SendMsg(&more); err != nil {
				return c.rpcErr(token, err)
			}
			pending = batch
		}
	}
}

// rpcErr drops a token the API rejected, so the reopened subscription logs
// in again.
func (c *Conn) rpcErr(token string, err error) error {
	if status.Code(err) == codes.Unauthenticated {
		c.sess.invalidate(token)
	}
	return fmt.Errorf("salesforce pub/sub: %w", err)
}

func (c *Conn) schema(ctx context.Context, cc *grpc.ClientConn, id string) (avro.Schema, error) {
	c.psMu.Lock()
	s, ok := c.schemas[id]
	c.psMu.Unlock()
	if ok {
		return s, nil
	}
	req := pubsub.SchemaRequest{SchemaID: id}.Encode()
	var out pubsub.Frame
	if err := cc.Invoke(ctx, pubsub.GetSchema, &req, &out); err != nil {
		return nil, fmt.Errorf("schema %s: %w", id, err)
	}
	info, err := pubsub.DecodeSchemaInfo(out)
	if err != nil {
		return nil, err
	}
	s, err = avro.Parse(info.SchemaJSON)
	if err != nil {
		return nil, fmt.Errorf("schema %s: %w", id, err)
	}
	c.psMu.Lock()
	c.schemas[id] = s
	c.psMu.Unlock()
	return s, nil
}

// convert decodes an event into Turgon events: one per record a change
// event covers (each with its Id), or one for a platform event.
func (c *Conn) convert(ctx context.Context, cc *grpc.ClientConn, event string, sub Subscription, ce pubsub.ConsumerEvent) ([]connector.Event, error) {
	schema, err := c.schema(ctx, cc, ce.Event.SchemaID)
	if err != nil {
		return nil, err
	}
	var raw any
	if err := avro.Unmarshal(schema, ce.Event.Payload, &raw); err != nil {
		return nil, fmt.Errorf("avro: %w", err)
	}
	rec, ok := plain(schema, raw).(map[string]any)
	if !ok {
		return nil, errors.New("payload is not a record")
	}
	id := ce.Event.ID
	if id == "" {
		id = hex.EncodeToString(ce.ReplayID)
	}
	header, isChange := rec["ChangeEventHeader"].(map[string]any)
	if !isChange {
		dropNulls(rec, nil)
		if !matches(rec, sub.Match) {
			return nil, nil
		}
		payload, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		return []connector.Event{{ID: id, Name: event, Payload: payload}}, nil
	}
	top := schema.(*avro.RecordSchema)
	for _, key := range []string{"changedFields", "nulledFields", "diffFields"} {
		if list, ok := header[key].([]any); ok {
			header[key] = bitmapFields(top, list)
		}
	}
	changeType, _ := header["changeType"].(string)
	if len(sub.ChangeTypes) > 0 && !containsFold(sub.ChangeTypes, changeType) {
		return nil, nil
	}
	nulled, _ := header["nulledFields"].([]string)
	dropNulls(rec, nulled)
	if !matches(rec, sub.Match) {
		return nil, nil
	}
	ids, _ := header["recordIds"].([]any)
	entity, _ := header["entityName"].(string)
	var out []connector.Event
	for i, rid := range ids {
		r := make(map[string]any, len(rec)+1)
		for k, v := range rec {
			r[k] = v
		}
		r["Id"] = rid
		if id, _ := rid.(string); len(sub.Fields) > 0 && changeType != "DELETE" && !strings.HasPrefix(changeType, "GAP_") {
			current, err := c.fetch(ctx, entity, id, sub.Fields)
			if err != nil {
				return nil, err
			}
			if current != nil {
				current["ChangeEventHeader"] = header
				r = current
			}
		}
		payload, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		evID := id
		if len(ids) > 1 {
			evID = id + "." + strconv.Itoa(i)
		}
		out = append(out, connector.Event{ID: evID, Name: event, Payload: payload})
	}
	return out, nil
}

// topic describes a topic through the Pub/Sub API.
func (c *Conn) topic(ctx context.Context, name string) (pubsub.TopicInfo, error) {
	cc, err := c.pubsubConn()
	if err != nil {
		return pubsub.TopicInfo{}, err
	}
	token, instance, org, err := c.sess.identity(ctx)
	if err != nil {
		return pubsub.TopicInfo{}, err
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("accesstoken", token, "instanceurl", instance, "tenantid", org))
	req := pubsub.TopicRequest{TopicName: name}.Encode()
	var out pubsub.Frame
	if err := cc.Invoke(ctx, pubsub.GetTopic, &req, &out); err != nil {
		return pubsub.TopicInfo{}, c.rpcErr(token, err)
	}
	return pubsub.DecodeTopicInfo(out)
}

func or(s, def string) string {
	if s != "" {
		return s
	}
	return def
}

// fetch reads a record's current fields; nil if it no longer exists.
func (c *Conn) fetch(ctx context.Context, sobject, id string, fields []string) (map[string]any, error) {
	if !identRE.MatchString(sobject) || !idRE.MatchString(id) {
		return nil, fmt.Errorf("invalid record %s %q", sobject, id)
	}
	fields = append([]string{}, fields...)
	if !containsFold(fields, "Id") {
		fields = append(fields, "Id")
	}
	soql := fmt.Sprintf("SELECT %s FROM %s WHERE (Id = '%s')", strings.Join(fields, ", "), sobject, id)
	var res struct {
		Records []map[string]any `json:"records"`
	}
	if err := c.do(ctx, http.MethodGet, c.base()+"/query?q="+url.QueryEscape(soql), nil, &res); err != nil {
		return nil, err
	}
	if len(res.Records) == 0 {
		return nil, nil
	}
	rec := res.Records[0]
	stripAttributes(rec)
	return rec, nil
}

// plain converts a decoded Avro value to plain JSON values: a union holding
// a record, map or array arrives wrapped as {"TypeName": value}.
func plain(s avro.Schema, v any) any {
	switch s := s.(type) {
	case *avro.UnionSchema:
		if v == nil {
			return nil
		}
		if m, ok := v.(map[string]any); ok && len(m) == 1 {
			for name, inner := range m {
				for _, t := range s.Types() {
					if typeName(t) == name {
						return plain(t, inner)
					}
				}
			}
		}
		for _, t := range s.Types() {
			if t.Type() != avro.Null {
				if _, isRec := t.(*avro.RecordSchema); !isRec {
					return plain(t, v)
				}
			}
		}
		return v
	case *avro.RecordSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		for _, f := range s.Fields() {
			if fv, ok := m[f.Name()]; ok {
				m[f.Name()] = plain(f.Type(), fv)
			}
		}
		return m
	case *avro.ArraySchema:
		if a, ok := v.([]any); ok {
			for i := range a {
				a[i] = plain(s.Items(), a[i])
			}
		}
		return v
	case *avro.MapSchema:
		if m, ok := v.(map[string]any); ok {
			for k := range m {
				m[k] = plain(s.Values(), m[k])
			}
		}
		return v
	case *avro.PrimitiveSchema:
		if t, ok := v.(time.Time); ok {
			if l := s.Logical(); l != nil && l.Type() == avro.Date {
				return t.UTC().Format("2006-01-02")
			}
			return t.UTC().Format(time.RFC3339Nano)
		}
	}
	return v
}

func typeName(s avro.Schema) string {
	if n, ok := s.(avro.NamedSchema); ok {
		return n.FullName()
	}
	return string(s.Type())
}

// bitmapFields turns Salesforce's field bitmaps into field names: "0x..."
// sets bit i for the record's i-th field, and "p-0x..." the fields of the
// compound field at position p (Name.FirstName).
func bitmapFields(top *avro.RecordSchema, list []any) []string {
	names := []string{}
	fields := top.Fields()
	for _, item := range list {
		s, _ := item.(string)
		parent, bitmap, nested := strings.Cut(s, "-")
		if !nested {
			bitmap, parent = parent, ""
		}
		n, ok := new(big.Int).SetString(strings.TrimPrefix(strings.ToLower(bitmap), "0x"), 16)
		if !ok {
			continue
		}
		within, prefix := fields, ""
		if nested {
			p, err := strconv.Atoi(parent)
			if err != nil || p < 0 || p >= len(fields) {
				continue
			}
			rec := recordOf(fields[p].Type())
			if rec == nil {
				continue
			}
			within, prefix = rec.Fields(), fields[p].Name()+"."
		}
		for i := range within {
			if n.Bit(i) == 1 {
				names = append(names, prefix+within[i].Name())
			}
		}
	}
	return names
}

func recordOf(s avro.Schema) *avro.RecordSchema {
	switch s := s.(type) {
	case *avro.RecordSchema:
		return s
	case *avro.UnionSchema:
		for _, t := range s.Types() {
			if r, ok := t.(*avro.RecordSchema); ok {
				return r
			}
		}
	}
	return nil
}

// dropNulls removes null fields, which in a change event are fields the
// change left alone, keeping those it set to null.
func dropNulls(rec map[string]any, nulled []string) {
	keep := map[string]bool{}
	for _, f := range nulled {
		keep[strings.SplitN(f, ".", 2)[0]] = true
	}
	for k, v := range rec {
		if v == nil && !keep[k] {
			delete(rec, k)
		}
	}
}

func matches(rec map[string]any, want map[string]any) bool {
	for k, v := range want {
		got, ok := rec[k]
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

// schemaCache is embedded in Conn.
type schemaCache struct {
	psMu    sync.Mutex
	ps      *grpc.ClientConn
	schemas map[string]avro.Schema
}
