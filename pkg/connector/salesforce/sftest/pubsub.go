package sftest

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/pubsub"
)

// OpportunityChangeSchema is the Avro schema of the fake org's
// /data/OpportunityChangeEvent, shaped like Salesforce's: a change event
// header, then every field as a nullable union.
const OpportunityChangeSchema = `{
  "type": "record", "name": "OpportunityChangeEvent", "namespace": "com.sforce.eventbus",
  "fields": [
    {"name": "ChangeEventHeader", "type": {"type": "record", "name": "ChangeEventHeader", "fields": [
      {"name": "entityName", "type": "string"},
      {"name": "recordIds", "type": {"type": "array", "items": "string"}},
      {"name": "changeType", "type": {"type": "enum", "name": "ChangeType", "symbols": ["CREATE", "UPDATE", "DELETE", "UNDELETE", "GAP_CREATE", "GAP_UPDATE", "GAP_DELETE", "GAP_UNDELETE", "GAP_OVERFLOW"]}},
      {"name": "changeOrigin", "type": "string"},
      {"name": "transactionKey", "type": "string"},
      {"name": "sequenceNumber", "type": "int"},
      {"name": "commitTimestamp", "type": "long"},
      {"name": "commitNumber", "type": "long"},
      {"name": "commitUser", "type": "string"},
      {"name": "nulledFields", "type": {"type": "array", "items": "string"}},
      {"name": "diffFields", "type": {"type": "array", "items": "string"}},
      {"name": "changedFields", "type": {"type": "array", "items": "string"}}
    ]}},
    {"name": "Name", "type": ["null", "string"], "default": null},
    {"name": "AccountId", "type": ["null", "string"], "default": null},
    {"name": "Amount", "type": ["null", "double"], "default": null},
    {"name": "CurrencyIsoCode", "type": ["null", "string"], "default": null},
    {"name": "StageName", "type": ["null", "string"], "default": null},
    {"name": "CloseDate", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
    {"name": "Description", "type": ["null", "string"], "default": null},
    {"name": "LastModifiedDate", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null}
  ]
}`

type psEvent struct {
	replay []byte
	event  pubsub.ProducerEvent
}

type pubSub struct {
	lis     net.Listener
	srv     *grpc.Server
	mu      sync.Mutex
	topics  map[string][]psEvent
	schemas map[string]avro.Schema
	json    map[string]string
	// subscribers counts open Subscribe streams.
	subscribers int
	fetches     []pubsub.FetchRequest
}

// PubSub starts the fake Pub/Sub API on a loopback port (once) and returns
// its address, for a connection's pubsubEndpoint.
func (s *Server) PubSub() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return s.ps.lis.Addr().String()
	}
	addr := s.PubSubAddr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	ps := &pubSub{lis: lis, topics: map[string][]psEvent{}, schemas: map[string]avro.Schema{}, json: map[string]string{}}
	ps.addSchema("opp-schema-1", OpportunityChangeSchema)
	ps.srv = grpc.NewServer(grpc.ForceServerCodec(pubsub.Codec{}))
	ps.srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: pubsub.Service,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "GetSchema", Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				return s.getSchema(ctx, dec)
			}},
			{MethodName: "GetTopic", Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				return s.getTopic(ctx, dec)
			}},
		},
		Streams: []grpc.StreamDesc{{StreamName: "Subscribe", ServerStreams: true, ClientStreams: true, Handler: func(_ any, st grpc.ServerStream) error {
			return s.subscribe(st)
		}}},
	}, struct{}{})
	go func() { _ = ps.srv.Serve(lis) }()
	s.ps = ps
	return lis.Addr().String()
}

func (ps *pubSub) addSchema(id, js string) {
	ps.schemas[id] = avro.MustParse(js)
	ps.json[id] = js
}

// ClosePubSub stops the fake Pub/Sub API.
func (s *Server) ClosePubSub() {
	s.mu.Lock()
	ps := s.ps
	s.mu.Unlock()
	if ps != nil {
		ps.srv.Stop()
	}
}

// Change describes a change event to publish.
type Change struct {
	Type      string // CREATE, UPDATE, ...
	RecordIDs []string
	// Fields are the Opportunity fields the change sets; Nulled lists the
	// ones it set to null.
	Fields map[string]any
	Nulled []string
}

var oppFields = []string{"ChangeEventHeader", "Name", "AccountId", "Amount", "CurrencyIsoCode", "StageName", "CloseDate", "Description", "LastModifiedDate"}

// bitmap sets bit i for each named field's position.
func bitmap(names []string) string {
	var n uint64
	for i, f := range oppFields {
		for _, name := range names {
			if f == name {
				n |= 1 << i
			}
		}
	}
	return fmt.Sprintf("0x%X", n)
}

// PublishChange publishes an Opportunity change event and returns its
// replay ID.
func (s *Server) PublishChange(c Change) []byte {
	s.PubSub()
	changed := []string{}
	for f := range c.Fields {
		changed = append(changed, f)
	}
	changed = append(changed, c.Nulled...)
	rec := map[string]any{
		"ChangeEventHeader": map[string]any{
			"entityName": "Opportunity", "recordIds": c.RecordIDs, "changeType": c.Type, "changeOrigin": "com/salesforce/api/rest/61.0",
			"transactionKey": "tx-" + randomHex(4), "sequenceNumber": 1, "commitTimestamp": time.Now().UnixMilli(), "commitNumber": int64(1),
			"commitUser": "005000000000001AAA", "nulledFields": []string{}, "diffFields": []string{}, "changedFields": []string{bitmap(changed)},
		},
	}
	if len(c.Nulled) > 0 {
		rec["ChangeEventHeader"].(map[string]any)["nulledFields"] = []string{bitmap(c.Nulled)}
	}
	for f, v := range c.Fields {
		switch v := v.(type) {
		case string:
			if f == "CloseDate" {
				t, err := time.Parse("2006-01-02", v)
				if err != nil {
					panic(err)
				}
				rec[f] = map[string]any{"int.date": t}
			} else {
				rec[f] = map[string]any{"string": v}
			}
		case float64:
			rec[f] = map[string]any{"double": v}
		case time.Time:
			rec[f] = map[string]any{"long.timestamp-millis": v}
		default:
			panic(fmt.Sprintf("sftest: unsupported field %s %T", f, v))
		}
	}
	payload, err := avro.Marshal(s.ps.schemas["opp-schema-1"], rec)
	if err != nil {
		panic(err)
	}
	return s.publish("/data/OpportunityChangeEvent", pubsub.ProducerEvent{ID: randomUUID(), SchemaID: "opp-schema-1", Payload: payload})
}

func (s *Server) publish(topic string, ev pubsub.ProducerEvent) []byte {
	s.ps.mu.Lock()
	defer s.ps.mu.Unlock()
	// Replay IDs are opaque; nine bytes here, so no one can take them for
	// a number.
	replay := append([]byte{0x5f}, randomBytes(8)...)
	s.ps.topics[topic] = append(s.ps.topics[topic], psEvent{replay: replay, event: ev})
	return replay
}

// Expire drops a topic's oldest events, as Salesforce does after its
// retention period.
func (s *Server) Expire(topic string, n int) {
	s.PubSub()
	s.ps.mu.Lock()
	defer s.ps.mu.Unlock()
	evs := s.ps.topics[topic]
	if n > len(evs) {
		n = len(evs)
	}
	s.ps.topics[topic] = evs[n:]
}

// Subscribers returns how many Subscribe streams are open.
func (s *Server) Subscribers() int {
	s.PubSub()
	s.ps.mu.Lock()
	defer s.ps.mu.Unlock()
	return s.ps.subscribers
}

// Fetches returns the first FetchRequest of every stream so far.
func (s *Server) Fetches() []pubsub.FetchRequest {
	s.PubSub()
	s.ps.mu.Lock()
	defer s.ps.mu.Unlock()
	return append([]pubsub.FetchRequest{}, s.ps.fetches...)
}

// KeepAlive is how long a subscription without events waits before an
// empty response carrying the latest replay ID (Salesforce: 270s).
var KeepAlive = 270 * time.Second

func (s *Server) authenticate(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	s.mu.Lock()
	ok := s.tokens[get("accesstoken")]
	s.mu.Unlock()
	if !ok || get("instanceurl") != s.URL || get("tenantid") != OrgID {
		return status.Error(codes.Unauthenticated, "Invalid session ID or tenant")
	}
	return nil
}

func (s *Server) getSchema(ctx context.Context, dec func(any) error) (any, error) {
	if err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	var in pubsub.Frame
	if err := dec(&in); err != nil {
		return nil, err
	}
	req, err := pubsub.DecodeSchemaRequest(in)
	if err != nil {
		return nil, err
	}
	s.ps.mu.Lock()
	js, ok := s.ps.json[req.SchemaID]
	s.ps.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "no schema "+req.SchemaID)
	}
	out := pubsub.SchemaInfo{SchemaJSON: js, SchemaID: req.SchemaID}.Encode()
	return &out, nil
}

// NoSubscribe lists topics the integration user may not subscribe to.
var NoSubscribe = map[string]bool{}

func (s *Server) getTopic(ctx context.Context, dec func(any) error) (any, error) {
	if err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	var in pubsub.Frame
	if err := dec(&in); err != nil {
		return nil, err
	}
	req, err := pubsub.DecodeTopicRequest(in)
	if err != nil {
		return nil, err
	}
	s.ps.mu.Lock()
	_, known := s.ps.topics[req.TopicName]
	s.ps.mu.Unlock()
	if !known && req.TopicName != "/data/OpportunityChangeEvent" {
		return nil, status.Error(codes.NotFound, "Topic not found: "+req.TopicName)
	}
	out := pubsub.TopicInfo{TopicName: req.TopicName, CanSubscribe: !NoSubscribe[req.TopicName], SchemaID: "opp-schema-1"}.Encode()
	return &out, nil
}

func (s *Server) subscribe(st grpc.ServerStream) error {
	if err := s.authenticate(st.Context()); err != nil {
		return err
	}
	var in pubsub.Frame
	if err := st.RecvMsg(&in); err != nil {
		return err
	}
	first, err := pubsub.DecodeFetchRequest(in)
	if err != nil {
		return err
	}
	ps := s.ps
	ps.mu.Lock()
	ps.fetches = append(ps.fetches, first)
	evs, known := ps.topics[first.TopicName]
	if !known && first.TopicName != "/data/OpportunityChangeEvent" {
		ps.mu.Unlock()
		return status.Error(codes.NotFound, "Topic not found: "+first.TopicName)
	}
	next := 0
	switch first.ReplayPreset {
	case pubsub.Latest:
		next = len(evs)
	case pubsub.Custom:
		next = -1
		for i, e := range evs {
			if bytes.Equal(e.replay, first.ReplayID) {
				next = i + 1
			}
		}
		if next < 0 {
			ps.mu.Unlock()
			return status.Error(codes.InvalidArgument, "The Replay ID validation failed.")
		}
	}
	// Positions are kept by replay ID, since Expire shifts indexes.
	var after []byte
	if next > 0 {
		after = evs[next-1].replay
	}
	ps.subscribers++
	ps.mu.Unlock()
	defer func() {
		ps.mu.Lock()
		ps.subscribers--
		ps.mu.Unlock()
	}()

	var credMu sync.Mutex
	credits := int(first.NumRequested)
	errc := make(chan error, 1)
	go func() {
		for {
			var in pubsub.Frame
			if err := st.RecvMsg(&in); err != nil {
				errc <- err
				return
			}
			r, err := pubsub.DecodeFetchRequest(in)
			if err != nil {
				errc <- err
				return
			}
			credMu.Lock()
			credits += int(r.NumRequested)
			credMu.Unlock()
		}
	}()
	idle := time.Now()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-errc:
			return nil
		case <-st.Context().Done():
			return nil
		case <-tick.C:
		}
		ps.mu.Lock()
		evs := ps.topics[first.TopicName]
		start := 0
		if after != nil {
			start = -1
			for i, e := range evs {
				if bytes.Equal(e.replay, after) {
					start = i + 1
				}
			}
			if start < 0 { // expired while subscribed: continue at the oldest kept
				start = 0
			}
		}
		credMu.Lock()
		n := min(len(evs)-start, credits, 3)
		credits -= max(n, 0)
		credMu.Unlock()
		var resp pubsub.FetchResponse
		if n > 0 {
			for _, e := range evs[start : start+n] {
				resp.Events = append(resp.Events, pubsub.ConsumerEvent{Event: e.event, ReplayID: e.replay})
			}
			after = evs[start+n-1].replay
			resp.LatestReplayID = after
		} else if time.Since(idle) >= KeepAlive && len(evs) > 0 {
			resp.LatestReplayID = evs[len(evs)-1].replay
			after = resp.LatestReplayID
		}
		ps.mu.Unlock()
		if resp.LatestReplayID == nil {
			continue
		}
		idle = time.Now()
		out := resp.Encode()
		if err := st.SendMsg(&out); err != nil {
			return err
		}
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func randomHex(n int) string { return fmt.Sprintf("%x", randomBytes(n)) }

func randomUUID() string {
	b := randomBytes(16)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
