// Package remote runs connectors outside the worker, over the connector
// protocol (proto/turgon/connector/v1): a Camel/Java connector in a
// sidecar container, say. The worker keeps policy, approval, idempotency,
// rate limits and audit; the connector only talks to its system.
//
// The sidecar listens on a Unix socket in a volume only its pod mounts
// (unix:///var/run/turgon/connectors/sap-ecc.sock), or on a TCP address
// with a shared token (TURGON_CONNECTOR_TOKEN): whoever reaches a sidecar
// can write to its system, so it must be the worker.
package remote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	pb "github.com/fduser123-coding/turgon/pkg/connector/remote/connectorpb"
	"github.com/fduser123-coding/turgon/pkg/meta"
	"github.com/fduser123-coding/turgon/pkg/semver"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Capabilities a connector can declare beyond Commit.
const (
	CapSimulate = "simulate"
	CapConfirm  = "confirm"
	CapRead     = "read"
	CapPoll     = "poll"
	CapExport   = "export"
	CapCheck    = "check"
	CapStream   = "stream"
	CapDiscover = "discover"
)

// StartTimeout is how long configuring an instance waits for its sidecar.
var StartTimeout = 60 * time.Second

// ErrUnsupported means the connector does not implement a call.
var ErrUnsupported = errors.New("the connector does not support this")

// Address is where a connector's sidecar listens, and the token it wants.
type Address struct {
	// Target is a gRPC target: unix:///path/to.sock or host:port.
	Target string
	Token  string
}

// ParseAddresses reads TURGON_CONNECTORS: "name=target,name=target", such
// as "sap-ecc=unix:///var/run/turgon/connectors/sap-ecc.sock".
func ParseAddresses(s, token string) (map[string]Address, error) {
	out := map[string]Address{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, target, ok := strings.Cut(part, "=")
		name, target = strings.TrimSpace(name), strings.TrimSpace(target)
		if !ok || name == "" || target == "" {
			return nil, fmt.Errorf("%q: want name=address, such as sap-ecc=unix:///var/run/turgon/connectors/sap-ecc.sock", part)
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("connector %s is listed twice", name)
		}
		// Whoever reaches a sidecar can write to its system without the
		// worker's governance: over TCP, even loopback, only with the token.
		if !strings.HasPrefix(target, "unix://") && token == "" {
			return nil, fmt.Errorf("connector %s at %s: a TCP address needs TURGON_CONNECTOR_TOKEN (or use a unix:// socket)", name, target)
		}
		out[name] = Address{Target: target, Token: token}
	}
	return out, nil
}

// Pool shares one connection per sidecar between the instances built on
// it, across secret rotations.
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPool returns an empty pool.
func NewPool() *Pool { return &Pool{conns: map[string]*grpc.ClientConn{}} }

func (p *Pool) conn(target string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[target]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		return nil, err
	}
	p.conns[target] = c
	return c, nil
}

// Close closes every connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for t, c := range p.conns {
		_ = c.Close()
		delete(p.conns, t)
	}
}

// Factory builds instances of the connector at addr.
func (p *Pool) Factory(addr Address) connector.Factory {
	return func(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
		return p.configure(ctx, addr, cfg, secrets)
	}
}

func (p *Pool) configure(ctx context.Context, addr Address, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	cc, err := p.conn(addr.Target)
	if err != nil {
		return nil, fmt.Errorf("endpoint %s: connector %s at %s: %w", cfg.Endpoint, cfg.Name, addr.Target, err)
	}
	base := &instance{client: pb.NewConnectorClient(cc), token: addr.Token, endpoint: cfg.Endpoint, connector: cfg.Name, target: addr.Target}
	ctx, cancel := context.WithTimeout(ctx, StartTimeout)
	defer cancel()
	// The sidecar may still be starting (it shares the pod): wait for it.
	desc, err := base.client.Describe(base.out(ctx), &pb.DescribeRequest{Protocol: pb.Protocol_PROTOCOL_V1}, grpc.WaitForReady(true))
	if err != nil {
		return nil, base.wrap("describe", err)
	}
	if desc.GetProtocol() != pb.Protocol_PROTOCOL_V1 {
		return nil, fmt.Errorf("endpoint %s: the sidecar at %s speaks connector protocol %d, this worker 1", cfg.Endpoint, addr.Target, desc.GetProtocol())
	}
	if desc.GetConnector() != cfg.Name {
		return nil, fmt.Errorf("endpoint %s: the sidecar at %s runs connector %q, the spec needs %q", cfg.Endpoint, addr.Target, desc.GetConnector(), cfg.Name)
	}
	if err := compatible(cfg.Version, desc.GetVersion()); err != nil {
		return nil, fmt.Errorf("endpoint %s: connector %s: %w", cfg.Endpoint, cfg.Name, err)
	}
	base.caps = map[string]bool{}
	for _, c := range desc.GetCapabilities() {
		base.caps[c] = true
	}
	base.version = desc.GetVersion()

	req := &pb.ConfigureRequest{Endpoint: cfg.Endpoint, Secrets: map[string]string{}}
	if cfg.SecretRef != "" {
		v, err := secrets.Resolve(ctx, cfg.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("endpoint %s: %w", cfg.Endpoint, err)
		}
		req.Secret = v
	}
	for _, ref := range connector.ConfigSecretRefs(cfg.Config) {
		v, err := secrets.Resolve(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("endpoint %s: %w", cfg.Endpoint, err)
		}
		req.Secrets[ref] = v
	}
	if req.Spec, err = json.Marshal(cfg); err != nil {
		return nil, err
	}
	var id [8]byte
	_, _ = rand.Read(id[:])
	req.Instance = cfg.Endpoint + "-" + hex.EncodeToString(id[:])
	base.id, base.configure = req.Instance, req
	resp, err := base.client.Configure(base.out(ctx), req)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
			return nil, fmt.Errorf("endpoint %s: connector %s refused its configuration: %s", cfg.Endpoint, cfg.Name, st.Message())
		}
		return nil, base.wrap("configure", err)
	}
	base.streamed = map[string]bool{}
	for _, e := range resp.GetStreamedEvents() {
		base.streamed[e] = true
	}
	if base.caps[CapPoll] || (base.caps[CapStream] && len(base.streamed) > 0) {
		return &pollingInstance{base}, nil
	}
	return base, nil
}

// compatible accepts a sidecar whose version satisfies ^want: the same
// major version (minor for 0.x), and no older.
func compatible(want, got string) error {
	if want == "" {
		return nil
	}
	c, err := semver.ParseConstraint("^" + want)
	if err != nil {
		return err
	}
	v, err := semver.Parse(got)
	if err != nil {
		return fmt.Errorf("the sidecar reports version %q: %w", got, err)
	}
	if !c.Check(v) {
		return fmt.Errorf("the sidecar runs version %s, the spec was compiled for ^%s", got, want)
	}
	return nil
}

// instance is one configured endpoint on a sidecar. It implements the
// write guard's Target, Reader and Confirmer, and the connector Checker
// and Exporter, reporting what the connector does not support.
type instance struct {
	client    pb.ConnectorClient
	token     string
	id        string
	endpoint  string
	connector string
	version   string
	target    string
	caps      map[string]bool
	closeOnce sync.Once
	// configure is the request that made the instance; a restarted
	// sidecar is configured again with it.
	configure *pb.ConfigureRequest
	// streamed are the events the system pushes (Stream, not Poll).
	streamed map[string]bool
}

// pollingInstance is an instance whose connector emits events: polled,
// or pushed over Stream.
type pollingInstance struct{ *instance }

var (
	_ connector.Instance   = (*instance)(nil)
	_ connector.Checker    = (*instance)(nil)
	_ connector.Exporter   = (*instance)(nil)
	_ writeguard.Reader    = (*instance)(nil)
	_ writeguard.Confirmer = (*instance)(nil)
	_ connector.Discoverer = (*instance)(nil)
	_ connector.Source     = (*pollingInstance)(nil)
	_ connector.Streamer   = (*pollingInstance)(nil)
)

func (x *instance) out(ctx context.Context) context.Context {
	if x.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+x.token)
}

// call runs one request, configuring the instance again if the sidecar
// lost it (restarted) and repeating the request once.
func call[R any](ctx context.Context, x *instance, f func(context.Context) (R, error)) (R, error) {
	r, err := f(x.out(ctx))
	if status.Code(err) != codes.Aborted {
		return r, err
	}
	if _, cerr := x.client.Configure(x.out(ctx), x.configure); cerr != nil {
		return r, cerr
	}
	return f(x.out(ctx))
}

// wrap turns a status into the errors the write guard and engine know.
func (x *instance) wrap(call string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("connector %s at %s: %s: %w", x.connector, x.target, call, err)
	}
	msg := st.Message()
	switch st.Code() {
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", writeguard.ErrInvalid, msg)
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", writeguard.ErrRejected, msg)
	case codes.NotFound:
		return fmt.Errorf("%w: %s", writeguard.ErrNotFound, msg)
	case codes.Unimplemented:
		return fmt.Errorf("%w: %s %s", ErrUnsupported, x.connector, call)
	case codes.Unauthenticated, codes.PermissionDenied:
		return fmt.Errorf("connector %s at %s refused the worker: %s (set the same TURGON_CONNECTOR_TOKEN on both)", x.connector, x.target, msg)
	}
	return fmt.Errorf("connector %s at %s: %s: %s: %s", x.connector, x.target, call, st.Code(), msg)
}

func (x *instance) Simulate(ctx context.Context, op string, payload json.RawMessage) (json.RawMessage, error) {
	if !x.caps[CapSimulate] {
		return nil, writeguard.ErrSimulationUnsupported
	}
	r, err := call(ctx, x, func(ctx context.Context) (*pb.WriteResponse, error) {
		return x.client.Simulate(ctx, &pb.WriteRequest{Instance: x.id, Operation: op, Payload: payload})
	})
	if err != nil {
		if errors.Is(x.wrap("simulate", err), ErrUnsupported) {
			return nil, writeguard.ErrSimulationUnsupported
		}
		return nil, x.wrap("simulate", err)
	}
	return r.GetResult(), nil
}

func (x *instance) Commit(ctx context.Context, op, key string, payload json.RawMessage) (json.RawMessage, error) {
	r, err := call(ctx, x, func(ctx context.Context) (*pb.WriteResponse, error) {
		return x.client.Commit(ctx, &pb.WriteRequest{Instance: x.id, Operation: op, IdempotencyKey: key, Payload: payload})
	})
	if err != nil {
		return nil, x.wrap("commit", err)
	}
	return r.GetResult(), nil
}

// Confirm reads a write back; a connector that cannot confirms nothing.
func (x *instance) Confirm(ctx context.Context, op string, result json.RawMessage) error {
	if !x.caps[CapConfirm] {
		return nil
	}
	if _, err := call(ctx, x, func(ctx context.Context) (*pb.ConfirmResponse, error) {
		return x.client.Confirm(ctx, &pb.ConfirmRequest{Instance: x.id, Operation: op, Result: result})
	}); err != nil {
		return x.wrap("confirm", err)
	}
	return nil
}

func (x *instance) Read(ctx context.Context, op, id string) (json.RawMessage, error) {
	if !x.caps[CapRead] {
		return nil, fmt.Errorf("%w: %s reads", ErrUnsupported, x.connector)
	}
	r, err := call(ctx, x, func(ctx context.Context) (*pb.ReadResponse, error) {
		return x.client.Read(ctx, &pb.ReadRequest{Instance: x.id, Operation: op, Id: id})
	})
	if err != nil {
		return nil, x.wrap("read", err)
	}
	return r.GetRecord(), nil
}

func (p *pollingInstance) Poll(ctx context.Context, event string, after int64, limit int) ([]connector.Event, error) {
	if !p.caps[CapPoll] {
		return nil, fmt.Errorf("%w: %s polls no events (%s arrives by stream)", ErrUnsupported, p.connector, event)
	}
	r, err := call(ctx, p.instance, func(ctx context.Context) (*pb.PollResponse, error) {
		return p.client.Poll(ctx, &pb.PollRequest{Instance: p.id, Event: event, After: after, Limit: int32(limit)})
	})
	if err != nil {
		return nil, p.wrap("poll", err)
	}
	out := make([]connector.Event, 0, len(r.GetEvents()))
	last := after
	for _, e := range r.GetEvents() {
		if e.GetPosition() <= last {
			return nil, fmt.Errorf("connector %s: poll returned position %d after %d: positions must increase", p.connector, e.GetPosition(), last)
		}
		last = e.GetPosition()
		out = append(out, connector.Event{ID: e.GetId(), Position: e.GetPosition(), Name: e.GetName(), Payload: e.GetPayload()})
	}
	if len(out) > limit {
		return nil, fmt.Errorf("connector %s: poll returned %d events, asked for %d", p.connector, len(out), limit)
	}
	return out, nil
}

func (x *instance) Export(ctx context.Context, name string, each func(map[string]string) error) (int, error) {
	if !x.caps[CapExport] {
		return 0, fmt.Errorf("%w: %s exports", ErrUnsupported, x.connector)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := call(ctx, x, func(ctx context.Context) (grpc.ServerStreamingClient[pb.ExportRecord], error) {
		return x.client.Export(ctx, &pb.ExportRequest{Instance: x.id, Name: name})
	})
	if err != nil {
		return 0, x.wrap("export", err)
	}
	n := 0
	for {
		rec, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, x.wrap("export", err)
		}
		if err := each(rec.GetFields()); err != nil {
			return n, err
		}
		n++
	}
}

// Check reports the sidecar and, if the connector can, its own checks.
func (x *instance) Check(ctx context.Context) []connector.CheckResult {
	caps := make([]string, 0, len(x.caps))
	for c := range x.caps {
		caps = append(caps, c)
	}
	sort.Strings(caps)
	out := []connector.CheckResult{connector.Pass("sidecar", fmt.Sprintf("%s %s at %s (%s)", x.connector, x.version, x.target, strings.Join(caps, ", ")))}
	if !x.caps[CapCheck] {
		return out
	}
	r, err := call(ctx, x, func(ctx context.Context) (*pb.CheckResponse, error) {
		return x.client.Check(ctx, &pb.InstanceRequest{Instance: x.id})
	})
	if err != nil {
		return append(out, connector.Fail("check", x.wrap("check", err).Error(), "See the sidecar's log."))
	}
	for _, c := range r.GetResults() {
		out = append(out, connector.CheckResult{Name: c.GetName(), OK: c.GetOk(), Detail: c.GetDetail(), Fix: c.GetFix()})
	}
	return out
}

// Close releases the instance on the sidecar; the connection stays for
// the pool's other instances.
func (x *instance) Close() {
	x.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = x.client.Release(x.out(ctx), &pb.InstanceRequest{Instance: x.id})
	})
}

// Streams reports whether event arrives over Stream.
func (p *pollingInstance) Streams(event string) bool { return p.caps[CapStream] && p.streamed[event] }

// Stream subscribes to a pushed event. Each batch is delivered (the
// runtime stores it in its inbox) before it is acknowledged; only then
// does the connector confirm it to the system.
func (p *pollingInstance) Stream(ctx context.Context, event string, resume []byte, deliver func([]connector.Event, []byte) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	open := func(ctx context.Context) (grpc.BidiStreamingClient[pb.StreamRequest, pb.StreamBatch], *pb.StreamBatch, error) {
		st, err := p.client.Stream(ctx)
		if err != nil {
			return nil, nil, err
		}
		if err := st.Send(&pb.StreamRequest{Request: &pb.StreamRequest_Open{Open: &pb.StreamOpen{Instance: p.id, Event: event, Resume: resume}}}); err != nil {
			return nil, nil, err
		}
		first, err := st.Recv()
		return st, first, err
	}
	st, batch, err := open(p.out(ctx))
	if status.Code(err) == codes.Aborted { // the sidecar restarted: configure again
		if _, cerr := p.client.Configure(p.out(ctx), p.configure); cerr != nil {
			return p.wrap("stream", cerr)
		}
		st, batch, err = open(p.out(ctx))
	}
	for {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("connector %s: the stream of %s ended", p.connector, event)
		}
		if err != nil {
			return p.wrap("stream", err)
		}
		events := make([]connector.Event, 0, len(batch.GetEvents()))
		for _, e := range batch.GetEvents() {
			if e.GetId() == "" {
				return fmt.Errorf("connector %s: a streamed %s has no ID", p.connector, event)
			}
			events = append(events, connector.Event{ID: e.GetId(), Position: e.GetPosition(), Name: e.GetName(), Payload: e.GetPayload()})
		}
		if err := deliver(events, batch.GetResume()); err != nil {
			return err // not acknowledged: the system delivers it again
		}
		if err := st.Send(&pb.StreamRequest{Request: &pb.StreamRequest_Ack{Ack: &pb.StreamAck{Batch: batch.GetBatch()}}}); err != nil {
			return p.wrap("stream", err)
		}
		batch, err = st.Recv()
	}
}

// Discover asks the connector what its system holds.
func (x *instance) Discover(ctx context.Context, objects []string) (meta.Catalog, error) {
	if !x.caps[CapDiscover] {
		return meta.Catalog{}, fmt.Errorf("%w: %s discovery", ErrUnsupported, x.connector)
	}
	r, err := call(ctx, x, func(ctx context.Context) (*pb.DiscoverResponse, error) {
		return x.client.Discover(ctx, &pb.DiscoverRequest{Instance: x.id, Objects: objects})
	})
	if err != nil {
		return meta.Catalog{}, x.wrap("discover", err)
	}
	var cat meta.Catalog
	if err := json.Unmarshal(r.GetCatalog(), &cat); err != nil {
		return meta.Catalog{}, fmt.Errorf("connector %s: the catalog is not valid JSON: %w", x.connector, err)
	}
	cat.Endpoint, cat.Connector, cat.Version = x.endpoint, x.connector, x.version
	cat.DiscoveredAt = time.Now().UTC()
	cat.Normalize()
	return cat, nil
}
