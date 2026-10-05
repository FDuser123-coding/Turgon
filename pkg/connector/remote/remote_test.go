package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	pb "github.com/fduser123-coding/turgon/pkg/connector/remote/connectorpb"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// fakeConnector is a sidecar for tests: an order system with one
// customer, which refuses orders for others.
type fakeConnector struct {
	pb.UnimplementedConnectorServer
	name, version string
	caps          []string
	token         string

	mu         sync.Mutex
	acks       []uint64
	configured map[string]*pb.ConfigureRequest
	released   []string
	orders     map[string]string // idempotency key -> order number
}

func (f *fakeConnector) auth(ctx context.Context) error {
	if f.token == "" {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("authorization"); len(v) == 1 && v[0] == "Bearer "+f.token {
		return nil
	}
	return status.Error(codes.Unauthenticated, "a bearer token is required")
}

func (f *fakeConnector) instance(ctx context.Context, id string) error {
	if err := f.auth(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.configured[id] == nil {
		return status.Errorf(codes.Aborted, "instance %s is not configured", id)
	}
	return nil
}

func (f *fakeConnector) Describe(ctx context.Context, _ *pb.DescribeRequest) (*pb.DescribeResponse, error) {
	if err := f.auth(ctx); err != nil {
		return nil, err
	}
	return &pb.DescribeResponse{Protocol: pb.Protocol_PROTOCOL_V1, Connector: f.name, Version: f.version, Capabilities: f.caps}, nil
}

func (f *fakeConnector) Configure(ctx context.Context, r *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	if err := f.auth(ctx); err != nil {
		return nil, err
	}
	if r.GetSecret() != "user:pw" {
		return nil, status.Error(codes.InvalidArgument, "the secret is not user:password")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configured[r.GetInstance()] = r
	return &pb.ConfigureResponse{StreamedEvents: []string{"SalesOrder.Created"}}, nil
}

func (f *fakeConnector) Release(ctx context.Context, r *pb.InstanceRequest) (*pb.ReleaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.configured, r.GetInstance())
	f.released = append(f.released, r.GetInstance())
	return &pb.ReleaseResponse{}, nil
}

func (f *fakeConnector) Check(ctx context.Context, r *pb.InstanceRequest) (*pb.CheckResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	return &pb.CheckResponse{Results: []*pb.CheckResult{{Name: "rfc", Ok: true, Detail: "RFC_PING answered"}, {Name: "BAPI_SALESORDER_CREATEFROMDAT2", Ok: false, Detail: "not authorized", Fix: "grant S_RFC"}}}, nil
}

func order(payload []byte) (string, error) {
	var p struct {
		CustomerID string `json:"customerId"`
	}
	if json.Unmarshal(payload, &p) != nil || p.CustomerID == "" {
		return "", status.Error(codes.InvalidArgument, "customerId is required")
	}
	if p.CustomerID != "C-100" {
		return "", status.Error(codes.FailedPrecondition, "sold-to party is blocked for sales")
	}
	return p.CustomerID, nil
}

func (f *fakeConnector) Simulate(ctx context.Context, r *pb.WriteRequest) (*pb.WriteResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	if _, err := order(r.GetPayload()); err != nil {
		return nil, err
	}
	return &pb.WriteResponse{Result: []byte(`{"testRun":true,"netValue":"100.00"}`)}, nil
}

func (f *fakeConnector) Commit(ctx context.Context, r *pb.WriteRequest) (*pb.WriteResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	if _, err := order(r.GetPayload()); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.orders[r.GetIdempotencyKey()]
	if !ok {
		n = "000000" + string(rune('1'+len(f.orders)))
		f.orders[r.GetIdempotencyKey()] = n
	}
	return &pb.WriteResponse{Result: []byte(`{"salesOrder":"` + n + `"}`)}, nil
}

func (f *fakeConnector) Confirm(ctx context.Context, r *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	if !strings.Contains(string(r.GetResult()), "salesOrder") {
		return nil, status.Error(codes.NotFound, "the order is not there")
	}
	return &pb.ConfirmResponse{}, nil
}

func (f *fakeConnector) Read(ctx context.Context, r *pb.ReadRequest) (*pb.ReadResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	if r.GetId() != "C-100" {
		return nil, status.Error(codes.NotFound, "no such customer")
	}
	return &pb.ReadResponse{Record: []byte(`{"id":"C-100","name":"Ada Lovelace GmbH"}`)}, nil
}

func (f *fakeConnector) Poll(ctx context.Context, r *pb.PollRequest) (*pb.PollResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	var out []*pb.Event
	for pos := r.GetAfter() + 1; pos <= 3 && len(out) < int(r.GetLimit()); pos++ {
		out = append(out, &pb.Event{Id: "e" + string(rune('0'+pos)), Position: pos, Name: r.GetEvent(), Payload: []byte(`{}`)})
	}
	return &pb.PollResponse{Events: out}, nil
}

func (f *fakeConnector) Export(r *pb.ExportRequest, s grpc.ServerStreamingServer[pb.ExportRecord]) error {
	if err := f.instance(s.Context(), r.GetInstance()); err != nil {
		return err
	}
	for _, id := range []string{"C-100", "C-200"} {
		if err := s.Send(&pb.ExportRecord{Fields: map[string]string{"id": id}}); err != nil {
			return err
		}
	}
	return nil
}

func serve(t *testing.T, f *fakeConnector) string {
	t.Helper()
	if f.configured == nil {
		f.configured = map[string]*pb.ConfigureRequest{}
		f.orders = map[string]string{}
	}
	sock := filepath.Join(t.TempDir(), "c.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterConnectorServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return "unix://" + sock
}

var spec = compiler.ConnectorConfig{Endpoint: "sap-ecc", Name: "sap-ecc", Version: "0.4.0", Runtime: "camel-java",
	SecretRef: "openbao://sap/ecc/prod", Config: json.RawMessage(`{"client":"100","webhook":{"secretRef":"openbao://sap/ecc/hook"}}`)}

var secrets = connector.StaticSecrets{"openbao://sap/ecc/prod": "user:pw", "openbao://sap/ecc/hook": "whsec"}

func TestRemoteConnector(t *testing.T) {
	f := &fakeConnector{name: "sap-ecc", version: "0.4.2", token: "tok",
		caps: []string{CapSimulate, CapConfirm, CapRead, CapPoll, CapExport, CapCheck}}
	target := serve(t, f)
	pool := NewPool()
	defer pool.Close()
	ctx := context.Background()

	inst, err := pool.Factory(Address{Target: target, Token: "tok"})(ctx, spec, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	var got *pb.ConfigureRequest
	for _, r := range f.configured {
		got = r
	}
	if got == nil || got.GetEndpoint() != "sap-ecc" || got.GetSecrets()["openbao://sap/ecc/hook"] != "whsec" || !strings.HasPrefix(got.GetInstance(), "sap-ecc-") {
		t.Fatalf("configured %+v", got)
	}
	var sent compiler.ConnectorConfig
	if json.Unmarshal(got.GetSpec(), &sent) != nil || sent.Version != "0.4.0" || strings.Contains(string(got.GetSpec()), "user:pw") {
		t.Fatalf("spec %s", got.GetSpec())
	}

	payload := json.RawMessage(`{"customerId":"C-100"}`)
	prev, err := inst.Simulate(ctx, "create-sales-order", payload)
	if err != nil || !strings.Contains(string(prev), "testRun") {
		t.Fatalf("simulate %s %v", prev, err)
	}
	res, err := inst.Commit(ctx, "create-sales-order", "K1", payload)
	if err != nil || string(res) != `{"salesOrder":"0000001"}` {
		t.Fatalf("commit %s %v", res, err)
	}
	if again, _ := inst.Commit(ctx, "create-sales-order", "K1", payload); string(again) != string(res) {
		t.Fatalf("idempotency key not passed: %s", again)
	}
	if err := inst.(writeguard.Confirmer).Confirm(ctx, "create-sales-order", res); err != nil {
		t.Fatal(err)
	}
	if err := inst.(writeguard.Confirmer).Confirm(ctx, "create-sales-order", json.RawMessage(`{}`)); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("unconfirmed: %v", err)
	}

	if _, err := inst.Commit(ctx, "create-sales-order", "K2", json.RawMessage(`{}`)); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "customerId is required") {
		t.Fatalf("invalid: %v", err)
	}
	if _, err := inst.Commit(ctx, "create-sales-order", "K3", json.RawMessage(`{"customerId":"C-9"}`)); !errors.Is(err, writeguard.ErrRejected) {
		t.Fatalf("rejected: %v", err)
	}

	r := inst.(writeguard.Reader)
	if rec, err := r.Read(ctx, "get-customer", "C-100"); err != nil || !strings.Contains(string(rec), "Ada") {
		t.Fatalf("read %s %v", rec, err)
	}
	if _, err := r.Read(ctx, "get-customer", "C-9"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("read missing: %v", err)
	}

	src, ok := inst.(connector.Source)
	if !ok {
		t.Fatal("a polling connector is not a source")
	}
	evs, err := src.Poll(ctx, "SalesOrder.Created", 1, 10)
	if err != nil || len(evs) != 2 || evs[0].Position != 2 || evs[1].ID != "e3" {
		t.Fatalf("poll %+v %v", evs, err)
	}

	var ids []string
	n, err := inst.(connector.Exporter).Export(ctx, "customers", func(rec map[string]string) error {
		ids = append(ids, rec["id"])
		return nil
	})
	if err != nil || n != 2 || strings.Join(ids, ",") != "C-100,C-200" {
		t.Fatalf("export %d %v %v", n, ids, err)
	}

	checks := inst.(connector.Checker).Check(ctx)
	if len(checks) != 3 || !checks[0].OK || !strings.Contains(checks[0].Detail, "sap-ecc 0.4.2") || checks[2].OK || checks[2].Fix != "grant S_RFC" {
		t.Fatalf("checks %+v", checks)
	}

	// The sidecar restarts and forgets the instance: the next call
	// configures it again.
	f.mu.Lock()
	f.configured = map[string]*pb.ConfigureRequest{}
	f.mu.Unlock()
	if res, err := inst.Commit(ctx, "create-sales-order", "K1", payload); err != nil || string(res) != `{"salesOrder":"0000001"}` {
		t.Fatalf("after a restart: %s %v", res, err)
	}

	id := got.GetInstance()
	inst.Close()
	inst.Close()
	if len(f.released) != 1 || f.released[0] != id {
		t.Fatalf("released %v", f.released)
	}
}

func TestRemoteConnectorRefusals(t *testing.T) {
	ctx := context.Background()
	pool := NewPool()
	defer pool.Close()
	cases := []struct {
		name string
		f    *fakeConnector
		addr func(string) Address
		want string
	}{
		{"wrong connector", &fakeConnector{name: "shopify", version: "0.4.0"}, func(t string) Address { return Address{Target: t} }, `runs connector "shopify"`},
		{"too new", &fakeConnector{name: "sap-ecc", version: "0.5.0"}, func(t string) Address { return Address{Target: t} }, "compiled for ^0.4.0"},
		{"too old", &fakeConnector{name: "sap-ecc", version: "0.3.9"}, func(t string) Address { return Address{Target: t} }, "compiled for ^0.4.0"},
		{"no token", &fakeConnector{name: "sap-ecc", version: "0.4.0", token: "tok"}, func(t string) Address { return Address{Target: t} }, "TURGON_CONNECTOR_TOKEN"},
		{"bad secret", &fakeConnector{name: "sap-ecc", version: "0.4.0"}, func(t string) Address { return Address{Target: t} }, "not user:password"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sec := secrets
			if c.name == "bad secret" {
				sec = connector.StaticSecrets{"openbao://sap/ecc/prod": "x", "openbao://sap/ecc/hook": "y"}
			}
			_, err := pool.Factory(c.addr(serve(t, c.f)))(ctx, spec, sec)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}

	t.Run("missing secret", func(t *testing.T) {
		_, err := pool.Factory(Address{Target: serve(t, &fakeConnector{name: "sap-ecc", version: "0.4.0"})})(ctx, spec, connector.StaticSecrets{})
		if err == nil || !strings.Contains(err.Error(), "openbao://sap/ecc/prod") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("no capabilities", func(t *testing.T) {
		f := &fakeConnector{name: "sap-ecc", version: "0.4.0"}
		inst, err := pool.Factory(Address{Target: serve(t, f)})(ctx, spec, secrets)
		if err != nil {
			t.Fatal(err)
		}
		defer inst.Close()
		if _, ok := inst.(connector.Source); ok {
			t.Fatal("a connector without poll is a source")
		}
		if _, err := inst.Simulate(ctx, "create-sales-order", json.RawMessage(`{}`)); !errors.Is(err, writeguard.ErrSimulationUnsupported) {
			t.Fatalf("simulate: %v", err)
		}
		if err := inst.(writeguard.Confirmer).Confirm(ctx, "x", nil); err != nil {
			t.Fatalf("confirm: %v", err)
		}
		if _, err := inst.(writeguard.Reader).Read(ctx, "x", "1"); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("read: %v", err)
		}
		if checks := inst.(connector.Checker).Check(ctx); len(checks) != 1 || !checks[0].OK {
			t.Fatalf("check: %+v", checks)
		}
	})

	t.Run("sidecar away", func(t *testing.T) {
		defer func(d time.Duration) { StartTimeout = d }(StartTimeout)
		StartTimeout = time.Second
		_, err := pool.Factory(Address{Target: "unix://" + filepath.Join(t.TempDir(), "none.sock")})(ctx, spec, secrets)
		if err == nil || !strings.Contains(err.Error(), "describe") {
			t.Fatalf("got %v", err)
		}
	})
}

func TestParseAddresses(t *testing.T) {
	got, err := ParseAddresses(" sap-ecc=unix:///run/c/sap.sock, shopify=127.0.0.1:7602 ", "tok")
	if err != nil || got["sap-ecc"].Target != "unix:///run/c/sap.sock" || got["shopify"].Target != "127.0.0.1:7602" {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"sap-ecc", "=x", "a=x:1,a=y:1", "a=10.0.0.5:7601", "a=127.0.0.1:7601"} {
		if _, err := ParseAddresses(bad, ""); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := ParseAddresses("a=10.0.0.5:7601", "tok"); err != nil {
		t.Errorf("remote address with token: %v", err)
	}
	if got, err := ParseAddresses("", ""); err != nil || len(got) != 0 {
		t.Errorf("empty: %v %v", got, err)
	}
}

// Stream pushes three batches of one IDoc each and records the acks; a
// batch the worker does not acknowledge ends the stream.
func (f *fakeConnector) Stream(s grpc.BidiStreamingServer[pb.StreamRequest, pb.StreamBatch]) error {
	req, err := s.Recv()
	if err != nil {
		return err
	}
	open := req.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "open the stream first")
	}
	if err := f.instance(s.Context(), open.GetInstance()); err != nil {
		return err
	}
	for b := uint64(1); b <= 3; b++ {
		ev := &pb.Event{Id: fmt.Sprintf("idoc-%d", b), Name: open.GetEvent(), Payload: []byte(`{"docnum":"` + fmt.Sprint(b) + `"}`)}
		if err := s.Send(&pb.StreamBatch{Batch: b, Events: []*pb.Event{ev}}); err != nil {
			return err
		}
		ack, err := s.Recv()
		if err != nil {
			return err
		}
		if ack.GetAck().GetBatch() != b {
			return status.Errorf(codes.InvalidArgument, "acked %d, sent %d", ack.GetAck().GetBatch(), b)
		}
		f.mu.Lock()
		f.acks = append(f.acks, b)
		f.mu.Unlock()
	}
	<-s.Context().Done()
	return nil
}

func TestRemoteStream(t *testing.T) {
	f := &fakeConnector{name: "sap-ecc", version: "0.4.0", caps: []string{CapStream}}
	pool := NewPool()
	defer pool.Close()
	ctx := context.Background()
	inst, err := pool.Factory(Address{Target: serve(t, f)})(ctx, spec, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	st, ok := inst.(connector.Streamer)
	if !ok || !st.Streams("SalesOrder.Created") || st.Streams("SalesOrder.Changed") {
		t.Fatalf("streamer %v", ok)
	}
	if _, err := inst.(connector.Source).Poll(ctx, "SalesOrder.Created", 0, 10); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("poll: %v", err)
	}

	// The second batch fails to be stored: it is not acknowledged.
	var got []string
	fail := errors.New("the inbox is away")
	err = st.Stream(ctx, "SalesOrder.Created", nil, func(evs []connector.Event, _ []byte) error {
		if len(got) == 1 {
			return fail
		}
		for _, e := range evs {
			got = append(got, e.ID+":"+string(e.Payload))
		}
		return nil
	})
	if !errors.Is(err, fail) || strings.Join(got, ",") != `idoc-1:{"docnum":"1"}` {
		t.Fatalf("got %v, %v", got, err)
	}
	time.Sleep(50 * time.Millisecond)
	f.mu.Lock()
	acks := fmt.Sprint(f.acks)
	f.mu.Unlock()
	if acks != "[1]" {
		t.Fatalf("acks %s", acks)
	}

	// After a sidecar restart the stream configures the instance again.
	f.mu.Lock()
	f.configured = map[string]*pb.ConfigureRequest{}
	f.acks = nil
	f.mu.Unlock()
	cctx, cancel := context.WithCancel(ctx)
	got = nil
	err = st.Stream(cctx, "SalesOrder.Created", nil, func(evs []connector.Event, _ []byte) error {
		got = append(got, evs[0].ID)
		if len(got) == 3 {
			cancel()
		}
		return nil
	})
	if cctx.Err() == nil || strings.Join(got, ",") != "idoc-1,idoc-2,idoc-3" {
		t.Fatalf("got %v, %v", got, err)
	}
}

func (f *fakeConnector) Discover(ctx context.Context, r *pb.DiscoverRequest) (*pb.DiscoverResponse, error) {
	if err := f.instance(ctx, r.GetInstance()); err != nil {
		return nil, err
	}
	cat := `{"objects":[{"name":"BAPISDHD1","kind":"structure","fields":[{"name":"PURCH_NO_C","type":"CHAR","length":35},{"name":"DOC_TYPE","type":"CHAR","length":4}]}` +
		`],"uses":[{"object":"BAPISDHD1","field":"PURCH_NO_C","by":"operation create-sales-order"}]}`
	if len(r.GetObjects()) > 0 {
		cat = strings.Replace(cat, `]}],`, `]},{"name":"`+r.GetObjects()[0]+`","kind":"bapi","fields":[]}],`, 1)
	}
	return &pb.DiscoverResponse{Catalog: []byte(cat)}, nil
}

func TestRemoteDiscover(t *testing.T) {
	ctx := context.Background()
	pool := NewPool()
	defer pool.Close()
	inst, err := pool.Factory(Address{Target: serve(t, &fakeConnector{name: "sap-ecc", version: "0.4.0", caps: []string{CapDiscover}})})(ctx, spec, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	cat, err := inst.(connector.Discoverer).Discover(ctx, []string{"BAPI_X"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Objects) != 2 || cat.Objects[0].Name != "BAPISDHD1" || cat.Objects[0].Fields[0].Name != "DOC_TYPE" ||
		cat.Objects[1].Name != "BAPI_X" || cat.DiscoveredAt.IsZero() || len(cat.Uses) != 1 {
		t.Fatalf("catalog %+v", cat)
	}

	none, err := pool.Factory(Address{Target: serve(t, &fakeConnector{name: "sap-ecc", version: "0.4.0"})})(ctx, spec, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer none.Close()
	if _, err := none.(connector.Discoverer).Discover(ctx, nil); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("no capability: %v", err)
	}
}
