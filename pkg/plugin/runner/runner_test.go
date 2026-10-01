package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/plugin"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

type fakeReader struct {
	rows map[string]string
	reqs []writeguard.ReadRequest
	err  error
}

func (f *fakeReader) Read(_ context.Context, req writeguard.ReadRequest) (json.RawMessage, error) {
	f.reqs = append(f.reqs, req)
	if f.err != nil {
		return nil, f.err
	}
	row, ok := f.rows[req.Entity+"/"+req.ID]
	if !ok {
		return nil, writeguard.ErrNotFound
	}
	return json.RawMessage(row), nil
}

type fakeProposer struct {
	mu   sync.Mutex
	ids  []string
	reqs []writeguard.Request
}

func (f *fakeProposer) Propose(_ context.Context, id string, req writeguard.Request) (engine.AgentWriteState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids, f.reqs = append(f.ids, id), append(f.reqs, req)
	return engine.AgentWriteCommitted, nil
}

func deployment(t *testing.T, name, file string, grants v1alpha1.EntityPermissions, publish ...string) compiler.PluginDeployment {
	t.Helper()
	mod, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(mod)
	return compiler.PluginDeployment{
		Name: name, Version: "1.2.0", Type: v1alpha1.PluginLogic, Runtime: v1alpha1.PluginRuntimeWasm,
		Subscribes: []string{"model.SalesOrder.created"}, Grants: v1alpha1.PluginPermissions{Entities: &grants, Events: &v1alpha1.EventPermissions{Publish: publish}},
		Limits: &v1alpha1.PluginLimits{MemoryMB: 32, TimeoutMs: 2000}, Module: mod, ModuleSHA256: hex.EncodeToString(sum[:]),
		Reads:     map[string]compiler.EntityOperation{"Customer": {Endpoint: "erp-db", Operation: "get-customer", Risk: "read", IDField: "id"}},
		Proposals: map[string]compiler.EntityOperation{"SalesOrder": {Endpoint: "erp-db", Operation: "update-sales-order", Risk: "low", IDField: "externalId"}},
	}
}

func spec(ds ...compiler.PluginDeployment) *compiler.RuntimeSpec {
	s := &compiler.RuntimeSpec{}
	s.Spec.Plugins = ds
	return s
}

var order = engine.PluginInput{
	Plugin: "credit-check", Event: "model.SalesOrder.created", Entity: "SalesOrder", Workflow: "shop-orders-with-credit-check",
	RunID: "shop-orders-with-credit-check/7", Step: "03-write",
	Record: json.RawMessage(`{"id": 12, "external_id": "SHOP-7", "customer_id": "C-100", "net_value": 1200, "currency": "EUR"}`),
}

func TestCreditCheckProposesWithinItsGrants(t *testing.T) {
	ctx := context.Background()
	var log bytes.Buffer
	prop := &fakeProposer{}
	r, err := New(ctx, spec(deployment(t, "credit-check", "../../../examples/plugins/credit-check/credit-check.wasm",
		v1alpha1.EntityPermissions{Read: []string{"Customer"}, Propose: []string{"SalesOrder.creditStatus"}}, "credit.checked")), prop, audit.New(&log), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	read := &fakeReader{rows: map[string]string{"Customer/C-100": `{"id": "C-100", "credit_limit": "1000.00"}`}}
	b := r.Bind(read)
	if err := b.Run(ctx, order); err != nil {
		t.Fatal(err)
	}
	// Read as the plugin, with its roles.
	if len(read.reqs) != 1 || read.reqs[0].Operation != "get-customer" || read.reqs[0].Subject.ID != "plugin:credit-check@1.2.0" || !read.reqs[0].Subject.Agent {
		t.Fatalf("reads %+v", read.reqs)
	}
	// 1200 > 1000: review, written to the order the event names. The risk
	// service is not granted here: the limit decides alone.
	if len(prop.reqs) != 1 {
		t.Fatalf("proposals %+v", prop.reqs)
	}
	p := prop.reqs[0]
	if string(p.Payload) != `{"creditStatus":"review","externalId":"SHOP-7"}` || p.Operation != "update-sales-order" || p.Risk != "low" ||
		p.Subject.ID != "plugin:credit-check@1.2.0" || !strings.Contains(p.Reason, "shop-orders-with-credit-check/7") || !strings.HasPrefix(prop.ids[0], "plugin/credit-check:") {
		t.Fatalf("proposal %s %+v", prop.ids[0], p)
	}
	// The same event again proposes under the same key: one write.
	if err := b.Run(ctx, order); err != nil || prop.ids[1] != prop.ids[0] || prop.reqs[1].IdempotencyKey != p.IdempotencyKey {
		t.Fatalf("again: %v %v", err, prop.ids)
	}
	if !strings.Contains(log.String(), `"action":"plugin.handled"`) || !strings.Contains(log.String(), `"id":"SHOP-7"`) {
		t.Fatalf("audit:\n%s", log.String())
	}

	// A customer it cannot find, and a target that is down.
	read.rows = nil
	if err := b.Run(ctx, order); !errors.Is(err, plugin.ErrPlugin) || !strings.Contains(err.Error(), "customer C-100: not found") {
		t.Fatalf("missing customer: %v", err)
	}
	read.err = errors.New("erp-db did not answer")
	if err := b.Run(ctx, order); err == nil || errors.Is(err, plugin.ErrPlugin) {
		t.Fatalf("target down: %v (want a retryable error)", err)
	}
	if !strings.Contains(log.String(), `"action":"plugin.failed"`) {
		t.Fatal("failure not audited")
	}
}

func TestHostEnforcesGrants(t *testing.T) {
	ctx := context.Background()
	prop := &fakeProposer{}
	// The probe may read Customer and propose SalesOrder.creditStatus only.
	r, err := New(ctx, spec(deployment(t, "probe", "../testdata/probe.wasm",
		v1alpha1.EntityPermissions{Read: []string{"Customer"}, Propose: []string{"SalesOrder.creditStatus"}})), prop, audit.New(&bytes.Buffer{}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	read := &fakeReader{rows: map[string]string{"Customer/C-1": `{"id":"C-1"}`, "SalesOrder/S-1": `{}`}}
	b := r.Bind(read)
	run := func(cmd string) error {
		in := order
		in.Plugin = "probe"
		// The probe takes commands as its event; feed it one directly.
		h := &host{r: b, p: b.plugins["probe"], in: in}
		return b.plugins["probe"].module.Handle(ctx, []byte(cmd), &capture{host: h})
	}
	for cmd, want := range map[string]string{
		"get Customer C-1":                             `ok {"id":"C-1"}`,
		"get SalesOrder S-1":                           "err denied", // not granted to read
		`propose SalesOrder S-1 {"creditStatus":"ok"}`: `ok {"proposal":`,
		`propose SalesOrder S-1 {"netValue":0}`:        "err denied", // field not granted
		`propose Customer C-1 {"creditStatus":"ok"}`:   "err denied",
		`propose SalesOrder S-1 {"externalId":"S-2"}`:  "err invalid the patch cannot change externalId",
		`propose SalesOrder S-1 [1,2]`:                 "err invalid the patch must be a JSON object",
		"publish anything":                             "err denied", // no topic granted
	} {
		c := lastCapture
		if err := run(cmd); err != nil {
			t.Fatalf("%s: %v", cmd, err)
		}
		if got := lastCapture.result; !strings.HasPrefix(got, want) || lastCapture == c {
			t.Errorf("%s: %q, want %q", cmd, got, want)
		}
	}
	if len(prop.reqs) != 1 || string(prop.reqs[0].Payload) != `{"creditStatus":"ok","externalId":"S-1"}` {
		t.Fatalf("proposals %+v", prop.reqs)
	}
	if len(read.reqs) != 1 {
		t.Fatalf("a denied read reached the target: %+v", read.reqs)
	}

	// A module that does not match the spec's digest is refused.
	d := deployment(t, "probe", "../testdata/probe.wasm", v1alpha1.EntityPermissions{})
	d.ModuleSHA256 = strings.Repeat("0", 64)
	if _, err := New(ctx, spec(d), prop, audit.New(&bytes.Buffer{}), Options{}); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("tampered module: %v", err)
	}
}

// capture passes calls to the real host and keeps what the probe
// publishes on "result".
type capture struct {
	*host
	result string
}

var lastCapture *capture

func (c *capture) Publish(ctx context.Context, topic string, payload []byte) error {
	if topic == "result" {
		c.result = string(payload)
		lastCapture = c
		return nil
	}
	return c.host.Publish(ctx, topic, payload)
}
