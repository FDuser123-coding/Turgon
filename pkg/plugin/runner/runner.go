// Package runner runs a runtime spec's logic plugins for the engine: it
// loads their embedded modules, answers their host calls within their
// grants, and audits each invocation.
package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/netguard"
	"github.com/fduser123-coding/turgon/pkg/plugin"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Proposer starts a governed write for a plugin's proposal, or finds the
// one already started under id, and reports its state without waiting
// for a person.
type Proposer interface {
	Propose(ctx context.Context, id string, req writeguard.Request) (engine.AgentWriteState, error)
}

// Reader performs governed reads: writeguard.Guard.
type Reader interface {
	Read(ctx context.Context, req writeguard.ReadRequest) (json.RawMessage, error)
}

type deployed struct {
	d      compiler.PluginDeployment
	module *plugin.Module
}

// Runner runs a spec's plugins. Modules are compiled once; Bind gives a
// runner the reads of the current connections, which change when secrets
// rotate.
type Runner struct {
	plugins map[string]*deployed
	read    Reader
	propose Proposer
	audit   audit.Recorder
	opts    Options
	client  *http.Client
}

// Options give plugins the network and secrets their grants allow.
type Options struct {
	// Network keeps plugins' HTTP requests on public addresses, unless it
	// allows more ranges (TURGON_PLUGIN_NETWORK_ALLOW).
	Network netguard.Guard
	// Secrets resolves the secret references plugins' handles map to.
	Secrets connector.SecretResolver
}

var _ engine.PluginRunner = (*Runner)(nil)

// New loads every logic plugin in the spec, checking each module against
// the digest the spec records.
func New(ctx context.Context, spec *compiler.RuntimeSpec, propose Proposer, rec audit.Recorder, opts Options) (*Runner, error) {
	r := &Runner{plugins: map[string]*deployed{}, propose: propose, audit: rec, opts: opts, client: opts.Network.Client()}
	for _, d := range spec.Spec.Plugins {
		if d.Type != v1alpha1.PluginLogic {
			continue
		}
		sum := sha256.Sum256(d.Module)
		if len(d.Module) == 0 || hex.EncodeToString(sum[:]) != d.ModuleSHA256 {
			r.Close(ctx)
			return nil, fmt.Errorf("plugin %s: the module does not match its digest", d.Name)
		}
		if d.Limits == nil {
			r.Close(ctx)
			return nil, fmt.Errorf("plugin %s: no limits", d.Name)
		}
		m, err := plugin.Load(ctx, d.Module, plugin.Limits{MemoryMB: d.Limits.MemoryMB, Timeout: time.Duration(d.Limits.TimeoutMs) * time.Millisecond})
		if err != nil {
			r.Close(ctx)
			return nil, fmt.Errorf("plugin %s: %w", d.Name, err)
		}
		r.plugins[d.Name] = &deployed{d: d, module: m}
	}
	return r, nil
}

// Names lists the plugins loaded.
func (r *Runner) Names() []string {
	var out []string
	for n, p := range r.plugins {
		out = append(out, n+"@"+p.d.Version)
	}
	slices.Sort(out)
	return out
}

// Bind returns a runner reading through read.
func (r *Runner) Bind(read Reader) *Runner {
	c := *r
	c.read = read
	return &c
}

// Close releases the modules.
func (r *Runner) Close(ctx context.Context) {
	for _, p := range r.plugins {
		p.module.Close(ctx)
	}
}

// event is what a plugin's handle function receives, as JSON: a model
// event (type model.<Entity>.<change>, with the written record), or an
// event another plugin published (type plugin.<publisher>.<topic>, with
// its payload).
type event struct {
	Type     string          `json:"type"`
	Source   string          `json:"source,omitempty"`
	Entity   string          `json:"entity"`
	ID       string          `json:"id,omitempty"`
	Record   json.RawMessage `json:"record,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Workflow string          `json:"workflow"`
	Run      string          `json:"run"`
}

// published is an event a plugin published during an invocation; it is
// delivered only if the invocation succeeds.
type published struct {
	topic   string
	payload []byte
}

// maxChain bounds how far events travel from plugin to plugin from one
// model event; a plugin never handles an event its own chain started.
const maxChain = 3

// Run runs one plugin on one event, then the plugins subscribed to what it
// published.
func (r *Runner) Run(ctx context.Context, in engine.PluginInput) error {
	p, ok := r.plugins[in.Plugin]
	if !ok {
		return fmt.Errorf("%w: %s is not deployed with this spec", plugin.ErrPlugin, in.Plugin)
	}
	if r.read == nil {
		return errors.New("plugin runner: no reader bound")
	}
	ev := event{Type: in.Event, Entity: in.Entity, ID: p.entityID(in.Entity, in.Record), Record: in.Record, Workflow: in.Workflow, Run: in.RunID}
	pubs, err := r.invoke(ctx, p, ev, in)
	if err != nil {
		return err
	}
	r.deliver(ctx, p, ev, pubs, in, []string{p.d.Name})
	return nil
}

// invoke runs a plugin on one event and audits it.
func (r *Runner) invoke(ctx context.Context, p *deployed, ev event, in engine.PluginInput) ([]published, error) {
	b, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}
	h := &host{r: r, p: p, in: in}
	start := time.Now()
	err = p.module.Handle(ctx, b, h)
	data := map[string]any{"plugin": p.d.Name, "version": p.d.Version, "event": ev.Type, "entity": ev.Entity, "id": ev.ID,
		"workflow": in.Workflow, "run": in.RunID, "ms": time.Since(start).Milliseconds(), "proposals": h.proposals, "reads": h.reads}
	if len(h.hosts) > 0 {
		data["requests"] = h.hosts // hosts only: never paths, headers or bodies
	}
	if len(h.published) > 0 {
		var topics []string
		for _, pub := range h.published {
			topics = append(topics, pub.topic)
		}
		data["published"] = topics
	}
	action := "plugin.handled"
	if err != nil {
		action, data["error"] = "plugin.failed", err.Error()
	}
	if _, aerr := r.audit.Record(p.actor(), action, data); aerr != nil && err == nil {
		err = aerr
	}
	if err != nil {
		return nil, err
	}
	return h.published, nil
}

// deliver runs the plugins subscribed to what from published. Their
// failures are audited but do not fail the publisher: it did its part.
func (r *Runner) deliver(ctx context.Context, from *deployed, cause event, pubs []published, in engine.PluginInput, chain []string) {
	if len(chain) >= maxChain {
		return
	}
	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, pub := range pubs {
		typ := "plugin." + from.d.Name + "." + pub.topic
		payload := json.RawMessage(pub.payload)
		if !json.Valid(pub.payload) {
			payload, _ = json.Marshal(map[string]string{"base64": base64.StdEncoding.EncodeToString(pub.payload)})
		}
		for _, n := range names {
			sub := r.plugins[n]
			if !slices.Contains(sub.d.Subscribes, typ) || slices.Contains(chain, n) {
				continue
			}
			ev := event{Type: typ, Source: from.d.Name, Entity: cause.Entity, ID: cause.ID, Payload: payload, Workflow: cause.Workflow, Run: cause.Run}
			next, err := r.invoke(ctx, sub, ev, in)
			if err == nil {
				r.deliver(ctx, sub, ev, next, in, append(slices.Clone(chain), n))
			}
		}
	}
}

func (p *deployed) actor() string { return "plugin:" + p.d.Name + "@" + p.d.Version }

// entityID finds the written entity's identifier in the record: the field
// its proposal (or read) operation keys on, in either spelling.
func (p *deployed) entityID(entity string, record json.RawMessage) string {
	var rec map[string]any
	if json.Unmarshal(record, &rec) != nil {
		return ""
	}
	var fields []string
	for _, ops := range []map[string]compiler.EntityOperation{p.d.Proposals, p.d.Reads} {
		if eo, ok := ops[entity]; ok && eo.IDField != "" {
			fields = append(fields, eo.IDField, snake(eo.IDField))
		}
	}
	for _, f := range append(fields, "id") {
		switch v := rec[f].(type) {
		case string:
			return v
		case float64:
			return fmt.Sprint(v)
		}
	}
	return ""
}

func snake(s string) string {
	var b strings.Builder
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			c += 'a' - 'A'
		}
		b.WriteRune(c)
	}
	return b.String()
}

// host answers one invocation's calls.
type host struct {
	r                *Runner
	p                *deployed
	in               engine.PluginInput
	reads, proposals int
	hosts            []string
	published        []published
}

func (h *host) subject() policy.Subject {
	roles := []string{policy.RoleReader}
	if e := h.p.d.Grants.Entities; e != nil && len(e.Propose) > 0 {
		roles = append(roles, policy.RoleOperator)
	}
	return policy.Subject{ID: h.p.actor(), Agent: true, Roles: roles}
}

// granted reports whether refs (Entity or Entity.field) allow entity, and
// which fields, if not all of them.
func granted(refs []string, entity string) (ok bool, fields []string) {
	for _, ref := range refs {
		e, f, hasField := strings.Cut(ref, ".")
		if e != entity {
			continue
		}
		if !hasField {
			return true, nil
		}
		ok, fields = true, append(fields, f)
	}
	return ok, fields
}

func (h *host) Get(ctx context.Context, kind, id string) (string, error) {
	var reads []string
	if e := h.p.d.Grants.Entities; e != nil {
		reads = e.Read
	}
	if ok, _ := granted(reads, kind); !ok {
		return "", &plugin.ErrorCode{Kind: plugin.Denied}
	}
	eo, ok := h.p.d.Reads[kind]
	if !ok {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "no " + plugin.ReadOperation(kind) + " operation serves " + kind}
	}
	h.reads++
	data, err := h.r.read.Read(ctx, writeguard.ReadRequest{
		Target: eo.Endpoint, Operation: eo.Operation, Tool: h.p.actor(), Entity: kind, ID: id, Subject: h.subject(),
	})
	switch {
	case errors.Is(err, writeguard.ErrNotFound):
		return "", &plugin.ErrorCode{Kind: plugin.NotFound}
	case errors.Is(err, writeguard.ErrDenied):
		return "", &plugin.ErrorCode{Kind: plugin.Denied}
	case err != nil:
		return "", err // the target is failing: the invocation is retried
	}
	return string(data), nil
}

func (h *host) ProposeChange(ctx context.Context, kind, id, patch string) (string, error) {
	var refs []string
	if e := h.p.d.Grants.Entities; e != nil {
		refs = e.Propose
	}
	ok, fields := granted(refs, kind)
	if !ok {
		return "", &plugin.ErrorCode{Kind: plugin.Denied}
	}
	eo, found := h.p.d.Proposals[kind]
	if !found {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "no " + plugin.ProposeOperation(kind) + " operation applies changes to " + kind}
	}
	if strings.TrimSpace(id) == "" {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "the entity id is empty"}
	}
	var change map[string]any
	dec := json.NewDecoder(strings.NewReader(patch))
	dec.UseNumber()
	if err := dec.Decode(&change); err != nil || len(change) == 0 {
		return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "the patch must be a JSON object of fields to change"}
	}
	for f := range change {
		if f == eo.IDField {
			return "", &plugin.ErrorCode{Kind: plugin.Invalid, Message: "the patch cannot change " + f + ", the entity's identifier"}
		}
		if fields != nil && !slices.Contains(fields, f) {
			return "", &plugin.ErrorCode{Kind: plugin.Denied}
		}
	}
	change[eo.IDField] = id
	payload, _ := json.Marshal(change) // sorted keys: the same proposal, the same digest
	// One proposal per plugin, run, step, entity and change: retries and
	// redeliveries find the first.
	sum := sha256.Sum256(bytes.Join([][]byte{[]byte(h.in.RunID), []byte(h.in.Step), []byte(kind), []byte(id), payload}, []byte{0}))
	key := "plugin:" + h.p.d.Name + ":" + hex.EncodeToString(sum[:12])
	req := writeguard.Request{
		Target: eo.Endpoint, Operation: eo.Operation, Tool: h.p.actor(), Risk: eo.Risk, Subject: h.subject(),
		IdempotencyKey: key, Payload: payload, Entity: kind, Recipe: h.in.Workflow,
		Reason: fmt.Sprintf("proposed by plugin %s@%s on %s (run %s)", h.p.d.Name, h.p.d.Version, h.in.Event, h.in.RunID),
	}
	h.proposals++
	state, err := h.r.propose.Propose(ctx, "plugin/"+h.p.d.Name+":"+hex.EncodeToString(sum[:12]), req)
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(map[string]string{"proposal": key, "state": string(state)})
	return string(out), nil
}

const (
	maxPublished   = 16
	maxPayload     = 64 << 10
	maxRequests    = 16
	maxResponse    = 1 << 20
	secretTemplate = `\{\{secret:([a-z0-9]([a-z0-9-]*[a-z0-9])?)\}\}`
)

var secretRE = regexp.MustCompile(secretTemplate)

// Publish keeps an event to deliver once the invocation succeeds.
func (h *host) Publish(_ context.Context, topic string, payload []byte) error {
	var topics []string
	if e := h.p.d.Grants.Events; e != nil {
		topics = e.Publish
	}
	switch {
	case !slices.Contains(topics, topic):
		return &plugin.ErrorCode{Kind: plugin.Denied}
	case len(payload) > maxPayload:
		return &plugin.ErrorCode{Kind: plugin.Invalid, Message: fmt.Sprintf("the payload is %d bytes; at most %d", len(payload), maxPayload)}
	case len(h.published) >= maxPublished:
		return &plugin.ErrorCode{Kind: plugin.Invalid, Message: fmt.Sprintf("at most %d events per invocation", maxPublished)}
	}
	h.published = append(h.published, published{topic: topic, payload: bytes.Clone(payload)})
	return nil
}

// Headers a plugin may not set: the transport's own, and hop-by-hop ones.
var reservedHeaders = []string{"Host", "Content-Length", "Transfer-Encoding", "Connection", "Upgrade", "Te", "Trailer", "Keep-Alive", "Proxy-Authorization", "Proxy-Connection"}

var methods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}

// Send makes an HTTP request to a host the plugin is granted, putting the
// secrets its headers name in on the way out.
func (h *host) Send(ctx context.Context, req plugin.HTTPRequest) (*plugin.HTTPResponse, error) {
	invalid := func(format string, args ...any) error {
		return &plugin.ErrorCode{Kind: plugin.Invalid, Message: fmt.Sprintf(format, args...)}
	}
	if len(h.hosts) >= maxRequests {
		return nil, invalid("at most %d requests per event", maxRequests)
	}
	if !slices.Contains(methods, req.Method) {
		return nil, invalid("method %q: use %s", req.Method, strings.Join(methods, ", "))
	}
	u, err := url.Parse(req.URL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, invalid("the URL must be https://host/path, without credentials in it")
	}
	if !h.p.allowsHost(u) {
		return nil, &plugin.ErrorCode{Kind: plugin.Denied}
	}
	if err := h.r.opts.Network.CheckURL(ctx, req.URL); err != nil {
		return nil, invalid("%v", err)
	}
	out, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return nil, invalid("%v", err)
	}
	for _, hd := range req.Headers {
		name := http.CanonicalHeaderKey(hd.Name)
		if slices.Contains(reservedHeaders, name) {
			return nil, invalid("the header %s is set by Turgon", name)
		}
		value, err := h.withSecrets(ctx, hd.Value)
		if err != nil {
			return nil, err
		}
		out.Header.Add(name, value)
	}
	h.hosts = append(h.hosts, u.Host)
	resp, err := h.r.client.Do(out)
	switch {
	case errors.Is(err, netguard.ErrRefused):
		return nil, invalid("%s is not a public address; Turgon only calls public addresses, or ranges its operators allow (TURGON_PLUGIN_NETWORK_ALLOW)", u.Hostname())
	case err != nil:
		return nil, &plugin.ErrorCode{Kind: plugin.Unavailable, Message: fmt.Sprintf("%s %s: %v", req.Method, u.Host, unwrapURLError(err))}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, &plugin.ErrorCode{Kind: plugin.Unavailable, Message: fmt.Sprintf("%s %s: %v", req.Method, u.Host, err)}
	}
	if len(body) > maxResponse {
		return nil, &plugin.ErrorCode{Kind: plugin.Unavailable, Message: fmt.Sprintf("%s %s: the response is larger than 1 MiB", req.Method, u.Host)}
	}
	r := &plugin.HTTPResponse{Status: uint16(resp.StatusCode), Body: body}
	for _, name := range slices.Sorted(maps.Keys(resp.Header)) {
		for _, v := range resp.Header[name] {
			r.Headers = append(r.Headers, plugin.Header{Name: name, Value: v})
		}
	}
	return r, nil
}

// withSecrets puts the values of the {{secret:<handle>}} a header names
// in. The plugin never sees them, and neither do audit entries or errors.
func (h *host) withSecrets(ctx context.Context, value string) (string, error) {
	var failure error
	out := secretRE.ReplaceAllStringFunc(value, func(m string) string {
		handle := secretRE.FindStringSubmatch(m)[1]
		ref, ok := h.p.d.Secrets[handle]
		if !ok || !slices.Contains(h.p.d.Grants.Secrets, handle) {
			failure = &plugin.ErrorCode{Kind: plugin.Denied}
			return ""
		}
		if h.r.opts.Secrets == nil {
			failure = &plugin.ErrorCode{Kind: plugin.Unavailable, Message: "secret " + handle + " is not available"}
			return ""
		}
		v, err := h.r.opts.Secrets.Resolve(ctx, ref)
		if err != nil {
			failure = &plugin.ErrorCode{Kind: plugin.Unavailable, Message: "secret " + handle + " is not available"}
			return ""
		}
		return v
	})
	if failure != nil {
		return "", failure
	}
	return out, nil
}

// allowsHost reports whether the plugin is granted u's host and port: a
// grant without a port allows the scheme's default one.
func (p *deployed) allowsHost(u *url.URL) bool {
	if p.d.Grants.Network == nil {
		return false
	}
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if port == "" {
		port = map[string]string{"https": "443", "http": "80"}[u.Scheme]
	}
	for _, g := range p.d.Grants.Network.Allow {
		gh, gp, hasPort := strings.Cut(strings.ToLower(g), ":")
		if !hasPort {
			gp = map[string]string{"https": "443", "http": "80"}[u.Scheme]
		}
		if gh == host && gp == port {
			return true
		}
	}
	return false
}

// unwrapURLError drops the URL net/http repeats in its errors.
func unwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// TemporalProposer starts proposals as agent writes on Temporal, so they
// get the same policy, approval and audit as an agent's.
type TemporalProposer struct {
	Writes          engine.AgentWrites
	SpecDigest      string
	ApprovalTimeout time.Duration
}

func (t TemporalProposer) Propose(ctx context.Context, id string, req writeguard.Request) (engine.AgentWriteState, error) {
	w := t.Writes
	if w.Wait <= 0 || w.Wait > 2*time.Second {
		w.Wait = 2 * time.Second
	}
	st, err := w.Submit(ctx, id, engine.AgentWriteInput{SpecDigest: t.SpecDigest, Request: req, ApprovalTimeout: t.ApprovalTimeout})
	return st.State, err
}
