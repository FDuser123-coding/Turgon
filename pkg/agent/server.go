// Package agent serves Turgon's MCP tools to AI agents (architecture §7.7).
// Tools are generated from the compiled runtime spec and named in business
// terms (get_customer, not BAPI_CUSTOMER_GETDETAIL), so agents stay
// portable across back ends. Read tools answer directly; write tools start
// a governed write (architecture §8) that waits, durably, for a person's
// approval when policy requires one.
//
// Every call is authorized per action for the agent and the person it acts
// for, passes the target's rate governor and circuit breaker, and is
// audited. Agents never hold system credentials: connectors use them on the
// agent's behalf.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// identityHeader carries the authenticated identity from the HTTP layer to
// tool handlers. Any value a client sends is removed first.
const identityHeader = "X-Turgon-Identity"

// Options assemble a server.
type Options struct {
	Registry connector.Registry
	Secrets  connector.SecretResolver
	Audit    audit.Recorder
	Auth     Authenticator
	// Policy decides each call; defaults to the built-in default, which
	// lets integration-reader and integration-operator roles read.
	Policy policy.Decider
	// Writes runs write tools. Without it only read tools are served.
	Writes WriteSubmitter
	// ApprovalTimeout rejects a write nobody approved after this long;
	// zero uses the engine's default.
	ApprovalTimeout time.Duration
	// A2AURL is where other agents reach the A2A endpoint, for the agent
	// card; defaults to the request's own host.
	A2AURL  string
	Version string
}

// WriteSubmitter starts agent writes, or reports on one already started
// under the same id. engine.AgentWrites implements it on Temporal.
type WriteSubmitter interface {
	Submit(ctx context.Context, id string, in engine.AgentWriteInput) (engine.AgentWriteStatus, error)
	// Status reports on a write already started; engine.ErrUnknownWrite
	// if there is none.
	Status(ctx context.Context, id string) (engine.AgentWriteStatus, error)
}

// Server is an MCP endpoint over a runtime spec's read tools.
type Server struct {
	auth      Authenticator
	guard     *writeguard.Guard
	writes    WriteSubmitter
	digest    string
	timeout   time.Duration
	version   string
	a2aURL    string
	a2a       http.Handler
	tools     []compiler.Tool
	instances []connector.Instance
	handler   http.Handler
}

// New connects the endpoints the spec's read tools use and registers them.
func New(ctx context.Context, spec *compiler.RuntimeSpec, opts Options) (*Server, error) {
	if opts.Auth == nil {
		return nil, errors.New("agent: an authenticator is required")
	}
	if opts.Policy == nil {
		opts.Policy = policy.WritebackDefault{}
	}
	s := &Server{auth: opts.Auth, writes: opts.Writes, digest: spec.Metadata.Digest, timeout: opts.ApprovalTimeout, a2aURL: opts.A2AURL}
	needed := map[string]bool{}
	for _, t := range spec.Spec.Tools {
		switch {
		case t.Risk == v1alpha1.RiskRead:
			s.tools = append(s.tools, t)
			needed[t.Endpoint] = true
		case opts.Writes != nil:
			// Writes run on the workers; this server only starts them.
			s.tools = append(s.tools, t)
		}
	}
	targets := map[string]writeguard.TargetConfig{}
	for _, c := range spec.Spec.Connectors {
		if !needed[c.Endpoint] {
			continue
		}
		factory, ok := opts.Registry[c.Name]
		if !ok {
			s.Close()
			return nil, fmt.Errorf("agent: no runtime for connector %s (endpoint %s)", c.Name, c.Endpoint)
		}
		inst, err := factory(ctx, c, opts.Secrets)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.instances = append(s.instances, inst)
		targets[c.Endpoint] = writeguard.TargetConfig{Target: inst, Limits: c.Limits, Metered: c.Metered}
	}
	g, err := writeguard.New(writeguard.Config{Targets: targets, Policy: opts.Policy, Audit: opts.Audit})
	if err != nil {
		s.Close()
		return nil, err
	}
	s.guard = g

	version := opts.Version
	if version == "" {
		version = "dev"
	}
	s.version = version
	m := mcp.NewServer(&mcp.Implementation{Name: "turgon", Title: "Turgon", Version: version}, &mcp.ServerOptions{
		Instructions: "Tools read and write business records in the customer's systems through Turgon. " +
			"Every call is authorized for you and the person you act for, and audited. " +
			"Writes are checked and previewed first, and may wait for a person to approve them: " +
			"give each write a new requestId, and call the tool again with the same arguments to see how it stands.",
	})
	sort.Slice(s.tools, func(i, j int) bool { return s.tools[i].Name < s.tools[j].Name })
	for _, t := range s.tools {
		if t.Risk == v1alpha1.RiskRead {
			m.AddTool(toolDef(t), s.call(t))
		} else {
			m.AddTool(writeToolDef(t), s.write(t))
		}
	}
	s.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return m },
		&mcp.StreamableHTTPOptions{Stateless: true})
	s.a2a = s.a2aHTTP()
	return s, nil
}

func toolDef(t compiler.Tool) *mcp.Tool {
	entity := t.Entity
	if entity == "" {
		entity = "record"
	}
	closedWorld := false
	return &mcp.Tool{
		Name:        t.Name,
		Description: t.Description,
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{"type": "string", "description": fmt.Sprintf("The %s's identifier in %s", entity, t.Endpoint)},
			},
			"required":             []string{"id"},
			"additionalProperties": false,
		},
		Annotations: &mcp.ToolAnnotations{Title: humanTitle(t), ReadOnlyHint: true, OpenWorldHint: &closedWorld},
	}
}

func humanTitle(t compiler.Tool) string {
	s := strings.ReplaceAll(t.Operation, "-", " ")
	if s == "" {
		return t.Name
	}
	return strings.ToUpper(s[:1]) + s[1:] + " (" + t.Endpoint + ")"
}

// call returns the MCP handler for one read tool.
func (s *Server) call(t compiler.Tool) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, ok := identity(req)
		if !ok {
			return failure("not authenticated"), nil
		}
		data, err := s.read(ctx, t, id, req.Params.Arguments)
		if err != nil {
			return failure(err.Error()), nil
		}
		var record map[string]any
		_ = json.Unmarshal(data, &record)
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: string(data)}},
			StructuredContent: record,
		}, nil
	}
}

// read runs a read tool for a caller. Its errors are messages for the agent.
func (s *Server) read(ctx context.Context, t compiler.Tool, id Identity, raw json.RawMessage) (json.RawMessage, error) {
	var args struct {
		ID string `json:"id"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&args); err != nil || strings.TrimSpace(args.ID) == "" {
		return nil, errors.New(`pass the record's identifier as {"id": "..."}`)
	}
	data, err := s.guard.Read(ctx, writeguard.ReadRequest{
		Target: t.Endpoint, Operation: t.Operation, Tool: t.Name, Entity: t.Entity, ID: args.ID,
		Subject: policy.Subject{ID: id.Agent, Agent: true, OnBehalfOf: id.OnBehalfOf, Roles: id.Roles},
	})
	switch {
	case errors.Is(err, writeguard.ErrNotFound):
		return nil, fmt.Errorf("no %s with identifier %q in %s", strings.ToLower(orEntity(t.Entity)), args.ID, t.Endpoint)
	case errors.Is(err, writeguard.ErrDenied):
		return nil, errors.New("denied by policy: " + err.Error())
	case errors.Is(err, writeguard.ErrCircuitOpen):
		return nil, errors.New(t.Endpoint + " is failing; reads are paused. Try again later.")
	case err != nil:
		return nil, errors.New(t.Endpoint + " did not answer: " + err.Error())
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, errors.New("unreadable record")
	}
	return data, nil
}

// identity returns the caller ServeHTTP authenticated.
func identity(req *mcp.CallToolRequest) (Identity, bool) {
	var id Identity
	if req.Extra == nil || json.Unmarshal([]byte(req.Extra.Header.Get(identityHeader)), &id) != nil || id.Agent == "" {
		return Identity{}, false
	}
	return id, true
}

func orEntity(e string) string {
	if e == "" {
		return "record"
	}
	return e
}

func failure(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: msg}}}
}

// Tools lists the tools served.
func (s *Server) Tools() []compiler.Tool { return s.tools }

// ServeHTTP authenticates the caller and hands the request to A2A (under
// /a2a) or MCP (anywhere else). MCP handlers read the identity from a
// header that replaces any the caller sent; A2A from the request context.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	id, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, "agents reach Turgon through the agent gateway", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == a2aPath || strings.HasPrefix(r.URL.Path, a2aPath+"/") {
		s.a2a.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, id)))
		return
	}
	b, _ := json.Marshal(id)
	r = r.Clone(r.Context())
	r.Header.Del(identityHeader)
	r.Header.Set(identityHeader, string(b))
	s.handler.ServeHTTP(w, r)
}

// Serve listens on addr. Dev authentication only listens on loopback.
func (s *Server) Serve(addr string) error {
	if _, dev := s.auth.(DevAuth); dev {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return fmt.Errorf("dev authentication only listens on loopback addresses, not %s", addr)
		}
	}
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
	return srv.ListenAndServe()
}

// Close releases connector resources.
func (s *Server) Close() {
	for _, inst := range s.instances {
		inst.Close()
	}
}
