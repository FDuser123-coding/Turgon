// Package agent serves Turgon's MCP tools to AI agents (architecture §7.7).
// Tools are generated from the compiled runtime spec and named in business
// terms (get_customer, not BAPI_CUSTOMER_GETDETAIL), so agents stay
// portable across back ends. This first version serves read-only tools.
//
// Every call is authorized per action for the agent and the person it acts
// for, passes the target's rate governor and circuit breaker, and is
// audited. Agents never hold system credentials: connectors use them on the
// agent's behalf.
package agent

import (
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
	Policy  policy.Decider
	Version string
}

// Server is an MCP endpoint over a runtime spec's read tools.
type Server struct {
	auth      Authenticator
	guard     *writeguard.Guard
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
	s := &Server{auth: opts.Auth}
	for _, t := range spec.Spec.Tools {
		if t.Risk == v1alpha1.RiskRead {
			s.tools = append(s.tools, t)
		}
	}
	needed := map[string]bool{}
	for _, t := range s.tools {
		needed[t.Endpoint] = true
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
	m := mcp.NewServer(&mcp.Implementation{Name: "turgon", Title: "Turgon", Version: version}, &mcp.ServerOptions{
		Instructions: "Tools read business records from the customer's systems through Turgon. " +
			"Every call is authorized for you and the person you act for, and audited.",
	})
	sort.Slice(s.tools, func(i, j int) bool { return s.tools[i].Name < s.tools[j].Name })
	for _, t := range s.tools {
		m.AddTool(toolDef(t), s.call(t))
	}
	s.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return m },
		&mcp.StreamableHTTPOptions{Stateless: true})
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

// call returns the handler for one tool.
func (s *Server) call(t compiler.Tool) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var id Identity
		if req.Extra == nil || json.Unmarshal([]byte(req.Extra.Header.Get(identityHeader)), &id) != nil || id.Agent == "" {
			return failure("not authenticated"), nil
		}
		var args struct {
			ID string `json:"id"`
		}
		dec := json.NewDecoder(strings.NewReader(string(req.Params.Arguments)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&args); err != nil || strings.TrimSpace(args.ID) == "" {
			return failure("pass the record's identifier as {\"id\": \"...\"}"), nil
		}
		data, err := s.guard.Read(ctx, writeguard.ReadRequest{
			Target: t.Endpoint, Operation: t.Operation, Tool: t.Name, Entity: t.Entity, ID: args.ID,
			Subject: policy.Subject{ID: id.Agent, Agent: true, OnBehalfOf: id.OnBehalfOf, Roles: id.Roles},
		})
		switch {
		case errors.Is(err, writeguard.ErrNotFound):
			return failure(fmt.Sprintf("no %s with identifier %q in %s", strings.ToLower(orEntity(t.Entity)), args.ID, t.Endpoint)), nil
		case errors.Is(err, writeguard.ErrDenied):
			return failure("denied by policy: " + err.Error()), nil
		case errors.Is(err, writeguard.ErrCircuitOpen):
			return failure(t.Endpoint + " is failing; reads are paused. Try again later."), nil
		case err != nil:
			return failure(t.Endpoint + " did not answer: " + err.Error()), nil
		}
		var record map[string]any
		if err := json.Unmarshal(data, &record); err != nil {
			return failure("unreadable record"), nil
		}
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: string(data)}},
			StructuredContent: record,
		}, nil
	}
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

// ServeHTTP authenticates the caller, replaces any identity header it sent
// with the authenticated one, and hands the request to MCP.
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
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

// Close releases connector resources.
func (s *Server) Close() {
	for _, inst := range s.instances {
		inst.Close()
	}
}
