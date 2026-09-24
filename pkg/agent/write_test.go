package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// fakeWrites records submitted writes and answers with a fixed status.
type fakeWrites struct {
	mu     sync.Mutex
	ids    []string
	inputs []engine.AgentWriteInput
	status engine.AgentWriteStatus
	err    error
}

func (f *fakeWrites) Submit(_ context.Context, id string, in engine.AgentWriteInput) (engine.AgentWriteStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.inputs = append(f.inputs, in)
	return f.status, f.err
}

var operator = map[string]string{"X-Agent-Id": "claude", "X-On-Behalf-Of": "ada@example.com", "X-Agent-Roles": "integration-operator"}

func order() map[string]any {
	return map[string]any{
		"externalId": "AGENT-1", "customerId": "C-100", "orderDate": "2026-09-24", "netValue": 480.5, "currency": "EUR",
		"lines": []any{map[string]any{"material": "M-1", "quantity": 4}},
	}
}

func structured(t *testing.T, res *mcp.CallToolResult) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &out); err != nil {
		t.Fatalf("result is not JSON: %v", res.Content[0])
	}
	return out
}

func TestWriteToolsTakeTheRecordTheRecipeWrites(t *testing.T) {
	url, _ := setupWith(t, "127.0.0.0/8", &fakeWrites{})
	s, err := connect(t, url, operator)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tools, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*mcp.Tool{}
	for _, tl := range tools.Tools {
		byName[tl.Name] = tl
	}
	create, ok := byName["create_sales_order"]
	if !ok || byName["update_opportunity"] == nil || byName["get_sales_order"] == nil {
		t.Fatalf("tools: %v", byName)
	}
	if create.Annotations.ReadOnlyHint || !create.Annotations.IdempotentHint || !*create.Annotations.DestructiveHint {
		t.Errorf("annotations %+v", create.Annotations)
	}
	if !strings.Contains(create.Description, "a person must approve it") {
		t.Errorf("description %q does not say it needs approval", create.Description)
	}
	schema, _ := json.Marshal(create.InputSchema)
	for _, want := range []string{`"customerId"`, `"netValue"`, `"requestId"`, `"additionalProperties":false`} {
		if !strings.Contains(string(schema), want) {
			t.Errorf("input schema lacks %s: %s", want, schema)
		}
	}
}

func TestWriteToolStartsAGovernedWriteAsTheAgent(t *testing.T) {
	w := &fakeWrites{status: engine.AgentWriteStatus{
		State: engine.AgentWritePending,
		Pending: &engine.PendingApproval{
			Step: engine.AgentStep, Reasons: []string{"high-risk tool"}, Preview: json.RawMessage(`{"external_id":"AGENT-1"}`),
		},
	}}
	url, _ := setupWith(t, "127.0.0.0/8", w)
	s, err := connect(t, url, operator)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	res := call(t, s, "create_sales_order", map[string]any{"requestId": "r-1", "record": order(), "reason": "customer asked by phone"})
	out := structured(t, res)
	if res.IsError || out["state"] != "pending_approval" || !strings.Contains(out["message"].(string), "must approve") || out["preview"] == nil {
		t.Fatalf("result %v", out)
	}
	if len(w.ids) != 1 || w.ids[0] != "create_sales_order/claude:r-1" {
		t.Fatalf("workflow ids %v", w.ids)
	}
	req := w.inputs[0].Request
	if req.Target != "erp-db" || req.Operation != "create-sales-order" || req.Risk != "high" || !req.Simulate ||
		req.IdempotencyKey != "agent:claude:r-1" || req.Amount != 480.5 ||
		!req.Subject.Agent || req.Subject.ID != "claude" || req.Subject.OnBehalfOf != "ada@example.com" ||
		req.Reason != "customer asked by phone" {
		t.Fatalf("request %+v", req)
	}
	if !strings.HasPrefix(string(req.Payload), `{"currency":"EUR","customerId":"C-100"`) {
		t.Errorf("payload is not canonical: %s", req.Payload)
	}

	// Calling again sends the same request, so the engine can find it.
	call(t, s, "create_sales_order", map[string]any{"requestId": "r-1", "record": order(), "reason": "customer asked by phone"})
	if string(w.inputs[1].Request.Payload) != string(req.Payload) || w.ids[1] != w.ids[0] {
		t.Error("the same call produced a different request")
	}
}

func TestWriteToolReportsOutcomes(t *testing.T) {
	cases := []struct {
		status engine.AgentWriteStatus
		err    error
		isErr  bool
		want   string
	}{
		{engine.AgentWriteStatus{State: engine.AgentWriteCommitted, Write: &engine.WriteRecord{Status: writeguard.StatusCommitted, Result: json.RawMessage(`{"id":42}`)}}, nil, false, "Done"},
		{engine.AgentWriteStatus{State: engine.AgentWriteCommitted, Write: &engine.WriteRecord{Status: writeguard.StatusDuplicate}}, nil, false, "nothing was written again"},
		{engine.AgentWriteStatus{State: engine.AgentWriteDenied, Message: "agents need the integration-operator role to write"}, nil, true, "Denied by policy"},
		{engine.AgentWriteStatus{State: engine.AgentWriteRejected}, nil, true, "rejected"},
		{engine.AgentWriteStatus{State: engine.AgentWriteRunning}, nil, false, "in progress"},
		{engine.AgentWriteStatus{State: engine.AgentWriteFailed, Message: "erp-db is down"}, nil, true, "new requestId"},
		{engine.AgentWriteStatus{}, engine.ErrRequestConflict, true, "already used"},
	}
	w := &fakeWrites{}
	url, _ := setupWith(t, "127.0.0.0/8", w)
	s, err := connect(t, url, operator)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, c := range cases {
		w.mu.Lock()
		w.status, w.err = c.status, c.err
		w.mu.Unlock()
		res := call(t, s, "create_sales_order", map[string]any{"requestId": "r-2", "record": order()})
		text := res.Content[0].(*mcp.TextContent).Text
		if res.IsError != c.isErr || !strings.Contains(text, c.want) {
			t.Errorf("%s/%v: isError=%v %s", c.status.State, c.err, res.IsError, text)
		}
	}
}

func TestWriteToolRejectsBadInput(t *testing.T) {
	w := &fakeWrites{}
	url, _ := setupWith(t, "127.0.0.0/8", w)
	s, err := connect(t, url, operator)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	extra := order()
	extra["status"] = "paid"
	for name, args := range map[string]map[string]any{
		"unknown field": {"requestId": "r-3", "record": extra},
		"bad id":        {"requestId": "r/3", "record": order()},
		"no record":     {"requestId": "r-3"},
		"long reason":   {"requestId": "r-3", "record": order(), "reason": strings.Repeat("x", 501)},
	} {
		res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "create_sales_order", Arguments: args})
		if err == nil && !res.IsError {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(w.ids) != 0 {
		t.Fatalf("bad input reached the engine: %v", w.ids)
	}
}
