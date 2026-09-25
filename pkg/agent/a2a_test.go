package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2aclient/agentcard"

	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// a2aClient discovers Turgon from its agent card and connects as the
// agent the gateway headers name.
func a2aClient(t *testing.T, url string, hdr map[string]string) (*a2aclient.Client, *a2a.AgentCard) {
	t.Helper()
	ctx := context.Background()
	hc := &http.Client{Transport: headers{set: hdr, base: http.DefaultTransport}}
	card, err := agentcard.NewResolver(hc).Resolve(ctx, url+"/a2a")
	if err != nil {
		t.Fatal(err)
	}
	c, err := a2aclient.NewFromCard(ctx, card, a2aclient.WithJSONRPCTransport(hc))
	if err != nil {
		t.Fatal(err)
	}
	return c, card
}

func ask(skill string, args map[string]any) *a2a.MessageSendParams {
	return &a2a.MessageSendParams{Message: a2a.NewMessage(a2a.MessageRoleUser,
		a2a.TextPart{Text: "please"},
		a2a.DataPart{Data: map[string]any{"skill": skill, "arguments": args}})}
}

func dataOf(t *testing.T, parts a2a.ContentParts) map[string]any {
	t.Helper()
	for _, p := range parts {
		if d, ok := p.(a2a.DataPart); ok {
			return d.Data
		}
	}
	t.Fatalf("no data part in %v", parts)
	return nil
}

func TestAgentCardListsTheSkills(t *testing.T) {
	url, _ := setupWith(t, "127.0.0.0/8", &fakeWrites{})
	_, card := a2aClient(t, url, operator)
	if card.Name != "Turgon" || card.URL != url+"/a2a" || card.PreferredTransport != a2a.TransportProtocolJSONRPC {
		t.Fatalf("card %+v", card)
	}
	skills := map[string]a2a.AgentSkill{}
	for _, s := range card.Skills {
		skills[s.ID] = s
	}
	if skills["get_sales_order"].ID == "" || !strings.Contains(skills["create_sales_order"].Description, "a person must approve it") {
		t.Fatalf("skills %+v", card.Skills)
	}
}

func TestAgentsDelegateReadsAndWritesOverA2A(t *testing.T) {
	w := &fakeWrites{status: engine.AgentWriteStatus{
		State:   engine.AgentWritePending,
		Pending: &engine.PendingApproval{Step: engine.AgentStep, Reasons: []string{"high-risk tool"}},
	}}
	url, log := setupWith(t, "127.0.0.0/8", w)
	c, _ := a2aClient(t, url, operator)
	ctx := context.Background()

	// A read answers at once, with the record as data.
	res, err := c.SendMessage(ctx, ask("erp_db_get_customer", map[string]any{"id": "C-100"}))
	if err != nil {
		t.Fatal(err)
	}
	msg, ok := res.(*a2a.Message)
	if !ok || dataOf(t, msg.Parts)["name"] != "Ada Lovelace GmbH" {
		t.Fatalf("read: %#v", res)
	}
	if !strings.Contains(log.String(), `"actor":"claude for ada@example.com","action":"read"`) {
		t.Error("A2A read not audited as the agent for its user")
	}

	// A write becomes a task that waits for a person.
	send := ask("create_sales_order", map[string]any{"record": order(), "reason": "quote 4471 accepted"})
	res, err = c.SendMessage(ctx, send)
	if err != nil {
		t.Fatal(err)
	}
	tk, ok := res.(*a2a.Task)
	if !ok || tk.Status.State != a2a.TaskStateWorking || !strings.Contains(dataOf(t, tk.Status.Message.Parts)["message"].(string), "must approve") {
		t.Fatalf("write: %#v", res)
	}
	// Its requestId is the message ID, so resending the message cannot write twice.
	wantID := "create_sales_order/claude:" + send.Message.ID
	if string(tk.ID) != wantID || w.ids[0] != wantID || w.inputs[0].Request.IdempotencyKey != "agent:claude:"+send.Message.ID {
		t.Fatalf("task %s, workflow %v", tk.ID, w.ids)
	}

	// The requesting agent follows the task; once approved it completes
	// with the result.
	w.mu.Lock()
	w.status = engine.AgentWriteStatus{State: engine.AgentWriteCommitted,
		Write: &engine.WriteRecord{Status: writeguard.StatusCommitted, Result: json.RawMessage(`{"id":42,"external_id":"AGENT-1"}`)}}
	w.mu.Unlock()
	got, err := c.GetTask(ctx, &a2a.TaskQueryParams{ID: tk.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.State != a2a.TaskStateCompleted || len(got.Artifacts) != 1 || dataOf(t, got.Artifacts[0].Parts)["id"] != float64(42) {
		t.Fatalf("task after approval: %+v", got)
	}

	// Another agent cannot see it, or cancel it.
	other, _ := a2aClient(t, url, map[string]string{"X-Agent-Id": "mallory", "X-Agent-Roles": "integration-operator"})
	if _, err := other.GetTask(ctx, &a2a.TaskQueryParams{ID: tk.ID}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("another agent's task: %v", err)
	}
	if _, err := c.CancelTask(ctx, &a2a.TaskIDParams{ID: tk.ID}); !errors.Is(err, a2a.ErrTaskNotCancelable) {
		t.Fatalf("cancel: %v", err)
	}

	// Denials are rejections.
	w.mu.Lock()
	w.status = engine.AgentWriteStatus{State: engine.AgentWriteDenied, Message: "agents need the integration-operator role to write"}
	w.mu.Unlock()
	res, err = c.SendMessage(ctx, ask("create_sales_order", map[string]any{"record": order()}))
	if tk, ok := res.(*a2a.Task); err != nil || !ok || tk.Status.State != a2a.TaskStateRejected {
		t.Fatalf("denied write: %#v %v", res, err)
	}
}

func TestA2ATakesOnlyStructuredRequests(t *testing.T) {
	url, _ := setupWith(t, "127.0.0.0/8", &fakeWrites{})
	c, _ := a2aClient(t, url, operator)
	ctx := context.Background()
	prose := &a2a.MessageSendParams{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: "create an order for Ada"})}
	if _, err := c.SendMessage(ctx, prose); !errors.Is(err, a2a.ErrUnsupportedContentType) {
		t.Fatalf("prose: %v", err)
	}
	if _, err := c.SendMessage(ctx, ask("drop_database", nil)); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("unknown skill: %v", err)
	}
	res, err := c.SendMessage(ctx, ask("get_sales_order", map[string]any{"id": "x", "sql": "drop table"}))
	if msg, ok := res.(*a2a.Message); err != nil || !ok || !strings.Contains(msg.Parts[0].(a2a.TextPart).Text, "identifier") {
		t.Fatalf("bad arguments: %#v %v", res, err)
	}
}

func TestA2AIsBehindTheGatewayToo(t *testing.T) {
	url, _ := setup(t, "10.0.0.0/8") // the test client is not the gateway
	resp, err := http.Get(url + "/a2a/.well-known/agent-card.json")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
