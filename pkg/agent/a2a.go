package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
)

// Turgon is also an A2A agent (architecture §7.7), so other agents can
// delegate integration tasks to it. Its skills are the spec's tools. A
// request is structured, not prose: a data part
//
//	{"skill": "create_sales_order", "arguments": {"record": {...}, "reason": "..."}}
//
// with the same arguments as the MCP tool. Turgon runs no language model at
// run time, so it does not interpret text (architecture §7.8).
//
// A read answers at once with a message. A write becomes a task whose ID
// is its durable agent-write workflow: it can wait days for a person's
// approval, survives restarts, and tasks/get reports it at any time.
// A write's requestId defaults to the message ID, so resending a message
// never writes twice.

const (
	a2aPath  = "/a2a"
	cardPath = "/.well-known/agent-card.json"
)

type identityKey struct{}

// a2aHandler implements a2asrv.RequestHandler over the server's tools.
type a2aHandler struct {
	s     *Server
	tools map[string]compiler.Tool
}

var _ a2asrv.RequestHandler = (*a2aHandler)(nil)

// agentCard describes Turgon to other agents; url is where it is reached.
func (s *Server) agentCard(url string) *a2a.AgentCard {
	card := &a2a.AgentCard{
		Name:               "Turgon",
		Description:        "Reads and writes business records in the customer's systems of record. Writes are checked, previewed and, where policy requires, approved by a person. Send a data part {\"skill\": ..., \"arguments\": {...}}.",
		URL:                url,
		Version:            s.version,
		ProtocolVersion:    "0.3.0",
		PreferredTransport: a2a.TransportProtocolJSONRPC,
		DefaultInputModes:  []string{"application/json"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{},
	}
	for _, t := range s.tools {
		skill := a2a.AgentSkill{ID: t.Name, Name: humanTitle(t), Description: t.Description + ".", Tags: []string{"risk:" + t.Risk}}
		if t.Entity != "" {
			skill.Tags = append(skill.Tags, strings.ToLower(t.Entity))
		}
		if t.Risk == v1alpha1.RiskRead {
			skill.Examples = []string{fmt.Sprintf(`{"skill": %q, "arguments": {"id": "..."}}`, t.Name)}
		} else {
			skill.Description = writeToolDef(t).Description
			skill.Examples = []string{fmt.Sprintf(`{"skill": %q, "arguments": {"record": {...}, "reason": "..."}}`, t.Name)}
		}
		card.Skills = append(card.Skills, skill)
	}
	return card
}

// a2aHTTP serves the agent card and the JSON-RPC endpoint. The request's
// context carries the authenticated identity.
func (s *Server) a2aHTTP() http.Handler {
	h := &a2aHandler{s: s, tools: map[string]compiler.Tool{}}
	for _, t := range s.tools {
		h.tools[t.Name] = t
	}
	rpc := a2asrv.NewJSONRPCHandler(h)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == a2aPath+cardPath {
			url := s.a2aURL
			if url == "" {
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				url = scheme + "://" + r.Host + a2aPath
			}
			a2asrv.NewStaticAgentCardHandler(s.agentCard(url)).ServeHTTP(w, r)
			return
		}
		rpc.ServeHTTP(w, r)
	})
}

func callerOf(ctx context.Context) (Identity, error) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	if !ok || id.Agent == "" {
		return Identity{}, a2a.ErrUnauthenticated
	}
	return id, nil
}

// request is the structured request a message carries.
type request struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

func parseRequest(m *a2a.Message) (request, error) {
	if m == nil {
		return request{}, fmt.Errorf("%w: no message", a2a.ErrInvalidParams)
	}
	for _, p := range m.Parts {
		d, ok := p.(a2a.DataPart)
		if !ok {
			continue
		}
		b, err := json.Marshal(d.Data)
		if err != nil {
			return request{}, fmt.Errorf("%w: %v", a2a.ErrInvalidParams, err)
		}
		var r request
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&r); err != nil || r.Skill == "" {
			return request{}, fmt.Errorf(`%w: the data part must be {"skill": "...", "arguments": {...}}`, a2a.ErrInvalidParams)
		}
		if len(r.Arguments) == 0 {
			r.Arguments = json.RawMessage("{}")
		}
		return r, nil
	}
	return request{}, fmt.Errorf(`%w: Turgon takes structured requests; send a data part {"skill": "...", "arguments": {...}}`, a2a.ErrUnsupportedContentType)
}

func (h *a2aHandler) OnSendMessage(ctx context.Context, p *a2a.MessageSendParams) (a2a.SendMessageResult, error) {
	id, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	req, err := parseRequest(p.Message)
	if err != nil {
		return nil, err
	}
	t, ok := h.tools[req.Skill]
	if !ok {
		return nil, fmt.Errorf("%w: no skill %q", a2a.ErrInvalidParams, req.Skill)
	}
	if t.Risk == v1alpha1.RiskRead {
		data, err := h.s.read(ctx, t, id, req.Arguments)
		if err != nil {
			return reply(p.Message, a2a.TextPart{Text: err.Error()}), nil
		}
		var record map[string]any
		_ = json.Unmarshal(data, &record)
		return reply(p.Message, a2a.DataPart{Data: record}), nil
	}
	w, err := h.s.submit(ctx, t, id, req.Arguments, p.Message.ID)
	if err != nil {
		return reply(p.Message, a2a.TextPart{Text: err.Error()}), nil
	}
	return task(t, w.workflowID, w.requestID, contextOf(p.Message, w.workflowID), w.status), nil
}

func (h *a2aHandler) OnGetTask(ctx context.Context, q *a2a.TaskQueryParams) (*a2a.Task, error) {
	id, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	toolName, owner, ok := writeOwner(string(q.ID))
	t, known := h.tools[toolName]
	// Another agent's task is reported as missing, not forbidden, so task
	// IDs reveal nothing.
	if !ok || !known || owner != id.Agent || t.Risk == v1alpha1.RiskRead {
		return nil, a2a.ErrTaskNotFound
	}
	st, err := h.s.writes.Status(ctx, string(q.ID))
	if errors.Is(err, engine.ErrUnknownWrite) {
		return nil, a2a.ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", a2a.ErrInternalError, err)
	}
	_, requestID, _ := strings.Cut(strings.TrimPrefix(string(q.ID), toolName+"/"), ":")
	return task(t, string(q.ID), requestID, string(q.ID), st), nil
}

// OnCancelTask: once asked for, a write is decided by a person, not
// withdrawn by the agent.
func (h *a2aHandler) OnCancelTask(ctx context.Context, p *a2a.TaskIDParams) (*a2a.Task, error) {
	if _, err := h.OnGetTask(ctx, &a2a.TaskQueryParams{ID: p.ID}); err != nil {
		return nil, err
	}
	return nil, a2a.ErrTaskNotCancelable
}

func (h *a2aHandler) OnListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (h *a2aHandler) OnSendMessageStream(context.Context, *a2a.MessageSendParams) iter.Seq2[a2a.Event, error] {
	return unsupported
}

func (h *a2aHandler) OnResubscribeToTask(context.Context, *a2a.TaskIDParams) iter.Seq2[a2a.Event, error] {
	return unsupported
}

func unsupported(yield func(a2a.Event, error) bool) { yield(nil, a2a.ErrUnsupportedOperation) }

func (h *a2aHandler) OnGetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigParams) (*a2a.TaskPushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (h *a2aHandler) OnListTaskPushConfig(context.Context, *a2a.ListTaskPushConfigParams) ([]*a2a.TaskPushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (h *a2aHandler) OnSetTaskPushConfig(context.Context, *a2a.TaskPushConfig) (*a2a.TaskPushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (h *a2aHandler) OnDeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigParams) error {
	return a2a.ErrPushNotificationNotSupported
}

func (h *a2aHandler) OnGetExtendedAgentCard(context.Context) (*a2a.AgentCard, error) {
	return nil, a2a.ErrAuthenticatedExtendedCardNotConfigured
}

func contextOf(m *a2a.Message, fallback string) string {
	if m != nil && m.ContextID != "" {
		return m.ContextID
	}
	return fallback
}

func reply(to *a2a.Message, part a2a.Part) *a2a.Message {
	m := a2a.NewMessage(a2a.MessageRoleAgent, part)
	m.ContextID = to.ContextID
	return m
}

// task reports an agent write as an A2A task.
func task(t compiler.Tool, workflowID, requestID, contextID string, st engine.AgentWriteStatus) *a2a.Task {
	summary, _ := writeSummary(t, requestID, st)
	now := time.Now().UTC()
	state := a2a.TaskStateFailed
	switch st.State {
	case engine.AgentWriteCommitted:
		state = a2a.TaskStateCompleted
	case engine.AgentWritePending:
		state = a2a.TaskStateWorking
		summary["message"] = "Nothing has been written yet: a person must approve this write in the Turgon console. Poll tasks/get for the outcome."
	case engine.AgentWriteRunning:
		state = a2a.TaskStateWorking
		summary["message"] = "The write is in progress. Poll tasks/get for the outcome."
	case engine.AgentWriteDenied, engine.AgentWriteRejected:
		state = a2a.TaskStateRejected
	}
	tk := &a2a.Task{
		ID: a2a.TaskID(workflowID), ContextID: contextID,
		Status: a2a.TaskStatus{
			State: state, Timestamp: &now,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.TextPart{Text: summary["message"].(string)}, a2a.DataPart{Data: summary}),
		},
	}
	if st.State == engine.AgentWriteCommitted && st.Write != nil {
		var result map[string]any
		if json.Unmarshal(st.Write.Result, &result) == nil {
			tk.Artifacts = []*a2a.Artifact{{ID: "result", Name: t.Name + " result", Parts: a2a.ContentParts{a2a.DataPart{Data: result}}}}
		}
	}
	return tk
}
