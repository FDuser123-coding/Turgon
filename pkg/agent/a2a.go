package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"regexp"
	"slices"
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
		Capabilities:       a2a.AgentCapabilities{Streaming: true, PushNotifications: s.writes != nil},
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
	// Keepalives hold streams open through proxies while a write waits.
	rpc := a2asrv.NewJSONRPCHandler(h, a2asrv.WithKeepAlive(15*time.Second))
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
		return request{}, a2a.NewError(a2a.ErrInvalidParams, "no message")
	}
	for _, p := range m.Parts {
		d, ok := p.(a2a.DataPart)
		if !ok {
			continue
		}
		b, err := json.Marshal(d.Data)
		if err != nil {
			return request{}, a2a.NewError(a2a.ErrInvalidParams, err.Error())
		}
		var r request
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&r); err != nil || r.Skill == "" {
			return request{}, a2a.NewError(a2a.ErrInvalidParams, `the data part must be {"skill": "...", "arguments": {...}}`)
		}
		if len(r.Arguments) == 0 {
			r.Arguments = json.RawMessage("{}")
		}
		return r, nil
	}
	return request{}, a2a.NewError(a2a.ErrUnsupportedContentType, `Turgon takes structured requests; send a data part {"skill": "...", "arguments": {...}}`)
}

func (h *a2aHandler) OnSendMessage(ctx context.Context, p *a2a.MessageSendParams) (a2a.SendMessageResult, error) {
	res, _, err := h.send(ctx, p)
	return res, err
}

// send answers a message: a read's reply, or a write's task. For a write
// it also returns what a stream needs to follow it.
func (h *a2aHandler) send(ctx context.Context, p *a2a.MessageSendParams) (a2a.SendMessageResult, *following, error) {
	id, err := callerOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	req, err := parseRequest(p.Message)
	if err != nil {
		return nil, nil, err
	}
	t, ok := h.tools[req.Skill]
	if !ok {
		return nil, nil, a2a.NewError(a2a.ErrInvalidParams, fmt.Sprintf("no skill %q", req.Skill))
	}
	if t.Risk == v1alpha1.RiskRead {
		data, err := h.s.read(ctx, t, id, req.Arguments)
		if err != nil {
			return reply(p.Message, a2a.TextPart{Text: err.Error()}), nil, nil
		}
		var record map[string]any
		_ = json.Unmarshal(data, &record)
		return reply(p.Message, a2a.DataPart{Data: record}), nil, nil
	}
	var push []engine.PushConfig
	if p.Config != nil && p.Config.PushConfig != nil {
		c, err := h.pushConfig(ctx, *p.Config.PushConfig)
		if err != nil {
			return nil, nil, err
		}
		push = append(push, c)
	}
	w, err := h.s.submit(ctx, t, id, req.Arguments, p.Message.ID, push)
	if err != nil {
		return reply(p.Message, a2a.TextPart{Text: err.Error()}), nil, nil
	}
	f := &following{tool: t, workflowID: w.workflowID, requestID: w.requestID, contextID: contextOf(p.Message, w.workflowID), last: w.status}
	return task(t, f.workflowID, f.requestID, f.contextID, w.status), f, nil
}

// following is a write a stream reports on.
type following struct {
	tool                             compiler.Tool
	workflowID, requestID, contextID string
	last                             engine.AgentWriteStatus
}

// ended reports whether a write has reached its outcome.
func ended(st engine.AgentWriteState) bool {
	return st != engine.AgentWritePending && st != engine.AgentWriteRunning
}

// watchWait is how long one wait for a write's outcome lasts; Temporal
// answers at once when the write ends.
const watchWait = 30 * time.Second

// follow streams a write's changes until it ends, or for the stream limit:
// a status update for each new state, and its result as an artifact
// before the final one.
func (h *a2aHandler) follow(ctx context.Context, f *following, yield func(a2a.Event, error) bool) {
	deadline := time.Now().Add(h.s.streamFor)
	for !ended(f.last.State) {
		left := time.Until(deadline)
		if left <= 0 {
			return // not final: the agent resubscribes, or polls tasks/get
		}
		st, err := h.s.writes.Watch(ctx, f.workflowID, min(watchWait, left))
		if err != nil {
			if ctx.Err() == nil {
				yield(nil, a2a.NewError(a2a.ErrInternalError, err.Error()))
			}
			return
		}
		if st.State == f.last.State {
			continue
		}
		f.last = st
		tk := task(f.tool, f.workflowID, f.requestID, f.contextID, st)
		final := ended(st.State)
		if final && len(tk.Artifacts) > 0 {
			if !yield(&a2a.TaskArtifactUpdateEvent{TaskID: tk.ID, ContextID: tk.ContextID, Artifact: tk.Artifacts[0], LastChunk: true}, nil) {
				return
			}
		}
		if !yield(&a2a.TaskStatusUpdateEvent{TaskID: tk.ID, ContextID: tk.ContextID, Status: tk.Status, Final: final}, nil) {
			return
		}
	}
}

// ownWrite returns the tool of a write task the caller started. Another
// agent's task is reported as missing, not forbidden, so task IDs reveal
// nothing.
func (h *a2aHandler) ownWrite(ctx context.Context, taskID a2a.TaskID) (compiler.Tool, error) {
	id, err := callerOf(ctx)
	if err != nil {
		return compiler.Tool{}, err
	}
	toolName, owner, ok := writeOwner(string(taskID))
	t, known := h.tools[toolName]
	if !ok || !known || owner != id.Agent || t.Risk == v1alpha1.RiskRead || h.s.writes == nil {
		return compiler.Tool{}, a2a.ErrTaskNotFound
	}
	return t, nil
}

func (h *a2aHandler) OnGetTask(ctx context.Context, q *a2a.TaskQueryParams) (*a2a.Task, error) {
	f, err := h.find(ctx, q.ID)
	if err != nil {
		return nil, err
	}
	return task(f.tool, f.workflowID, f.requestID, f.contextID, f.last), nil
}

// find reports on a write task the caller started.
func (h *a2aHandler) find(ctx context.Context, taskID a2a.TaskID) (*following, error) {
	t, err := h.ownWrite(ctx, taskID)
	if err != nil {
		return nil, err
	}
	st, err := h.s.writes.Status(ctx, string(taskID))
	if errors.Is(err, engine.ErrUnknownWrite) {
		return nil, a2a.ErrTaskNotFound
	}
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInternalError, err.Error())
	}
	_, requestID, _ := strings.Cut(strings.TrimPrefix(string(taskID), t.Name+"/"), ":")
	return &following{tool: t, workflowID: string(taskID), requestID: requestID, contextID: string(taskID), last: st}, nil
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

// OnSendMessageStream answers a read with its reply, and follows a write:
// the task first, then each change until its outcome.
func (h *a2aHandler) OnSendMessageStream(ctx context.Context, p *a2a.MessageSendParams) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		res, f, err := h.send(ctx, p)
		if err != nil {
			yield(nil, err)
			return
		}
		if !yield(res, nil) || f == nil {
			return
		}
		h.follow(ctx, f, yield)
	}
}

// OnResubscribeToTask follows a write again: where it stands now, then
// each change until its outcome.
func (h *a2aHandler) OnResubscribeToTask(ctx context.Context, p *a2a.TaskIDParams) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		f, err := h.find(ctx, p.ID)
		if err != nil {
			yield(nil, err)
			return
		}
		if !yield(task(f.tool, f.workflowID, f.requestID, f.contextID, f.last), nil) {
			return
		}
		h.follow(ctx, f, yield)
	}
}

var pushIDRE = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// pushConfig checks a push configuration an agent sent.
func (h *a2aHandler) pushConfig(ctx context.Context, c a2a.PushConfig) (engine.PushConfig, error) {
	if h.s.writes == nil {
		return engine.PushConfig{}, a2a.ErrPushNotificationNotSupported
	}
	if c.ID == "" {
		c.ID = "default"
	}
	if !pushIDRE.MatchString(c.ID) {
		return engine.PushConfig{}, a2a.NewError(a2a.ErrInvalidParams, "a push notification config id is 1 to 64 letters, digits, '.', '_', ':' or '-'")
	}
	if len(c.Token) > 512 {
		return engine.PushConfig{}, a2a.NewError(a2a.ErrInvalidParams, "the token is longer than 512 characters")
	}
	if err := h.s.push.CheckURL(ctx, c.URL); err != nil {
		return engine.PushConfig{}, a2a.NewError(a2a.ErrInvalidParams, fmt.Sprintf("push URL: %v", err))
	}
	out := engine.PushConfig{ID: c.ID, URL: c.URL, Token: c.Token}
	if c.Auth != nil && c.Auth.Credentials != "" {
		for _, s := range c.Auth.Schemes {
			if l := strings.ToLower(s); l == "bearer" || l == "basic" {
				out.Schemes, out.Credentials = []string{s}, c.Auth.Credentials
				break
			}
		}
		if out.Credentials == "" {
			return engine.PushConfig{}, a2a.NewError(a2a.ErrInvalidParams, "Turgon authenticates to push URLs with the Bearer or Basic scheme")
		}
		if len(out.Credentials) > 4096 {
			return engine.PushConfig{}, a2a.NewError(a2a.ErrInvalidParams, "the credentials are longer than 4096 characters")
		}
	}
	return out, nil
}

// asTaskPushConfig reports a configuration back, without its credentials.
func asTaskPushConfig(taskID a2a.TaskID, c engine.PushConfig) *a2a.TaskPushConfig {
	out := &a2a.TaskPushConfig{TaskID: taskID, Config: a2a.PushConfig{ID: c.ID, URL: c.URL, Token: c.Token}}
	if len(c.Schemes) > 0 {
		out.Config.Auth = &a2a.PushAuthInfo{Schemes: c.Schemes}
	}
	return out
}

func (h *a2aHandler) OnSetTaskPushConfig(ctx context.Context, p *a2a.TaskPushConfig) (*a2a.TaskPushConfig, error) {
	if _, err := h.ownWrite(ctx, p.TaskID); err != nil {
		return nil, err
	}
	c, err := h.pushConfig(ctx, p.Config)
	if err != nil {
		return nil, err
	}
	have, err := h.configs(ctx, p.TaskID)
	if err != nil {
		return nil, err
	}
	if len(have) >= engine.MaxPushConfigs && !slices.ContainsFunc(have, func(o engine.PushConfig) bool { return o.ID == c.ID }) {
		return nil, a2a.NewError(a2a.ErrInvalidParams, fmt.Sprintf("a task has at most %d push notification configs", engine.MaxPushConfigs))
	}
	switch err := h.s.writes.SetPush(ctx, string(p.TaskID), c); {
	case errors.Is(err, engine.ErrWriteFinished):
		return nil, a2a.NewError(a2a.ErrInvalidParams, "the task has ended; tasks/get reports its outcome")
	case err != nil:
		return nil, a2a.NewError(a2a.ErrInternalError, err.Error())
	}
	return asTaskPushConfig(p.TaskID, c), nil
}

func (h *a2aHandler) configs(ctx context.Context, taskID a2a.TaskID) ([]engine.PushConfig, error) {
	cs, err := h.s.writes.Push(ctx, string(taskID))
	if errors.Is(err, engine.ErrUnknownWrite) {
		return nil, a2a.ErrTaskNotFound
	}
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInternalError, err.Error())
	}
	return cs, nil
}

func (h *a2aHandler) OnGetTaskPushConfig(ctx context.Context, p *a2a.GetTaskPushConfigParams) (*a2a.TaskPushConfig, error) {
	if _, err := h.ownWrite(ctx, p.TaskID); err != nil {
		return nil, err
	}
	cs, err := h.configs(ctx, p.TaskID)
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		if p.ConfigID == "" || c.ID == p.ConfigID {
			return asTaskPushConfig(p.TaskID, c), nil
		}
	}
	return nil, a2a.NewError(a2a.ErrInvalidParams, fmt.Sprintf("no push notification config %q", p.ConfigID))
}

func (h *a2aHandler) OnListTaskPushConfig(ctx context.Context, p *a2a.ListTaskPushConfigParams) ([]*a2a.TaskPushConfig, error) {
	if _, err := h.ownWrite(ctx, p.TaskID); err != nil {
		return nil, err
	}
	cs, err := h.configs(ctx, p.TaskID)
	if err != nil {
		return nil, err
	}
	out := []*a2a.TaskPushConfig{}
	for _, c := range cs {
		out = append(out, asTaskPushConfig(p.TaskID, c))
	}
	return out, nil
}

// OnDeleteTaskPushConfig: deleting from a write that has ended does
// nothing, as there is nothing left to push.
func (h *a2aHandler) OnDeleteTaskPushConfig(ctx context.Context, p *a2a.DeleteTaskPushConfigParams) error {
	if _, err := h.ownWrite(ctx, p.TaskID); err != nil {
		return err
	}
	if err := h.s.writes.DeletePush(ctx, string(p.TaskID), p.ConfigID); err != nil && !errors.Is(err, engine.ErrWriteFinished) {
		return a2a.NewError(a2a.ErrInternalError, err.Error())
	}
	return nil
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
		summary["message"] = "Nothing has been written yet: a person must approve this write in the Turgon console. Follow it with tasks/resubscribe, a push notification config or tasks/get."
	case engine.AgentWriteRunning:
		state = a2a.TaskStateWorking
		summary["message"] = "The write is in progress. Follow it with tasks/resubscribe, a push notification config or tasks/get."
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
