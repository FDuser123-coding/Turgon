package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// requestIDRE limits request IDs to characters that are safe in workflow
// IDs and idempotency keys.
var requestIDRE = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

const maxReason = 500

func writeToolDef(t compiler.Tool) *mcp.Tool {
	entity := strings.ToLower(orEntity(t.Entity))
	record := map[string]any{"type": "object", "description": fmt.Sprintf("The %s, in business terms", entity)}
	if len(t.Fields) > 0 {
		props := map[string]any{}
		for _, f := range t.Fields {
			props[f] = map[string]any{}
		}
		record["properties"] = props
		record["additionalProperties"] = false
	}
	desc := t.Description + "."
	if t.Risk == v1alpha1.RiskHigh {
		desc += " High-risk: a person must approve it before anything is written."
	}
	if t.Simulate {
		desc += " The target previews the result first."
	}
	desc += " Give each new write its own requestId; calling again with the same arguments reports how the write stands and never writes twice."
	closedWorld, destructive := false, t.Risk == v1alpha1.RiskHigh
	return &mcp.Tool{
		Name:        t.Name,
		Description: desc,
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"requestId": map[string]any{
					"type": "string", "pattern": requestIDRE.String(),
					"description": "Your identifier for this write, unique per write, for example a UUID",
				},
				"record": record,
				"reason": map[string]any{
					"type": "string", "maxLength": maxReason,
					"description": "Why the write is needed; the approver and the audit log see it",
				},
			},
			"required":             []string{"requestId", "record"},
			"additionalProperties": false,
		},
		Annotations: &mcp.ToolAnnotations{
			Title: humanTitle(t), IdempotentHint: true, DestructiveHint: &destructive, OpenWorldHint: &closedWorld,
		},
	}
}

// write returns the MCP handler for one write tool.
func (s *Server) write(t compiler.Tool) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, ok := identity(req)
		if !ok {
			return failure("not authenticated"), nil
		}
		w, err := s.submit(ctx, t, id, req.Params.Arguments, "")
		if err != nil {
			return failure(err.Error()), nil
		}
		return writeResult(t, w.requestID, w.status), nil
	}
}

// writeCall is a write a caller asked for, and where it stands.
type writeCall struct {
	workflowID, requestID string
	status                engine.AgentWriteStatus
}

// submit starts a write tool's governed write for a caller, or reports on
// the one already started for the same request. defaultRequestID is used
// when the arguments carry no requestId (A2A messages have their own ID).
// Its errors are messages for the agent.
func (s *Server) submit(ctx context.Context, t compiler.Tool, id Identity, raw json.RawMessage, defaultRequestID string) (writeCall, error) {
	var args struct {
		RequestID string         `json:"requestId"`
		Record    map[string]any `json:"record"`
		Reason    string         `json:"reason"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&args); err != nil || args.Record == nil {
		return writeCall{}, errors.New(`pass {"requestId": "...", "record": {...}, "reason": "..."}`)
	}
	if args.RequestID == "" {
		args.RequestID = defaultRequestID
	}
	if !requestIDRE.MatchString(args.RequestID) {
		return writeCall{}, errors.New("requestId must be 1 to 128 letters, digits, '.', '_', ':' or '-'")
	}
	if len(args.Reason) > maxReason {
		return writeCall{}, fmt.Errorf("reason is longer than %d characters", maxReason)
	}
	if len(t.Fields) > 0 {
		allowed := map[string]bool{}
		for _, f := range t.Fields {
			allowed[f] = true
		}
		var unknown []string
		for k := range args.Record {
			if !allowed[k] {
				unknown = append(unknown, k)
			}
		}
		if len(unknown) > 0 {
			sort.Strings(unknown)
			return writeCall{}, fmt.Errorf("record has unknown field(s) %s; %s takes %s",
				strings.Join(unknown, ", "), t.Name, strings.Join(t.Fields, ", "))
		}
	}
	// Maps marshal with sorted keys, so the same record always gives the
	// same payload and the same request digest.
	payload, err := json.Marshal(args.Record)
	if err != nil {
		return writeCall{}, errors.New("unreadable record")
	}
	var amount float64
	if n, ok := args.Record["netValue"].(json.Number); ok {
		amount, _ = n.Float64()
	}
	// The audit log and the approver see who asked; the reason is the
	// agent's own words.
	reason := strings.TrimSpace(args.Reason)
	if reason == "" {
		reason = "no reason given"
	}
	wreq := writeguard.Request{
		Target: t.Endpoint, Operation: t.Operation, Tool: t.Name, Risk: t.Risk,
		Subject:        policy.Subject{ID: id.Agent, Agent: true, OnBehalfOf: id.OnBehalfOf, Roles: id.Roles},
		IdempotencyKey: "agent:" + id.Agent + ":" + args.RequestID,
		Payload:        payload, Entity: t.Entity, Amount: amount, Simulate: t.Simulate, Reason: reason,
	}
	w := writeCall{workflowID: writeID(t.Name, id.Agent, args.RequestID), requestID: args.RequestID}
	w.status, err = s.writes.Submit(ctx, w.workflowID, engine.AgentWriteInput{SpecDigest: s.digest, Request: wreq, ApprovalTimeout: s.timeout})
	switch {
	case errors.Is(err, engine.ErrRequestConflict):
		return w, fmt.Errorf("requestId %q was already used for a different %s request; use a new requestId", args.RequestID, t.Name)
	case err != nil:
		return w, errors.New("could not start the write: " + err.Error())
	}
	return w, nil
}

// writeID names the workflow of one agent's request: the tool first, so
// the console lists agent writes by tool.
func writeID(tool, agent, requestID string) string {
	return tool + "/" + escapeAgent(agent) + ":" + requestID
}

// escapeAgent escapes an agent's name for a write ID; ':' is escaped too,
// so the first ':' always ends the agent's part.
func escapeAgent(agent string) string {
	return strings.ReplaceAll(url.PathEscape(agent), ":", "%3A")
}

// writeOwner returns the tool and the agent a write workflow belongs to.
func writeOwner(workflowID string) (tool, agent string, ok bool) {
	tool, rest, ok := strings.Cut(workflowID, "/")
	if !ok {
		return "", "", false
	}
	escaped, _, ok := strings.Cut(rest, ":")
	if !ok {
		return "", "", false
	}
	agent, err := url.PathUnescape(escaped)
	return tool, agent, err == nil && tool != "" && agent != ""
}

func writeResult(t compiler.Tool, requestID string, st engine.AgentWriteStatus) *mcp.CallToolResult {
	out, isErr := writeSummary(t, requestID, st)
	b, _ := json.Marshal(out)
	return &mcp.CallToolResult{
		IsError:           isErr,
		Content:           []mcp.Content{&mcp.TextContent{Text: string(b)}},
		StructuredContent: out,
	}
}

// writeSummary describes where a write stands, for the agent: its state,
// a message, and the result, preview or reasons. isErr is set when nothing
// was or will be written.
func writeSummary(t compiler.Tool, requestID string, st engine.AgentWriteStatus) (out map[string]any, isErr bool) {
	out = map[string]any{"state": st.State, "requestId": requestID}
	var text string
	switch st.State {
	case engine.AgentWriteCommitted:
		text = fmt.Sprintf("Done: %s committed in %s.", t.Name, t.Endpoint)
		if st.Write != nil {
			var result any
			if json.Unmarshal(st.Write.Result, &result) == nil {
				out["result"] = result
			}
			if st.Write.Status == writeguard.StatusDuplicate {
				text = fmt.Sprintf("Done: %s had already been committed in %s; nothing was written again.", t.Name, t.Endpoint)
			}
		}
	case engine.AgentWritePending:
		text = "Nothing has been written yet: a person must approve this write in the Turgon console"
		if st.Pending != nil {
			if len(st.Pending.Reasons) > 0 {
				text += " (" + strings.Join(st.Pending.Reasons, ", ") + ")"
				out["reasons"] = st.Pending.Reasons
			}
			var preview any
			if json.Unmarshal(st.Pending.Preview, &preview) == nil && preview != nil {
				out["preview"] = preview
			}
		}
		text += fmt.Sprintf(". Call %s again with the same arguments to check on it.", t.Name)
	case engine.AgentWriteRunning:
		text = fmt.Sprintf("The write is in progress. Call %s again with the same arguments to check on it.", t.Name)
	case engine.AgentWriteDenied:
		text, isErr = "Denied by policy; nothing was written. "+st.Message, true
	case engine.AgentWriteRejected:
		text, isErr = "The approver rejected this write; nothing was written. "+st.Message, true
	case engine.AgentWriteInvalid:
		text, isErr = "The record is not valid for "+t.Endpoint+"; nothing was written. Fix it and use a new requestId. "+st.Message, true
	default:
		text, isErr = "The write failed. Use a new requestId to try again. "+st.Message, true
	}
	out["message"] = strings.TrimSpace(text)
	return out, isErr
}
