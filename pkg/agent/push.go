package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/netguard"
)

// A2A push notifications are HTTP POSTs to URLs agents choose, sent from
// inside the customer's network: netguard keeps them on the public
// internet unless TURGON_A2A_PUSH_ALLOW lists more ranges.

// PushGuard decides which URLs Turgon notifies.
type PushGuard = netguard.Guard

// ParseAllow reads a comma-separated list of CIDRs (or addresses).
var ParseAllow = netguard.ParseAllow

// Pusher delivers push notifications for agent writes: the activity
// (engine.ActivityAgentPush) their workflow runs on `turgon run` workers.
// The body is the write as an A2A task, as tasks/get reports it.
type Pusher struct {
	tools  map[string]compiler.Tool
	guard  PushGuard
	client *http.Client
}

// NewPusher serves a spec's write tools.
func NewPusher(spec *compiler.RuntimeSpec, guard PushGuard) *Pusher {
	p := &Pusher{tools: map[string]compiler.Tool{}, guard: guard, client: guard.Client()}
	for _, t := range spec.Spec.Tools {
		p.tools[t.Name] = t
	}
	return p
}

// Push sends one notification. A receiver's 4xx answer or a URL the guard
// refuses is final; other failures are retried by the workflow.
func (p *Pusher) Push(ctx context.Context, in engine.AgentPushInput) error {
	t, ok := p.tools[in.Tool]
	if !ok {
		t = compiler.Tool{Name: in.Tool}
	}
	_, requestID, _ := strings.Cut(strings.TrimPrefix(in.WorkflowID, in.Tool+"/"), ":")
	body, err := json.Marshal(task(t, in.WorkflowID, requestID, in.WorkflowID, in.Status))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.Config.URL, bytes.NewReader(body))
	if err != nil {
		return temporal.NewNonRetryableApplicationError("bad push URL", engine.ErrTypePushRefused, err)
	}
	if req.URL.Scheme != "https" && !(req.URL.Scheme == "http" && p.guard.CheckURL(ctx, in.Config.URL) == nil) {
		return temporal.NewNonRetryableApplicationError("the push URL must use https", engine.ErrTypePushRefused, nil)
	}
	req.Header.Set("Content-Type", "application/json")
	if in.Config.Token != "" {
		req.Header.Set("X-A2A-Notification-Token", in.Config.Token)
	}
	if in.Config.Credentials != "" {
		for _, s := range in.Config.Schemes {
			if scheme := strings.ToLower(s); scheme == "bearer" || scheme == "basic" {
				req.Header.Set("Authorization", strings.ToUpper(scheme[:1])+scheme[1:]+" "+in.Config.Credentials)
				break
			}
		}
	}
	resp, err := p.client.Do(req)
	if errors.Is(err, netguard.ErrRefused) {
		return temporal.NewNonRetryableApplicationError(err.Error(), engine.ErrTypePushRefused, nil)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	// Redirects are not followed: the guard checked this URL, not another.
	case resp.StatusCode >= 300 && resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests:
		return temporal.NewNonRetryableApplicationError("the receiver answered "+resp.Status, engine.ErrTypePushRefused, nil)
	}
	return fmt.Errorf("the receiver answered %s", resp.Status)
}
