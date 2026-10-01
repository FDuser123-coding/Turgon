package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/engine"
)

// A2A push notifications are HTTP POSTs to URLs agents choose, sent from
// inside the customer's network. PushGuard keeps them from reaching
// anything but the public internet: an agent must not be able to make
// Turgon call an internal system (or a cloud metadata endpoint) for it.

// PushGuard decides which URLs Turgon notifies.
type PushGuard struct {
	// Allow lists address ranges that may be notified although they are
	// not public, for agent platforms inside the network. Plain http is
	// accepted only for these.
	Allow []netip.Prefix
	// Resolver looks up host names; default net.DefaultResolver.
	Resolver interface {
		LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
	}
}

// Not public, though IsGlobalUnicast says so, or able to reach private
// addresses through a translation.
var notPublic = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),   // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // documentation
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64
	netip.MustParsePrefix("64:ff9b:1::/48"),  // local NAT64
	netip.MustParsePrefix("2001::/32"),       // Teredo
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("2002::/16"),       // 6to4
}

// permitted reports whether Turgon may connect to ip.
func (g PushGuard) permitted(ip netip.Addr) bool {
	ip = ip.Unmap()
	for _, p := range g.Allow {
		if p.Contains(ip) {
			return true
		}
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	for _, p := range notPublic {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

func (g PushGuard) allowed(ip netip.Addr) bool {
	ip = ip.Unmap()
	for _, p := range g.Allow {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// CheckURL tells an agent at once whether Turgon will call a URL. The
// addresses are checked again at every connection, since a name can
// resolve differently later.
func (g PushGuard) CheckURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("the URL must be https://host/path, without credentials in it")
	}
	ips, err := g.resolve(ctx, u.Hostname())
	if err != nil {
		return fmt.Errorf("%s does not resolve: %v", u.Hostname(), err)
	}
	for _, ip := range ips {
		if !g.permitted(ip) {
			return fmt.Errorf("%s is not a public address; Turgon only notifies public URLs, or ranges its operators allow (TURGON_A2A_PUSH_ALLOW)", u.Hostname())
		}
		if u.Scheme == "http" && !g.allowed(ip) {
			return errors.New("the URL must use https")
		}
	}
	return nil
}

func (g PushGuard) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	r := g.Resolver
	if r == nil {
		r = net.DefaultResolver
	}
	ips, err := r.LookupNetIP(ctx, "ip", host)
	if err == nil && len(ips) == 0 {
		err = errors.New("no addresses")
	}
	return ips, err
}

// errRefused marks a connection the guard stopped.
var errRefused = errors.New("not a permitted address")

// Client returns an HTTP client that connects only where the guard
// permits, goes direct (an egress proxy would connect on its behalf,
// unchecked) and follows no redirects.
func (g PushGuard) Client() *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second, Control: func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil || !g.permitted(ap.Addr()) {
			return fmt.Errorf("%s: %w", address, errRefused)
		}
		return nil
	}}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DialContext = d.DialContext
	return &http.Client{
		Transport: t, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// ParseAllow reads a comma-separated list of CIDRs (or addresses).
func ParseAllow(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		p, err := netip.ParsePrefix(f)
		if err != nil {
			a, aerr := netip.ParseAddr(f)
			if aerr != nil {
				return nil, fmt.Errorf("%q is not a CIDR or an address", f)
			}
			p = netip.PrefixFrom(a, a.BitLen())
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

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
	if errors.Is(err, errRefused) {
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
