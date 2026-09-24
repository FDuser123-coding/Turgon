package agent

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Identity is who is calling: the agent and the person it acts for.
type Identity struct {
	Agent      string   `json:"agent"`
	OnBehalfOf string   `json:"onBehalfOf,omitempty"`
	Roles      []string `json:"roles"`
}

// Authenticator identifies the agent behind a request.
type Authenticator interface {
	Authenticate(r *http.Request) (Identity, error)
}

// ErrUnauthenticated is returned for requests without a trusted identity.
var ErrUnauthenticated = errors.New("not authenticated")

// DevAuth treats every request as one agent; loopback only (see Server.Serve).
type DevAuth struct{ Identity Identity }

func (d DevAuth) Authenticate(*http.Request) (Identity, error) { return d.Identity, nil }

// GatewayAuth trusts identity headers set by the agent gateway
// (agentgateway, architecture §7.7), which authenticates agents with JWTs,
// but only on requests arriving from the gateway's addresses.
type GatewayAuth struct {
	Trusted     []netip.Prefix
	AgentHeader string // default X-Agent-Id
	UserHeader  string // default X-On-Behalf-Of
	RolesHeader string // default X-Agent-Roles, comma-separated
}

func (g GatewayAuth) Authenticate(r *http.Request) (Identity, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return Identity{}, ErrUnauthenticated
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return Identity{}, ErrUnauthenticated
	}
	trusted := false
	for _, p := range g.Trusted {
		trusted = trusted || p.Contains(addr.Unmap())
	}
	if !trusted {
		return Identity{}, ErrUnauthenticated
	}
	ah, uh, rh := or(g.AgentHeader, "X-Agent-Id"), or(g.UserHeader, "X-On-Behalf-Of"), or(g.RolesHeader, "X-Agent-Roles")
	id := Identity{Agent: strings.TrimSpace(r.Header.Get(ah)), OnBehalfOf: strings.TrimSpace(r.Header.Get(uh))}
	if id.Agent == "" {
		return Identity{}, ErrUnauthenticated
	}
	for _, role := range strings.Split(r.Header.Get(rh), ",") {
		if role = strings.TrimSpace(role); role != "" {
			id.Roles = append(id.Roles, role)
		}
	}
	return id, nil
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
