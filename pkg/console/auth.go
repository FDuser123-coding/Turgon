package console

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Roles.
const (
	RoleViewer   = "viewer"
	RoleApprover = "approver"
	// RoleSteward links source records to master records.
	RoleSteward = "steward"
	// RoleOperator retries failed runs.
	RoleOperator = "operator"
)

// User is an authenticated console user.
type User struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles"`
}

func (u User) Has(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Authenticator identifies the user behind a request.
type Authenticator interface {
	Authenticate(r *http.Request) (User, error)
}

// ErrUnauthenticated is returned when a request carries no valid identity.
var ErrUnauthenticated = errors.New("not authenticated")

// DevAuth treats every request as one local user with every role. It is
// only allowed on a loopback listener (see Server.Serve).
type DevAuth struct{ User string }

func (d DevAuth) Authenticate(*http.Request) (User, error) {
	return User{ID: d.User, Roles: []string{RoleViewer, RoleApprover, RoleSteward, RoleOperator}}, nil
}

// ProxyAuth trusts identity headers set by an authenticating reverse proxy
// in front of the console (for example oauth2-proxy with the customer's
// OIDC provider), but only on requests arriving from the proxy's addresses.
type ProxyAuth struct {
	UserHeader    string // default X-Auth-Request-Email
	GroupsHeader  string // default X-Auth-Request-Groups, comma-separated
	Trusted       []netip.Prefix
	ApproverGroup string
	// StewardGroup's members resolve records in the data-steward queue.
	StewardGroup string
	// OperatorGroup's members retry failed runs.
	OperatorGroup string
	// ViewerGroup, if set, is required to see anything; otherwise every
	// authenticated user may view.
	ViewerGroup string
}

func (p ProxyAuth) Authenticate(r *http.Request) (User, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return User{}, ErrUnauthenticated
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return User{}, ErrUnauthenticated
	}
	trusted := false
	for _, pfx := range p.Trusted {
		if pfx.Contains(addr.Unmap()) {
			trusted = true
		}
	}
	if !trusted {
		return User{}, ErrUnauthenticated
	}
	uh, gh := p.UserHeader, p.GroupsHeader
	if uh == "" {
		uh = "X-Auth-Request-Email"
	}
	if gh == "" {
		gh = "X-Auth-Request-Groups"
	}
	id := strings.TrimSpace(r.Header.Get(uh))
	if id == "" {
		return User{}, ErrUnauthenticated
	}
	groups := map[string]bool{}
	for _, g := range strings.Split(r.Header.Get(gh), ",") {
		groups[strings.TrimSpace(g)] = true
	}
	u := User{ID: id}
	if p.ViewerGroup == "" || groups[p.ViewerGroup] {
		u.Roles = append(u.Roles, RoleViewer)
	}
	if p.ApproverGroup != "" && groups[p.ApproverGroup] {
		u.Roles = append(u.Roles, RoleApprover)
	}
	if p.StewardGroup != "" && groups[p.StewardGroup] {
		u.Roles = append(u.Roles, RoleSteward)
	}
	if p.OperatorGroup != "" && groups[p.OperatorGroup] {
		u.Roles = append(u.Roles, RoleOperator)
	}
	return u, nil
}
