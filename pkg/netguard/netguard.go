// Package netguard keeps requests Turgon makes on someone else's behalf
// (A2A push notifications to URLs agents choose, HTTP calls from logic
// plugins) on the public internet. They leave from inside the customer's
// network, and must not reach an internal system or a cloud metadata
// endpoint unless an operator allowed its address range.
package netguard

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// Guard decides which URLs Turgon calls on someone else's behalf.
type Guard struct {
	// Allow lists address ranges that may be notified although they are
	// not public, for agent platforms inside the network. Plain http is
	// accepted only for these.
	Allow []netip.Prefix
	// Resolver looks up host names; default net.DefaultResolver.
	Resolver interface {
		LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
	}
	// TLSConfig, if set, replaces the default TLS settings (tests).
	TLSConfig *tls.Config
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
func (g Guard) permitted(ip netip.Addr) bool {
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

func (g Guard) allowed(ip netip.Addr) bool {
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
func (g Guard) CheckURL(ctx context.Context, raw string) error {
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
			return fmt.Errorf("%s is not a public address; Turgon only calls public addresses, or ranges its operators allow", u.Hostname())
		}
		if u.Scheme == "http" && !g.allowed(ip) {
			return errors.New("the URL must use https")
		}
	}
	return nil
}

func (g Guard) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
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
var ErrRefused = errors.New("not a permitted address")

// Client returns an HTTP client that connects only where the guard
// permits, goes direct (an egress proxy would connect on its behalf,
// unchecked) and follows no redirects.
func (g Guard) Client() *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second, Control: func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil || !g.permitted(ap.Addr()) {
			return fmt.Errorf("%s: %w", address, ErrRefused)
		}
		return nil
	}}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DialContext = d.DialContext
	if g.TLSConfig != nil {
		t.TLSClientConfig = g.TLSConfig
	}
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
