package agent

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/policy"
)

// GatewayVersion is the agentgateway release the generated configuration
// is written for, and the image the Helm chart runs.
const GatewayVersion = "v1.5.0"

// GatewayRoute exposes one spec's MCP server through the gateway at
// /<Name>/mcp, and its A2A agent at /<Name>/a2a.
type GatewayRoute struct {
	Name     string
	Spec     *compiler.RuntimeSpec
	Upstream string // the `porter mcp` URL, e.g. http://127.0.0.1:8090/
	// Writes reports whether the upstream serves write tools.
	Writes bool
}

// GatewayOptions configure agent authentication at the gateway.
type GatewayOptions struct {
	Port int // default 3000
	// ReadinessAddr serves the gateway's readiness probe; default 0.0.0.0:15021.
	ReadinessAddr string
	// The identity provider that issues agents' access tokens.
	Issuer    string
	Audiences []string
	// JWKS is the provider's key set: an https URL, or a file path.
	JWKS string
	// Claims naming the agent, the user it acts for and the roles it holds
	// (a list of strings). Defaults: azp, sub, roles. Nested claims use dots
	// (act.sub).
	AgentClaim, UserClaim, RolesClaim string
	// RateLimit caps tool traffic per spec; zero means no limit.
	RateLimit RateLimit
}

// RateLimit is a token bucket: Burst requests at most, refilled at
// Requests per Per.
type RateLimit struct {
	Requests, Burst int
	Per             time.Duration
}

var (
	claimRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	routeNameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	toolNameRE  = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

// GatewayConfig writes the agentgateway configuration that fronts Turgon's
// MCP servers (architecture §7.7). The gateway:
//
//   - requires a valid JWT from the customer's identity provider on every
//     call, and drops it before forwarding, so Turgon never sees tokens;
//   - sets X-Agent-Id, X-On-Behalf-Of and X-Agent-Roles from the token's
//     claims, which Turgon trusts only from the gateway. When a claim is
//     missing the gateway removes the header instead, so a caller can
//     never supply its own identity;
//   - lists and allows only the tools the caller's roles may use, by risk
//     tier: read tools for integration-reader and integration-operator,
//     write tools for integration-operator. Turgon's own policy still
//     decides every call;
//   - rate-limits each spec's tool traffic;
//   - routes /<spec>/a2a to the same server's A2A agent, with the same
//     authentication, identity headers and rate limit.
func GatewayConfig(routes []GatewayRoute, o GatewayOptions) ([]byte, error) {
	if len(routes) == 0 {
		return nil, errors.New("gateway: no specs to route")
	}
	if o.Issuer == "" || o.JWKS == "" {
		return nil, errors.New("gateway: the token issuer and its JWKS are required")
	}
	if o.Port == 0 {
		o.Port = 3000
	}
	if o.ReadinessAddr == "" {
		o.ReadinessAddr = "0.0.0.0:15021"
	}
	agent, err := claim(o.AgentClaim, "azp")
	if err != nil {
		return nil, err
	}
	user, err := claim(o.UserClaim, "sub")
	if err != nil {
		return nil, err
	}
	roles, err := claim(o.RolesClaim, "roles")
	if err != nil {
		return nil, err
	}
	jwks := map[string]any{"file": o.JWKS}
	if strings.HasPrefix(o.JWKS, "https://") || strings.HasPrefix(o.JWKS, "http://") {
		jwks = map[string]any{"url": o.JWKS}
	}
	provider := map[string]any{"issuer": o.Issuer, "jwks": jwks}
	if len(o.Audiences) > 0 {
		provider["audiences"] = o.Audiences
	}

	sorted := append([]GatewayRoute{}, routes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var out []any
	seen := map[string]bool{}
	for _, r := range sorted {
		if !routeNameRE.MatchString(r.Name) || seen[r.Name] {
			return nil, fmt.Errorf("gateway: route name %q must be a unique DNS label", r.Name)
		}
		seen[r.Name] = true
		rules, err := toolRules(r, roles)
		if err != nil {
			return nil, err
		}
		policies := map[string]any{
			"jwtAuth": map[string]any{"mode": "strict", "providers": []any{provider}},
			"transformations": map[string]any{"request": map[string]any{"set": map[string]any{
				"x-agent-id":     agent,
				"x-on-behalf-of": user,
				"x-agent-roles":  roles + `.join(",")`,
			}}},
			"mcpAuthorization": map[string]any{"rules": rules},
		}
		if rl := o.RateLimit; rl.Requests > 0 {
			burst := rl.Burst
			if burst < rl.Requests {
				burst = rl.Requests
			}
			per := rl.Per
			if per <= 0 {
				per = time.Second
			}
			policies["localRateLimit"] = []any{map[string]any{
				"maxTokens": burst, "tokensPerFill": rl.Requests, "fillInterval": interval(per),
			}}
		}
		out = append(out, map[string]any{
			"name":     r.Name,
			"matches":  []any{map[string]any{"path": map[string]any{"exact": "/" + r.Name + "/mcp"}}},
			"policies": policies,
			"backends": []any{map[string]any{"mcp": map[string]any{
				"targets": []any{map[string]any{"name": r.Name, "mcp": map[string]any{"host": r.Upstream}}},
			}}},
		})
		// The same server is an A2A agent under /a2a. The gateway rewrites
		// the agent card's URL to its own address. Skills are authorized by
		// Turgon's policy; the gateway authenticates and rate-limits.
		hostPort, err := upstreamHost(r.Upstream)
		if err != nil {
			return nil, err
		}
		a2aPolicies := map[string]any{"a2a": map[string]any{}}
		for _, k := range []string{"jwtAuth", "transformations", "localRateLimit"} {
			if v, ok := policies[k]; ok {
				a2aPolicies[k] = v
			}
		}
		a2aPolicies["urlRewrite"] = map[string]any{"path": map[string]any{"prefix": a2aPath}}
		out = append(out, map[string]any{
			"name":     r.Name + "-a2a",
			"matches":  []any{map[string]any{"path": map[string]any{"pathPrefix": "/" + r.Name + a2aPath}}},
			"policies": a2aPolicies,
			"backends": []any{map[string]any{"host": hostPort}},
		})
	}
	cfg := map[string]any{
		"config": map[string]any{"readinessAddr": o.ReadinessAddr},
		"binds": []any{map[string]any{
			"port":      o.Port,
			"listeners": []any{map[string]any{"protocol": "HTTP", "routes": out}},
		}},
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	head := "# agentgateway " + GatewayVersion + " configuration, generated by `porter gateway-config`.\n" +
		"# yaml-language-server: $schema=https://agentgateway.dev/schema/config\n"
	return append([]byte(head), body...), nil
}

// upstreamHost returns the host:port of an http upstream URL.
func upstreamHost(upstream string) (string, error) {
	u, err := url.Parse(upstream)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return "", fmt.Errorf("gateway: upstream %q must be an http://host:port/ URL", upstream)
	}
	if u.Port() == "" {
		return u.Host + ":80", nil
	}
	return u.Host, nil
}

// interval writes a duration the way agentgateway's examples do: "60s",
// or milliseconds when it is not a whole number of seconds.
func interval(d time.Duration) string {
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
}

// claim returns the CEL expression reading a JWT claim.
func claim(name, def string) (string, error) {
	if name == "" {
		name = def
	}
	if !claimRE.MatchString(name) {
		return "", fmt.Errorf("gateway: %q is not a claim name", name)
	}
	return "jwt." + name, nil
}

// toolRules allows each tool, by risk tier, to callers holding a role
// that the built-in policy lets use that tier.
func toolRules(r GatewayRoute, roles string) ([]string, error) {
	var reads, writes []string
	for _, t := range r.Spec.Spec.Tools {
		if !toolNameRE.MatchString(t.Name) {
			return nil, fmt.Errorf("gateway: tool name %q", t.Name)
		}
		switch {
		case t.Risk == v1alpha1.RiskRead:
			reads = append(reads, strconv.Quote(t.Name))
		case r.Writes:
			writes = append(writes, strconv.Quote(t.Name))
		}
	}
	has := func(role string) string { return strconv.Quote(role) + " in " + roles }
	var rules []string
	if len(reads) > 0 {
		sort.Strings(reads)
		rules = append(rules, fmt.Sprintf("mcp.tool.name in [%s] && (%s || %s)",
			strings.Join(reads, ", "), has(policy.RoleReader), has(policy.RoleOperator)))
	}
	if len(writes) > 0 {
		sort.Strings(writes)
		rules = append(rules, fmt.Sprintf("mcp.tool.name in [%s] && %s", strings.Join(writes, ", "), has(policy.RoleOperator)))
	}
	if len(rules) == 0 {
		// An empty rule set would allow everything.
		rules = []string{"false"}
	}
	return rules, nil
}
