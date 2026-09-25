package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/agent"
)

func gatewayConfigCmd() *cobra.Command {
	var specs, upstreams map[string]string
	var writes []string
	var output, rate string
	var o agent.GatewayOptions
	cmd := &cobra.Command{
		Use:   "gateway-config",
		Short: "Write the agentgateway configuration that fronts `turgon mcp`",
		Long: "Write an agentgateway " + agent.GatewayVersion + " configuration exposing each spec's MCP server at\n" +
			"/<name>/mcp. The gateway requires a JWT from your identity provider, passes the agent, the\n" +
			"user it acts for and its roles to Turgon from the token's claims, lists and allows only the\n" +
			"tools the caller's roles may use, and rate-limits tool traffic.\n\n" +
			"By default spec number i (in name order) is reached at http://127.0.0.1:<8090+i>/, which is\n" +
			"how the Helm chart runs `turgon mcp` next to the gateway.",
		Example: "  turgon gateway-config --spec shop=shop.json --writes shop \\\n" +
			"    --issuer https://login.example.com --audience turgon --jwks https://login.example.com/jwks.json",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(specs) == 0 {
				return errors.New("pass at least one --spec name=runtime-spec.json")
			}
			names := make([]string, 0, len(specs))
			for n := range specs {
				names = append(names, n)
			}
			sort.Strings(names)
			withWrites := map[string]bool{}
			for _, w := range writes {
				if _, ok := specs[w]; !ok {
					return fmt.Errorf("--writes %s: no such --spec", w)
				}
				withWrites[w] = true
			}
			var routes []agent.GatewayRoute
			for i, n := range names {
				spec, err := loadSpec(specs[n])
				if err != nil {
					return err
				}
				up := upstreams[n]
				if up == "" {
					up = fmt.Sprintf("http://127.0.0.1:%d/", 8090+i)
				}
				routes = append(routes, agent.GatewayRoute{Name: n, Spec: spec, Upstream: up, Writes: withWrites[n]})
			}
			if rate != "" {
				rl, err := parseRate(rate)
				if err != nil {
					return err
				}
				o.RateLimit = rl
			}
			cfg, err := agent.GatewayConfig(routes, o)
			if err != nil {
				return err
			}
			if output == "" || output == "-" {
				_, err = cmd.OutOrStdout().Write(cfg)
				return err
			}
			return os.WriteFile(output, cfg, 0o644)
		},
	}
	f := cmd.Flags()
	f.StringToStringVar(&specs, "spec", nil, "name=runtime-spec.json, repeatable; name is the route (/<name>/mcp)")
	f.StringToStringVar(&upstreams, "upstream", nil, "name=URL of that spec's `turgon mcp`")
	f.StringSliceVar(&writes, "writes", nil, "specs whose MCP server runs with --writes")
	f.StringVar(&o.Issuer, "issuer", "", "issuer of agents' access tokens")
	f.StringSliceVar(&o.Audiences, "audience", nil, "accepted token audiences")
	f.StringVar(&o.JWKS, "jwks", "", "the issuer's JWKS: https URL or file path")
	f.StringVar(&o.AgentClaim, "agent-claim", "azp", "claim naming the agent")
	f.StringVar(&o.UserClaim, "user-claim", "sub", "claim naming the user the agent acts for")
	f.StringVar(&o.RolesClaim, "roles-claim", "roles", "claim listing the caller's roles")
	f.IntVar(&o.Port, "port", 3000, "port the gateway listens on")
	f.StringVar(&rate, "rate-limit", "", `tool calls allowed per spec, e.g. "100/1m" or "20/1s"`)
	f.StringVarP(&output, "output", "o", "", "write to a file instead of stdout")
	return cmd
}

// parseRate reads "N/duration".
func parseRate(s string) (agent.RateLimit, error) {
	n, per, ok := strings.Cut(s, "/")
	requests, err := strconv.Atoi(n)
	if !ok || err != nil || requests <= 0 {
		return agent.RateLimit{}, fmt.Errorf("--rate-limit %q: want N/duration, e.g. 100/1m", s)
	}
	d, err := time.ParseDuration(per)
	if err != nil || d <= 0 {
		return agent.RateLimit{}, fmt.Errorf("--rate-limit %q: want N/duration, e.g. 100/1m", s)
	}
	return agent.RateLimit{Requests: requests, Per: d}, nil
}
