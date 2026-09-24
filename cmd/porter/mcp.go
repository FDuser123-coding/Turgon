package main

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/agent"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func mcpCmd() *cobra.Command {
	var specPath, listen, authMode, dbURL, auditPath string
	var devAgent, devUser, devRoles string
	var trusted []string
	var writes bool
	var wait, approvalTimeout time.Duration
	var tf temporalFlags
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve a spec's tools to AI agents over MCP",
		Long: "Serve the tools of a compiled runtime spec over MCP (streamable HTTP).\n" +
			"With --auth gateway, run it behind the agent gateway, which authenticates agents and sets\n" +
			"X-Agent-Id, X-On-Behalf-Of and X-Agent-Roles; those are trusted only from --trusted-gateway\n" +
			"addresses. Reads need the integration-reader or integration-operator role and are audited.\n\n" +
			"With --writes, write tools start governed writes on Temporal, run by `porter run` workers\n" +
			"for the same spec. Agents need the integration-operator role to write, and high-risk\n" +
			"writes wait for a person to approve them in the console.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec, err := loadSpec(specPath)
			if err != nil {
				return err
			}
			var auth agent.Authenticator
			switch authMode {
			case "dev":
				if devAgent == "" {
					return errors.New("--dev-agent is required with --auth dev")
				}
				auth = agent.DevAuth{Identity: agent.Identity{Agent: devAgent, OnBehalfOf: devUser, Roles: strings.Split(devRoles, ",")}}
			case "gateway":
				if len(trusted) == 0 {
					return errors.New("--auth gateway needs --trusted-gateway")
				}
				g := agent.GatewayAuth{}
				for _, t := range trusted {
					p, err := netip.ParsePrefix(t)
					if err != nil {
						return fmt.Errorf("--trusted-gateway %q: %w", t, err)
					}
					g.Trusted = append(g.Trusted, p)
				}
				auth = g
			default:
				return fmt.Errorf("--auth must be dev or gateway, got %q", authMode)
			}

			ctx := cmd.Context()
			var rec audit.Recorder
			if auditPath == "postgres" {
				if dbURL == "" {
					return errors.New(`--audit-log postgres needs --database-url (or PORTER_DATABASE_URL)`)
				}
				pool, err := pgxpool.New(ctx, dbURL)
				if err != nil {
					return err
				}
				defer pool.Close()
				if err := pgstore.Migrate(ctx, pool); err != nil {
					return err
				}
				rec = pgstore.NewAuditLog(pool)
			} else {
				l, f, err := audit.OpenFile(auditPath)
				if err != nil {
					return err
				}
				defer f.Close()
				rec = l
			}
			opts := agent.Options{
				Registry: connectorRegistry(), Secrets: connector.EnvSecrets{}, Audit: rec, Auth: auth, Version: version,
				ApprovalTimeout: approvalTimeout,
			}
			if writes {
				c, err := tf.dial()
				if err != nil {
					return fmt.Errorf("temporal: %w", err)
				}
				defer c.Close()
				queue := tf.taskQueue
				if queue == "" {
					queue = engine.TaskQueueFor(spec)
				}
				opts.Writes = engine.AgentWrites{Client: c, TaskQueue: queue, Wait: wait}
			}
			s, err := agent.New(ctx, spec, opts)
			if err != nil {
				return err
			}
			defer s.Close()
			var names []string
			for _, t := range s.Tools() {
				names = append(names, t.Name)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "turgon mcp on http://%s (auth: %s), tools: %s\n", listen, authMode, strings.Join(names, ", "))
			return s.Serve(listen)
		},
	}
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&listen, "listen", "127.0.0.1:8090", "address to listen on")
	cmd.Flags().StringVar(&authMode, "auth", "dev", "authentication: dev or gateway")
	cmd.Flags().StringSliceVar(&trusted, "trusted-gateway", nil, "CIDRs the agent gateway connects from")
	cmd.Flags().StringVar(&devAgent, "dev-agent", "", "agent identity for --auth dev")
	cmd.Flags().StringVar(&devUser, "dev-user", os.Getenv("USER"), "user the dev agent acts for")
	cmd.Flags().StringVar(&devRoles, "dev-roles", "integration-reader", "roles for --auth dev, comma-separated")
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("PORTER_DATABASE_URL"), "Postgres URL for the audit log")
	cmd.Flags().StringVar(&auditPath, "audit-log", "postgres", `audit log: "postgres" or a file path`)
	cmd.Flags().BoolVar(&writes, "writes", false, "serve write tools, run as governed writes on Temporal")
	cmd.Flags().DurationVar(&wait, "wait", 15*time.Second, "how long a write tool call waits for the write to finish")
	cmd.Flags().DurationVar(&approvalTimeout, "approval-timeout", engine.DefaultApprovalTimeout, "reject agent writes nobody approves within this time")
	tf.register(cmd)
	return cmd
}
