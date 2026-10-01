package main

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/agent"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func mcpCmd() *cobra.Command {
	var specPath, listen, metricsAddr, authMode, dbURL, auditPath string
	var devAgent, devUser, devRoles string
	var trusted []string
	var writes bool
	var a2aURL string
	var wait, approvalTimeout, secretsRefresh, streamLimit time.Duration
	var tf temporalFlags
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve a spec's tools to AI agents over MCP and A2A",
		Long: "Serve the tools of a compiled runtime spec over MCP (streamable HTTP).\n" +
			"With --auth gateway, run it behind the agent gateway, which authenticates agents and sets\n" +
			"X-Agent-Id, X-On-Behalf-Of and X-Agent-Roles; those are trusted only from --trusted-gateway\n" +
			"addresses. Reads need the integration-reader or integration-operator role and are audited.\n\n" +
			"With --writes, write tools start governed writes on Temporal, run by `turgon run` workers\n" +
			"for the same spec. Agents need the integration-operator role to write, and high-risk\n" +
			"writes wait for a person to approve them in the console.\n\n" +
			"The same server is an A2A agent at /a2a (agent card at /a2a/.well-known/agent-card.json):\n" +
			"skills are the tools, called with a data part {\"skill\": ..., \"arguments\": {...}}.\n" +
			"An agent follows a write by streaming (message/stream, tasks/resubscribe) or by push\n" +
			"notifications, which `turgon run` sends to public https URLs only, unless\n" +
			"TURGON_A2A_PUSH_ALLOW lists more address ranges.\n\n" +
			"With OpenBao or a cloud secret manager, a rotated secret reconnects the read tools'\n" +
			"connectors without dropping requests, and SIGHUP reconnects them at once.",
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
					return errors.New(`--audit-log postgres needs --database-url (or TURGON_DATABASE_URL)`)
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
			secretsFrom, err := openSecrets()
			if err != nil {
				return err
			}
			guard, err := pushGuard()
			if err != nil {
				return err
			}
			opts := agent.Options{
				Registry: connectorRegistry(), Secrets: secretsFrom, Audit: rec, Auth: auth, Version: version,
				ApprovalTimeout: approvalTimeout, A2AURL: a2aURL, Push: guard, StreamLimit: streamLimit,
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
			out := cmd.ErrOrStderr()
			rot, err := newRotation[*agent.Server](ctx, out, rec, spec, secretsFrom)
			if err != nil {
				return err
			}
			s, err := agent.New(ctx, spec, opts)
			if err != nil {
				return err
			}
			// The read tools' connectors are rebuilt when a secret rotates;
			// requests in flight finish on the connections they started on.
			live := agent.NewLive(s)
			defer live.Close()
			rot.connect = func() (*agent.Server, error) { return agent.New(ctx, spec, opts) }
			rot.discard = (*agent.Server).Close
			rot.regressions = func(ctx context.Context, next *agent.Server, endpoints []string) []string {
				return regressions(ctx, live.Current(), next, endpoints)
			}
			rot.use = func(next *agent.Server) bool {
				live.Swap(next)
				return true
			}
			refresh, stopRefresh := rot.every(secretsRefresh)
			defer stopRefresh()
			hup := make(chan os.Signal, 1)
			signal.Notify(hup, syscall.SIGHUP)
			defer signal.Stop(hup)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-hup:
						rot.hup(ctx)
					case <-refresh:
						rot.refresh(ctx)
					}
				}
			}()

			var names []string
			for _, t := range s.Tools() {
				names = append(names, t.Name)
			}
			fmt.Fprintf(out, "turgon mcp on http://%s (auth: %s), tools: %s\n", listen, authMode, strings.Join(names, ", "))
			if metricsAddr != "" {
				go serveMetrics(ctx, metricsAddr, out)
			}
			return agent.ListenAndServe(listen, auth, live)
		},
	}
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&metricsAddr, "metrics-listen", "", "serve Prometheus /metrics on this address, e.g. :9090")
	cmd.Flags().StringVar(&listen, "listen", "127.0.0.1:8090", "address to listen on")
	cmd.Flags().StringVar(&authMode, "auth", "dev", "authentication: dev or gateway")
	cmd.Flags().StringSliceVar(&trusted, "trusted-gateway", nil, "CIDRs the agent gateway connects from")
	cmd.Flags().StringVar(&devAgent, "dev-agent", "", "agent identity for --auth dev")
	cmd.Flags().StringVar(&devUser, "dev-user", os.Getenv("USER"), "user the dev agent acts for")
	cmd.Flags().StringVar(&devRoles, "dev-roles", "integration-reader", "roles for --auth dev, comma-separated")
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for the audit log")
	cmd.Flags().StringVar(&auditPath, "audit-log", "postgres", `audit log: "postgres" or a file path`)
	cmd.Flags().StringVar(&a2aURL, "a2a-url", "", "public URL of the A2A endpoint, for the agent card (default: from the request)")
	cmd.Flags().BoolVar(&writes, "writes", false, "serve write tools, run as governed writes on Temporal")
	cmd.Flags().DurationVar(&wait, "wait", 15*time.Second, "how long a write tool call waits for the write to finish")
	cmd.Flags().DurationVar(&approvalTimeout, "approval-timeout", engine.DefaultApprovalTimeout, "reject agent writes nobody approves within this time")
	cmd.Flags().DurationVar(&streamLimit, "a2a-stream-limit", time.Hour, "end an A2A stream following a write after this long; the agent resubscribes")
	cmd.Flags().DurationVar(&secretsRefresh, "secrets-refresh", 5*time.Minute, "with OpenBao or a cloud secret manager, how often secrets are read again; a changed one reconnects the connectors (0: never; SIGHUP reconnects at once)")
	tf.register(cmd)
	return cmd
}
