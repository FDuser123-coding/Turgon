package main

import (
	"errors"
	"fmt"
	"net/netip"
	"os"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/console"
)

func consoleCmd() *cobra.Command {
	var tf temporalFlags
	var listen, authMode, devUser, approverGroup, viewerGroup string
	var catalogs, auditLogs, trusted []string
	cmd := &cobra.Command{
		Use:   "console",
		Short: "Serve the web console: runs, approvals, audit and catalog",
		Long: "Serve the console. With --auth proxy, run it behind an authenticating reverse proxy\n" +
			"(for example oauth2-proxy with your identity provider) that sets X-Auth-Request-Email\n" +
			"and X-Auth-Request-Groups; the headers are only trusted from --trusted-proxy addresses.\n" +
			"With --auth dev, every request is --dev-user and the console only listens on loopback.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			var auth console.Authenticator
			switch authMode {
			case "dev":
				if devUser == "" {
					return errors.New("--dev-user is required with --auth dev")
				}
				auth = console.DevAuth{User: devUser}
			case "proxy":
				if len(trusted) == 0 || approverGroup == "" {
					return errors.New("--auth proxy needs --trusted-proxy and --approver-group")
				}
				p := console.ProxyAuth{ApproverGroup: approverGroup, ViewerGroup: viewerGroup}
				for _, t := range trusted {
					pfx, err := netip.ParsePrefix(t)
					if err != nil {
						return fmt.Errorf("--trusted-proxy %q: %w", t, err)
					}
					p.Trusted = append(p.Trusted, pfx)
				}
				auth = p
			default:
				return fmt.Errorf("--auth must be dev or proxy, got %q", authMode)
			}
			c, err := tf.dial()
			if err != nil {
				return fmt.Errorf("temporal: %w", err)
			}
			defer c.Close()
			s := console.New(console.Config{
				Runs:      console.TemporalRuns{Client: c, Namespace: tf.namespace},
				Auth:      auth,
				Catalogs:  catalogs,
				AuditLogs: auditLogs,
			})
			fmt.Fprintf(cmd.ErrOrStderr(), "porter console on http://%s (auth: %s)\n", listen, authMode)
			return s.Serve(listen)
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVar(&listen, "listen", "127.0.0.1:8080", "address to listen on")
	cmd.Flags().StringSliceVarP(&catalogs, "catalog", "c", nil, "catalog directories to show verifier reports for")
	cmd.Flags().StringSliceVar(&auditLogs, "audit-log", nil, "audit log files to show and verify")
	cmd.Flags().StringVar(&authMode, "auth", "dev", "authentication: dev or proxy")
	cmd.Flags().StringVar(&devUser, "dev-user", os.Getenv("USER"), "identity for --auth dev")
	cmd.Flags().StringSliceVar(&trusted, "trusted-proxy", nil, "CIDRs the authenticating proxy connects from")
	cmd.Flags().StringVar(&approverGroup, "approver-group", "", "group whose members may approve writes")
	cmd.Flags().StringVar(&viewerGroup, "viewer-group", "", "group required to view the console (default: any authenticated user)")
	return cmd
}
