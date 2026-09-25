package main

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/console"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func consoleCmd() *cobra.Command {
	var tf temporalFlags
	var listen, authMode, devUser, approverGroup, stewardGroup, operatorGroup, viewerGroup, dbURL string
	var oidcIssuer, oidcClientID, oidcGroupsClaim, publicURL string
	var oidcScopes []string
	var sessionTTL time.Duration
	var catalogs, auditLogs, trusted []string
	cmd := &cobra.Command{
		Use:   "console",
		Short: "Serve the web console: runs, approvals, the data-steward queue, audit and catalog",
		Long: "Serve the console. With --auth oidc, it signs people in with your OpenID Connect\n" +
			"provider itself (authorization code flow with PKCE) and keeps them in a signed session\n" +
			"cookie; roles come from the groups claim. With --auth proxy, run it behind an authenticating reverse proxy\n" +
			"(for example oauth2-proxy with your identity provider) that sets X-Auth-Request-Email\n" +
			"and X-Auth-Request-Groups; the headers are only trusted from --trusted-proxy addresses.\n" +
			"With --auth dev, every request is --dev-user and the console only listens on loopback.\n\n" +
			"With --database-url, data stewards (--steward-group) link records the queue is waiting\n" +
			"for to their master records; each link is audited and the waiting runs are retried.\n" +
			"Operators (--operator-group) retry failed runs, each retry audited with its reason.",
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
				p := console.ProxyAuth{ApproverGroup: approverGroup, StewardGroup: stewardGroup, OperatorGroup: operatorGroup, ViewerGroup: viewerGroup}
				for _, t := range trusted {
					pfx, err := netip.ParsePrefix(t)
					if err != nil {
						return fmt.Errorf("--trusted-proxy %q: %w", t, err)
					}
					p.Trusted = append(p.Trusted, pfx)
				}
				auth = p
			case "oidc":
				if oidcIssuer == "" || oidcClientID == "" || publicURL == "" || approverGroup == "" {
					return errors.New("--auth oidc needs --oidc-issuer, --oidc-client-id, --url and --approver-group")
				}
				key := os.Getenv("TURGON_CONSOLE_SESSION_KEY")
				if len(key) < 32 {
					return errors.New("--auth oidc signs sessions with TURGON_CONSOLE_SESSION_KEY: set it to at least 32 random characters, the same on every replica (e.g. openssl rand -base64 48)")
				}
				o, err := console.NewOIDCAuth(cmd.Context(), console.OIDCConfig{
					Issuer: oidcIssuer, ClientID: oidcClientID, ClientSecret: os.Getenv("TURGON_OIDC_CLIENT_SECRET"),
					URL: publicURL, Scopes: oidcScopes, GroupsClaim: oidcGroupsClaim,
					ApproverGroup: approverGroup, StewardGroup: stewardGroup, OperatorGroup: operatorGroup, ViewerGroup: viewerGroup,
					SessionKey: []byte(key), SessionTTL: sessionTTL,
				})
				if err != nil {
					return err
				}
				auth = o
			default:
				return fmt.Errorf("--auth must be dev, proxy or oidc, got %q", authMode)
			}
			c, err := tf.dial()
			if err != nil {
				return fmt.Errorf("temporal: %w", err)
			}
			defer c.Close()
			runs := console.TemporalRuns{Client: c, Namespace: tf.namespace}
			cfg := console.Config{Runs: runs, Auth: auth, Catalogs: catalogs, Steward: runs}
			var pool *pgxpool.Pool
			if dbURL != "" {
				if pool, err = pgxpool.New(cmd.Context(), dbURL); err != nil {
					return err
				}
				defer pool.Close()
				if err := pgstore.Migrate(cmd.Context(), pool); err != nil {
					return err
				}
				// Stewards write cross-references, audited in the shared log.
				cfg.Xref, cfg.Recorder = pgstore.New(pool), pgstore.NewAuditLog(pool)
				cfg.Retry = runs
			}
			for _, a := range auditLogs {
				if a != "postgres" {
					cfg.Audit = append(cfg.Audit, console.AuditFile(a))
					continue
				}
				if pool == nil {
					return errors.New(`--audit-log postgres needs --database-url (or TURGON_DATABASE_URL)`)
				}
				cfg.Audit = append(cfg.Audit, console.AuditTable{Name: "Postgres audit log", Log: pgstore.NewAuditLog(pool)})
			}
			s := console.New(cfg)
			fmt.Fprintf(cmd.ErrOrStderr(), "turgon console on http://%s (auth: %s)\n", listen, authMode)
			return s.Serve(listen)
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVar(&listen, "listen", "127.0.0.1:8080", "address to listen on")
	cmd.Flags().StringSliceVarP(&catalogs, "catalog", "c", nil, "catalog directories to show verifier reports for")
	cmd.Flags().StringSliceVar(&auditLogs, "audit-log", nil, `audit logs to show and verify: "postgres" or file paths`)
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state (audit log and the data-steward queue)")
	cmd.Flags().StringVar(&authMode, "auth", "dev", "authentication: dev, proxy or oidc")
	cmd.Flags().StringVar(&oidcIssuer, "oidc-issuer", "", "OpenID Connect issuer URL, e.g. https://login.microsoftonline.com/<tenant>/v2.0 (--auth oidc; the client secret is read from TURGON_OIDC_CLIENT_SECRET)")
	cmd.Flags().StringVar(&oidcClientID, "oidc-client-id", "", "OpenID Connect client ID of the console's app registration")
	cmd.Flags().StringSliceVar(&oidcScopes, "oidc-scopes", nil, "scopes beyond openid, email and profile, e.g. groups")
	cmd.Flags().StringVar(&oidcGroupsClaim, "oidc-groups-claim", "groups", "ID token claim holding the user's groups")
	cmd.Flags().StringVar(&publicURL, "url", os.Getenv("TURGON_CONSOLE_URL"), "the console's public URL; the provider redirects to <url>/auth/callback")
	cmd.Flags().DurationVar(&sessionTTL, "session-ttl", 8*time.Hour, "how long a sign-in lasts (--auth oidc)")
	cmd.Flags().StringVar(&devUser, "dev-user", os.Getenv("USER"), "identity for --auth dev")
	cmd.Flags().StringSliceVar(&trusted, "trusted-proxy", nil, "CIDRs the authenticating proxy connects from")
	cmd.Flags().StringVar(&approverGroup, "approver-group", "", "group whose members may approve writes")
	cmd.Flags().StringVar(&stewardGroup, "steward-group", "", "group whose members resolve records in the data-steward queue")
	cmd.Flags().StringVar(&operatorGroup, "operator-group", "", "group whose members retry failed runs")
	cmd.Flags().StringVar(&viewerGroup, "viewer-group", "", "group required to view the console (default: any authenticated user)")
	return cmd
}
