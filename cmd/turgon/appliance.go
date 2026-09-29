package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/appliance"
	"github.com/fduser123-coding/turgon/pkg/signing"
)

func applianceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "appliance",
		Short: "Run Turgon on one host without Kubernetes: a worker per spec in a directory",
	}
	cmd.AddCommand(applianceRunCmd(), applianceStatusCmd())
	return cmd
}

func applianceRunCmd() *cobra.Command {
	var (
		specs, state, statusAddr string
		workerArgs               []string
		healthBase               int
		interval, readyTimeout   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Keep a worker running for each valid spec in --specs",
		Long: "Each <name>.json in --specs is a compiled runtime spec. It is parsed strictly, its digest\n" +
			"checked and, with --trusted-keys, its signature; a spec that fails is refused and the worker\n" +
			"of its last valid version keeps running. A new version starts next to the old one, which is\n" +
			"stopped once the new worker is ready. Workers that exit are restarted with backoff, and\n" +
			"removing a spec stops its worker. Workers read the environment this command runs in\n" +
			"(TURGON_DATABASE_URL, TURGON_SECRET_*, Temporal settings).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			exe, err := os.Executable()
			if err != nil {
				return err
			}
			sup := &appliance.Supervisor{Dir: specs, StateDir: state, Exe: exe, HealthBase: healthBase,
				Interval: interval, ReadyTimeout: readyTimeout, Log: cmd.ErrOrStderr()}
			if trustedKeysPath != "" {
				data, err := os.ReadFile(trustedKeysPath)
				if err != nil {
					return fmt.Errorf("trusted keys: %w", err)
				}
				if sup.TrustedKeys, err = signing.ParsePublicKeys(data); err != nil {
					return err
				}
				if len(sup.TrustedKeys) == 0 {
					return fmt.Errorf("trusted keys: %s holds no public key", trustedKeysPath)
				}
				// The workers check the signature again when they load it.
				workerArgs = append([]string{"--trusted-keys=" + trustedKeysPath}, workerArgs...)
			}
			sup.WorkerArgs = workerArgs
			if _, err := os.Stat(specs); err != nil {
				return fmt.Errorf("--specs: %w", err)
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			if statusAddr != "" {
				ln, err := net.Listen("tcp", statusAddr)
				if err != nil {
					return fmt.Errorf("--status-listen: %w", err)
				}
				srv := &http.Server{Handler: sup.Handler(), ReadHeaderTimeout: 5 * time.Second}
				go func() { _ = srv.Serve(ln) }()
				defer srv.Close()
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "turgon appliance: running the specs in %s (signed only: %v), status at http://%s/status\n",
				specs, len(sup.TrustedKeys) > 0, statusAddr)
			err = sup.Run(ctx)
			fmt.Fprintln(cmd.ErrOrStderr(), "turgon appliance: stopped")
			return err
		},
	}
	cmd.Flags().StringVar(&specs, "specs", "/var/lib/turgon/specs", "directory of compiled specs to run, one <name>.json each")
	cmd.Flags().StringVar(&state, "state", "/var/lib/turgon/state", "directory for the copies workers run from")
	cmd.Flags().StringVar(&statusAddr, "status-listen", "127.0.0.1:8079", "serve GET /status on this address")
	cmd.Flags().StringArrayVar(&workerArgs, "worker-arg", nil, "argument added to every worker's turgon run (repeatable), e.g. --worker-arg=--poll=2s")
	cmd.Flags().IntVar(&healthBase, "health-base", 18100, "first loopback port for the workers' health and metrics endpoints")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "how often --specs is read")
	cmd.Flags().DurationVar(&readyTimeout, "ready-timeout", 2*time.Minute, "how long a new version may take to become ready before it is given up")
	return cmd
}

func applianceStatusCmd() *cobra.Command {
	var addr string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the appliance's specs and workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/status", nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("is turgon appliance run listening on %s? %w", addr, err)
			}
			defer resp.Body.Close()
			var body struct {
				Specs []appliance.Status `json:"specs"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(body)
			}
			tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tSTATE\tDIGEST\tLEVEL\tPID\tRESTARTS\tNOTE")
			problems := 0
			for _, s := range body.Specs {
				var note []string
				if s.Pending != "" {
					note = append(note, "starting "+shortDigest(s.Pending))
				}
				if s.Refused != "" {
					note = append(note, "refused: "+s.Refused)
					problems++
				}
				if s.FailedDigest != "" {
					note = append(note, shortDigest(s.FailedDigest)+" never became ready: "+s.FailedReason)
					problems++
				}
				if s.State == "restarting" && s.LastError != "" {
					note = append(note, s.LastError)
				}
				pid := ""
				if s.PID > 0 {
					pid = fmt.Sprint(s.PID)
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\n", s.Name, s.State, shortDigest(s.Digest), s.Level, pid, s.Restarts, strings.Join(note, "; "))
				if s.State != "running" {
					problems++
				}
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			if problems > 0 {
				return errors.New("some specs are not running as they should")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "status", "127.0.0.1:8079", "the appliance's --status-listen address")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func shortDigest(d string) string {
	d = strings.TrimPrefix(d, "sha256:")
	if len(d) > 12 {
		return d[:12]
	}
	return d
}
