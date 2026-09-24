package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/postgres"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

type temporalFlags struct {
	address, namespace, taskQueue string
}

func (f *temporalFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.address, "temporal", envOr("PORTER_TEMPORAL_ADDRESS", "localhost:7233"), "Temporal frontend address")
	cmd.Flags().StringVar(&f.namespace, "namespace", envOr("PORTER_TEMPORAL_NAMESPACE", "default"), "Temporal namespace")
	cmd.Flags().StringVar(&f.taskQueue, "task-queue", "", "Temporal task queue (default: porter-<spec name>)")
}

func (f *temporalFlags) dial() (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:  f.address,
		Namespace: f.namespace,
		Logger:    tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	})
}

// connectorRegistry lists the connectors built into this worker.
func connectorRegistry() connector.Registry {
	return connector.Registry{postgres.Name: postgres.Factory, salesforce.Name: salesforce.Factory}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// loadSpec reads a runtime spec and refuses one whose body does not match
// its digest: the worker runs exactly what the compiler produced.
func loadSpec(path string) (*compiler.RuntimeSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var spec compiler.RuntimeSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if spec.Kind != compiler.KindRuntimeSpec {
		return nil, fmt.Errorf("%s is a %q, not a RuntimeSpec; run porter compile first", path, spec.Kind)
	}
	if got := compiler.Digest(spec.Spec); got != spec.Metadata.Digest {
		return nil, fmt.Errorf("%s: digest mismatch (spec says %s, body hashes to %s); refusing to run a modified spec", path, spec.Metadata.Digest, got)
	}
	return &spec, nil
}

func runCmd() *cobra.Command {
	var tf temporalFlags
	var specPath, dbURL, auditPath string
	var poll, approvalTimeout time.Duration
	var healthAddr string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a compiled runtime spec: Temporal worker plus event dispatcher",
		Long: "Run connects the spec's endpoints, registers the integration workflow with Temporal\n" +
			"and polls event sources, starting one workflow run per event. Secrets are read from\n" +
			"PORTER_SECRET_* environment variables (see `porter secrets`).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dbURL == "" {
				return errors.New("--database-url (or PORTER_DATABASE_URL) is required for Porter's state")
			}
			spec, err := loadSpec(specPath)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := pgstore.Migrate(ctx, pool); err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			store := pgstore.New(pool)
			var log audit.Recorder
			if auditPath == "postgres" {
				log = pgstore.NewAuditLog(pool)
			} else {
				fl, file, err := audit.OpenFile(auditPath)
				if err != nil {
					return fmt.Errorf("audit log: %w", err)
				}
				defer file.Close()
				log = fl
			}

			rt, err := engine.New(ctx, spec, engine.Options{
				Registry: connectorRegistry(),
				Secrets:  connector.EnvSecrets{},
				Store:    store, Resolver: store, Audit: log,
			})
			if err != nil {
				return err
			}
			defer rt.Close()

			c, err := tf.dial()
			if err != nil {
				return fmt.Errorf("temporal: %w", err)
			}
			defer c.Close()
			if tf.taskQueue == "" {
				tf.taskQueue = engine.TaskQueueFor(spec)
			}
			w := worker.New(c, tf.taskQueue, worker.Options{})
			engine.Register(w, rt.Activities)
			if err := w.Start(); err != nil {
				return err
			}
			defer w.Stop()
			if healthAddr != "" {
				go serveHealth(ctx, healthAddr, pool, cmd.ErrOrStderr())
			}

			d := &engine.Dispatcher{Runtime: rt, Cursors: store, Starter: engine.TemporalStarter{Client: c, TaskQueue: tf.taskQueue}, ApprovalTimeout: approvalTimeout}
			out := cmd.ErrOrStderr()
			fmt.Fprintf(out, "porter: running %s (%s, level %s), %d workflow(s) on task queue %s, polling every %s\n",
				spec.Metadata.Name, spec.Metadata.Digest[:19], spec.Metadata.Level, len(spec.Spec.Workflows), tf.taskQueue, poll)
			ticker := time.NewTicker(poll)
			defer ticker.Stop()
			for {
				n, err := d.Poll(ctx)
				if n > 0 {
					fmt.Fprintf(out, "porter: started %d run(s)\n", n)
				}
				if err != nil && ctx.Err() == nil {
					fmt.Fprintf(out, "porter: poll: %v\n", err)
				}
				select {
				case <-ctx.Done():
					fmt.Fprintln(out, "porter: shutting down")
					return nil
				case <-ticker.C:
				}
			}
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("PORTER_DATABASE_URL"), "Postgres URL for Porter's state")
	cmd.Flags().StringVar(&auditPath, "audit-log", "postgres", `audit log: "postgres" (shared by all workers) or a file path`)
	cmd.Flags().DurationVar(&poll, "poll", 2*time.Second, "event source poll interval")
	cmd.Flags().StringVar(&healthAddr, "health-listen", "", "serve /healthz and /readyz on this address, e.g. :8081")
	cmd.Flags().DurationVar(&approvalTimeout, "approval-timeout", engine.DefaultApprovalTimeout, "reject approvals nobody answers within this time")
	return cmd
}

func approveCmd() *cobra.Command {
	var tf temporalFlags
	var step, by, note string
	var reject bool
	cmd := &cobra.Command{
		Use:   "approve RUN_ID",
		Short: "Approve (or --reject) a write waiting in a run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if by == "" {
				return errors.New("--by is required: approvals are audited with the approver's identity")
			}
			c, err := tf.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			status := "approved"
			if reject {
				status = "rejected"
			}
			// Decide on exactly what the run is waiting for: the signal
			// carries the pending request's digest.
			p, err := engine.Pending(cmd.Context(), c, args[0])
			if err != nil {
				return err
			}
			if p == nil {
				return fmt.Errorf("%s is not waiting for an approval", args[0])
			}
			if step != "" && step != p.Step {
				return fmt.Errorf("%s is waiting on step %s, not %s", args[0], p.Step, step)
			}
			sig := engine.ApprovalSignal{Step: p.Step, Digest: p.Digest, Status: status, By: by, Note: note}
			if err := c.SignalWorkflow(cmd.Context(), args[0], "", engine.SignalApproval, sig); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s %s step %s (request %s)\n", status, args[0], p.Step, p.Digest[:12])
			return nil
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVar(&step, "step", "", "step to approve (default: whichever is waiting)")
	cmd.Flags().StringVar(&by, "by", os.Getenv("USER"), "approver identity")
	cmd.Flags().StringVar(&note, "note", "", "note recorded with the decision")
	cmd.Flags().BoolVar(&reject, "reject", false, "reject instead of approve")
	return cmd
}

func pendingCmd() *cobra.Command {
	var tf temporalFlags
	cmd := &cobra.Command{
		Use:   "pending RUN_ID",
		Short: "Show the write a run is waiting to have approved, with its dry-run preview",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := tf.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			p, err := engine.Pending(cmd.Context(), c, args[0])
			if err != nil {
				return err
			}
			if p == nil {
				fmt.Fprintln(cmd.OutOrStdout(), "nothing pending")
				return nil
			}
			return writeJSON(cmd.OutOrStdout(), p)
		},
	}
	tf.register(cmd)
	return cmd
}

func retryCmd() *cobra.Command {
	var tf temporalFlags
	var specPath string
	cmd := &cobra.Command{
		Use:   "retry RUN_ID",
		Short: "Start a failed run again for the same event",
		Long: "Retry re-runs a failed run with the event it started with. With --spec, the run\n" +
			"uses that spec's version of the workflow (for example after fixing a mapping).\n" +
			"Writes already committed are recognized by their idempotency keys.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var spec *compiler.RuntimeSpec
			if specPath != "" {
				s, err := loadSpec(specPath)
				if err != nil {
					return err
				}
				spec = s
			}
			c, err := tf.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			in, err := engine.TemporalStarter{Client: c, TaskQueue: tf.taskQueue}.Retry(cmd.Context(), args[0], spec)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restarted %s (event %s, spec %s)\n", args[0], in.Event.ID, in.SpecDigest)
			return nil
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVarP(&specPath, "spec", "s", "", "run with this spec's workflow definition")
	return cmd
}

func xrefCmd() *cobra.Command {
	var dbURL, entity, system, source, master string
	cmd := &cobra.Command{Use: "xref", Short: "Manage identity cross-references"}
	set := &cobra.Command{
		Use:   "set",
		Short: "Link a source record to a master record",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := pgstore.Migrate(ctx, pool); err != nil {
				return err
			}
			return pgstore.New(pool).PutXref(ctx, entity, system, source, master)
		},
	}
	set.Flags().StringVar(&dbURL, "database-url", os.Getenv("PORTER_DATABASE_URL"), "Postgres URL for Porter's state")
	set.Flags().StringVar(&entity, "entity", "", "semantic entity, e.g. Customer")
	set.Flags().StringVar(&system, "system", "", "source endpoint, e.g. shop-db")
	set.Flags().StringVar(&source, "source", "", "record ID in the source system")
	set.Flags().StringVar(&master, "master", "", "master record ID")
	for _, f := range []string{"entity", "system", "source", "master"} {
		_ = set.MarkFlagRequired(f)
	}
	cmd.AddCommand(set)
	return cmd
}

func secretsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "secrets SPEC",
		Short: "List the environment variables `porter run` reads secrets from",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := loadSpec(args[0])
			if err != nil {
				return err
			}
			for _, c := range spec.Spec.Connectors {
				fmt.Fprintf(cmd.OutOrStdout(), "%-12s %-40s %s\n", c.Endpoint, c.SecretRef, connector.EnvName(c.SecretRef))
			}
			return nil
		},
	}
}

// serveHealth answers Kubernetes probes: /healthz while the process runs,
// /readyz while Porter's database answers.
func serveHealth(ctx context.Context, addr string, pool *pgxpool.Pool, logw io.Writer) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(pctx); err != nil {
			http.Error(w, "state database: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(logw, "porter: health endpoint: %v\n", err)
	}
}
