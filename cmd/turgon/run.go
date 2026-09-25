package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/postgres"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/notify"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

type temporalFlags struct {
	address, namespace, taskQueue string
	tls                           bool
	caFile, certFile, keyFile     string
	serverName                    string
}

func (f *temporalFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.address, "temporal", envOr("TURGON_TEMPORAL_ADDRESS", "localhost:7233"), "Temporal frontend address")
	cmd.Flags().StringVar(&f.namespace, "namespace", envOr("TURGON_TEMPORAL_NAMESPACE", "default"), "Temporal namespace")
	cmd.Flags().StringVar(&f.taskQueue, "task-queue", "", "Temporal task queue (default: turgon-<spec name>)")
	cmd.Flags().BoolVar(&f.tls, "temporal-tls", os.Getenv("TURGON_TEMPORAL_TLS") == "true", "connect to Temporal over TLS (implied by a client certificate or TURGON_TEMPORAL_API_KEY)")
	cmd.Flags().StringVar(&f.caFile, "temporal-ca", os.Getenv("TURGON_TEMPORAL_CA"), "CA certificate file that signed Temporal's server certificate (default: the system's)")
	cmd.Flags().StringVar(&f.certFile, "temporal-cert", os.Getenv("TURGON_TEMPORAL_CERT"), "client certificate file, for mutual TLS")
	cmd.Flags().StringVar(&f.keyFile, "temporal-key", os.Getenv("TURGON_TEMPORAL_KEY"), "client key file, for mutual TLS")
	cmd.Flags().StringVar(&f.serverName, "temporal-server-name", os.Getenv("TURGON_TEMPORAL_SERVER_NAME"), "server name to verify in Temporal's certificate (default: the address's host)")
}

// dial connects to Temporal. TURGON_TEMPORAL_API_KEY authenticates to
// Temporal Cloud; TURGON_PAYLOAD_KEYS ("id:base64key,...", see
// engine.NewCodec) encrypts everything Turgon stores in Temporal.
func (f *temporalFlags) dial() (client.Client, error) {
	opts := client.Options{
		HostPort:  f.address,
		Namespace: f.namespace,
		Logger:    tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	}
	apiKey := os.Getenv("TURGON_TEMPORAL_API_KEY")
	if f.tls || f.certFile != "" || f.caFile != "" || apiKey != "" {
		cfg, err := f.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts.ConnectionOptions.TLS = cfg
	}
	if apiKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
	}
	if keys := os.Getenv("TURGON_PAYLOAD_KEYS"); keys != "" {
		codec, err := engine.NewCodec(keys)
		if err != nil {
			return nil, err
		}
		engine.EncryptPayloads(codec)
	}
	opts.DataConverter = engine.DataConverter()
	opts.FailureConverter = engine.FailureConverter()
	return client.Dial(opts)
}

func (f *temporalFlags) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: f.serverName}
	if f.caFile != "" {
		pem, err := os.ReadFile(f.caFile)
		if err != nil {
			return nil, fmt.Errorf("--temporal-ca: %w", err)
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--temporal-ca %s: no PEM certificates", f.caFile)
		}
	}
	if (f.certFile == "") != (f.keyFile == "") {
		return nil, errors.New("--temporal-cert and --temporal-key go together")
	}
	if f.certFile != "" {
		cert, err := tls.LoadX509KeyPair(f.certFile, f.keyFile)
		if err != nil {
			return nil, fmt.Errorf("temporal client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// connectorRegistry lists the connectors built into this worker.
func connectorRegistry() connector.Registry {
	return connector.Registry{postgres.Name: postgres.Factory, salesforce.Name: salesforce.Factory, rest.Name: rest.Factory}
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
		return nil, fmt.Errorf("%s is a %q, not a RuntimeSpec; run turgon compile first", path, spec.Kind)
	}
	if got := compiler.Digest(spec.Spec); got != spec.Metadata.Digest {
		return nil, fmt.Errorf("%s: digest mismatch (spec says %s, body hashes to %s); refusing to run a modified spec", path, spec.Metadata.Digest, got)
	}
	return &spec, nil
}

func runCmd() *cobra.Command {
	var tf temporalFlags
	var specPath, dbURL, auditPath string
	var poll, approvalTimeout, reconcile time.Duration
	var healthAddr, webhookAddr, consoleURL, pollers string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a compiled runtime spec: Temporal worker plus event dispatcher",
		Long: "Run connects the spec's endpoints, registers the integration workflow with Temporal\n" +
			"and polls event sources, starting one workflow run per event. Secrets are read from\n" +
			"TURGON_SECRET_* environment variables (see `turgon secrets`).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dbURL == "" {
				return errors.New("--database-url (or TURGON_DATABASE_URL) is required for Turgon's state")
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
				Webhooks: webhookAddr != "",
			})
			if err != nil {
				return err
			}
			defer rt.Close()
			// Channels come from TURGON_NOTIFY_* variables (the webhook URLs
			// are credentials).
			hub, err := notify.FromEnv(consoleURL, &http.Client{Timeout: 15 * time.Second})
			if err != nil {
				return fmt.Errorf("notifications: %w", err)
			}
			if hub != nil {
				rt.Activities.Notifier = hub
			}

			c, err := tf.dial()
			if err != nil {
				return fmt.Errorf("temporal: %w", err)
			}
			defer c.Close()
			if tf.taskQueue == "" {
				tf.taskQueue = engine.TaskQueueFor(spec)
			}
			wopts, err := workerOptions(pollers)
			if err != nil {
				return err
			}
			w := worker.New(c, tf.taskQueue, wopts)
			engine.Register(w, rt.Activities)
			if err := w.Start(); err != nil {
				return err
			}
			defer w.Stop()
			if healthAddr != "" {
				go serveHealth(ctx, healthAddr, pool, cmd.ErrOrStderr())
			}

			d := &engine.Dispatcher{Runtime: rt, Cursors: store, Starter: engine.TemporalStarter{Client: c, TaskQueue: tf.taskQueue},
				ApprovalTimeout: approvalTimeout, Inbox: store, Reconcile: reconcile}
			out := cmd.ErrOrStderr()
			fmt.Fprintf(out, "turgon: running %s (%s, level %s), %d workflow(s) on task queue %s, polling every %s\n",
				spec.Metadata.Name, spec.Metadata.Digest[:19], spec.Metadata.Level, len(spec.Spec.Workflows), tf.taskQueue, poll)
			if hub != nil {
				var names []string
				for _, ch := range hub.Channels {
					names = append(names, ch.Name())
				}
				fmt.Fprintf(out, "turgon: notifying %s about approvals, steward work and failed compensations\n", strings.Join(names, ", "))
			}
			wake := make(chan struct{}, 1)
			if webhookAddr != "" {
				srv := &http.Server{Addr: webhookAddr, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
					Handler: engine.WebhookHandler(rt, store, func() {
						select {
						case wake <- struct{}{}:
						default:
						}
					})}
				ln, err := net.Listen("tcp", webhookAddr)
				if err != nil {
					return fmt.Errorf("webhooks: %w", err)
				}
				go func() { _ = srv.Serve(ln) }()
				defer srv.Close()
				for ep, events := range rt.Webhooks {
					for ev := range events {
						fmt.Fprintf(out, "turgon: receiving %s %s by webhook at http://%s/webhooks/%s/%s, reconciling every %s\n",
							ep, ev, ln.Addr(), ep, ev, or(reconcile, engine.DefaultReconcile))
					}
				}
			}
			ticker := time.NewTicker(poll)
			defer ticker.Stop()
			pruned := time.Time{}
			for {
				n, err := d.Poll(ctx)
				if n > 0 {
					fmt.Fprintf(out, "turgon: started %d run(s)\n", n)
				}
				if err != nil && ctx.Err() == nil {
					fmt.Fprintf(out, "turgon: poll: %v\n", err)
				}
				if time.Since(pruned) > time.Hour {
					// Deliveries older than any provider retries: Stripe
					// retries for three days.
					if _, err := store.PruneInbox(ctx, time.Now().Add(-inboxRetention)); err != nil && ctx.Err() == nil {
						fmt.Fprintf(out, "turgon: prune inbox: %v\n", err)
					}
					pruned = time.Now()
				}
				select {
				case <-ctx.Done():
					fmt.Fprintln(out, "turgon: shutting down")
					return nil
				case <-ticker.C:
				case <-wake:
				}
			}
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	cmd.Flags().StringVar(&auditPath, "audit-log", "postgres", `audit log: "postgres" (shared by all workers) or a file path`)
	cmd.Flags().DurationVar(&poll, "poll", 2*time.Second, "event source poll interval")
	cmd.Flags().StringVar(&healthAddr, "health-listen", "", "serve /healthz and /readyz on this address, e.g. :8081")
	cmd.Flags().StringVar(&webhookAddr, "webhook-listen", "", "receive events configured for webhooks on this address, e.g. :8082 (POST /webhooks/<endpoint>/<event>)")
	cmd.Flags().StringVar(&pollers, "pollers", "auto", `task queue pollers per kind: "auto" scales them with the load (5 to 100), or a fixed number`)
	cmd.Flags().StringVar(&consoleURL, "console-url", os.Getenv("TURGON_CONSOLE_URL"), "the console's URL, linked from notifications, e.g. https://turgon.example.com")
	cmd.Flags().DurationVar(&reconcile, "reconcile", engine.DefaultReconcile, "how often events received by webhook are also polled, for missed deliveries")
	cmd.Flags().DurationVar(&approvalTimeout, "approval-timeout", engine.DefaultApprovalTimeout, "reject approvals nobody answers within this time")
	return cmd
}

// workerOptions sets how many requests the worker keeps open to Temporal
// for workflow and activity tasks. Each run is a dozen or more tasks, so
// the SDK's default of two pollers each caps a worker at a few runs a
// second however fast the systems are; "auto" lets the SDK scale them.
func workerOptions(pollers string) (worker.Options, error) {
	var b worker.PollerBehavior
	if pollers == "auto" {
		b = worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{})
	} else {
		n, err := strconv.Atoi(pollers)
		if err != nil || n < 1 || n > 500 {
			return worker.Options{}, fmt.Errorf(`--pollers must be "auto" or a number from 1 to 500, not %q`, pollers)
		}
		b = worker.NewPollerBehaviorSimpleMaximum(worker.PollerBehaviorSimpleMaximumOptions{MaximumNumberOfPollers: n})
	}
	return worker.Options{WorkflowTaskPollerBehavior: b, ActivityTaskPollerBehavior: b}, nil
}

// inboxRetention keeps webhook deliveries long enough to drop every
// redelivery of them.
const inboxRetention = 30 * 24 * time.Hour

func or(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
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
	var dbURL, entity, system, source, master, email, name string
	cmd := &cobra.Command{Use: "xref", Short: "Manage identity cross-references"}
	set := &cobra.Command{
		Use:   "set",
		Short: "Link a source record to a master record",
		Long: "Link a source record to a master record. With --email and --name, the record's\n" +
			"contact is kept for matching: later records with the same address, company email\n" +
			"domain or name are suggested to stewards, or linked by the probabilistic strategy.",
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
			attrs := identity.Extract(map[string]any{"email": email, "name": name}, []v1alpha1.MatchField{
				{Field: "email", Kind: v1alpha1.MatchEmail}, {Field: "email", Kind: v1alpha1.MatchDomain}, {Field: "name", Kind: v1alpha1.MatchName},
			})
			return pgstore.New(pool).Link(ctx, entity, system, source, master, attrs)
		},
	}
	set.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	set.Flags().StringVar(&entity, "entity", "", "semantic entity, e.g. Customer")
	set.Flags().StringVar(&system, "system", "", "source endpoint, e.g. shop-db")
	set.Flags().StringVar(&source, "source", "", "record ID in the source system")
	set.Flags().StringVar(&master, "master", "", "master record ID")
	set.Flags().StringVar(&email, "email", "", "the record's email address, kept for matching")
	set.Flags().StringVar(&name, "name", "", "the record's name, kept for matching")
	for _, f := range []string{"entity", "system", "source", "master"} {
		_ = set.MarkFlagRequired(f)
	}
	cmd.AddCommand(set)
	return cmd
}

func secretsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "secrets SPEC",
		Short: "List the environment variables `turgon run` reads secrets from",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := loadSpec(args[0])
			if err != nil {
				return err
			}
			for _, c := range spec.Spec.Connectors {
				fmt.Fprintf(cmd.OutOrStdout(), "%-14s %-44s %s\n", c.Endpoint, c.SecretRef, connector.EnvName(c.SecretRef))
				for _, ref := range connector.ConfigSecretRefs(c.Config) {
					fmt.Fprintf(cmd.OutOrStdout(), "%-14s %-44s %s\n", c.Endpoint, ref, connector.EnvName(ref))
				}
			}
			return nil
		},
	}
}

// serveHealth answers Kubernetes probes: /healthz while the process runs,
// /readyz while Turgon's database answers.
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
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(logw, "turgon: health endpoint: %v\n", err)
	}
}
