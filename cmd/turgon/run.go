package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/fduser123-coding/turgon/pkg/connector/debezium"
	"github.com/fduser123-coding/turgon/pkg/connector/postgres"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce"
	"github.com/fduser123-coding/turgon/pkg/connector/sap"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/metrics"
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
	// The SDK's metrics, and those workflows record, go to /metrics.
	opts.MetricsHandler = metrics.Temporal()
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
	return connector.Registry{postgres.Name: postgres.Factory, salesforce.Name: salesforce.Factory, rest.Name: rest.Factory, sap.Name: sap.Factory,
		debezium.Name: debezium.Factory}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// loadSpec reads a runtime spec and refuses one whose body does not match
// its digest, or, with --trusted-keys, one no trusted key signed: the
// worker runs exactly what the signing pipeline produced.
func loadSpec(path string) (*compiler.RuntimeSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	spec, err := compiler.ParseSpec(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := verifySignature(path, spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func runCmd() *cobra.Command {
	var tf temporalFlags
	var specPath, dbURL, auditPath string
	var poll, approvalTimeout, reconcile time.Duration
	var healthAddr, webhookAddr, consoleURL, pollers string
	var secretsRefresh time.Duration
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a compiled runtime spec: Temporal worker plus event dispatcher",
		Long: "Run connects the spec's endpoints, registers the integration workflow with Temporal\n" +
			"and polls event sources, starting one workflow run per event. Secrets are read from\n" +
			"TURGON_SECRET_* environment variables or OpenBao/Vault (see `turgon secrets`); with\n" +
			"OpenBao a rotated secret reconnects the connectors, and SIGHUP reconnects them at once.",
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

			secretsFrom, err := openSecrets()
			if err != nil {
				return err
			}
			// Channels come from TURGON_NOTIFY_* variables (the webhook URLs
			// are credentials).
			hub, err := notify.FromEnv(consoleURL, &http.Client{Timeout: 15 * time.Second})
			if err != nil {
				return fmt.Errorf("notifications: %w", err)
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
			if healthAddr != "" {
				go serveHealth(ctx, healthAddr, pool, cmd.ErrOrStderr())
			}
			out := cmd.ErrOrStderr()
			wake := make(chan struct{}, 1)
			woken := func() {
				select {
				case wake <- struct{}{}:
				default:
				}
			}

			// The connectors, the Temporal worker, the dispatcher and the
			// subscriptions form a generation, rebuilt when a secret changes
			// (or on SIGHUP) so rotated credentials are used without a restart.
			newRuntime := func() (*engine.Runtime, error) {
				rt, err := engine.New(ctx, spec, engine.Options{
					Registry: connectorRegistry(),
					Secrets:  secretsFrom,
					Store:    store, Resolver: store, Audit: log,
					Webhooks: webhookAddr != "",
				})
				if err == nil && hub != nil {
					rt.Activities.Notifier = hub
				}
				return rt, err
			}
			rot, err := newRotation[*engine.Runtime](ctx, out, log, spec, secretsFrom)
			if err != nil {
				return err
			}
			rt, err := newRuntime()
			if err != nil {
				return err
			}
			var hooks atomic.Pointer[http.Handler]
			if webhookAddr != "" {
				srv := &http.Server{Addr: webhookAddr, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
					Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						h := hooks.Load()
						if h == nil { // not ready yet: the provider retries
							http.Error(w, "starting", http.StatusServiceUnavailable)
							return
						}
						(*h).ServeHTTP(w, r)
					})}
				ln, err := net.Listen("tcp", webhookAddr)
				if err != nil {
					rt.Close()
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
			start := func(rt *engine.Runtime) (*generation, error) {
				g := &generation{rt: rt, done: make(chan struct{})}
				g.w = worker.New(c, tf.taskQueue, wopts)
				engine.Register(g.w, rt.Activities)
				if err := g.w.Start(); err != nil {
					rt.Close()
					return nil, err
				}
				g.d = &engine.Dispatcher{Runtime: rt, Cursors: store, Starter: engine.TemporalStarter{Client: c, TaskQueue: tf.taskQueue},
					ApprovalTimeout: approvalTimeout, Inbox: store, Reconcile: reconcile}
				if webhookAddr != "" {
					h := engine.WebhookHandler(rt, store, woken)
					hooks.Store(&h)
				}
				sctx, cancel := context.WithCancel(ctx)
				g.cancel = cancel
				go func() {
					defer close(g.done)
					if len(rt.Streams) > 0 {
						(&engine.Streams{Runtime: rt, Inbox: store, Locker: store, Delivered: woken,
							Log: func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }}).Run(sctx)
					}
				}()
				return g, nil
			}
			gen, err := start(rt)
			if err != nil {
				return err
			}
			defer func() { gen.stop() }()

			fmt.Fprintf(out, "turgon: running %s (%s, level %s), %d workflow(s) on task queue %s, polling every %s\n",
				spec.Metadata.Name, spec.Metadata.Digest[:19], spec.Metadata.Level, len(spec.Spec.Workflows), tf.taskQueue, poll)
			if hub != nil {
				var names []string
				for _, ch := range hub.Channels {
					names = append(names, ch.Name())
				}
				fmt.Fprintf(out, "turgon: notifying %s about approvals, steward work and failed compensations\n", strings.Join(names, ", "))
			}
			for ep, events := range rt.Streams {
				for ev := range events {
					fmt.Fprintf(out, "turgon: subscribed to %s %s (one worker at a time holds the subscription)\n", ep, ev)
				}
			}
			hup := make(chan os.Signal, 1)
			signal.Notify(hup, syscall.SIGHUP)
			defer signal.Stop(hup)
			refresh, stopRefresh := rot.every(secretsRefresh)
			defer stopRefresh()
			rot.connect = newRuntime
			rot.discard = (*engine.Runtime).Close
			rot.regressions = func(ctx context.Context, next *engine.Runtime, endpoints []string) []string {
				return regressions(ctx, gen.rt, next, endpoints)
			}
			rot.use = func(next *engine.Runtime) bool {
				gen.stop()
				g, err := start(next)
				if err != nil {
					// The worker could not start again: exit, and let the
					// supervisor restart the process.
					fmt.Fprintf(out, "turgon: restarting the worker failed: %v\n", err)
					stop()
					return false
				}
				gen = g
				return true
			}

			ticker := time.NewTicker(poll)
			defer ticker.Stop()
			pruned := time.Time{}
			for {
				n, err := gen.d.Poll(ctx)
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
				case <-hup:
					rot.hup(ctx)
				case <-refresh:
					rot.refresh(ctx)
				}
			}
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	cmd.Flags().StringVar(&auditPath, "audit-log", "postgres", `audit log: "postgres" (shared by all workers) or a file path`)
	cmd.Flags().DurationVar(&poll, "poll", 2*time.Second, "event source poll interval")
	cmd.Flags().StringVar(&healthAddr, "health-listen", "", "serve /healthz, /readyz and /metrics on this address, e.g. :8081")
	cmd.Flags().StringVar(&webhookAddr, "webhook-listen", "", "receive events configured for webhooks on this address, e.g. :8082 (POST /webhooks/<endpoint>/<event>)")
	cmd.Flags().StringVar(&pollers, "pollers", "auto", `task queue pollers per kind: "auto" scales them with the load (5 to 100), or a fixed number`)
	cmd.Flags().StringVar(&consoleURL, "console-url", os.Getenv("TURGON_CONSOLE_URL"), "the console's URL, linked from notifications, e.g. https://turgon.example.com")
	cmd.Flags().DurationVar(&reconcile, "reconcile", engine.DefaultReconcile, "how often events received by webhook are also polled, for missed deliveries")
	cmd.Flags().DurationVar(&approvalTimeout, "approval-timeout", engine.DefaultApprovalTimeout, "reject approvals nobody answers within this time")
	cmd.Flags().DurationVar(&secretsRefresh, "secrets-refresh", 5*time.Minute, "with OpenBao or a cloud secret manager, how often secrets are read again; a changed one reconnects the connectors (0: never; SIGHUP reconnects at once)")
	return cmd
}

// regressions checks the new connections against the current ones and
// returns the checks the current connections pass and the new ones fail: a
// wrong password in the secret manager. A check failing either way (a
// missing permission), or one the current connections cannot even reach
// (their password was revoked), does not hold the new connections back.
func regressions(ctx context.Context, now, next connChecker, endpoints []string) []string {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return regressionsOf(now.Verify(ctx, endpoints...), next.Verify(ctx, endpoints...))
}

func regressionsOf(before, after map[string][]connector.CheckResult) []string {
	var out []string
	for ep, rs := range after {
		for _, r := range rs {
			if !r.OK && slices.ContainsFunc(before[ep], func(o connector.CheckResult) bool { return o.Name == r.Name && o.OK }) {
				out = append(out, fmt.Sprintf("%s: %s: %s", ep, r.Name, r.Detail))
			}
		}
	}
	sort.Strings(out)
	return out
}

// connChecker runs connection checks: an engine.Runtime or an agent.Server.
type connChecker interface {
	Verify(ctx context.Context, endpoints ...string) map[string][]connector.CheckResult
}

// endpointsUsing returns the endpoints whose connectors use one of the
// secret references; none for no references (then every endpoint counts).
func endpointsUsing(spec *compiler.RuntimeSpec, refs []string) []string {
	var out []string
	for _, c := range spec.Spec.Connectors {
		uses := append([]string{c.SecretRef}, connector.ConfigSecretRefs(c.Config)...)
		for _, u := range uses {
			if slices.Contains(refs, u) {
				out = append(out, c.Endpoint)
				break
			}
		}
	}
	return out
}

// generation is what `turgon run` rebuilds when secrets change: the
// connectors, the Temporal worker, the dispatcher and the subscriptions.
type generation struct {
	rt     *engine.Runtime
	w      worker.Worker
	d      *engine.Dispatcher
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// stop ends the subscriptions, lets the worker finish the activities it is
// running (up to the worker's stop timeout; Temporal retries the rest on
// the next generation, and writes are idempotent), then closes the
// connectors.
func (g *generation) stop() {
	g.once.Do(func() {
		g.cancel()
		<-g.done
		g.w.Stop()
		g.rt.Close()
	})
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
	// Running activities get a minute to finish when the worker stops (at
	// shutdown, or to reconnect with rotated secrets).
	return worker.Options{WorkflowTaskPollerBehavior: b, ActivityTaskPollerBehavior: b, WorkerStopTimeout: time.Minute}, nil
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
	var dbURL, entity, system, source, master, email, name, by string
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
			if err := pgstore.New(pool).Link(ctx, entity, system, source, master, attrs); err != nil {
				return err
			}
			// Linking decides which master record later writes go to: it is
			// audited like a steward's link in the console.
			_, err = pgstore.NewAuditLog(pool).Record(by, "xref.linked", map[string]any{
				"entity": entity, "system": system, "ref": source, "master": master, "via": "cli",
			})
			return err
		},
	}
	set.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	set.Flags().StringVar(&entity, "entity", "", "semantic entity, e.g. Customer")
	set.Flags().StringVar(&system, "system", "", "source endpoint, e.g. shop-db")
	set.Flags().StringVar(&source, "source", "", "record ID in the source system")
	set.Flags().StringVar(&master, "master", "", "master record ID")
	set.Flags().StringVar(&email, "email", "", "the record's email address, kept for matching")
	set.Flags().StringVar(&name, "name", "", "the record's name, kept for matching")
	set.Flags().StringVar(&by, "by", os.Getenv("USER"), "who links, for the audit log")
	for _, f := range []string{"entity", "system", "source", "master"} {
		_ = set.MarkFlagRequired(f)
	}
	cmd.AddCommand(set, xrefLoadCmd())
	return cmd
}

func secretsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "secrets SPEC",
		Short: "List where `turgon run` reads each secret: environment variables, or OpenBao/Vault paths",
		Long: "List each secret reference a spec uses and where it is read from: the TURGON_SECRET_*\n" +
			"variable (TURGON_SECRETS=env, the default) or the OpenBao/Vault KV path and key\n" +
			"(TURGON_SECRETS=openbao). `turgon check` reads them.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := loadSpec(args[0])
			if err != nil {
				return err
			}
			b, err := openSecrets()
			if err != nil {
				return err
			}
			for _, c := range spec.Spec.Connectors {
				refs := append([]string{c.SecretRef}, connector.ConfigSecretRefs(c.Config)...)
				for _, ref := range refs {
					if ref != "" {
						fmt.Fprintf(cmd.OutOrStdout(), "%-14s %-44s %s\n", c.Endpoint, ref, b.Where(ref))
					}
				}
			}
			return nil
		},
	}
}

// serveMetrics serves /metrics on its own address, for the console and the
// MCP servers, whose main listener sits behind authentication.
func serveMetrics(ctx context.Context, addr string, logw io.Writer) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(logw, "turgon: metrics endpoint: %v\n", err)
	}
}

// serveHealth answers Kubernetes probes: /healthz while the process runs,
// /readyz while Turgon's database answers.
func serveHealth(ctx context.Context, addr string, pool *pgxpool.Pool, logw io.Writer) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.Handle("/metrics", metrics.Handler())
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
