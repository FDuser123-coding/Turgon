// Command turgon-operator runs Integrations: each one's compiled runtime
// spec on a Deployment of Turgon workers (see pkg/operator).
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	turgonv1 "github.com/fduser123-coding/turgon/apis/operator/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/operator"
	"github.com/fduser123-coding/turgon/pkg/signing"
)

type args []string

func (a *args) String() string     { return strings.Join(*a, " ") }
func (a *args) Set(v string) error { *a = append(*a, v); return nil }

func main() {
	var (
		namespace, templatePath, trustedKeys, metricsAddr, probeAddr string
		leaderElect, sloRules                                        bool
		replicas                                                     int
		workerArgs                                                   args
	)
	flag.StringVar(&namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace whose Integrations to run (default: the operator's own)")
	flag.StringVar(&templatePath, "worker-template", "/etc/turgon-operator/worker-template.yaml", "pod template of the workers (YAML)")
	flag.StringVar(&trustedKeys, "trusted-keys", os.Getenv("TURGON_TRUSTED_KEYS"), "PEM file of public keys; only specs one of them signed are rolled out")
	flag.IntVar(&replicas, "default-replicas", 2, "workers per Integration unless it says")
	flag.Var(&workerArgs, "worker-arg", "argument appended to every worker's turgon run (repeatable), e.g. --worker-arg=--poll=2s")
	flag.BoolVar(&sloRules, "slo-rules", false, "create a PrometheusRule per Integration for its latency SLOs")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health endpoint")
	flag.BoolVar(&leaderElect, "leader-elect", true, "elect a leader, so replicas of the operator never act at once")
	validate := flag.String("validate", "", "check a rendered manifest instead of running: its worker template loads and each Integration's spec verifies")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	if *validate != "" {
		if err := validateManifest(*validate, trustedKeys); err != nil {
			fmt.Fprintln(os.Stderr, "turgon-operator:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(namespace, templatePath, trustedKeys, metricsAddr, probeAddr, leaderElect, sloRules, int32(replicas), workerArgs); err != nil {
		fmt.Fprintln(os.Stderr, "turgon-operator:", err)
		os.Exit(1)
	}
}

func run(namespace, templatePath, trustedKeys, metricsAddr, probeAddr string, leaderElect, sloRules bool, replicas int32, workerArgs []string) error {
	if namespace == "" {
		return fmt.Errorf("--namespace (or POD_NAMESPACE) is required")
	}
	data, err := os.ReadFile(templatePath)
	if err != nil {
		return err
	}
	tmpl, err := operator.LoadTemplate(data)
	if err != nil {
		return err
	}
	cfg := &operator.Config{Template: tmpl, WorkerArgs: workerArgs, DefaultReplicas: replicas, SLORules: sloRules}
	if trustedKeys != "" {
		pem, err := os.ReadFile(trustedKeys)
		if err != nil {
			return err
		}
		if cfg.TrustedKeys, err = signing.ParsePublicKeys(pem); err != nil {
			return err
		}
		if len(cfg.TrustedKeys) == 0 {
			return fmt.Errorf("%s holds no public key", trustedKeys)
		}
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := turgonv1.AddToScheme(scheme); err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Cache:                   cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          leaderElect,
		LeaderElectionID:        "turgon-operator.turgon.dev",
		LeaderElectionNamespace: namespace,
	})
	if err != nil {
		return err
	}
	r := &operator.Reconciler{Client: mgr.GetClient(), Config: cfg, Recorder: mgr.GetEventRecorderFor("turgon-operator")}
	if err := r.SetupWithManager(mgr); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	ctrl.Log.Info("running Integrations", "namespace", namespace, "signedSpecsOnly", len(cfg.TrustedKeys) > 0, "sloRules", sloRules)
	return mgr.Start(ctrl.SetupSignalHandler())
}
