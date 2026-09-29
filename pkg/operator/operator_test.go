package operator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	turgonv1 "github.com/fduser123-coding/turgon/apis/operator/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/signing"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// compile compiles an example recipe to runtime spec JSON.
func compile(t *testing.T, recipe string) []byte {
	t.Helper()
	cat, err := catalog.Load("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := cat.Find(recipe)
	spec, rep, err := compiler.Compile(cat, obj, verifier.Options{})
	if err != nil {
		t.Fatalf("%v: %+v", err, rep.Errors())
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

const workerTemplate = `
metadata:
  labels: { team: integration }
spec:
  serviceAccountName: turgon
  containers:
    - name: worker
      image: ghcr.io/fduser123-coding/turgon:1.0.0
      env:
        - { name: TURGON_DATABASE_URL, valueFrom: { secretKeyRef: { name: turgon-db, key: url } } }
      volumeMounts:
        - { name: tmp, mountPath: /tmp }
  volumes:
    - { name: tmp, emptyDir: {} }
`

func testConfig(t *testing.T) *Config {
	t.Helper()
	tmpl, err := LoadTemplate([]byte(workerTemplate))
	if err != nil {
		t.Fatal(err)
	}
	return &Config{Template: tmpl, WorkerArgs: []string{"--poll=2s"}, SLORules: true}
}

func TestVerify(t *testing.T) {
	cfg := testConfig(t)
	raw := compile(t, "shop-orders-to-erp")
	v, err := cfg.Verify(raw)
	if err != nil || v.Spec.Metadata.Name != "shop-orders-to-erp" || v.SignedBy != "" {
		t.Fatalf("valid spec: %+v %v", v, err)
	}
	tampered := strings.Replace(string(raw), `"autoMatchAbove":1`, `"autoMatchAbove":0.1`, 1)
	if tampered == string(raw) {
		t.Fatal("test spec has no autoMatchAbove to tamper with")
	}
	if _, err := cfg.Verify([]byte(tampered)); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("tampered spec: %v", err)
	}
	// A field the spec schema lacks rides along under an unchanged digest:
	// refused.
	smuggled := strings.Replace(string(raw), `"simulation":"rollback"`, `"simulation":"rollback","simulate":false`, 1)
	if _, err := cfg.Verify([]byte(smuggled)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field: %v", err)
	}
	if _, err := cfg.Verify([]byte(`{"kind":"Recipe"}`)); err == nil {
		t.Fatal("a non-spec was accepted")
	}

	// With trusted keys, only signed specs pass.
	priv, pub, err := signing.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := signing.ParsePublicKeys(pub)
	cfg.TrustedKeys = keys
	if _, err := cfg.Verify(raw); err == nil || !strings.Contains(err.Error(), "not signed") {
		t.Fatalf("unsigned spec: %v", err)
	}
	var s compiler.RuntimeSpec
	_ = json.Unmarshal(raw, &s)
	key, _ := signing.ParsePrivateKey(priv)
	if err := signing.Sign(&s, key); err != nil {
		t.Fatal(err)
	}
	signed, _ := json.Marshal(&s)
	if v, err := cfg.Verify(signed); err != nil || v.SignedBy != signing.KeyID(keys[0]) {
		t.Fatalf("signed spec: %+v %v", v, err)
	}
}

func TestDerivedFromTheSpec(t *testing.T) {
	cfg := testConfig(t)
	stripe, _ := cfg.Verify(compile(t, "stripe-payments-to-erp"))
	if got := WebhookPaths(stripe.Spec); strings.Join(got, ",") != "/webhooks/stripe-billing/Invoice.Paid" {
		t.Fatalf("webhook paths: %v", got)
	}
	shop, _ := cfg.Verify(compile(t, "shop-orders-to-erp"))
	if got := WebhookPaths(shop.Spec); len(got) != 0 {
		t.Fatalf("an outbox event has webhook paths: %v", got)
	}
	rules := SLORules(shop.Spec)
	if len(rules) != 1 || !strings.Contains(rules[0].(map[string]any)["expr"].(string), `workflow="shop-orders-to-erp"`) ||
		!strings.Contains(rules[0].(map[string]any)["expr"].(string), "> 2\n") {
		t.Fatalf("rules: %v", rules)
	}

	in := &turgonv1.Integration{ObjectMeta: metav1.ObjectMeta{Name: "shop", Namespace: "turgon"},
		Spec: turgonv1.IntegrationSpec{Webhooks: &turgonv1.Webhooks{Enabled: true}}}
	var d appsv1.Deployment
	cfg.MutateDeployment(&d, in, shop)
	ct := d.Spec.Template.Spec.Containers[0]
	if *d.Spec.Replicas != 2 || ct.Image != "ghcr.io/fduser123-coding/turgon:1.0.0" || len(ct.Env) != 1 ||
		!slices.Equal(ct.Args, []string{"run", "--spec=/specs/spec.json", "--audit-log=postgres", "--health-listen=:8081", "--poll=2s", "--webhook-listen=:8082"}) ||
		d.Spec.Template.Labels["team"] != "integration" || d.Spec.Template.Labels[LabelIntegration] != "shop" ||
		d.Spec.Template.Annotations[AnnotationDigest] != shop.Spec.Metadata.Digest || d.Spec.Template.Spec.ServiceAccountName != "turgon" {
		t.Fatalf("deployment: %+v", d.Spec)
	}
	if vol := d.Spec.Template.Spec.Volumes[1]; vol.ConfigMap == nil || vol.ConfigMap.Name != SpecConfigMapName(in, shop.Spec.Metadata.Digest) {
		t.Fatalf("spec volume: %+v", d.Spec.Template.Spec.Volumes)
	}
	// The template is copied, never changed.
	if len(cfg.Template.Spec.Containers[0].VolumeMounts) != 1 || cfg.Template.Spec.Containers[0].Args != nil {
		t.Fatal("the template was modified")
	}
	in.Spec.Paused = true
	cfg.MutateDeployment(&d, in, shop)
	if *d.Spec.Replicas != 0 {
		t.Fatal("paused but not scaled to zero")
	}
	if _, err := LoadTemplate([]byte("spec: {containers: []}")); err == nil {
		t.Fatal("template without a container")
	}
	if _, err := LoadTemplate([]byte("spec: {contaners: [{name: x}]}")); err == nil {
		t.Fatal("template with a misspelt field")
	}
}

// env runs a real API server (envtest) with the operator.
type env struct {
	t   *testing.T
	c   client.Client
	cfg *Config
}

func startEnv(t *testing.T) *env {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("KUBEBUILDER_ASSETS is not set: the operator tests need kube-apiserver and etcd")
		}
		t.Skip("KUBEBUILDER_ASSETS not set; skipping the operator's API server tests")
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(testWriter{t})))
	te := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "deploy", "helm", "turgon", "crds"), "testdata"},
		ErrorIfCRDPathMissing: true}
	rc, err := te.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = te.Stop() })
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = turgonv1.AddToScheme(scheme)
	skip := true // each test starts its own manager in this process
	mgr, err := ctrl.NewManager(rc, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skip}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	r := &Reconciler{Client: mgr.GetClient(), Config: cfg, Recorder: mgr.GetEventRecorderFor("turgon-operator")}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(ctx); err != nil {
			t.Error(err)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	c, err := client.New(rc, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, c: c, cfg: cfg}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// eventually retries check until it passes or 20 seconds pass.
func (e *env) eventually(what string, check func() error) {
	e.t.Helper()
	var err error
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if err = check(); err == nil {
			return
		}
	}
	e.t.Fatalf("%s: %v", what, err)
}

type fail string

func (f fail) Error() string { return string(f) }

func (e *env) get(name string, obj client.Object) error {
	return e.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, obj)
}

func (e *env) integration(name string) *turgonv1.Integration {
	e.t.Helper()
	var in turgonv1.Integration
	if err := e.get(name, &in); err != nil {
		e.t.Fatal(err)
	}
	return &in
}

// ready makes the Deployment look rolled out: envtest runs no kubelet.
func (e *env) ready(name string) {
	e.t.Helper()
	e.eventually("mark workers ready", func() error {
		var d appsv1.Deployment
		if err := e.get(name, &d); err != nil {
			return err
		}
		n := *d.Spec.Replicas
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: n, UpdatedReplicas: n, ReadyReplicas: n, AvailableReplicas: n}
		return e.c.Status().Update(context.Background(), &d)
	})
}

func condition(in *turgonv1.Integration, typ string) *metav1.Condition {
	return meta.FindStatusCondition(in.Status.Conditions, typ)
}

func TestOperatorRunsIntegrations(t *testing.T) {
	e := startEnv(t)
	ctx := context.Background()
	stripe := compile(t, "stripe-payments-to-erp")
	in := &turgonv1.Integration{ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default"}, Spec: turgonv1.IntegrationSpec{
		RuntimeSpec: &runtime.RawExtension{Raw: stripe},
		Webhooks:    &turgonv1.Webhooks{Enabled: true, Host: "hooks.example.com", TLSSecretName: "hooks-tls"},
	}}
	if err := e.c.Create(ctx, in); err != nil {
		t.Fatal(err)
	}
	var s compiler.RuntimeSpec
	_ = json.Unmarshal(stripe, &s)
	firstCM := SpecConfigMapName(in, s.Metadata.Digest)

	// The spec version, the workers mounting it, and what the spec implies.
	e.eventually("workers", func() error {
		var cm corev1.ConfigMap
		if err := e.get(firstCM, &cm); err != nil {
			return err
		}
		if cm.Immutable == nil || !*cm.Immutable || !metav1.IsControlledBy(&cm, e.integration("payments")) {
			return fail("spec configmap not immutable or not owned")
		}
		var d appsv1.Deployment
		if err := e.get("payments-worker", &d); err != nil {
			return err
		}
		if d.Spec.Template.Spec.Volumes[1].ConfigMap.Name != firstCM || !slices.Contains(d.Spec.Template.Spec.Containers[0].Args, "--webhook-listen=:8082") {
			return fail("deployment does not run the spec")
		}
		var ing networkingv1.Ingress
		if err := e.get("payments-webhooks", &ing); err != nil {
			return err
		}
		if p := ing.Spec.Rules[0].HTTP.Paths; len(p) != 1 || p[0].Path != "/webhooks/stripe-billing/Invoice.Paid" || ing.Spec.TLS[0].SecretName != "hooks-tls" {
			return fail("ingress paths")
		}
		var svc corev1.Service
		if err := e.get("payments-webhooks", &svc); err != nil {
			return err
		}
		rule := &unstructured.Unstructured{}
		rule.SetGroupVersionKind(PrometheusRuleGVK)
		if err := e.get("payments-slo", rule); err != nil {
			return err
		}
		got := e.integration("payments")
		if c := condition(got, turgonv1.ConditionSpecValid); c == nil || c.Status != metav1.ConditionTrue {
			return fail("SpecValid not true")
		}
		if c := condition(got, turgonv1.ConditionAvailable); c == nil || c.Reason != "RollingOut" {
			return fail("Available not RollingOut")
		}
		if got.Status.Spec == nil || got.Status.Spec.Level != s.Metadata.Level || strings.Join(got.Status.WebhookPaths, ",") != "/webhooks/stripe-billing/Invoice.Paid" {
			return fail("status spec")
		}
		return nil
	})
	e.ready("payments-worker")
	e.eventually("available", func() error {
		if c := condition(e.integration("payments"), turgonv1.ConditionAvailable); c == nil || c.Status != metav1.ConditionTrue {
			return fail("not available")
		}
		return nil
	})

	// A tampered spec is refused; the workers keep the last valid one.
	cur := e.integration("payments")
	cur.Spec.RuntimeSpec = &runtime.RawExtension{Raw: []byte(strings.Replace(string(stripe), `"simulate":true`, `"simulate":false`, 1))}
	if err := e.c.Update(ctx, cur); err != nil {
		t.Fatal(err)
	}
	e.eventually("refusal", func() error {
		got := e.integration("payments")
		c := condition(got, turgonv1.ConditionSpecValid)
		if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "digest mismatch") || got.Status.ObservedGeneration != got.Generation {
			return fail("SpecValid not false")
		}
		return nil
	})
	var d appsv1.Deployment
	if err := e.get("payments-worker", &d); err != nil || d.Spec.Template.Spec.Volumes[1].ConfigMap.Name != firstCM {
		t.Fatalf("the refused spec was rolled out: %v", err)
	}
	if got := e.integration("payments"); got.Status.Spec.Digest != s.Metadata.Digest {
		t.Fatalf("status names the refused spec: %+v", got.Status.Spec)
	}

	// A new valid spec, read from a ConfigMap: rolled out, with no webhook
	// events its Service and Ingress go, and once its workers are ready the
	// old version is pruned.
	shop := compile(t, "shop-orders-to-erp")
	src := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "specs", Namespace: "default"}, Data: map[string]string{"shop.json": string(shop)}}
	if err := e.c.Create(ctx, src); err != nil {
		t.Fatal(err)
	}
	cur = e.integration("payments")
	cur.Spec.RuntimeSpec = nil
	cur.Spec.SpecFrom = &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "specs"}, Key: "shop.json"}
	if err := e.c.Update(ctx, cur); err != nil {
		t.Fatal(err)
	}
	var shopSpec compiler.RuntimeSpec
	_ = json.Unmarshal(shop, &shopSpec)
	secondCM := SpecConfigMapName(cur, shopSpec.Metadata.Digest)
	e.eventually("second spec", func() error {
		var d appsv1.Deployment
		if err := e.get("payments-worker", &d); err != nil {
			return err
		}
		if d.Spec.Template.Spec.Volumes[1].ConfigMap.Name != secondCM {
			return fail("not rolled out")
		}
		for _, o := range []client.Object{&corev1.Service{}, &networkingv1.Ingress{}} {
			if err := e.get("payments-webhooks", o); !apierrors.IsNotFound(err) {
				return fail("webhook objects remain")
			}
		}
		var old corev1.ConfigMap
		if err := e.get(firstCM, &old); err != nil {
			return fail("old spec version pruned before the rollout finished")
		}
		return nil
	})
	e.ready("payments-worker")
	e.eventually("prune", func() error {
		var old corev1.ConfigMap
		if err := e.get(firstCM, &old); !apierrors.IsNotFound(err) {
			return fail("old spec version kept")
		}
		var cur corev1.ConfigMap
		return e.get(secondCM, &cur)
	})

	// Editing the ConfigMap the spec is read from rolls it out too.
	invalid := src.DeepCopy()
	invalid.Data["shop.json"] = `{"not":"a spec"}`
	if err := e.c.Update(ctx, invalid); err != nil {
		t.Fatal(err)
	}
	e.eventually("configmap edit noticed", func() error {
		if c := condition(e.integration("payments"), turgonv1.ConditionSpecValid); c == nil || c.Status != metav1.ConditionFalse {
			return fail("edit not noticed")
		}
		return nil
	})

	// Paused: no workers.
	cur = e.integration("payments")
	cur.Spec.SpecFrom = nil
	cur.Spec.RuntimeSpec = &runtime.RawExtension{Raw: shop}
	cur.Spec.Paused = true
	if err := e.c.Update(ctx, cur); err != nil {
		t.Fatal(err)
	}
	e.eventually("paused", func() error {
		var d appsv1.Deployment
		if err := e.get("payments-worker", &d); err != nil {
			return err
		}
		c := condition(e.integration("payments"), turgonv1.ConditionAvailable)
		if *d.Spec.Replicas != 0 || c == nil || c.Reason != "Paused" {
			return fail("not paused")
		}
		return nil
	})
}

// An Integration whose first spec is invalid gets no workers.
func TestNoWorkersWithoutAValidSpec(t *testing.T) {
	e := startEnv(t)
	in := &turgonv1.Integration{ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "default"}, Spec: turgonv1.IntegrationSpec{
		RuntimeSpec: &runtime.RawExtension{Raw: []byte(`{"metadata":{"name":"x","digest":"sha256:00"},"spec":{}}`)}}}
	if err := e.c.Create(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	e.eventually("refused", func() error {
		got := e.integration("broken")
		if c := condition(got, turgonv1.ConditionSpecValid); c == nil || c.Status != metav1.ConditionFalse {
			return fail("not refused")
		}
		if c := condition(got, turgonv1.ConditionAvailable); c == nil || c.Reason != "NoWorkers" {
			return fail("available condition")
		}
		return nil
	})
	var d appsv1.Deployment
	if err := e.get("broken-worker", &d); !apierrors.IsNotFound(err) {
		t.Fatalf("workers for an invalid spec: %v", err)
	}
	var events corev1.EventList
	e.eventually("warning event", func() error {
		if err := e.c.List(context.Background(), &events, client.InNamespace("default")); err != nil {
			return err
		}
		for _, ev := range events.Items {
			if ev.Reason == "InvalidSpec" && ev.InvolvedObject.Name == "broken" {
				return nil
			}
		}
		return fail("no InvalidSpec event")
	})
}
