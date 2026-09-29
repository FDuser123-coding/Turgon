// Package operator is the Turgon Kubernetes operator: it runs each
// Integration's compiled runtime spec on a Deployment of workers. It checks
// a spec before rolling it out (digest and, with trusted keys, signature),
// keeps the workers on the last valid spec when a new one is not, and
// derives what the spec implies: the webhook Service and Ingress with
// exactly its webhook paths, and alert rules for its latency SLOs.
package operator

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	turgonv1 "github.com/fduser123-coding/turgon/apis/operator/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/signing"
)

// Config is how the operator builds workers.
type Config struct {
	// Template is the workers' pod template: image, environment, secrets,
	// mounts and security settings shared by every Integration. Its
	// container named "worker" (or its first) runs the spec.
	Template corev1.PodTemplateSpec
	// WorkerArgs are appended to every worker's "turgon run" arguments.
	WorkerArgs []string
	// TrustedKeys, if set, admit only specs one of them signed.
	TrustedKeys []ed25519.PublicKey
	// DefaultReplicas is used when an Integration does not say. Default 2.
	DefaultReplicas int32
	// SLORules creates a PrometheusRule per Integration for its latency SLOs.
	SLORules bool
}

// LoadTemplate reads a pod template from YAML.
func LoadTemplate(data []byte) (corev1.PodTemplateSpec, error) {
	var t corev1.PodTemplateSpec
	if err := yaml.UnmarshalStrict(data, &t); err != nil {
		return t, fmt.Errorf("worker template: %w", err)
	}
	if len(t.Spec.Containers) == 0 {
		return t, errors.New("worker template: no container")
	}
	return t, nil
}

// Labels.
const (
	LabelIntegration = "turgon.dev/integration"
	LabelComponent   = "app.kubernetes.io/component"
	LabelName        = "app.kubernetes.io/name"
	LabelManagedBy   = "app.kubernetes.io/managed-by"
	AnnotationDigest = "turgon.dev/spec-digest"
	managedBy        = "turgon-operator"
	specKey          = "spec.json"
)

func labels(in *turgonv1.Integration) map[string]string {
	return map[string]string{LabelName: "turgon", LabelComponent: "worker", LabelIntegration: in.Name, LabelManagedBy: managedBy}
}

func selector(in *turgonv1.Integration) map[string]string {
	return map[string]string{LabelName: "turgon", LabelComponent: "worker", LabelIntegration: in.Name}
}

// Verified is a runtime spec that passed the operator's checks.
type Verified struct {
	Spec     *compiler.RuntimeSpec
	Raw      []byte
	SignedBy string
}

// Verify parses a runtime spec strictly and checks its digest and, with
// trusted keys, its signature.
func (c *Config) Verify(raw []byte) (*Verified, error) {
	s, err := compiler.ParseSpec(raw)
	if err != nil {
		return nil, err
	}
	v := &Verified{Spec: s, Raw: raw}
	if len(c.TrustedKeys) > 0 {
		id, err := signing.Verify(s, c.TrustedKeys)
		if err != nil {
			return nil, err
		}
		v.SignedBy = id
	}
	return v, nil
}

// shortDigest is the hex digest's first 12 characters.
func shortDigest(d string) string {
	d = strings.TrimPrefix(d, "sha256:")
	if len(d) > 12 {
		d = d[:12]
	}
	return d
}

// SpecConfigMapName names the immutable ConfigMap holding one version of
// an Integration's spec.
func SpecConfigMapName(in *turgonv1.Integration, digest string) string {
	return in.Name + "-spec-" + shortDigest(digest)
}

// SpecConfigMap holds a verified spec. It never changes: a new spec gets a
// new ConfigMap, so pods of the old version keep reading theirs until the
// rollout replaces them.
func SpecConfigMap(in *turgonv1.Integration, v *Verified) *corev1.ConfigMap {
	immutable := true
	l := labels(in)
	l["turgon.dev/spec-digest"] = shortDigest(v.Spec.Metadata.Digest)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: SpecConfigMapName(in, v.Spec.Metadata.Digest), Namespace: in.Namespace, Labels: l,
			Annotations: map[string]string{AnnotationDigest: v.Spec.Metadata.Digest}},
		Data:      map[string]string{specKey: string(v.Raw)},
		Immutable: &immutable,
	}
}

// WorkerName names an Integration's Deployment.
func WorkerName(in *turgonv1.Integration) string { return in.Name + "-worker" }

// Replicas is how many workers an Integration wants.
func (c *Config) Replicas(in *turgonv1.Integration) int32 {
	switch {
	case in.Spec.Paused:
		return 0
	case in.Spec.Replicas != nil:
		return *in.Spec.Replicas
	case c.DefaultReplicas > 0:
		return c.DefaultReplicas
	}
	return 2
}

// MutateDeployment sets the Deployment that runs a verified spec.
func (c *Config) MutateDeployment(d *appsv1.Deployment, in *turgonv1.Integration, v *Verified) {
	replicas := c.Replicas(in)
	d.Labels = merge(d.Labels, labels(in))
	d.Spec.Replicas = &replicas
	d.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector(in)}

	tmpl := *c.Template.DeepCopy()
	tmpl.Labels = merge(tmpl.Labels, labels(in))
	tmpl.Annotations = merge(tmpl.Annotations, map[string]string{AnnotationDigest: v.Spec.Metadata.Digest})
	idx := 0
	for i, ct := range tmpl.Spec.Containers {
		if ct.Name == "worker" {
			idx = i
		}
	}
	ct := &tmpl.Spec.Containers[idx]
	ct.Name = "worker"
	ct.Args = append([]string{"run", "--spec=/specs/" + specKey, "--audit-log=postgres", "--health-listen=:8081"}, c.WorkerArgs...)
	ct.Ports = []corev1.ContainerPort{{Name: "health", ContainerPort: 8081}}
	if webhooksOn(in) {
		ct.Args = append(ct.Args, "--webhook-listen=:8082")
		ct.Ports = append(ct.Ports, corev1.ContainerPort{Name: "webhooks", ContainerPort: 8082})
	}
	if ct.LivenessProbe == nil {
		ct.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("health")}}, PeriodSeconds: 20}
	}
	if ct.ReadinessProbe == nil {
		ct.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("health")}}, PeriodSeconds: 10}
	}
	if in.Spec.Resources != nil {
		ct.Resources = *in.Spec.Resources
	}
	ct.VolumeMounts = append(ct.VolumeMounts, corev1.VolumeMount{Name: "turgon-spec", MountPath: "/specs", ReadOnly: true})
	tmpl.Spec.Volumes = append(tmpl.Spec.Volumes, corev1.Volume{Name: "turgon-spec", VolumeSource: corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: SpecConfigMapName(in, v.Spec.Metadata.Digest)}}}})
	d.Spec.Template = tmpl
}

func webhooksOn(in *turgonv1.Integration) bool {
	return in.Spec.Webhooks != nil && in.Spec.Webhooks.Enabled
}

// WebhookPaths are the paths of the spec's trigger events configured to
// arrive by webhook, sorted.
func WebhookPaths(s *compiler.RuntimeSpec) []string {
	cfg := map[string]map[string]any{}
	for _, c := range s.Spec.Connectors {
		var m struct {
			Events map[string]map[string]any `json:"events"`
		}
		if json.Unmarshal(c.Config, &m) == nil {
			for ev, e := range m.Events {
				if _, ok := e["webhook"]; ok {
					if cfg[c.Endpoint] == nil {
						cfg[c.Endpoint] = map[string]any{}
					}
					cfg[c.Endpoint][ev] = true
				}
			}
		}
	}
	seen := map[string]bool{}
	var out []string
	for _, wf := range s.Spec.Workflows {
		p := "/webhooks/" + wf.Trigger.Endpoint + "/" + wf.Trigger.Event
		if cfg[wf.Trigger.Endpoint][wf.Trigger.Event] != nil && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// WebhookName names the webhook Service and Ingress.
func WebhookName(in *turgonv1.Integration) string { return in.Name + "-webhooks" }

// MutateService sets the Service in front of the workers' webhook port.
func MutateService(s *corev1.Service, in *turgonv1.Integration) {
	s.Labels = merge(s.Labels, labels(in))
	s.Spec.Selector = selector(in)
	s.Spec.Ports = []corev1.ServicePort{{Name: "webhooks", Port: 8082, TargetPort: intstr.FromString("webhooks")}}
}

// MutateIngress routes exactly the spec's webhook paths to the workers.
func MutateIngress(ing *networkingv1.Ingress, in *turgonv1.Integration, paths []string) {
	ing.Labels = merge(ing.Labels, labels(in))
	w := in.Spec.Webhooks
	ing.Spec.IngressClassName = w.IngressClassName
	exact := networkingv1.PathTypeExact
	var hp []networkingv1.HTTPIngressPath
	for _, p := range paths {
		hp = append(hp, networkingv1.HTTPIngressPath{Path: p, PathType: &exact, Backend: networkingv1.IngressBackend{
			Service: &networkingv1.IngressServiceBackend{Name: WebhookName(in), Port: networkingv1.ServiceBackendPort{Name: "webhooks"}}}})
	}
	ing.Spec.Rules = []networkingv1.IngressRule{{Host: w.Host, IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: hp}}}}
	ing.Spec.TLS = nil
	if w.TLSSecretName != "" {
		ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{w.Host}, SecretName: w.TLSSecretName}}
	}
}

// SLORuleName names an Integration's PrometheusRule.
func SLORuleName(in *turgonv1.Integration) string { return in.Name + "-slo" }

// SLORules returns the spec's latency SLO alerts as PrometheusRule rules,
// the same as the chart renders for specs in its values; nil if it has
// none.
func SLORules(s *compiler.RuntimeSpec) []any {
	var rules []any
	for _, m := range s.Spec.Monitors {
		if m.Kind != "latency-p95" || m.TargetSeconds <= 0 {
			continue
		}
		rules = append(rules, map[string]any{
			"alert": "TurgonRecipeLatencySLO",
			"expr": fmt.Sprintf("histogram_quantile(0.95, sum by (le) (rate(turgon_run_active_seconds_bucket{workflow=%q}[30m])))\n  > %g\n",
				m.Workflow, m.TargetSeconds),
			"for":    "15m",
			"labels": map[string]any{"severity": "warning", "workflow": m.Workflow},
			"annotations": map[string]any{
				"summary": fmt.Sprintf("%s: p95 run time above its %s SLO", m.Workflow, m.Target),
				"description": fmt.Sprintf("95%% of %s runs should finish within %s, not counting approvals. "+
					"Check the target systems' latency (turgon_write_duration_seconds) and open circuit breakers.", m.Workflow, m.Target),
			},
		})
	}
	return rules
}

// MutateSLORule sets a PrometheusRule's groups.
func MutateSLORule(u *unstructured.Unstructured, in *turgonv1.Integration, rules []any) {
	u.SetLabels(merge(u.GetLabels(), labels(in)))
	u.Object["spec"] = map[string]any{"groups": []any{map[string]any{"name": "turgon-slo-" + in.Name, "rules": rules}}}
}

func merge(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = map[string]string{}
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Workflows lists a spec's workflow names.
func Workflows(s *compiler.RuntimeSpec) []string {
	out := []string{}
	for _, wf := range s.Spec.Workflows {
		out = append(out, wf.Name)
	}
	sort.Strings(out)
	return out
}
