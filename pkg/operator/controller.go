package operator

import (
	"context"
	"errors"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	turgonv1 "github.com/fduser123-coding/turgon/apis/operator/v1alpha1"
)

// PrometheusRuleGVK is the Prometheus Operator's rule kind.
var PrometheusRuleGVK = schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "PrometheusRule"}

// Reconciler runs Integrations.
type Reconciler struct {
	client.Client
	Config   *Config
	Recorder record.EventRecorder
}

// SetupWithManager registers the reconciler: it watches Integrations,
// what it creates for them, and the ConfigMaps specs are read from.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&turgonv1.Integration{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.readers))
	return b.Complete(r)
}

// readers maps a ConfigMap to the Integrations reading their spec from it.
func (r *Reconciler) readers(ctx context.Context, cm client.Object) []reconcile.Request {
	var list turgonv1.IntegrationList
	if err := r.List(ctx, &list, client.InNamespace(cm.GetNamespace())); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, in := range list.Items {
		if in.Spec.SpecFrom != nil && in.Spec.SpecFrom.Name == cm.GetName() {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: in.Namespace, Name: in.Name}})
		}
	}
	return out
}

// Reconcile brings an Integration's workers to its spec.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var in turgonv1.Integration
	if err := r.Get(ctx, req.NamespacedName, &in); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !in.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // owned objects are garbage-collected
	}
	status := in.Status.DeepCopy()
	status.ObservedGeneration = in.Generation

	raw, err := r.specBytes(ctx, &in)
	var v *Verified
	if err == nil {
		v, err = r.Config.Verify(raw)
	}
	if err != nil {
		// Leave the workers on the last valid spec.
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: turgonv1.ConditionSpecValid, Status: metav1.ConditionFalse,
			Reason: "InvalidSpec", Message: err.Error(), ObservedGeneration: in.Generation})
		if r.Recorder != nil {
			r.Recorder.Event(&in, corev1.EventTypeWarning, "InvalidSpec", err.Error())
		}
		logger.Info("runtime spec refused", "error", err.Error())
		if err := r.observeDeployment(ctx, &in, status); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.updateStatus(ctx, &in, status)
	}
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: turgonv1.ConditionSpecValid, Status: metav1.ConditionTrue,
		Reason: "Verified", Message: verifiedMessage(v), ObservedGeneration: in.Generation})
	changed := status.Spec == nil || status.Spec.Digest != v.Spec.Metadata.Digest
	status.Spec = &turgonv1.RunningSpec{Name: v.Spec.Metadata.Name, Digest: v.Spec.Metadata.Digest, Level: v.Spec.Metadata.Level,
		SignedBy: v.SignedBy, Workflows: Workflows(v.Spec)}

	// The spec version, then the workers that mount it.
	cm := SpecConfigMap(&in, v)
	if err := controllerutil.SetControllerReference(&in, cm, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("spec configmap: %w", err)
	}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: WorkerName(&in), Namespace: in.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		r.Config.MutateDeployment(dep, &in, v)
		return controllerutil.SetControllerReference(&in, dep, r.Scheme())
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("deployment: %w", err)
	}
	if changed && r.Recorder != nil {
		r.Recorder.Eventf(&in, corev1.EventTypeNormal, "RollingOut", "rolling out %s %s (level %s)", v.Spec.Metadata.Name,
			shortDigest(v.Spec.Metadata.Digest), v.Spec.Metadata.Level)
	}

	if err := r.reconcileWebhooks(ctx, &in, v, status); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileSLORule(ctx, &in, v); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.observeDeployment(ctx, &in, status); err != nil {
		return ctrl.Result{}, err
	}
	if rolledOut(dep, r.Config.Replicas(&in)) {
		if err := r.pruneSpecs(ctx, &in, cm.Name); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, r.updateStatus(ctx, &in, status)
}

func verifiedMessage(v *Verified) string {
	msg := fmt.Sprintf("%s %s, level %s", v.Spec.Metadata.Name, shortDigest(v.Spec.Metadata.Digest), v.Spec.Metadata.Level)
	if v.SignedBy != "" {
		msg += ", signed by " + v.SignedBy
	}
	return msg
}

// specBytes reads the Integration's runtime spec.
func (r *Reconciler) specBytes(ctx context.Context, in *turgonv1.Integration) ([]byte, error) {
	switch {
	case in.Spec.RuntimeSpec != nil && in.Spec.SpecFrom != nil:
		return nil, errors.New("set runtimeSpec or specFrom, not both")
	case in.Spec.RuntimeSpec != nil:
		return in.Spec.RuntimeSpec.Raw, nil
	case in.Spec.SpecFrom != nil:
		var cm corev1.ConfigMap
		if err := r.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: in.Spec.SpecFrom.Name}, &cm); err != nil {
			return nil, fmt.Errorf("specFrom: configmap %s: %w", in.Spec.SpecFrom.Name, err)
		}
		data, ok := cm.Data[in.Spec.SpecFrom.Key]
		if !ok {
			return nil, fmt.Errorf("specFrom: configmap %s has no key %s", in.Spec.SpecFrom.Name, in.Spec.SpecFrom.Key)
		}
		return []byte(data), nil
	}
	return nil, errors.New("no runtime spec: set runtimeSpec or specFrom")
}

func (r *Reconciler) reconcileWebhooks(ctx context.Context, in *turgonv1.Integration, v *Verified, status *turgonv1.IntegrationStatus) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: WebhookName(in), Namespace: in.Namespace}}
	ing := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: WebhookName(in), Namespace: in.Namespace}}
	paths := WebhookPaths(v.Spec)
	status.WebhookPaths = nil
	if !webhooksOn(in) || len(paths) == 0 {
		for _, o := range []client.Object{svc, ing} {
			if err := r.Delete(ctx, o); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		return nil
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		MutateService(svc, in)
		return controllerutil.SetControllerReference(in, svc, r.Scheme())
	}); err != nil {
		return fmt.Errorf("webhook service: %w", err)
	}
	if in.Spec.Webhooks.Host == "" {
		if err := r.Delete(ctx, ing); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ing, func() error {
		MutateIngress(ing, in, paths)
		return controllerutil.SetControllerReference(in, ing, r.Scheme())
	}); err != nil {
		return fmt.Errorf("webhook ingress: %w", err)
	}
	status.WebhookPaths = paths
	return nil
}

func (r *Reconciler) reconcileSLORule(ctx context.Context, in *turgonv1.Integration, v *Verified) error {
	if !r.Config.SLORules {
		return nil
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PrometheusRuleGVK)
	u.SetNamespace(in.Namespace)
	u.SetName(SLORuleName(in))
	rules := SLORules(v.Spec)
	if len(rules) == 0 {
		if err := r.Delete(ctx, u); err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			return err
		}
		return nil
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, u, func() error {
		MutateSLORule(u, in, rules)
		return controllerutil.SetControllerReference(in, u, r.Scheme())
	})
	if meta.IsNoMatchError(err) {
		log.FromContext(ctx).Info("PrometheusRule is not installed; skipping SLO alerts")
		return nil
	}
	return err
}

// observeDeployment copies the workers' state into the status.
func (r *Reconciler) observeDeployment(ctx context.Context, in *turgonv1.Integration, status *turgonv1.IntegrationStatus) error {
	var dep appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: WorkerName(in)}, &dep)
	if apierrors.IsNotFound(err) {
		status.Replicas, status.ReadyReplicas = 0, 0
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: turgonv1.ConditionAvailable, Status: metav1.ConditionFalse,
			Reason: "NoWorkers", Message: "no valid spec has been rolled out", ObservedGeneration: in.Generation})
		return nil
	}
	if err != nil {
		return err
	}
	status.Replicas, status.ReadyReplicas = dep.Status.Replicas, dep.Status.ReadyReplicas
	want := r.Config.Replicas(in)
	cond := metav1.Condition{Type: turgonv1.ConditionAvailable, ObservedGeneration: in.Generation}
	switch {
	case in.Spec.Paused:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "Paused", "paused: no workers run; events wait at their sources"
	case rolledOut(&dep, want):
		cond.Status, cond.Reason, cond.Message = metav1.ConditionTrue, "WorkersReady", fmt.Sprintf("%d of %d workers ready", dep.Status.ReadyReplicas, want)
	default:
		cond.Status, cond.Reason = metav1.ConditionFalse, "RollingOut"
		cond.Message = fmt.Sprintf("%d of %d workers of the current spec ready", dep.Status.UpdatedReplicas, want)
	}
	meta.SetStatusCondition(&status.Conditions, cond)
	return nil
}

// rolledOut: every wanted worker runs the Deployment's current template
// and is ready.
func rolledOut(dep *appsv1.Deployment, want int32) bool {
	s := dep.Status
	return s.ObservedGeneration >= dep.Generation && s.UpdatedReplicas == want && s.ReadyReplicas == want && s.Replicas == want
}

// pruneSpecs deletes spec versions no worker mounts any more.
func (r *Reconciler) pruneSpecs(ctx context.Context, in *turgonv1.Integration, current string) error {
	var list corev1.ConfigMapList
	if err := r.List(ctx, &list, client.InNamespace(in.Namespace), client.MatchingLabels{LabelIntegration: in.Name, LabelManagedBy: managedBy}); err != nil {
		return err
	}
	var old []string
	for i := range list.Items {
		cm := &list.Items[i]
		if cm.Name == current || !metav1.IsControlledBy(cm, in) {
			continue
		}
		if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		old = append(old, cm.Name)
	}
	if len(old) > 0 {
		sort.Strings(old)
		log.FromContext(ctx).Info("pruned old spec versions", "configmaps", old)
	}
	return nil
}

func (r *Reconciler) updateStatus(ctx context.Context, in *turgonv1.Integration, status *turgonv1.IntegrationStatus) error {
	in.Status = *status
	return r.Status().Update(ctx, in)
}
