// Package v1alpha1 is the Kubernetes API of the Turgon operator: an
// Integration runs one compiled runtime spec.
//
// +kubebuilder:object:generate=true
// +groupName=turgon.dev
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the API's group and version.
var GroupVersion = schema.GroupVersion{Group: "turgon.dev", Version: "v1alpha1"}

var (
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)

func init() { SchemeBuilder.Register(&Integration{}, &IntegrationList{}) }

// IntegrationSpec says which runtime spec to run and how.
type IntegrationSpec struct {
	// RuntimeSpec is the compiled runtime spec (the output of turgon
	// compile), inline.
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	RuntimeSpec *runtime.RawExtension `json:"runtimeSpec,omitempty"`
	// SpecFrom reads the runtime spec from a ConfigMap key instead, for
	// specs too large to inline.
	// +optional
	SpecFrom *corev1.ConfigMapKeySelector `json:"specFrom,omitempty"`
	// Replicas is the number of workers. They share the load; each event
	// subscription is held by one of them at a time. Default 2.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// Paused scales the workers to zero. Events wait at their source (and
	// their cursors) until it is unset.
	// +optional
	Paused bool `json:"paused,omitempty"`
	// Webhooks receives the spec's webhook events on the workers, behind a
	// Service and, with a host, an Ingress routing exactly their paths.
	// +optional
	Webhooks *Webhooks `json:"webhooks,omitempty"`
	// Resources of each worker container; the operator's default otherwise.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Webhooks configures webhook delivery.
type Webhooks struct {
	Enabled bool `json:"enabled"`
	// Host the Ingress serves; no Ingress without it.
	// +optional
	Host string `json:"host,omitempty"`
	// +optional
	IngressClassName *string `json:"ingressClassName,omitempty"`
	// TLSSecretName holds the Ingress's certificate.
	// +optional
	TLSSecretName string `json:"tlsSecretName,omitempty"`
}

// Condition types.
const (
	// ConditionSpecValid: the runtime spec parses, its digest matches its
	// body and, if the operator trusts signing keys, one of them signed it.
	// A spec that is not valid is never rolled out: the workers keep
	// running the last valid one.
	ConditionSpecValid = "SpecValid"
	// ConditionAvailable: the workers of the current spec are ready.
	ConditionAvailable = "Available"
)

// IntegrationStatus is what the operator last observed.
type IntegrationStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Spec is the runtime spec the workers run (the last valid one).
	// +optional
	Spec *RunningSpec `json:"spec,omitempty"`
	// Replicas and ReadyReplicas count the workers of the current spec.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// WebhookPaths are the paths the Ingress routes.
	// +optional
	WebhookPaths []string `json:"webhookPaths,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// RunningSpec identifies a runtime spec.
type RunningSpec struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
	Level  string `json:"level"`
	// SignedBy is the trusted key that signed it, if the operator checks
	// signatures.
	// +optional
	SignedBy  string   `json:"signedBy,omitempty"`
	Workflows []string `json:"workflows"`
}

// Integration runs one compiled runtime spec on a set of workers.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tint
// +kubebuilder:printcolumn:name="Spec",type=string,JSONPath=`.status.spec.name`
// +kubebuilder:printcolumn:name="Level",type=string,JSONPath=`.status.spec.level`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Valid",type=string,JSONPath=`.status.conditions[?(@.type=="SpecValid")].status`
// +kubebuilder:printcolumn:name="Available",type=string,JSONPath=`.status.conditions[?(@.type=="Available")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Integration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IntegrationSpec   `json:"spec,omitempty"`
	Status IntegrationStatus `json:"status,omitempty"`
}

// IntegrationList is a list of Integrations.
//
// +kubebuilder:object:root=true
type IntegrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Integration `json:"items"`
}
