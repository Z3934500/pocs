package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// JvmAutoTunerSpec defines the desired state of JvmAutoTuner.
type JvmAutoTunerSpec struct {
	// TargetDeployment is the name of the Deployment whose JAVA_OPTS will be patched.
	TargetDeployment string `json:"targetDeployment"`

	// TargetNamespace is the namespace of the target Deployment.
	// Defaults to the namespace of the JvmAutoTuner CR if omitted.
	TargetNamespace string `json:"targetNamespace,omitempty"`

	// ContainerName is the container within the Deployment that carries JAVA_OPTS.
	ContainerName string `json:"containerName"`

	// PrometheusURL is the base URL of the Prometheus API (e.g. http://prometheus:9090).
	PrometheusURL string `json:"prometheusURL"`

	// HeapQuery is a PromQL expression that returns the current JVM heap used bytes
	// for the target pods (e.g. avg(jvm_memory_used_bytes{area="heap",...})).
	HeapQuery string `json:"heapQuery"`

	// MaxHeapQuery is an optional PromQL expression returning the committed/max heap
	// bytes. When omitted the controller uses the current -Xmx value as denominator.
	MaxHeapQuery string `json:"maxHeapQuery,omitempty"`

	// ScaleUpThreshold: when heap usage percentage exceeds this value, -Xmx is
	// increased by StepMB (up to MaxHeapMB). Range: 1–99.
	ScaleUpThreshold int32 `json:"scaleUpThreshold"`

	// ScaleDownThreshold: when heap usage percentage drops below this value, -Xmx
	// is decreased by StepMB (down to MinHeapMB). Range: 1–99.
	ScaleDownThreshold int32 `json:"scaleDownThreshold"`

	// MinHeapMB is the lower bound for -Xmx in megabytes.
	MinHeapMB int32 `json:"minHeapMB"`

	// MaxHeapMB is the upper bound for -Xmx in megabytes.
	MaxHeapMB int32 `json:"maxHeapMB"`

	// StepMB is how many megabytes to increase or decrease -Xmx per reconcile cycle.
	StepMB int32 `json:"stepMB"`

	// ReconcilePeriod controls how often the controller re-evaluates heap usage.
	// Accepts Go duration strings (e.g. "5m", "1m30s"). Defaults to "5m".
	ReconcilePeriod string `json:"reconcilePeriod,omitempty"`
}

// JvmAutoTunerStatus defines the observed state of JvmAutoTuner.
type JvmAutoTunerStatus struct {
	// CurrentXmxMB is the -Xmx value (in MB) most recently written by the controller.
	CurrentXmxMB int32 `json:"currentXmxMB,omitempty"`

	// HeapUsagePct is the heap usage percentage observed during the last reconcile.
	HeapUsagePct int32 `json:"heapUsagePct,omitempty"`

	// LastTunedAt is the timestamp of the last reconcile that changed JAVA_OPTS.
	LastTunedAt *metav1.Time `json:"lastTunedAt,omitempty"`

	// LastAction is a human-readable description of what the last reconcile did
	// (e.g. "scale-up: 1024MB → 1280MB (heap=83%)") or "no-op".
	LastAction string `json:"lastAction,omitempty"`

	// Conditions follows the standard Kubernetes condition convention.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=jat
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=".spec.targetDeployment"
// +kubebuilder:printcolumn:name="HeapPct",type=integer,JSONPath=".status.heapUsagePct"
// +kubebuilder:printcolumn:name="XmxMB",type=integer,JSONPath=".status.currentXmxMB"
// +kubebuilder:printcolumn:name="LastAction",type=string,JSONPath=".status.lastAction"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// JvmAutoTuner watches Prometheus JVM heap metrics for a target Deployment and
// automatically adjusts the -Xmx flag in its JAVA_OPTS environment variable,
// keeping heap utilisation between ScaleDownThreshold and ScaleUpThreshold.
type JvmAutoTuner struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   JvmAutoTunerSpec   `json:"spec,omitempty"`
	Status JvmAutoTunerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// JvmAutoTunerList contains a list of JvmAutoTuner resources.
type JvmAutoTunerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []JvmAutoTuner `json:"items"`
}

func init() {
	SchemeBuilder.Register(&JvmAutoTuner{}, &JvmAutoTunerList{})
}
