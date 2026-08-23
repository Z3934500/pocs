package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	jvmv1alpha1 "github.com/OWNER/jvm-autotuner/api/v1alpha1"
)

const (
	defaultReconcilePeriod = 5 * time.Minute
	javaOptsEnvVar         = "JAVA_OPTS"
	xmxRegexp              = `-Xmx(\d+)[mM]`
)

// JvmAutoTunerReconciler reconciles JvmAutoTuner objects.
//
// Design: each Reconcile call is a single observe-decide-act loop.
//   1. Fetch the JvmAutoTuner CR and the target Deployment.
//   2. Read the current -Xmx value from the Deployment's JAVA_OPTS env var.
//   3. Query Prometheus for the current heap usage percentage.
//   4. If usage > ScaleUpThreshold → increase -Xmx by StepMB (capped at MaxHeapMB).
//      If usage < ScaleDownThreshold → decrease -Xmx by StepMB (floored at MinHeapMB).
//   5. Strategic-merge-patch the Deployment if -Xmx changed.
//   6. Update the CR status; requeue after ReconcilePeriod.
type JvmAutoTunerReconciler struct {
	client.Client
	HTTPClient *http.Client
}

// +kubebuilder:rbac:groups=jvm.oms.io,resources=jvmautotuners,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=jvm.oms.io,resources=jvmautotuners/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=jvm.oms.io,resources=jvmautotuners/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch

func (r *JvmAutoTunerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the JvmAutoTuner CR.
	tuner := &jvmv1alpha1.JvmAutoTuner{}
	if err := r.Get(ctx, req.NamespacedName, tuner); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get JvmAutoTuner: %w", err)
	}

	// 2. Resolve the target namespace (default: same as CR).
	targetNS := tuner.Spec.TargetNamespace
	if targetNS == "" {
		targetNS = tuner.Namespace
	}

	// 3. Fetch the target Deployment.
	deploy := &appsv1.Deployment{}
	deployKey := types.NamespacedName{Name: tuner.Spec.TargetDeployment, Namespace: targetNS}
	if err := r.Get(ctx, deployKey, deploy); err != nil {
		return ctrl.Result{}, fmt.Errorf("get Deployment %s/%s: %w", targetNS, tuner.Spec.TargetDeployment, err)
	}

	// 4. Extract the current -Xmx value from JAVA_OPTS in the named container.
	currentXmxMB, javaOpts, containerIdx := extractXmx(deploy, tuner.Spec.ContainerName)
	if containerIdx < 0 {
		return ctrl.Result{}, fmt.Errorf("container %q not found in Deployment %s", tuner.Spec.ContainerName, tuner.Spec.TargetDeployment)
	}
	if currentXmxMB == 0 {
		// No -Xmx yet — start at midpoint so first reconcile isn't a hard jump.
		currentXmxMB = (tuner.Spec.MinHeapMB + tuner.Spec.MaxHeapMB) / 2
		logger.Info("No -Xmx found; initialising at midpoint", "xmxMB", currentXmxMB)
	}

	// 5. Query Prometheus for current heap usage %.
	heapPct, err := r.queryHeapUsagePct(tuner, currentXmxMB)
	if err != nil {
		logger.Error(err, "Prometheus query failed; will retry in 30s")
		// Don't propagate — Prometheus may be temporarily unavailable.
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	logger.Info("Heap usage polled", "pct", heapPct, "currentXmxMB", currentXmxMB)

	// 6. Decide: scale-up, scale-down, or no-op.
	newXmxMB := currentXmxMB
	action := "no-op"

	if int32(heapPct) > tuner.Spec.ScaleUpThreshold && currentXmxMB < tuner.Spec.MaxHeapMB {
		newXmxMB = min32(currentXmxMB+tuner.Spec.StepMB, tuner.Spec.MaxHeapMB)
		action = fmt.Sprintf("scale-up: %dMB → %dMB (heap=%d%%)", currentXmxMB, newXmxMB, int32(heapPct))
		logger.Info("Scaling up Xmx", "from", currentXmxMB, "to", newXmxMB)
	} else if int32(heapPct) < tuner.Spec.ScaleDownThreshold && currentXmxMB > tuner.Spec.MinHeapMB {
		newXmxMB = max32(currentXmxMB-tuner.Spec.StepMB, tuner.Spec.MinHeapMB)
		action = fmt.Sprintf("scale-down: %dMB → %dMB (heap=%d%%)", currentXmxMB, newXmxMB, int32(heapPct))
		logger.Info("Scaling down Xmx", "from", currentXmxMB, "to", newXmxMB)
	}

	// 7. Patch the Deployment's JAVA_OPTS only when -Xmx actually changes.
	if newXmxMB != currentXmxMB {
		newJavaOpts := replaceXmx(javaOpts, newXmxMB)
		if err := r.patchJavaOpts(ctx, deploy, containerIdx, newJavaOpts); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch JAVA_OPTS: %w", err)
		}
		logger.Info("Patched JAVA_OPTS", "value", newJavaOpts)
	}

	// 8. Update CR status (always — reflects latest heap observation).
	now := metav1.Now()
	tuner.Status.CurrentXmxMB = newXmxMB
	tuner.Status.HeapUsagePct = int32(heapPct)
	tuner.Status.LastTunedAt = &now
	tuner.Status.LastAction = action
	if err := r.Status().Update(ctx, tuner); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	// 9. Requeue after the configured period.
	return ctrl.Result{RequeueAfter: parsePeriod(tuner.Spec.ReconcilePeriod)}, nil
}

// queryHeapUsagePct returns heap usage as a percentage (0–100).
// It queries Prometheus for used bytes and divides by the committed/max heap.
func (r *JvmAutoTunerReconciler) queryHeapUsagePct(tuner *jvmv1alpha1.JvmAutoTuner, currentXmxMB int32) (float64, error) {
	usedBytes, err := r.queryPrometheus(tuner.Spec.PrometheusURL, tuner.Spec.HeapQuery)
	if err != nil {
		return 0, fmt.Errorf("query heap used: %w", err)
	}

	var maxBytes float64
	if tuner.Spec.MaxHeapQuery != "" {
		maxBytes, err = r.queryPrometheus(tuner.Spec.PrometheusURL, tuner.Spec.MaxHeapQuery)
		if err != nil || maxBytes == 0 {
			// Prometheus returned nothing — fall back to configured ceiling.
			maxBytes = float64(tuner.Spec.MaxHeapMB) * 1024 * 1024
		}
	} else {
		// Use the current -Xmx as the denominator so the % reflects actual JVM headroom.
		maxBytes = float64(currentXmxMB) * 1024 * 1024
	}

	if maxBytes == 0 {
		return 0, fmt.Errorf("max heap bytes resolved to zero")
	}
	return (usedBytes / maxBytes) * 100, nil
}

// prometheusResponse is a minimal struct for Prometheus instant-query API responses.
type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Value []json.RawMessage `json:"value"` // [unixTimestamp, "valueString"]
		} `json:"result"`
	} `json:"data"`
}

// queryPrometheus executes an instant PromQL query and returns the first scalar value.
func (r *JvmAutoTunerReconciler) queryPrometheus(baseURL, query string) (float64, error) {
	endpoint := strings.TrimRight(baseURL, "/") + "/api/v1/query?query=" + url.QueryEscape(query)
	resp, err := r.HTTPClient.Get(endpoint)
	if err != nil {
		return 0, fmt.Errorf("prometheus GET: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response body: %w", err)
	}

	var result prometheusResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("unmarshal prometheus response: %w", err)
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, fmt.Errorf("prometheus query returned no data for: %q", query)
	}

	values := result.Data.Result[0].Value
	if len(values) < 2 {
		return 0, fmt.Errorf("unexpected prometheus value shape (want [ts, val], got %d elements)", len(values))
	}
	var valueStr string
	if err := json.Unmarshal(values[1], &valueStr); err != nil {
		return 0, fmt.Errorf("unmarshal prometheus scalar value: %w", err)
	}
	return strconv.ParseFloat(valueStr, 64)
}

// extractXmx scans the named container's env vars for JAVA_OPTS and parses -Xmx<n>m.
// Returns (currentXmxMB, javaOptsValue, containerIndex).
// containerIndex is -1 when the container is not found in the Deployment spec.
func extractXmx(deploy *appsv1.Deployment, containerName string) (int32, string, int) {
	re := regexp.MustCompile(xmxRegexp)
	for i, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		for _, env := range c.Env {
			if env.Name != javaOptsEnvVar {
				continue
			}
			m := re.FindStringSubmatch(env.Value)
			if len(m) < 2 {
				// JAVA_OPTS exists but has no -Xmx yet.
				return 0, env.Value, i
			}
			mb, _ := strconv.Atoi(m[1])
			return int32(mb), env.Value, i
		}
		// Container found but JAVA_OPTS env var is absent.
		return 0, "", i
	}
	return 0, "", -1
}

// replaceXmx replaces the -Xmx flag inside a JAVA_OPTS string with -Xmx<newXmxMB>m.
// If no -Xmx flag is present, the new flag is appended.
func replaceXmx(javaOpts string, newXmxMB int32) string {
	re := regexp.MustCompile(`-Xmx\d+[mM]`)
	newFlag := fmt.Sprintf("-Xmx%dm", newXmxMB)
	if re.MatchString(javaOpts) {
		return re.ReplaceAllString(javaOpts, newFlag)
	}
	if javaOpts == "" {
		return newFlag
	}
	return strings.TrimSpace(javaOpts) + " " + newFlag
}

// patchJavaOpts issues a strategic merge patch that updates only JAVA_OPTS on the
// named container, leaving every other field of the Deployment untouched.
func (r *JvmAutoTunerReconciler) patchJavaOpts(
	ctx context.Context,
	deploy *appsv1.Deployment,
	containerIdx int,
	newJavaOpts string,
) error {
	containerName := deploy.Spec.Template.Spec.Containers[containerIdx].Name

	// Build a minimal strategic-merge-patch. The containers list is matched by
	// name so only the target container's env is touched.
	type envEntry struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	type containerEntry struct {
		Name string     `json:"name"`
		Env  []envEntry `json:"env"`
	}
	patchObj := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": []containerEntry{
						{
							Name: containerName,
							Env:  []envEntry{{Name: javaOptsEnvVar, Value: newJavaOpts}},
						},
					},
				},
			},
		},
	}

	patchBytes, err := json.Marshal(patchObj)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	return r.Patch(ctx, deploy, client.RawPatch(types.StrategicMergePatchType, patchBytes))
}

func parsePeriod(s string) time.Duration {
	if s == "" {
		return defaultReconcilePeriod
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return defaultReconcilePeriod
	}
	if d <= 0 {
		return defaultReconcilePeriod
	}
	return d
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// SetupWithManager registers the controller with the Manager.
func (r *JvmAutoTunerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&jvmv1alpha1.JvmAutoTuner{}).
		Complete(r)
}
