//go:build e2e

// Package e2e verifies the JVM auto-tuner against a real Kubernetes API server.
// Run it only against an isolated Kind cluster:
//   $env:KUBECONFIG = "$(kind get kubeconfig --name oms-e2e)"
//   $env:E2E_KIND = "1"
//   go test -tags=e2e ./e2e -v -timeout 3m
package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	stdRuntime "runtime"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	jvmv1alpha1 "github.com/OWNER/jvm-autotuner/api/v1alpha1"
	"github.com/OWNER/jvm-autotuner/internal/controller"
)

const (
	e2eNamespace = "jvm-autotuner-e2e"
	deploymentName = "order-service"
	tunerName = "order-service-tuner"
)

func TestKindMetricToRollout(t *testing.T) {
	if os.Getenv("E2E_KIND") != "1" {
		t.Skip("set E2E_KIND=1 to run this test against an isolated Kind cluster")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	config, err := ctrl.GetConfig()
	if err != nil {
		t.Fatalf("load KUBECONFIG: %v", err)
	}

	scheme := k8sruntime.NewScheme()
	mustAddToScheme(t, scheme)
	clusterClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create Kubernetes client: %v", err)
	}

	installCRD(t, ctx, clusterClient)
	createNamespace(t, ctx, clusterClient)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := clusterClient.Delete(cleanupCtx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}); err != nil && !apierrors.IsNotFound(err) {
			t.Logf("cleanup namespace: %v", err)
		}
	})

	metrics := newPrometheusPOC()
	defer metrics.Close()

	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		t.Fatalf("create controller manager: %v", err)
	}
	if err := (&controller.JvmAutoTunerReconciler{
		Client:     manager.GetClient(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
	}).SetupWithManager(manager); err != nil {
		t.Fatalf("register reconciler: %v", err)
	}
	managerCtx, stopManager := context.WithCancel(ctx)
	managerDone := make(chan error, 1)
	go func() {
		managerDone <- manager.Start(managerCtx)
	}()
	defer func() {
		stopManager()
		if err := <-managerDone; err != nil {
			t.Logf("controller manager exited: %v", err)
		}
	}()

	deploy := testDeployment()
	if err := clusterClient.Create(ctx, deploy); err != nil {
		t.Fatalf("create Deployment: %v", err)
	}
	originalGeneration := deploy.Generation

	tuner := testTuner(metrics.URL)
	if err := clusterClient.Create(ctx, tuner); err != nil {
		t.Fatalf("create JvmAutoTuner: %v", err)
	}

	updated := &appsv1.Deployment{}
	waitFor(t, ctx, "controller patch and Deployment rollout", func() (bool, error) {
		if err := clusterClient.Get(ctx, types.NamespacedName{Namespace: e2eNamespace, Name: deploymentName}, updated); err != nil {
			return false, err
		}
		return updated.Generation > originalGeneration &&
			updated.Status.ObservedGeneration == updated.Generation &&
			updated.Status.UpdatedReplicas == 1 &&
			updated.Status.AvailableReplicas == 1, nil
	})

	javaOpts := updated.Spec.Template.Spec.Containers[0].Env[0].Value
	if javaOpts != "-Xmx1280m" {
		t.Fatalf("JAVA_OPTS = %q, want -Xmx1280m", javaOpts)
	}

	observed := &jvmv1alpha1.JvmAutoTuner{}
	waitFor(t, ctx, "controller status update", func() (bool, error) {
		if err := clusterClient.Get(ctx, types.NamespacedName{Namespace: e2eNamespace, Name: tunerName}, observed); err != nil {
			return false, err
		}
		return observed.Status.CurrentXmxMB == 1280 && observed.Status.HeapUsagePct == 90, nil
	})
}

func mustAddToScheme(t *testing.T, scheme *k8sruntime.Scheme) {
	t.Helper()
	for _, add := range []func(*k8sruntime.Scheme) error{clientgoscheme.AddToScheme, appsv1.AddToScheme, jvmv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("add scheme: %v", err)
		}
	}
}

func installCRD(t *testing.T, ctx context.Context, c client.Client) {
	t.Helper()
	_, thisFile, _, ok := stdRuntime.Caller(0)
	if !ok {
		t.Fatal("resolve E2E test directory")
	}
	crdFile := filepath.Join(filepath.Dir(thisFile), "..", "config", "crd", "jvmautotuner.yaml")
	contents, err := os.ReadFile(crdFile)
	if err != nil {
		t.Fatalf("read CRD %s: %v", crdFile, err)
	}

	var crd unstructured.Unstructured
	if err := utilyaml.Unmarshal(contents, &crd); err != nil {
		t.Fatalf("decode CRD: %v", err)
	}
	if err := c.Create(ctx, &crd); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("install CRD: %v", err)
	}
	waitFor(t, ctx, "CRD establishment", func() (bool, error) {
		current := &unstructured.Unstructured{}
		current.SetAPIVersion("apiextensions.k8s.io/v1")
		current.SetKind("CustomResourceDefinition")
		if err := c.Get(ctx, types.NamespacedName{Name: "jvmautotuners.jvm.oms.io"}, current); err != nil {
			return false, err
		}
		conditions, _, err := unstructured.NestedSlice(current.Object, "status", "conditions")
		if err != nil {
			return false, err
		}
		for _, condition := range conditions {
			status, _, _ := unstructured.NestedString(condition.(map[string]interface{}), "status")
			typeName, _, _ := unstructured.NestedString(condition.(map[string]interface{}), "type")
			if typeName == "Established" && status == "True" {
				return true, nil
			}
		}
		return false, nil
	})
}

func createNamespace(t *testing.T, ctx context.Context, c client.Client) {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}
	if err := c.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace: %v", err)
	}
}

func testDeployment() *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: e2eNamespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": deploymentName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": deploymentName}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "order-service",
					Image: "registry.k8s.io/pause:3.9",
					Env:   []corev1.EnvVar{{Name: "JAVA_OPTS", Value: "-Xmx1024m"}},
				}}},
			},
		},
	}
}

func testTuner(prometheusURL string) *jvmv1alpha1.JvmAutoTuner {
	return &jvmv1alpha1.JvmAutoTuner{
		ObjectMeta: metav1.ObjectMeta{Name: tunerName, Namespace: e2eNamespace},
		Spec: jvmv1alpha1.JvmAutoTunerSpec{
			TargetDeployment:   deploymentName,
			ContainerName:      "order-service",
			PrometheusURL:      prometheusURL,
			HeapQuery:          "jvm_heap_used_bytes",
			ScaleUpThreshold:   80,
			ScaleDownThreshold: 40,
			MinHeapMB:          512,
			MaxHeapMB:          4096,
			StepMB:             256,
		},
	}
}

func newPrometheusPOC() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// 90% of the Deployment's initial 1024 MiB Xmx forces one scale-up step.
		_, _ = fmt.Fprint(w, `{"status":"success","data":{"result":[{"value":["0","966367641.6"]}]}}`)
	}))
}

func waitFor(t *testing.T, ctx context.Context, description string, condition func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := condition()
		if err == nil && ready {
			return
		}
		select {
		case <-ctx.Done():
			if err != nil {
				t.Fatalf("wait for %s: %v", description, err)
			}
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}
