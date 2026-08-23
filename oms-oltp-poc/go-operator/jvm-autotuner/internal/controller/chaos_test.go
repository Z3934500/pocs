// chaos_test.go — fault-injection unit tests for JvmAutoTuner controller.
// Covers failure modes not exercised by jvmautotuner_controller_test.go.
// Shared helpers (newScheme, newPromStub, makeDeployment, makeTuner,
// getJavaOpts, heapResponse, heap90pct) are defined there — same package.
package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	jvmv1alpha1 "github.com/OWNER/jvm-autotuner/api/v1alpha1"
)

// ── F-01: Prometheus HTTP timeout ──────────────────────────────────────────
// A hung connection outlasts HTTPClient.Timeout.
// Expected: no error propagated, RequeueAfter=30s, JAVA_OPTS unchanged.
func TestPrometheusTimeout_NoOpAndRequeue(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second) // outlasts the 200 ms client timeout
	}))
	defer hung.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, hung.URL, 512, 4096, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	res, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 200 * time.Millisecond},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("RequeueAfter = %v, want 30s", res.RequeueAfter)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q, want unchanged -Xmx1024m", got)
	}
}

// ── F-02: Prometheus returns HTTP 200 but malformed JSON body ──────────────
// Expected: unmarshal error → no patch, RequeueAfter=30s.
func TestPrometheusMalformedJSON_NoOpAndRequeue(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	prom := newPromStub(200, `this is not json {{{`)
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	res, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("RequeueAfter = %v, want 30s", res.RequeueAfter)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q, want unchanged", got)
	}
}

// ── F-03: Prometheus returns "NaN" as scalar value ─────────────────────────
// strconv.ParseFloat("NaN") succeeds. IEEE 754: NaN > X and NaN < X are both
// false → neither scale branch fires → documented no-op (no patch).
// Production fix: add math.IsNaN guard before int32 cast.
func TestPrometheusNaN_DocumentedNoOp(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	prom := newPromStub(200, `{"status":"success","data":{"result":[{"value":["0","NaN"]}]}}`)
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	_, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err != nil {
		t.Fatalf("unexpected error on NaN heap value: %v", err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q; NaN comparisons are false → must be no-op", got)
	}
}

// ── F-04: Target Deployment deleted after CR was created ───────────────────
// Expected: Reconcile returns non-nil error (controller-runtime retries).
// Must not panic or silently swallow the error.
func TestDeploymentNotFound_ReturnsError(t *testing.T) {
	const ns, tunerName = "default", "tuner"

	scheme := newScheme(t)
	tuner := makeTuner(ns, tunerName, "missing-deploy", "app", "http://unused", 512, 4096, 256, 80, 40)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(tuner).WithStatusSubresource(tuner).Build()

	_, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err == nil {
		t.Fatal("expected non-nil error for missing Deployment, got nil")
	}
}

// ── F-05: spec.containerName does not match any container ──────────────────
// Expected: Reconcile returns non-nil error ("container X not found").
func TestContainerNameMismatch_ReturnsError(t *testing.T) {
	const ns, tunerName, deployName = "default", "tuner", "svc"

	scheme := newScheme(t)
	deploy := makeDeployment(ns, deployName, "real-container", "-Xmx1024m")
	tuner := makeTuner(ns, tunerName, deployName, "wrong-container", "http://unused", 512, 4096, 256, 80, 40)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(deploy, tuner).WithStatusSubresource(tuner).Build()

	_, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err == nil {
		t.Fatal("expected error for wrong containerName, got nil")
	}
}

// ── F-06: MinHeapMB == MaxHeapMB (zero-width tuning band) ──────────────────
// When min == max, Xmx is pinned; neither scale branch can fire.
func TestMinMaxHeapEqual_AlwaysNoOp(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	prom := newPromStub(200, heapResponse(heap90pct)) // 90% would normally scale-up
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 1024, 1024, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q; min==max must prevent all scale actions", got)
	}
}

// ── F-07: Heap exactly at ScaleUpThreshold (strict > boundary) ─────────────
// Controller uses strictly-greater (>), so heap == threshold is a no-op.
func TestHeapExactlyAtScaleUpThreshold_NoOp(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	exactlyAt80pct := 1024.0 * 1024 * 1024 * 0.80 // == scaleUpThreshold=80
	prom := newPromStub(200, heapResponse(exactlyAt80pct))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q; heap at exact scaleUpThreshold must be no-op", got)
	}
}

// ── F-08: Heap exactly at ScaleDownThreshold (strict < boundary) ───────────
func TestHeapExactlyAtScaleDownThreshold_NoOp(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	exactlyAt40pct := 1024.0 * 1024 * 1024 * 0.40 // == scaleDownThreshold=40
	prom := newPromStub(200, heapResponse(exactlyAt40pct))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q; heap at exact scaleDownThreshold must be no-op", got)
	}
}

// ── F-09: Deployment patch returns 409 Conflict ────────────────────────────
// A concurrent write causes resourceVersion mismatch.
// Expected: Reconcile returns non-nil Conflict error so controller-runtime
// applies exponential backoff. The error must NOT be swallowed.
func TestPatchConflict_ReturnsError(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	prom := newPromStub(200, heapResponse(heap90pct)) // triggers scale-up → patch
	defer prom.Close()

	scheme := newScheme(t)
	deploy := makeDeployment(ns, deployName, ctr, "-Xmx1024m")
	tuner := makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)
	inner := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(deploy, tuner).WithStatusSubresource(tuner).Build()

	_, err := (&JvmAutoTunerReconciler{
		Client:     &conflictOnPatchClient{Client: inner},
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err == nil {
		t.Fatal("expected error from 409 Conflict patch, got nil")
	}
	if !apierrors.IsConflict(err) {
		t.Errorf("expected Conflict error, got: %v", err)
	}
}

// conflictOnPatchClient injects a 409 Conflict on every Patch call.
type conflictOnPatchClient struct{ client.Client }

func (c *conflictOnPatchClient) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return apierrors.NewConflict(
		schema.GroupResource{Group: "apps", Resource: "deployments"},
		obj.GetName(), fmt.Errorf("injected conflict"),
	)
}

// ── F-10: Prometheus returns "+Inf" as scalar value ────────────────────────
// IEEE 754: +Inf > 80 is true → scale-up branch fires.
// int32(+Inf) in Go is undefined; on amd64 it produces math.MinInt32.
// This test verifies the controller does NOT panic on +Inf input.
// Production fix: add math.IsInf/IsNaN guard in queryHeapUsagePct.
func TestPrometheusInf_NoPanic(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "svc", "app"

	prom := newPromStub(200, `{"status":"success","data":{"result":[{"value":["0","+Inf"]}]}}`)
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40),
		).WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("controller panicked on +Inf heap value: %v", r)
		}
	}()
	_, _ = (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	t.Log("DOCUMENTED GAP: +Inf heap triggers scale-up branch; add math.IsInf guard in production")
}

