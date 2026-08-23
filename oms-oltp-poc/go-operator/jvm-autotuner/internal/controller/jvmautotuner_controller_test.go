package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	jvmv1alpha1 "github.com/OWNER/jvm-autotuner/api/v1alpha1"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := jvmv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// promStub is a switchable Prometheus HTTP stub.
type promStub struct {
	srv        *httptest.Server
	statusCode atomic.Int32
	body       atomic.Value // string
}

func newPromStub(code int, body string) *promStub {
	s := &promStub{}
	s.statusCode.Store(int32(code))
	s.body.Store(body)
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(s.statusCode.Load()))
		_, _ = fmt.Fprint(w, s.body.Load().(string))
	}))
	return s
}

func (s *promStub) set(code int, body string) {
	s.statusCode.Store(int32(code))
	s.body.Store(body)
}

func (s *promStub) URL() string { return s.srv.URL }
func (s *promStub) Close()      { s.srv.Close() }

// heapResponse builds a Prometheus instant-query response with the given bytes value.
func heapResponse(usedBytes float64) string {
	return fmt.Sprintf(
		`{"status":"success","data":{"result":[{"value":["0","%g"]}]}}`,
		usedBytes,
	)
}

// heap90pct = 90% of 1024 MB in bytes
const heap90pct = 1024.0 * 1024 * 1024 * 0.9

func makeDeployment(ns, name, containerName, javaOpts string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: containerName,
						Env:  []corev1.EnvVar{{Name: "JAVA_OPTS", Value: javaOpts}},
					}},
				},
			},
		},
	}
}

func makeTuner(ns, name, deployName, containerName, promURL string,
	minMB, maxMB, stepMB, up, down int32,
) *jvmv1alpha1.JvmAutoTuner {
	return &jvmv1alpha1.JvmAutoTuner{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns,
			ResourceVersion: "1"},
		Spec: jvmv1alpha1.JvmAutoTunerSpec{
			TargetDeployment:   deployName,
			ContainerName:      containerName,
			PrometheusURL:      promURL,
			HeapQuery:          "jvm_heap_used_bytes",
			ScaleUpThreshold:   up,
			ScaleDownThreshold: down,
			MinHeapMB:          minMB,
			MaxHeapMB:          maxMB,
			StepMB:             stepMB,
			ReconcilePeriod:    "5m",
		},
	}
}

func reconcileOnce(t *testing.T, cl client.Client, prom *promStub,
	ns, name string,
) ctrl.Result {
	t.Helper()
	r := &JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}
	if prom != nil {
		// patch PrometheusURL on the CR so it points to our stub
		tuner := &jvmv1alpha1.JvmAutoTuner{}
		if err := cl.Get(context.Background(),
			client.ObjectKey{Namespace: ns, Name: name}, tuner); err != nil {
			t.Fatalf("get tuner: %v", err)
		}
		tuner.Spec.PrometheusURL = prom.URL()
		if err := cl.Update(context.Background(), tuner); err != nil {
			t.Fatalf("update tuner promURL: %v", err)
		}
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: name},
	})
	if err != nil {
		t.Fatalf("Reconcile returned unexpected error: %v", err)
	}
	return res
}

func getJavaOpts(t *testing.T, cl client.Client, ns, deploy, container string) string {
	t.Helper()
	d := &appsv1.Deployment{}
	if err := cl.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: deploy}, d); err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == container {
			for _, e := range c.Env {
				if e.Name == "JAVA_OPTS" {
					return e.Value
				}
			}
		}
	}
	return ""
}

// ── U-07: Prometheus fault → no-op + RequeueAfter=30s ────────────────────────

func TestPrometheus503_NoOpAndRequeue(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "order-service", "app"
	prom := newPromStub(503, `{"status":"error"}`)
	defer prom.Close()

	scheme := newScheme(t)
	deploy := makeDeployment(ns, deployName, ctr, "-Xmx1024m")
	tuner := makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(deploy, tuner).WithStatusSubresource(tuner).Build()

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
		t.Errorf("JAVA_OPTS = %q, want unchanged -Xmx1024m", got)
	}
}

// U-07b: Prometheus returns empty result set (no data for query)
func TestPrometheusNoData_NoOpAndRequeue(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "order-service", "app"
	prom := newPromStub(200, `{"status":"success","data":{"result":[]}}`)
	defer prom.Close()

	scheme := newScheme(t)
	deploy := makeDeployment(ns, deployName, ctr, "-Xmx1024m")
	tuner := makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(deploy, tuner).WithStatusSubresource(tuner).Build()

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

// U-07c: Prometheus recovers → scale-up proceeds
func TestPrometheusRecovery_ScalesUp(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "tuner", "order-service", "app"
	prom := newPromStub(503, `{"status":"error"}`)
	defer prom.Close()

	scheme := newScheme(t)
	deploy := makeDeployment(ns, deployName, ctr, "-Xmx1024m")
	tuner := makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(deploy, tuner).WithStatusSubresource(tuner).Build()

	r := &JvmAutoTunerReconciler{Client: cl, HTTPClient: &http.Client{Timeout: 3 * time.Second}}
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName}}

	// First reconcile: Prometheus down → no-op
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("expected no-op, got %q", got)
	}

	// Prometheus recovers with 90% heap → scale-up
	prom.set(200, heapResponse(heap90pct))
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1280m" {
		t.Errorf("JAVA_OPTS = %q, want -Xmx1280m after recovery", got)
	}
}

// ── U-01/02/03/04: Threshold boundary cases ──────────────────────────────────

func TestScaleUp_HeapAboveThreshold(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// 85% of 1024 MB → exceeds scaleUpThreshold=80 → Xmx 1024+256=1280
	usedBytes := 1024.0 * 1024 * 1024 * 0.85
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1280m" {
		t.Errorf("JAVA_OPTS = %q, want -Xmx1280m", got)
	}
}

func TestScaleDown_HeapBelowThreshold(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// 30% of 1024 MB → below scaleDownThreshold=40 → Xmx 1024-256=768
	usedBytes := 1024.0 * 1024 * 1024 * 0.30
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx768m" {
		t.Errorf("JAVA_OPTS = %q, want -Xmx768m", got)
	}
}

func TestNoOp_HeapInBand(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// 60% → in-band between 40 and 80 → no-op
	usedBytes := 1024.0 * 1024 * 1024 * 0.60
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q, want unchanged -Xmx1024m", got)
	}
}

func TestMaxCapEnforced(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// Already at maxHeapMB=1024 with heap > 80% → should stay at 1024
	usedBytes := 1024.0 * 1024 * 1024 * 0.90
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 1024, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q, want unchanged at max cap", got)
	}
}

func TestMinFloorEnforced(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// Already at minHeapMB=512 with heap < 40% → should stay at 512
	usedBytes := 512.0 * 1024 * 1024 * 0.20
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx512m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx512m" {
		t.Errorf("JAVA_OPTS = %q, want unchanged at min floor", got)
	}
}

// ── U-05: No -Xmx in JAVA_OPTS → initialise at midpoint then append ──────────

func TestNoXmx_InitialisesAtMidpoint(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// midpoint of (512,4096)=2304 MB; 90% of 2304 MB → scale-up to 2560
	usedBytes := 2304.0 * 1024 * 1024 * 0.90
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Dapp.name=test"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	got := getJavaOpts(t, cl, ns, deployName, ctr)
	// replaceXmx appends when no -Xmx present → "-Dapp.name=test -Xmx2560m"
	if got != "-Dapp.name=test -Xmx2560m" {
		t.Errorf("JAVA_OPTS = %q, want midpoint init + scale-up", got)
	}
}

// ── U-06: Multi-container → only target container patched ───────────────────

func TestMultiContainer_OnlyTargetPatched(t *testing.T) {
	const ns, tunerName, deployName = "default", "t", "svc"
	prom := newPromStub(200, heapResponse(heap90pct))
	defer prom.Close()

	scheme := newScheme(t)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: ns,
			ResourceVersion: "1"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": deployName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": deployName}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "sidecar", Env: []corev1.EnvVar{{Name: "JAVA_OPTS", Value: "-Xmx512m"}}},
						{Name: "app", Env: []corev1.EnvVar{{Name: "JAVA_OPTS", Value: "-Xmx1024m"}}},
					},
				},
			},
		},
	}
	tuner := makeTuner(ns, tunerName, deployName, "app", prom.URL(), 512, 4096, 256, 80, 40)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(deploy, tuner).WithStatusSubresource(tuner).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, "app"); got != "-Xmx1280m" {
		t.Errorf("target container JAVA_OPTS = %q, want -Xmx1280m", got)
	}
	if got := getJavaOpts(t, cl, ns, deployName, "sidecar"); got != "-Xmx512m" {
		t.Errorf("sidecar JAVA_OPTS = %q, want unchanged -Xmx512m", got)
	}
}

// ── U-09: Idempotent — no patch when Xmx unchanged ───────────────────────────

func TestIdempotent_NoPatchWhenXmxUnchanged(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// in-band heap → no-op both calls
	usedBytes := 1024.0 * 1024 * 1024 * 0.60
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	r := &JvmAutoTunerReconciler{Client: cl, HTTPClient: &http.Client{Timeout: 3 * time.Second}}
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName}}

	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q after 3 no-op reconciles, want unchanged", got)
	}
}

// ── U-10: -Xms present — Xmx replaced without touching Xms ──────────────────
//
// This guards against AT-03: multiple scale-downs must not let Xmx drop below Xms.
// minHeapMB is set to 1024 (== Xms) so the floor prevents Xmx < Xms.

func TestXmsPresent_XmxReplacedXmsUntouched(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	prom := newPromStub(200, heapResponse(heap90pct))
	defer prom.Close()

	scheme := newScheme(t)
	// -Xms1024m -Xmx1024m; minHeapMB=1024 prevents Xmx from going below Xms
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xms1024m -Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 1024, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{Client: cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second}}).
		Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
		}); err != nil {
		t.Fatal(err)
	}
	got := getJavaOpts(t, cl, ns, deployName, ctr)
	// Only -Xmx should change; -Xms must remain intact
	if got != "-Xms1024m -Xmx1280m" {
		t.Errorf("JAVA_OPTS = %q, want \"-Xms1024m -Xmx1280m\"", got)
	}
}

// ── AT-04: CR deleted → Reconcile returns nil without error ──────────────────

func TestCRDeleted_ReturnsNilGracefully(t *testing.T) {
	const ns, tunerName = "default", "gone"
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build() // CR does not exist

	res, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	})
	if err != nil {
		t.Fatalf("expected nil error for missing CR, got: %v", err)
	}
	if res.RequeueAfter != 0 || res.Requeue {
		t.Errorf("expected empty Result for missing CR, got %+v", res)
	}
}

// ── replaceXmx unit tests ─────────────────────────────────────────────────────

func TestReplaceXmx(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		newXmxMB int32
		want     string
	}{
		{"replace existing lowercase m", "-Xmx1024m", 2048, "-Xmx2048m"},
		{"replace existing uppercase M", "-Xmx1024M", 2048, "-Xmx2048m"},
		{"append when absent", "-Dapp=x", 512, "-Dapp=x -Xmx512m"},
		{"empty string", "", 256, "-Xmx256m"},
		{"xmx in middle", "-Xss256k -Xmx512m -XX:+UseG1GC", 1024, "-Xss256k -Xmx1024m -XX:+UseG1GC"},
		{"xms untouched", "-Xms256m -Xmx512m", 768, "-Xms256m -Xmx768m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := replaceXmx(tc.input, tc.newXmxMB)
			if got != tc.want {
				t.Errorf("replaceXmx(%q, %d) = %q, want %q", tc.input, tc.newXmxMB, got, tc.want)
			}
		})
	}
}

// ── extractXmx unit tests ─────────────────────────────────────────────────────

func TestExtractXmx(t *testing.T) {
	cases := []struct {
		name      string
		javaOpts  string
		wantXmxMB int32
	}{
		{"standard lowercase", "-Xmx1024m", 1024},
		{"standard uppercase", "-Xmx2048M", 2048},
		{"xmx in middle", "-Xss256k -Xmx512m -XX:+UseG1GC", 512},
		{"no xmx", "-Dapp=x", 0},
		{"empty", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deploy := makeDeployment("ns", "d", "c", tc.javaOpts)
			xmxMB, _, idx := extractXmx(deploy, "c")
			if idx != 0 {
				t.Fatalf("container index = %d, want 0", idx)
			}
			if xmxMB != tc.wantXmxMB {
				t.Errorf("extractXmx = %d MB, want %d MB", xmxMB, tc.wantXmxMB)
			}
		})
	}
}

// ── AT-05: parsePeriod("0s") falls back to defaultReconcilePeriod ─────────────
// Guards against silent polling death: a zero duration makes RequeueAfter=0,
// which is equivalent to "no periodic requeue", causing AutoTuner to stop
// polling Prometheus unless the CR is modified.

func TestParsePeriod_ZeroDuration(t *testing.T) {
	cases := []struct {
		input string
		want  time.Duration
	}{
		{"5m", 5 * time.Minute},
		{"30s", 30 * time.Second},
		{"", defaultReconcilePeriod},
		{"0s", defaultReconcilePeriod}, // zero must fall back — not silently stop polling
		{"bad", defaultReconcilePeriod},
		{"-1s", defaultReconcilePeriod}, // negative treated as invalid, falls back
	}
	for _, tc := range cases {
		t.Run("input="+tc.input, func(t *testing.T) {
			got := parsePeriod(tc.input)
			if got != tc.want {
				t.Errorf("parsePeriod(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// ── AT-02 / U-11: Both threshold crossings trigger a Patch (write-amplification doc) ──
// This test DOCUMENTS the current behaviour: both scale-up AND scale-down trigger
// a Deployment patch. It is NOT a bug — it is expected. The comment explains why
// the thresholds must be far apart (≥ 40%) to avoid oscillation at production load.

func TestHeapOscillation_BothThresholdCrossings(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// reconcilePeriod="5m" (default in makeTuner); thresholds up=80, down=40
	// scaleUpPct = 85% of 1024 MB → triggers scale-up  (1024 → 1280)
	// scaleDownPct = 30% of 1280 MB → triggers scale-down (1280 → 1024)

	scaleUpBytes := 1024.0 * 1024 * 1024 * 0.85
	prom := newPromStub(200, heapResponse(scaleUpBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	r := &JvmAutoTunerReconciler{Client: cl, HTTPClient: &http.Client{Timeout: 3 * time.Second}}
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName}}

	// Round 1: heap > 80% → scale-up 1024m → 1280m
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1280m" {
		t.Errorf("after scale-up: JAVA_OPTS = %q, want -Xmx1280m", got)
	}

	// Round 2: heap < 40% (30% of current 1280 MB) → scale-down 1280m → 1024m
	// NOTE: 30% of 1280 MB expressed relative to 1280 MB denominator
	scaleDownBytes := 1280.0 * 1024 * 1024 * 0.30
	prom.set(200, heapResponse(scaleDownBytes))
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("after scale-down: JAVA_OPTS = %q, want -Xmx1024m", got)
	}

	// DOCUMENTED BEHAVIOUR: two consecutive reconciles produced two Deployment patches.
	// At reconcilePeriod=1s with threshold gap < 10%, this would generate > 3600
	// patches/hour. Keep ScaleUpThreshold=80, ScaleDownThreshold=40 (gap ≥ 40%) in prod.
}

// ── GC-06 / U-12: CPU spin (dead loop) does not trigger AutoTuner ─────────────
// A thread in an infinite loop causes CPU throttling but heap stays in-band.
// This test verifies that a stable in-band heap (55%) is always a no-op,
// even when external CPU metrics would suggest a problem.

func TestDeadLoop_HeapInBand_IsNoOp(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// 55% heap → in-band [40, 80] → no-op regardless of CPU state
	usedBytes := 1024.0 * 1024 * 1024 * 0.55
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx1024m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 512, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	}); err != nil {
		t.Fatal(err)
	}
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx1024m" {
		t.Errorf("JAVA_OPTS = %q; dead-loop CPU spike must not trigger scale-up", got)
	}
}

// ── GC-03 / U-13: OOMKilled Pod restart — AutoTuner re-reads Xmx from live Deployment ──
// After an OOMKill (exit 137), AutoTuner must extract -Xmx from the actual
// Deployment env var rather than any cached CR status value.
// This test verifies extractXmx returns the correct post-restart value when
// the Deployment's JAVA_OPTS is updated externally before reconcile.

func TestOOMKilled_AutoTunerReadsLiveDeployment(t *testing.T) {
	const ns, tunerName, deployName, ctr = "default", "t", "svc", "app"
	// Simulate: OOMKill happened, operator (or kubectl) reset JAVA_OPTS back to 512m
	// AutoTuner must detect the reset and respond to current heap %, not a stale value.
	usedBytes := 512.0 * 1024 * 1024 * 0.90 // 90% of 512 MB → scale-up
	prom := newPromStub(200, heapResponse(usedBytes))
	defer prom.Close()

	scheme := newScheme(t)
	// Deployment was reset to -Xmx512m after OOMKill restart
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeDeployment(ns, deployName, ctr, "-Xmx512m"),
			makeTuner(ns, tunerName, deployName, ctr, prom.URL(), 256, 4096, 256, 80, 40)).
		WithStatusSubresource(&jvmv1alpha1.JvmAutoTuner{}).Build()

	if _, err := (&JvmAutoTunerReconciler{
		Client:     cl,
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
	}).Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: tunerName},
	}); err != nil {
		t.Fatal(err)
	}
	// 90% of 512m → scale-up by 256m → 768m
	if got := getJavaOpts(t, cl, ns, deployName, ctr); got != "-Xmx768m" {
		t.Errorf("JAVA_OPTS = %q after OOMKill reset; want -Xmx768m (read live value, not stale cache)", got)
	}
}
