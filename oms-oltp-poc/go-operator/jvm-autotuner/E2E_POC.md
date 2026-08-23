# Go Controller E2E POC

This proof of concept validates the complete JVM auto-tuner control loop against a real Kind Kubernetes cluster:

```text
Prometheus-compatible HTTP endpoint
  -> JvmAutoTuner Reconcile()
  -> Deployment JAVA_OPTS strategic merge patch
  -> Deployment generation change
  -> ReplicaSet / Pod rolling update
  -> JvmAutoTuner status update
```

The test is deliberately separated from ordinary Go tests. It requires Docker, Kind and a cluster-admin kubeconfig, and it creates a real Deployment using `registry.k8s.io/pause:3.9`. The test installs the local CRD, runs the controller manager in the Go test process, returns a deterministic 90 percent heap metric through a Prometheus-compatible endpoint, and deletes its test namespace when it finishes.

## Run

From `go-operator/jvm-autotuner`:

```powershell
kind create cluster --name oms-e2e
$env:KUBECONFIG = "$(kind get kubeconfig --name oms-e2e)"
$env:E2E_KIND = "1"
go test -tags=e2e ./e2e -v -timeout 3m
```

The expected result is a single passing test: `TestKindMetricToRollout`. It verifies that the initial `JAVA_OPTS=-Xmx1024m` becomes `-Xmx1280m`, the Deployment reaches an observed and available new generation, and the CR status records `currentXmxMB=1280` and `heapUsagePct=90`.

Remove the isolated cluster after the POC:

```powershell
kind delete cluster --name oms-e2e
```

## Test Boundaries

- `go test ./...`: local Go unit and package tests; no cluster required.
- `go test -tags=e2e ./e2e`: Kind-backed E2E proof that crosses the Kubernetes API, controller-runtime cache/watch, Prometheus HTTP client, Deployment controller and kubelet-managed Pod rollout.
- This POC uses an in-process Prometheus-compatible endpoint to make the heap value deterministic. In production, `PrometheusURL` points to the Prometheus service and the same controller query path is used.

This is an E2E POC, not a production load or chaos test. It does not claim production Prometheus, HPA, multi-replica leader election, or application-level JVM memory pressure validation.
