# inject-prometheus-down.ps1 — CH-01: Kill Prometheus pod to simulate unavailability
param(
    [string]$Namespace = "monitoring",
    [string]$PromSelector = "app=prometheus",
    [string]$TunerNamespace = "jvm-autotuner-e2e",
    [string]$DeployName = "order-service"
)

Write-Host "=== CH-01: Prometheus Unavailable ===" -ForegroundColor Cyan

# 1. Record current JAVA_OPTS baseline
$before = kubectl get deploy $DeployName -n $TunerNamespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
Write-Host "[BEFORE] JAVA_OPTS = $before"

# 2. Inject: delete Prometheus pods (will restart, simulating a gap)
Write-Host "`n[INJECT] Deleting Prometheus pods in namespace $Namespace ..."
kubectl delete pod -n $Namespace -l $PromSelector --grace-period=0
Write-Host "         Done. Prometheus is unavailable until pod restarts."

# 3. Wait for one controller reconcile cycle (default 5m; shortened here to 35s)
Write-Host "`n[WAIT] Sleeping 35s to let controller attempt one reconcile ..."
Start-Sleep 35

# 4. Observe
Write-Host "`n[OBSERVE] Controller logs (last 15 lines):"
kubectl logs -n $TunerNamespace deploy/jvm-autotuner-controller --tail=15
# Expected: "Prometheus query failed; will retry in 30s"

Write-Host "`n[OBSERVE] JvmAutoTuner status:"
kubectl get jat -n $TunerNamespace

$after = kubectl get deploy $DeployName -n $TunerNamespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
Write-Host "`n[VERIFY] JAVA_OPTS before = $before"
Write-Host "[VERIFY] JAVA_OPTS after  = $after"
if ($before -eq $after) {
    Write-Host "[PASS] JAVA_OPTS unchanged during Prometheus outage." -ForegroundColor Green
} else {
    Write-Host "[FAIL] JAVA_OPTS was modified during Prometheus outage!" -ForegroundColor Red
}

# 5. Recover: wait for Prometheus to come back
Write-Host "`n[RECOVER] Waiting for Prometheus pod to be Running again ..."
kubectl wait pod -n $Namespace -l $PromSelector --for=condition=Ready --timeout=120s
Write-Host "[RECOVER] Prometheus is back."
