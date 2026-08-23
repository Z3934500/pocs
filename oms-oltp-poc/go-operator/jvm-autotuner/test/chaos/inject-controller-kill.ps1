# inject-controller-kill.ps1 — CH-08: Kill controller pod mid-reconcile
param(
    [string]$Namespace = "jvm-autotuner-e2e",
    [string]$DeployName = "order-service"
)

Write-Host "=== CH-08: Controller Pod Kill ===" -ForegroundColor Cyan

$before = kubectl get deploy $DeployName -n $Namespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
Write-Host "[BEFORE] JAVA_OPTS = $before"

Write-Host "`n[INJECT] Deleting controller pod ..."
kubectl delete pod -n $Namespace -l control-plane=controller-manager --grace-period=0

Write-Host "[WAIT] Waiting for controller to restart ..."
kubectl wait pod -n $Namespace -l control-plane=controller-manager `
  --for=condition=Ready --timeout=60s

Write-Host "`n[OBSERVE] Controller logs after restart (last 10 lines):"
kubectl logs -n $Namespace deploy/jvm-autotuner-controller --tail=10

$after = kubectl get deploy $DeployName -n $Namespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
$crXmx = kubectl get jat order-service-gc-tuner -n $Namespace `
  -o jsonpath='{.status.currentXmxMB}' 2>$null

Write-Host "`n[VERIFY] JAVA_OPTS after restart: $after"
Write-Host "[VERIFY] CR status.currentXmxMB : $crXmx"
Write-Host "[PASS criteria] Controller resumes reconcile; CR status matches Deployment JAVA_OPTS." -ForegroundColor Cyan
