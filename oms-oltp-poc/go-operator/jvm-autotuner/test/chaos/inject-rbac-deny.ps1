# inject-rbac-deny.ps1 — CH-06: Remove patch permission from controller ServiceAccount
param(
    [string]$Namespace = "jvm-autotuner-e2e",
    [string]$DeployName = "order-service"
)

Write-Host "=== CH-06: RBAC Forbidden (patch denied) ===" -ForegroundColor Cyan

$before = kubectl get deploy $DeployName -n $Namespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
Write-Host "[BEFORE] JAVA_OPTS = $before"

# 1. Apply read-only role (removes patch/update verbs)
Write-Host "`n[INJECT] Applying read-only RBAC role ..."
@"
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: jvm-autotuner
  namespace: $Namespace
rules:
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get","list","watch"]
- apiGroups: ["jvm.oms.io"]
  resources: ["jvmautotuners","jvmautotuners/status"]
  verbs: ["get","list","watch"]
"@ | kubectl apply -f -

# 2. Verify permission is now denied
Write-Host "`n[VERIFY] Can controller SA patch deployments?"
kubectl auth can-i patch deployments `
  --as="system:serviceaccount:${Namespace}:jvm-autotuner" `
  -n $Namespace
# Expected: no

# 3. Trigger a reconcile
Write-Host "`n[INJECT] Annotating CR to force reconcile ..."
kubectl annotate jat order-service-gc-tuner -n $Namespace `
  chaos-rbac-ts="$(Get-Date -Format o)" --overwrite

Start-Sleep 15

Write-Host "`n[OBSERVE] Controller logs (last 10 lines):"
kubectl logs -n $Namespace deploy/jvm-autotuner-controller --tail=10
# Expected: "is forbidden: User ... cannot patch resource"

$after = kubectl get deploy $DeployName -n $Namespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
if ($before -eq $after) {
    Write-Host "[PASS] JAVA_OPTS unchanged despite Forbidden error." -ForegroundColor Green
} else {
    Write-Host "[FAIL] JAVA_OPTS was modified — Forbidden should have blocked patch!" -ForegroundColor Red
}

# 4. Recover
Write-Host "`n[RECOVER] Restoring full RBAC role ..."
kubectl apply -f test/fixtures/kind/rbac.yaml
Write-Host "[RECOVER] Done."
