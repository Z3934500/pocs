# inject-network-partition.ps1 — CH-09: iptables DROP on Prometheus port inside Kind node
param(
    [string]$Namespace = "jvm-autotuner-e2e",
    [string]$PromPort = "9090",
    [string]$DeployName = "order-service"
)

Write-Host "=== CH-09: Network Partition (Prometheus port DROP) ===" -ForegroundColor Cyan

$before = kubectl get deploy $DeployName -n $Namespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
Write-Host "[BEFORE] JAVA_OPTS = $before"

# Find the Kind node container name
$node = kubectl get nodes -o jsonpath='{.items[0].metadata.name}'
Write-Host "[INFO] Kind node: $node"

# 1. Inject: drop outbound traffic to Prometheus port
Write-Host "`n[INJECT] Adding iptables DROP rule for port $PromPort on node $node ..."
docker exec $node iptables -A OUTPUT -p tcp --dport $PromPort -j DROP
Write-Host "         Done. Controller HTTP calls to Prometheus will time out."

Start-Sleep 35

# 2. Observe
Write-Host "`n[OBSERVE] Controller logs (last 10 lines):"
kubectl logs -n $Namespace deploy/jvm-autotuner-controller --tail=10
# Expected: "prometheus GET: ... connection refused" or "context deadline exceeded"

$after = kubectl get deploy $DeployName -n $Namespace `
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}' 2>$null
if ($before -eq $after) {
    Write-Host "[PASS] JAVA_OPTS unchanged during network partition." -ForegroundColor Green
} else {
    Write-Host "[FAIL] JAVA_OPTS was modified during partition!" -ForegroundColor Red
}

# 3. Recover
Write-Host "`n[RECOVER] Removing iptables rule ..."
docker exec $node iptables -D OUTPUT -p tcp --dport $PromPort -j DROP
Write-Host "[RECOVER] Network restored."
