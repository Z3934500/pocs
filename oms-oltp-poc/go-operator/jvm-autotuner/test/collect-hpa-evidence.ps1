param(
    [Parameter(Mandatory=$true)][string]$PrometheusUrl,
    [string]$Namespace = "oms-prod",
    [string]$Deployment = "order-service",
    [string]$Hpa = "order-service",
    [string]$LagMetric = "kafka_consumergroup_lag",
    [string]$Output = "test/artifacts/hpa-evidence"
)

$ErrorActionPreference = "Stop"
foreach ($tool in @("kubectl", "curl")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool is required" }
}

$root = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root $Output
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"

kubectl get hpa $Hpa -n $Namespace -o yaml | Out-File (Join-Path $outDir "$stamp-hpa.yaml")
kubectl get deployment $Deployment -n $Namespace -o yaml | Out-File (Join-Path $outDir "$stamp-deployment.yaml")
kubectl get pods -n $Namespace -l "app=$Deployment" -o wide | Out-File (Join-Path $outDir "$stamp-pods.txt")
kubectl get events -n $Namespace --sort-by=.lastTimestamp | Out-File (Join-Path $outDir "$stamp-events.txt")

$queries = @{
    lag = $LagMetric
    hpa_desired = "kube_hpa_status_desired_replicas{namespace=\"$Namespace\",horizontalpodautoscaler=\"$Hpa\"}"
    deployment_available = "kube_deployment_status_replicas_available{namespace=\"$Namespace\",deployment=\"$Deployment\"}"
    deployment_spec = "kube_deployment_spec_replicas{namespace=\"$Namespace\",deployment=\"$Deployment\"}"
    pod_created = "kube_pod_created{namespace=\"$Namespace\",pod=~\"$Deployment-.*\"}"
    pod_ready = "kube_pod_status_ready{namespace=\"$Namespace\",pod=~\"$Deployment-.*\",condition=\"true\"}"
}

foreach ($name in $queries.Keys) {
    $encoded = [uri]::EscapeDataString($queries[$name])
    $url = "$($PrometheusUrl.TrimEnd('/'))/api/v1/query?query=$encoded"
    $body = curl.exe --fail-with-body --silent --show-error $url
    $body | Out-File (Join-Path $outDir "$stamp-prometheus-$name.json")
}

@"
Collected: $(Get-Date -Format o)
Namespace: $Namespace
Deployment: $Deployment
HPA: $Hpa
Lag metric/query: $LagMetric
Prometheus: $PrometheusUrl

Interpretation:
- Compare the first Lag threshold crossing with hpa_desired replica increase.
- Compare desired replica increase with pod_ready transitions.
- Use events and Deployment conditions to explain startup delay.
"@ | Out-File (Join-Path $outDir "$stamp-summary.txt")

Write-Host "Evidence collected in $outDir"
