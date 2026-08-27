param(
    [Parameter(Mandatory=$true)][ValidateSet("dev", "staging", "production")][string]$Environment,
    [Parameter(Mandatory=$true)][string]$ImageRepository,
    [Parameter(Mandatory=$true)][string]$ImageTag,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\..\.."
$envPath = Join-Path $root "deploy\k8s\environments\$Environment"
$namespace = if ($Environment -eq "production") { "cce-production" } else { "cce-$Environment" }
$rendered = Join-Path $env:TEMP "cce-$Environment-rendered.yaml"
$patched = Join-Path $env:TEMP "cce-$Environment-patched.yaml"
$image = "${ImageRepository}:${ImageTag}"

Write-Host "Rendering environment: $Environment"
kubectl kustomize --load-restrictor=LoadRestrictionsNone $envPath | Out-File -Encoding utf8 $rendered

Write-Host "Setting CCE API/importer image to: $image"
(Get-Content $rendered) `
    -replace 'ghcr\.io/OWNER/cce-feature-platform:[A-Za-z0-9._-]+', $image |
    Out-File -Encoding utf8 $patched

if ($DryRun) {
    kubectl apply --dry-run=server -f $patched
    Write-Host "Dry run completed for $Environment."
    exit 0
}

kubectl create namespace $namespace --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f $patched
kubectl rollout status deployment/cce-feature-platform -n $namespace
Write-Host "Deployment completed for $Environment with image $image."
