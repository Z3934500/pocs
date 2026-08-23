param(
    [Parameter(Mandatory=$true)][ValidateSet("dev", "staging", "production")][string]$Environment,
    [Parameter(Mandatory=$true)][string]$ImageRepository,
    [Parameter(Mandatory=$true)][string]$ImageTag,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\..\.."
$envPath = Join-Path $root "deploy\k8s\environments\$Environment"
$namespace = "cce-$Environment"

if ($Environment -eq "production") {
    $namespace = "cce-production"
}

Write-Host "Rendering environment: $Environment"
Write-Host "Image: ${ImageRepository}:${ImageTag}"

kubectl kustomize --load-restrictor=LoadRestrictionsNone $envPath | Out-File -Encoding utf8 "$env:TEMP\cce-$Environment-rendered.yaml"

if ($DryRun) {
    kubectl apply --dry-run=server -f "$env:TEMP\cce-$Environment-rendered.yaml"
    Write-Host "Dry run completed."
    exit 0
}

kubectl create namespace $namespace --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$env:TEMP\cce-$Environment-rendered.yaml"
kubectl rollout status deployment/cce-feature-platform -n $namespace

Write-Host "Deployment completed for $Environment."
