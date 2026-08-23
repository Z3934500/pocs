param(
    [switch]$E2E,
    [string]$E2EKind = "oms-e2e",
    [string]$Artifacts = "test/artifacts",
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$moduleRoot = Split-Path -Parent $PSScriptRoot
$artifactPath = Join-Path $moduleRoot $Artifacts
New-Item -ItemType Directory -Force -Path $artifactPath | Out-Null

foreach ($tool in @("go")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool is required" }
}

Push-Location $moduleRoot
try {
    $unitLog = Join-Path $artifactPath "unit-test.log"
    go test ./... -count=1 2>&1 | Tee-Object -FilePath $unitLog
    if ($LASTEXITCODE -ne 0) { throw "unit/package tests failed; see $unitLog" }

    if ($E2E) {
        foreach ($tool in @("kind", "kubectl", "docker")) {
            if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool is required for E2E" }
        }
        $env:E2E_KIND = "1"
        $env:KUBECONFIG = (kind get kubeconfig --name $E2EKind)
        $e2eLog = Join-Path $artifactPath "e2e-test.log"
        go test -tags=e2e ./e2e -v -count=1 -timeout "${TimeoutSeconds}s" 2>&1 | Tee-Object -FilePath $e2eLog
        if ($LASTEXITCODE -ne 0) { throw "Kind E2E failed; see $e2eLog" }
    }
} finally {
    Pop-Location
}

Write-Host "Test run completed. Artifacts: $artifactPath"
