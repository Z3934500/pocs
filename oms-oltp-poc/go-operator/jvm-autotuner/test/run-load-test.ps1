param(
    [Parameter(Mandatory=$true)][string]$Url,
    [ValidateSet("wrk", "jmeter")][string]$Tool = "wrk",
    [int]$DurationSeconds = 60,
    [int]$Threads = 4,
    [int]$Connections = 64,
    [string]$Output = "test/artifacts/load"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root $Output
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$meta = Join-Path $outDir "$stamp-run.txt"
"Started: $(Get-Date -Format o)`nURL: $Url`nTool: $Tool`nDurationSeconds: $DurationSeconds`nThreads: $Threads`nConnections: $Connections" | Out-File $meta

if ($Tool -eq "wrk") {
    if (-not (Get-Command wrk -ErrorAction SilentlyContinue)) { throw "wrk is required" }
    wrk -t$Threads -c$Connections -d"${DurationSeconds}s" $Url 2>&1 | Tee-Object -FilePath (Join-Path $outDir "$stamp-wrk.txt")
} else {
    if (-not (Get-Command jmeter -ErrorAction SilentlyContinue)) { throw "jmeter is required" }
    throw "For JMeter, provide a prepared .jmx plan and run it in the distributed Perf environment."
}

"Finished: $(Get-Date -Format o)" | Add-Content $meta
Write-Host "Load-test output written to $outDir"
