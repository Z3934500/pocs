[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [int]$ProcessId,
    [string]$OutputDirectory = ".\artifacts\jfr",
    [string]$RecordingName = "oms-jvm",
    [string]$Settings = "profile",
    [string]$MaxAge = "15m",
    [string]$MaxSize = "256m"
)

$ErrorActionPreference = "Stop"

Get-Process -Id $ProcessId -ErrorAction Stop | Out-Null
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outputFile = Join-Path $OutputDirectory ("{0}-{1}-{2}.jfr" -f $RecordingName, $ProcessId, $timestamp)

& jcmd $ProcessId JFR.start "name=$RecordingName" "filename=$outputFile" `
    "settings=$Settings" "disk=true" "maxage=$MaxAge" "maxsize=$MaxSize"
if ($LASTEXITCODE -ne 0) {
    throw "jcmd JFR.start failed with exit code $LASTEXITCODE"
}

Write-Output "JFR recording started: $RecordingName"
Write-Output "Output file: $outputFile"
