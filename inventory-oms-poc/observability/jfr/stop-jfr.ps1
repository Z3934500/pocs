[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [int]$ProcessId,
    [string]$RecordingName = "oms-jvm"
)

$ErrorActionPreference = "Stop"
Get-Process -Id $ProcessId -ErrorAction Stop | Out-Null

& jcmd $ProcessId JFR.stop "name=$RecordingName"
if ($LASTEXITCODE -ne 0) {
    throw "jcmd JFR.stop failed with exit code $LASTEXITCODE"
}

Write-Output "JFR recording stopped: $RecordingName"
