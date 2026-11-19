#Requires -Version 5.1
<# Offline, READ ONLY BTCUSDT signed-window continuity audit for dev.3.128.
  A caller-supplied manifest is NOT full-account evidence or exchange proof.
  No Binance API, order POST/DELETE, settlement, or source writes.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [Parameter(Mandatory=$true)][string]$IncidentPath,
    [Parameter(Mandatory=$true)][string]$HistoryManifestPath,
    [string]$DataDir=''
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$release=(Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw -Encoding UTF8).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $release" }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required' }
if ([string]::IsNullOrWhiteSpace($DataDir)) { $DataDir=Join-Path $root 'data' }
if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { throw "Missing offline data directory: $DataDir" }
foreach ($path in @($IncidentPath,$HistoryManifestPath)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Missing JSON evidence: $path" }
}
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
    $listeners=@(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)
    if ($listeners.Count -gt 0) { throw 'Port 8080 in use: stop trader and verify offline snapshot first' }
} else {
    $listeners=@(& netstat.exe -ano -p TCP | Where-Object { $_ -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+\d+\s*$' })
    if ($listeners.Count -gt 0) { throw 'Port 8080 is listening; offline inspection denied' }
}
Push-Location $root
try {
    $old=$ErrorActionPreference
    try {
        $ErrorActionPreference='Continue'
        & go run ./cmd/demo-close-financial-review -model-evidence -history-windows $HistoryManifestPath -data $DataDir -id $ClientOrderID -incident $IncidentPath
        $code=$LASTEXITCODE
    } finally { $ErrorActionPreference=$old }
    if ($code -ne 0) { throw "History continuity BLOCKED (exit=$code). No settlement permitted." }
} finally { Pop-Location }
