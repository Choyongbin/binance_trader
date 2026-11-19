#Requires -Version 5.1
<#
OFFLINE dev.3.129 simulated signed GET response capture/checkpoint rehearsal.
Does NOT request Binance, use credentials, place orders or post settlement.
The source JSON is operator-provided mock data, NOT exchange-attested history.
Capture writes ONLY into an existing independent -CheckpointRoot, never DataDir.
#>
[CmdletBinding()]
param(
  [ValidateSet('inspect','capture','claim','baseline')][string]$Action='inspect',
  [Parameter(Mandatory=$true)][string]$ClientOrderID,
  [Parameter(Mandatory=$true)][string]$IncidentPath,
  [Parameter(Mandatory=$true)][string]$OfflineSourcePath,
  [Parameter(Mandatory=$true)][string]$CheckpointRoot,
  [Parameter(Mandatory=$true)][string]$DataDir,
  [string]$BaselineClaimPath
)
$ErrorActionPreference='Stop'
$project=Split-Path -Parent $PSScriptRoot
$release=(Get-Content -LiteralPath (Join-Path $project 'VERSION') -Raw).Trim()
if ($release -cne 'v0.4.11-dev.3.138') {throw "Requires dev.3.138; found $release"}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go required'}
foreach ($f in @($IncidentPath,$OfflineSourcePath)) {
  if (-not (Test-Path -LiteralPath $f -PathType Leaf)) {throw "Missing offline JSON: $f"}
}
foreach ($p in @($CheckpointRoot,$DataDir)) {
  $item=Get-Item -LiteralPath $p -Force -ErrorAction Stop
  if (-not $item.PSIsContainer -or ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
    throw "Expected existing non-symlink directory: $p"
  }
}
if (($Action -eq 'baseline') -ne (-not [string]::IsNullOrWhiteSpace($BaselineClaimPath))) {throw 'Baseline requires -BaselineClaimPath; other actions disallow it'}
if ($BaselineClaimPath -and -not (Test-Path -LiteralPath $BaselineClaimPath -PathType Leaf)) {throw 'Missing baseline claim JSON'}
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
  $listeners=@(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)
  if ($listeners.Count -gt 0) {throw 'Trader or another process listens on 8080; use stopped/offline data only'}
}
Push-Location -LiteralPath $project
try {
  $old=$ErrorActionPreference
  try {
    $ErrorActionPreference='Continue'
    $arguments = @('run','./cmd/demo-close-history-capture','-action',$Action,'-id',$ClientOrderID,'-incident',$IncidentPath,'-source',$OfflineSourcePath,'-data',$DataDir,'-checkpoint',$CheckpointRoot)
    if ($Action -eq 'baseline') {$arguments += @('-claim',$BaselineClaimPath)}
    & go @arguments
    $code=$LASTEXITCODE
  } finally {$ErrorActionPreference=$old}
  if ($code -ne 0) {throw "Offline capture $Action BLOCKED; no trading or settlement authorized"}
} finally {Pop-Location}
