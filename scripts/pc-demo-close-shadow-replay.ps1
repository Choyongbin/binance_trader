#Requires -Version 5.1
# OFFLINE forensic SHADOW ONLY. Never posts a settlement or sends exchange orders.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [ValidateSet('inspect','prepare','replay')][string]$Action='inspect'
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {throw 'Require dev.3.138'}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go compiler required'}
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {throw 'Stop trader server first'}
Push-Location $root
try {
    & go test ./cmd/demo-close-shadow-replay
    if ($LASTEXITCODE -ne 0) {throw 'Shadow command compilation failed'}
    $arguments=@('run','./cmd/demo-close-shadow-replay','-data',(Join-Path $root 'data'),'-client',$ClientOrderID,'-action',$Action)
    if ($Action -ne 'inspect') {
        Write-Host 'NON-POSTED simulation only. No order, lifecycle, ownership or PnL writes.' -ForegroundColor Yellow
        $arguments+=@('-confirm','SIMULATE_ONLY_NO_SETTLEMENT')
    }
    $old=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & go @arguments; $exit=$LASTEXITCODE }
    finally { $ErrorActionPreference=$old }
    if ($exit -ne 0) {throw 'Shadow replay blocked; preserve original data and signed evidence.'}
} finally {Pop-Location}
