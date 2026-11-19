#Requires -Version 5.1
# OFFLINE, NON-POSTED: no signed exchange calls and no mutation of actual trading ledgers.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [ValidateSet('inspect','prepare','verify')][string]$Action='inspect'
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {throw 'Require dev.3.138'}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go compiler required'}
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {throw 'Stop the server before offline inspection'}
Push-Location $root
try {
    & go test ./cmd/demo-close-dual-ledger
    if ($LASTEXITCODE -ne 0) {throw 'Dual-ledger preview command compilation failed'}
    $arguments=@('run','./cmd/demo-close-dual-ledger','-data',(Join-Path $root 'data'),'-client',$ClientOrderID,'-action',$Action)
    if ($Action -ne 'inspect') {
        Write-Host 'NON-POSTED immutable sidecar rehearsal ONLY. No settlement or account order.' -ForegroundColor Yellow
        $arguments+=@('-confirm','SIMULATE_ONLY_NO_SETTLEMENT')
    }
    $old=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & go @arguments; $exit=$LASTEXITCODE }
    finally { $ErrorActionPreference=$old }
    if ($exit -ne 0) {throw 'Fail-closed: dual-ledger plan unavailable; preserve the original records'}
} finally {Pop-Location}
