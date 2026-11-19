#Requires -Version 5.1
# Binance Demo signed GET + non-posted Package A evidence analysis.
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$ClientOrderID,
  [ValidateSet('entry-probe','entry-archive','verify-entry-archive','incident-archive','verify-incident-archive','exit-discovery','discovery-archive','verify-discovery-archive','diagnose-discovery-archive','plan-entry-recovery','recheck-entry-recovery','probe','stage','recheck')][string]$Action='probe',
  [long]$ExitOrderID=0
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {throw 'Require dev.3.138'}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go required'}
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {throw 'Stop the server before auditing'}
$results=Join-Path $root 'pc-results'
New-Item -ItemType Directory -Path $results -Force | Out-Null
$out=Join-Path $results ('package-a-signed-' + (Get-Date -Format 'yyyyMMdd_HHmmss_fff') + '.json')
if (($Action -eq 'incident-archive' -and $ExitOrderID -le 0) -or ($Action -ne 'incident-archive' -and $ExitOrderID -ne 0)) {throw 'ExitOrderID is required for incident-archive only'}
$args=@('run','./cmd/demo-signed-inspect','-root',$root,'-out',$out,'-package-a-action',$Action,'-package-a-client',$ClientOrderID)
if ($Action -eq 'incident-archive') {$args+=@('-package-a-exit-order-id',([string]$ExitOrderID))}
if (($Action -ne 'probe') -and ($Action -ne 'entry-probe') -and ($Action -ne 'entry-archive') -and ($Action -ne 'verify-entry-archive') -and ($Action -ne 'incident-archive') -and ($Action -ne 'verify-incident-archive') -and ($Action -ne 'exit-discovery') -and ($Action -ne 'discovery-archive') -and ($Action -ne 'verify-discovery-archive') -and ($Action -ne 'diagnose-discovery-archive') -and ($Action -ne 'plan-entry-recovery') -and ($Action -ne 'recheck-entry-recovery')) {
    Write-Host 'Append-only NON-POSTED evidence receipts only. No settlement.' -ForegroundColor Yellow
    $args+=@('-package-a-confirm','PACKAGE_A_NO_POSTING')
}
Push-Location $root
try {
    & go test ./cmd/demo-signed-inspect
    if ($LASTEXITCODE -ne 0) {throw 'Audit tool did not compile'}
    $old=$ErrorActionPreference
    try {$ErrorActionPreference='Continue'; & go @args; $rc=$LASTEXITCODE} finally {$ErrorActionPreference=$old}
    if ($rc -ne 0) {throw 'Fail-closed signed audit: original data preserved'}
    Write-Host ('NON-POSTED audit file: '+$out) -ForegroundColor Green
} finally {Pop-Location}
