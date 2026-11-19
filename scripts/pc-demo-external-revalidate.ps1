#Requires -Version 5.1
# Binance Futures Demo signed GET revalidation only. Never places or cancels an order.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [ValidateSet('probe','stage','recheck')][string]$Action='probe'
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go compiler required'}
$version=(Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
if ($version -ne 'v0.4.11-dev.3.138') {throw "Expected dev.3.138, got $version"}
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {throw 'Stop trader server first'}
$dir=Join-Path $root 'pc-results'
New-Item -ItemType Directory -Path $dir -Force | Out-Null
$out=Join-Path $dir ('signed-replay-' + (Get-Date -Format 'yyyyMMdd_HHmmss_fff') + '.json')
$args=@('run','./cmd/demo-signed-inspect','-root',$root,'-out',$out,'-replay-action',$Action,'-replay-client',$ClientOrderID)
if ($Action -ne 'probe') { $args+=@('-replay-confirm','CHECKPOINT_ONLY_NOT_SETTLEMENT') }
Push-Location $root
try {
    & go test ./cmd/demo-signed-inspect
    if ($LASTEXITCODE -ne 0) { throw 'Signed diagnostic compilation failed' }
    $old=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & go @args; $exit=$LASTEXITCODE }
    finally { $ErrorActionPreference=$old }
    if ($exit -ne 0) {throw 'Signed revalidation blocked. Preserve the original data/ and report files.'}
    Write-Host ('READ-ONLY/BLOCKED result: ' + $out) -ForegroundColor Green
} finally { Pop-Location }
