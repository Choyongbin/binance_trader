#Requires -Version 5.1
# Signed READ-ONLY Binance Demo incident inspection; no POST/DELETE, no data mutation.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go compiler is required.' }
$version = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
if ($version -ne 'v0.4.11-dev.3.138') { throw "Unexpected release: $version" }
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {
    throw 'Trader server is running. Run .\stop.ps1 before signed review.'
}
$dir = Join-Path $root 'pc-results'
New-Item -ItemType Directory -Path $dir -Force | Out-Null
$name = 'signed-order-review-' + (Get-Date -Format 'yyyyMMdd_HHmmss_fff') + '.json'
$out = Join-Path $dir $name
Push-Location $root
try {
    Write-Host 'Validating read-only Go diagnostic (no orders)...' -ForegroundColor Cyan
    & go test ./cmd/demo-signed-inspect
    if ($LASTEXITCODE -ne 0) { throw 'Diagnostic compile failed; NO ORDERS.' }
    Write-Host 'Performing signed GET on Binance Futures DEMO only...' -ForegroundColor Yellow
    # PowerShell 5.1 can promote a native stderr line to a terminating error.
    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & go run ./cmd/demo-signed-inspect -root $root -out $out
        $exit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $oldPreference
    }
    if ($exit -ne 0) { throw 'Signed GET review failed; NO ORDERS. Do not repeat the single-order test.' }
    Write-Host ("Please upload this JSON (no API keys): " + $out) -ForegroundColor Green
} finally { Pop-Location }
