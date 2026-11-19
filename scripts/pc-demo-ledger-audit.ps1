#Requires -Version 5.1
# Signed GET only: NEVER submit/cancel orders, mutate ownership, or clear locks.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {
    throw 'Expected dev.3.129; no network requests were made.'
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go 1.23+ must be installed.' }
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {
    throw 'Stop the local trader server before read-only signed audit.'
}
$dir = Join-Path $root 'pc-results'
if (-not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
$out = Join-Path $dir ('signed-ledger-audit-' + (Get-Date -Format 'yyyyMMdd_HHmmss_fff') + '.json')
Push-Location $root
try {
    & go test ./cmd/demo-signed-inspect ./internal/trading
    if ($LASTEXITCODE -ne 0) { throw 'Go diagnostic tests failed; nothing queried.' }
    $before = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & go run ./cmd/demo-signed-inspect -root $root -ledger -out $out
        $status = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $before
    }
    if ($status -ne 0) { throw 'Read-only signed ledger audit failed; do not enable trading.' }
    Write-Host ('Please upload the redacted-free evidence JSON: ' + $out) -ForegroundColor Green
    Write-Host 'The JSON never authorizes orders or lifecycle settlement.' -ForegroundColor Yellow
} finally { Pop-Location }
