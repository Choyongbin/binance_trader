#Requires -Version 5.1
# No credentials, no wallet mutation, no order content. Requires a started server.
[CmdletBinding()]
param([string]$OutDir = '')
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$base = 'http://127.0.0.1:8080'
$expected = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
$version = Invoke-RestMethod -Method Get -Uri "$base/api/v1/version" -TimeoutSec 5
$health = Invoke-RestMethod -Method Get -Uri "$base/api/v1/health" -TimeoutSec 5
$boot = Invoke-RestMethod -Method Get -Uri "$base/api/v1/bootstrap" -TimeoutSec 5
if ($version.release -ne $expected -or $version.trading -ne 'LOCKED' -or $health.trading -ne 'disabled' -or $boot.trading_enabled -ne $false) {
    throw 'Wrong release or UNSAFE trade enabled state; stop tests immediately.'
}
if ($boot.csrf_token -isnot [string] -or $boot.csrf_token.Length -lt 16) { throw 'Missing CSRF token.' }
$readiness = Invoke-RestMethod -Method Get -Uri "$base/api/v1/trading/local-readiness" -TimeoutSec 5
if ($readiness.trading_authorized -ne $false -or $readiness.wallet_edit_authorized -ne $false -or
    $readiness.signed_whole_account_audit_required -ne $true) { throw 'Local readiness unexpectedly authorized a trade.' }
# These calls NEVER contain a planned order; only the hard-locked routes are tested.
foreach ($path in @('/api/v1/trading/manual/orders', '/api/v1/trading/auto/start', '/api/v1/trading/auto/stop')) {
    $status = 0
    try {
        $response = Invoke-WebRequest -Method Post -Uri ($base + $path) -Headers @{'X-CSRF-Token' = $boot.csrf_token; 'Origin' = $base} -ContentType 'application/json' -Body '{}' -TimeoutSec 5 -UseBasicParsing
        $status = [int]$response.StatusCode
    } catch {
        if ($null -eq $_.Exception.Response) { throw }
        $status = [int]$_.Exception.Response.StatusCode
    }
    if ($status -ne 423) { throw ("FAIL: expected 423 LOCKED on {0}, got HTTP {1}" -f $path, $status) }
}
$recovery = $null
$index = $null
if ($boot.environment -eq 'demo') {
    $recovery = Invoke-RestMethod -Method Get -Uri "$base/api/v1/recovery/status" -TimeoutSec 5
    $index = Invoke-RestMethod -Method Get -Uri "$base/api/v1/trading/accounting/index/status" -TimeoutSec 5
    if ($recovery.trading_authorized -ne $false -or $recovery.manual_review_required -ne $true -or
        $index.trading_authorized -ne $false -or $index.actual_net_verified -ne $false) { throw 'Diagnostic granted trade authorization.' }
}
if ([string]::IsNullOrWhiteSpace($OutDir)) { $OutDir = Join-Path $root 'pc-results' }
$recoveryStatus = 'SKIPPED_NOT_DEMO'
$indexState = 'SKIPPED_NOT_DEMO'
if ($null -ne $recovery) { $recoveryStatus = [string]$recovery.status }
if ($null -ne $index) { $indexState = [string]$index.state }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$out = [ordered]@{
    checked_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    release = [string]$version.release
    status = 'PASS_LOCAL_READONLY_ONLY'
    trading = [string]$version.trading
    demo_selected = ($boot.environment -eq 'demo')
    history_status = [string]$readiness.status
    recovery_status = $recoveryStatus
    index_state = $indexState
    test_scope = 'loopback API safety and local diagnostic only; not Windows UI/private WS/real Demo order proof'
}
$outPath = Join-Path $OutDir 'pc-readonly-result.json'
$out | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $outPath -Encoding UTF8
Write-Host ('PASS: local API and all 3 order routes stayed HTTP 423. Result: ' + $outPath) -ForegroundColor Green
Write-Host 'This test NEVER triggers an exchange order or private Demo diagnostic.'
