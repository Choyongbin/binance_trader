#Requires -Version 5.1
# Historical, loopback GET only. No credentials, no order calls or file changes.
[CmdletBinding()]
param([string]$OutDir = '')
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$version = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
$base = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod -Uri "$base/api/v1/version" -Method Get -TimeoutSec 6
$h = Invoke-RestMethod -Uri "$base/api/v1/health" -Method Get -TimeoutSec 6
if ($v.release -ne $version -or $v.trading -ne 'LOCKED' -or $h.trading -ne 'disabled') {
    throw 'STOP: wrong release or the server is no longer trade-locked.'
}
$a = Invoke-RestMethod -Uri "$base/api/v1/trading/stream-gaps/status" -Method Get -TimeoutSec 10
if ($a.trading_authorized -ne $false -or $a.stream_continuity_proven -ne $false -or
    $a.manual_review_required -ne $true -or $a.historical_only -ne $true) {
    throw 'STOP: historical report has unexpected execution/continuity flags.'
}
if ([string]::IsNullOrWhiteSpace($OutDir)) { $OutDir = Join-Path $root 'pc-results' }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$out = Join-Path $OutDir 'stream-gaps-safe-report.json'
# This API is exclusively a sanitized summary: no raw event payloads, keys or filenames.
$a | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $out -Encoding UTF8
Write-Host ('PASS: release=' + $version + '; trading LOCKED; historical evidence read only.')
Write-Host ('markers=' + $a.marker_count + ' historical diagnostic sessions=' + $a.session_count +
    ' outside matched historical windows=' + $a.outside_historical_sessions +
    ' incomplete=' + $a.incomplete_session_count)
foreach ($entry in @($a.sessions)) {
    Write-Host ('  start=' + $entry.started_at + '; stage=' + $entry.stage +
        '; candidate markers=' + $entry.marker_count + '; repeated DISCONNECT candidate=' + $entry.repeated_disconnect_candidate)
}
Write-Host ('Sanitized result: ' + $out)
Write-Host 'Historical timestamp overlap is NOT proof of complete account history; no markers are cleared.'
