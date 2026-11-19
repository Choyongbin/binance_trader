#Requires -Version 5.1
# Safe GET-only diagnostic summary. Does not start Binance connections and
# never displays credentials, raw URLs, WebSocket headers or response bodies.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$base = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod ($base + '/api/v1/version') -TimeoutSec 5
if ($v.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim() -or $v.trading -ne 'LOCKED') {
    throw 'Unexpected version or trading unlock. Stop here.'
}
$p = Invoke-RestMethod ($base + '/api/v1/trading/private-probe/status') -TimeoutSec 5
$r = Invoke-RestMethod ($base + '/api/v1/recovery/status') -TimeoutSec 5
if ($p.trading_authorized -ne $false -or $p.stream_continuity_proven -ne $false -or
    $r.trading_authorized -ne $false -or $r.stream_continuity_proven -ne $false) {
    throw 'Unsafe diagnostic flags. Stop here.'
}
Write-Host ('Version: ' + $v.release + ' / orders LOCKED')
Write-Host ('Private probe: ' + $p.status + ' / handshake=' + $p.handshake_ok + ' / reports=' + $p.report_count)
if ($p.dial_route) { Write-Host ('Route (non-secret): ' + $p.dial_route) }
if ([string]::IsNullOrWhiteSpace([string]$p.dial_phase)) {
    Write-Host 'Phase: old record has no diagnostic metadata. Re-run the short UI probe ONCE after update.'
} else {
    $msg = 'Phase: ' + $p.dial_phase
    if ($p.dial_http_status -ge 100) { $msg += ' / HTTP=' + $p.dial_http_status }
    if ($null -ne $p.dial_elapsed_ms) { $msg += ' / elapsed_ms=' + $p.dial_elapsed_ms }
    Write-Host $msg
}
Write-Host ('Recovery: ' + $r.status + ' / historical reports=' + $r.report_count)
if ($r.last_report) {
    Write-Host ('  historical stage=' + $r.last_report.stage + ' / WS=' + $r.last_report.observer_connected + ' / REST=' + $r.last_report.rest_reviewed)
    foreach ($x in @($r.last_report.blockers)) {
        if ($x -match '^사설 WS 접속 단계: (CONNECTED|URL_VALIDATION|TCP_DIAL|TLS_HANDSHAKE|UPGRADE_WRITE|UPGRADE_RESPONSE|UPGRADE_HEADERS|UPGRADE_ACCEPT|HTTP_STATUS|UNCLASSIFIED|TIMEOUT)( \(HTTP [1-5][0-9][0-9]\))?$') {
            Write-Host ('  ' + $x)
        }
    }
}
Write-Host 'PASS: status read only. Even a successful handshake never authorizes trading.' -ForegroundColor Green
