#Requires -Version 5.1
# READ-ONLY summary AFTER the operator runs the separate private/recovery
# diagnostics in the UI. Never accepts or logs keys/passwords or raw reports.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$base = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod "$base/api/v1/version" -TimeoutSec 5
$b = Invoke-RestMethod "$base/api/v1/bootstrap" -TimeoutSec 5
if ($v.trading -ne 'LOCKED' -or $b.trading_enabled -ne $false) { throw 'Trade lock unexpectedly changed.' }
if ($b.environment -ne 'demo') { throw 'Switch to Demo in the local browser first.' }
$local = Invoke-RestMethod "$base/api/v1/trading/local-readiness" -TimeoutSec 5
$probe = Invoke-RestMethod "$base/api/v1/trading/private-probe/status" -TimeoutSec 5
$recovery = Invoke-RestMethod "$base/api/v1/recovery/status" -TimeoutSec 5
$accounting = Invoke-RestMethod "$base/api/v1/trading/accounting/status" -TimeoutSec 5
foreach ($x in @($probe, $recovery, $accounting)) {
    if ($x.trading_authorized -ne $false) { throw 'Unsafe diagnostic flag.' }
}
if ($probe.stream_continuity_proven -ne $false -or $recovery.stream_continuity_proven -ne $false -or
    $accounting.actual_net_verified -ne $false) { throw 'Historical result mislabeled current/verified.' }
Write-Host ('Local evidence: ' + $local.status)
Write-Host ('Private WS diagnostic: ' + $probe.status + ' (historical reports=' + $probe.report_count + ')')
Write-Host ('Signed REST + WS bounded recovery: ' + $recovery.status + ' (historical reports=' + $recovery.report_count + ')')
Write-Host ('Historical accounting: ' + $accounting.status + ' (report count=' + $accounting.report_count + ')')
Write-Host 'PASS: diagnostic safety flags. Connectivity SUCCESS requires inspecting these statuses and server logs manually.'
