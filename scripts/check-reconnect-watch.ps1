#Requires -Version 5.1
[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$r=Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/recovery/reconnect-watch/status' -TimeoutSec 12
if ($r.trading_authorized -ne $false -or $r.stream_continuity_proven -ne $false -or $r.manual_review_required -ne $true -or $r.historical_only -ne $true) { throw 'STOP: unsafe reconnect status' }
Write-Host ('History: '+$r.status+' reports='+$r.report_count+' busy='+$r.diagnostic_busy)
$epochCount = if ($null -eq $r.epochs) { 0 } else { @($r.epochs).Count }
Write-Host ("Epochs={0}; reconnects={1}; final REST={2}" -f $epochCount, $r.reconnects, $r.final_rest_reviewed)
Write-Host 'PASS: read-only status only. Historical diagnostics never unlock orders.'
