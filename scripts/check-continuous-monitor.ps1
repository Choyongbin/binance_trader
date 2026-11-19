#Requires -Version 5.1
# Read-only; never prompts for or prints credentials, never starts exchange APIs.
[CmdletBinding()]
param([string]$BaseURL = 'http://127.0.0.1:8080')
$ErrorActionPreference = 'Stop'
$r = Invoke-RestMethod -Uri ($BaseURL.TrimEnd('/') + '/api/v1/recovery/continuous-monitor/status') -TimeoutSec 15

# Invariants that must remain false/true regardless of whether a process-local
# continuity checkpoint has been established.
if ($r.trading_authorized -ne $false -or
    $r.manual_review_required -ne $true -or
    $r.read_only -ne $true -or
    $r.restart_requires_passphrase -ne $true) {
    throw 'STOP: unsafe monitor status, do not enable orders.'
}

# stream_continuity_proven=true is an expected successful monitor state. It is
# NOT trading authorization. When true, validate the stronger checkpoint
# contract instead of rejecting it.
if ($r.stream_continuity_proven -eq $true) {
    if ($r.running -ne $true -or
        $r.state -ne 'CONNECTED' -or
        $r.stale_or_interrupted -ne $false -or
        $r.continuity_scope -ne 'PROCESS_LOCAL_CHECKPOINT_TO_NOW' -or
        [string]::IsNullOrWhiteSpace([string]$r.continuity_since) -or
        [string]$r.continuity_since -eq '0001-01-01T00:00:00Z') {
        throw 'STOP: continuity=true but checkpoint contract is incomplete or stale.'
    }
}

Write-Host ('Monitor state={0}; running={1}; stale={2}' -f $r.state, $r.running, $r.stale_or_interrupted)
Write-Host ('Epoch={0}; reconnects={1}; last signed REST={2}' -f $r.epoch, $r.reconnects, $r.last_signed_rest_at)
Write-Host ('Reason={0}; safety issues={1}' -f $r.reason, @($r.issues).Count)
if ($r.stream_continuity_proven -eq $true) {
    Write-Host ('Continuity checkpoint=TRUE; scope={0}; since={1}' -f $r.continuity_scope, $r.continuity_since)
} else {
    Write-Host 'Continuity checkpoint=FALSE; waiting for a stable same-epoch signed checkpoint pair.'
}
if ($r.stale_or_interrupted) { Write-Warning 'Prior unclosed monitor requires manual offline review. Do NOT remove lock files.' }
Write-Host 'PASS: read-only monitor safety invariants hold; trading authorization remains FALSE.'
