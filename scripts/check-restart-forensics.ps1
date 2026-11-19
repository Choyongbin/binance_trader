#Requires -Version 5.1
# Loopback GET only. No exchange request, file mutation, lock removal, or secrets.
[CmdletBinding()]
param(
    [string]$BaseURL = 'http://127.0.0.1:8080',
    [string]$OutDir = ''
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$release = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
$base = $BaseURL.TrimEnd('/')
$v = Invoke-RestMethod -Uri "$base/api/v1/version" -Method Get -TimeoutSec 6
$h = Invoke-RestMethod -Uri "$base/api/v1/health" -Method Get -TimeoutSec 6
if ($v.release -ne $release -or $v.trading -ne 'LOCKED' -or $h.trading -ne 'disabled') {
    throw 'STOP: wrong release or disabled trade lock; do not trust results.'
}
$r = Invoke-RestMethod -Uri "$base/api/v1/recovery/restart-forensics/status" -Method Get -TimeoutSec 25
if ($r.evidence_stable -ne $true -or $r.trading_authorized -ne $false -or
    $r.auto_rearm_allowed -ne $false -or $r.stream_continuity_proven -ne $false -or
    $r.manual_review_required -ne $true) {
    throw 'STOP: forensic invariants violated or archive changing.'
}
if ([string]::IsNullOrWhiteSpace($OutDir)) { $OutDir = Join-Path $root 'pc-results' }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$out = Join-Path $OutDir 'restart-forensics-safe.json'
$r | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $out -Encoding UTF8
Write-Host ('PASS: release=' + $release + '; readonly restart forensics; trading LOCKED.')
Write-Host ('Monitor state={0}; archives={1}; checkpoints={2}; interrupted_or_active={3}' -f $r.monitor_state, $r.monitor_session_count, $r.monitor_checkpoint_count, $r.interrupted_or_active)
Write-Host ('Gaps={0}; unmatched legacy bounded-diagnostic candidates={1}; old unfinished diagnostics={2}' -f $r.gap_marker_count, $r.timestamp_unmatched_gaps, $r.incomplete_historical_sessions)
Write-Host ('Recovery disposition={0}; no automatic re-arm or gap clearance' -f $r.restart_disposition)
Write-Host ('Sanitized output: ' + $out)
