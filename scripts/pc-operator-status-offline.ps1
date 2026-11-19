#Requires -Version 5.1
# Unified read-only forensic status for a stopped-server COPY. No Binance
# request, no order submission, no lock deletion, no archive mutation.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$DataCopy,
    [string]$OutFile = ''
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$data = (Resolve-Path -LiteralPath $DataCopy).Path
$live = Join-Path $root 'data'
if ([string]::Equals($data.TrimEnd('\'), $live.TrimEnd('\'), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'STOP: inspect a stopped-server copy outside the installed data path.'
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required for offline operator status.' }
Push-Location $root
try {
    $raw = & go run ./cmd/operator-status -data $data
    if ($LASTEXITCODE -ne 0) { throw 'Unified evidence invalid or changing; no action taken.' }
    $r = ($raw -join "`n") | ConvertFrom-Json
    if ($r.trading_authorized -ne $false -or $r.stream_continuity_proven -ne $false -or $r.manual_review_required -ne $true) {
        throw 'STOP: operator status safety invariants violated.'
    }
    Write-Host ('STATE: {0}' -f $r.state)
    Write-Host ('Restart: {0}; monitor={1}; gaps={2}' -f $r.restart_disposition, $r.monitor_state, $r.gap_marker_count)
    Write-Host ('Recovery: probes={0}; interrupted={1}; reviews={2}' -f $r.recovery.probe_count, $r.recovery.interrupted_count, $r.recovery.review_count)
    Write-Host ('Protection: plans={0}; settled={1}; unsettled={2}; fills={3}' -f $r.protection.plan_count, $r.protection.settled_group_count, $r.protection.unsettled_group_count, $r.protection.protection_fill_count)
    Write-Host ('Accounting: reports={0}; latest fills={1}; protection fills={2}' -f $r.accounting.report_count, $r.accounting.latest_known_fills, $r.accounting.latest_protection_fills)
    if ($r.blockers.Count -gt 0) { $r.blockers | ForEach-Object { Write-Host ('- ' + $_) } }
    Write-Host 'Trading remains LOCKED. This report is historical/local evidence only.'
    if ($OutFile -ne '') {
        $resolved = [System.IO.Path]::GetFullPath($OutFile)
        $dataRoot = $data.TrimEnd('\') + '\'
        if ($resolved.StartsWith($dataRoot, [System.StringComparison]::OrdinalIgnoreCase) -or [string]::Equals($resolved, $data, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw 'STOP: do not write output into the inspected data copy.'
        }
        $r | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $resolved -Encoding UTF8
        Write-Host ('Sanitized output: ' + $resolved)
    }
} finally { Pop-Location }
