#Requires -Version 5.1
# Local, offline source-COPY inspection. Does not write to the data copy or
# remove old locks; never request credentials or access Binance networking.
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
    throw 'STOP: inspect a STOPPED-SERVER COPY outside the installed data path, never the live data folder.'
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required for offline forensic inspection.' }
Push-Location $root
try {
    $raw = & go run ./cmd/monitor-forensics -data $data
    if ($LASTEXITCODE -ne 0) { throw 'Offline evidence inconsistent; NO lock removal or recovery.' }
    $r = ($raw -join "`n") | ConvertFrom-Json
    if ($r.evidence_stable -ne $true -or $r.trading_authorized -ne $false -or
        $r.auto_rearm_allowed -ne $false -or $r.stream_continuity_proven -ne $false -or
        $r.manual_review_required -ne $true) { throw 'STOP: unsafe forensic CLI result.' }
    Write-Host ('PASS: offline data copy classified as {0} / monitor state {1}' -f $r.restart_disposition, $r.monitor_state)
    Write-Host ('Legacy markers={0}, monitor archives={1}, no lock was cleared.' -f $r.gap_marker_count, $r.monitor_session_count)
    if ($OutFile -ne '') {
        $resolved = [System.IO.Path]::GetFullPath($OutFile)
        $dataRoot = $data.TrimEnd('\') + '\'
        if ($resolved.StartsWith($dataRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
            [string]::Equals($resolved, $data, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw 'STOP: do not write forensic results into the inspected data copy.'
        }
        $r | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolved -Encoding UTF8
        Write-Host ('Sanitized output: ' + $resolved)
    }
} finally { Pop-Location }
