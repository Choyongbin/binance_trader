#Requires -Version 5.1
# Read-only pooled Hedge-side reservation/fence audit for a stopped-server COPY.
# No Binance request, reservation mutation, order submission or lock removal.
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
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required for offline pooled-exit audit.' }
Push-Location $root
try {
    $raw = & go run ./cmd/pooled-exit-audit -data $data
    if ($LASTEXITCODE -ne 0) { throw 'Pooled exit reservation archive invalid or changing; no action taken.' }
    $r = ($raw -join "`n") | ConvertFrom-Json
    if ($r.trading_authorized -ne $false -or $r.manual_review_required -ne $true) {
        throw 'STOP: pooled exit audit safety invariants violated.'
    }
    Write-Host ('PASS: pooled exit serialization state={0}; active={1}; completed={2}' -f $r.state, $r.active_reservations, $r.completed_reservations)
    Write-Host 'Trading remains LOCKED; this audit never creates or settles a reservation.'
    if ($OutFile -ne '') {
        $resolved = [System.IO.Path]::GetFullPath($OutFile)
        $dataRoot = $data.TrimEnd('\') + '\'
        if ($resolved.StartsWith($dataRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
            [string]::Equals($resolved, $data, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw 'STOP: do not write audit output into the inspected data copy.'
        }
        $r | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolved -Encoding UTF8
        Write-Host ('Sanitized output: ' + $resolved)
    }
} finally { Pop-Location }
