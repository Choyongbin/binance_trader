#Requires -Version 5.1
# Read-only local protection-plan audit for a stopped-server COPY. No Binance
# request, no order submission, no archive mutation.
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
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required for offline protection audit.' }
Push-Location $root
try {
    $raw = & go run ./cmd/protection-audit -data $data
    if ($LASTEXITCODE -ne 0) { throw 'Protection archive invalid or changing; no action taken.' }
    $r = ($raw -join "`n") | ConvertFrom-Json
    if ($r.trading_authorized -ne $false -or $r.per_owner_algo_arming_allowed -ne $false -or $r.manual_review_required -ne $true) {
        throw 'STOP: protection audit safety invariants violated.'
    }
    Write-Host ('PASS: protection plan state={0}; plans={1}; same-side conflict={2}' -f $r.state, $r.plan_count, $r.same_side_multi_owner_present)
    Write-Host 'Trading remains LOCKED; this audit never arms TP/SL.'
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
