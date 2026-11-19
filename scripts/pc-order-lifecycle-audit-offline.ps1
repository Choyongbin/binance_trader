#Requires -Version 5.1
# Read-only durable order-lifecycle audit for a stopped-server DATA COPY.
# No Binance request, order submission, reconciliation write, settlement or unlock.
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
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required for offline order-lifecycle audit.' }
Push-Location $root
try {
    $raw = & go run ./cmd/order-lifecycle-audit -data $data
    if ($LASTEXITCODE -ne 0) { throw 'Order lifecycle archive invalid or changing; no action taken.' }
    $r = ($raw -join "`n") | ConvertFrom-Json
    if ($r.trading_authorized -ne $false -or $r.manual_review_required -ne $true) {
        throw 'STOP: order lifecycle audit safety invariants violated.'
    }
    Write-Host ('PASS: execution lifecycle state={0}; total={1}; settled={2}; pending={3}' -f $r.state, $r.total, $r.settled, $r.pending)
    Write-Host 'Trading remains LOCKED; this audit never sends, retries, reconciles or settles an order.'
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
