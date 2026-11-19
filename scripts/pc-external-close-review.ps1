#Requires -Version 5.1
# Review-only sidecar receipt. Does NOT settle entries, import fills or submit orders.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Show','Stage','Acknowledge','Reject')]
    [string]$Action,
    [Parameter(Mandatory = $true)]
    [string]$ClientOrderID,
    [string]$Report = '',
    [string]$Reviewer = '',
    [string]$Note = '',
    [switch]$ConfirmReviewOnly
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$version = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
if ($version -ne 'v0.4.11-dev.3.138') { throw "Expected dev.3.138, got $version" }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required.' }
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {
    throw 'Trader is running. Run stop.ps1 first.'
}
$argsForGo = @('run', './cmd/demo-close-review-ledger', '-root', $root, '-action', $Action.ToLowerInvariant(), '-client-id', $ClientOrderID)
if ($Action -ne 'Show') {
    if (-not $ConfirmReviewOnly) { throw 'ConfirmReviewOnly is required. No trading/settlement will be authorized.' }
    $argsForGo += @('-confirm', 'REVIEW_ONLY_NOT_SETTLEMENT')
    if ($Action -eq 'Stage') {
        if ([string]::IsNullOrWhiteSpace($Report)) { throw 'Stage needs -Report (a signed-ledger-audit JSON path).' }
        $argsForGo += @('-report', $Report)
    } else {
        if ([string]::IsNullOrWhiteSpace($Reviewer) -or [string]::IsNullOrWhiteSpace($Note)) { throw 'Reviewer and Note are required.' }
        $argsForGo += @('-reviewer', $Reviewer, '-note', $Note)
    }
}
Push-Location $root
try {
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & go @argsForGo
        $status = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    if ($status -ne 0) { throw "Review-only ledger operation failed with exit code $status. Never unlock trading." }
    Write-Host 'Review receipt only. Original data/trading lifecycle, positions and HTTP 423 trading lock remain unchanged.' -ForegroundColor Yellow
} finally { Pop-Location }
