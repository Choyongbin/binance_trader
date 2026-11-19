#Requires -Version 5.1
# Offline proof-binding accounting PROPOSAL only. NEVER settles or trades.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidateSet('Show','Prepare')][string]$Action,
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [switch]$ConfirmProposalOnly
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') { throw 'Require dev.3.138' }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required' }
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) { throw 'Stop trader before offline review' }
$goArgs=@('run','./cmd/demo-close-settlement-proposal','-root',$root,'-action',$Action.ToLowerInvariant(),'-client-id',$ClientOrderID)
if ($Action -eq 'Prepare') {
    if (-not $ConfirmProposalOnly) { throw 'Explicit ConfirmProposalOnly is mandatory. It does NOT settle a trade.' }
    $goArgs+=@('-confirm','PROPOSAL_ONLY_NOT_SETTLEMENT')
}
Push-Location $root
try {
    $old=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & go @goArgs; $result=$LASTEXITCODE } finally { $ErrorActionPreference=$old }
    if ($result -ne 0) { throw "Proposal denied: go exit code $result" }
    Write-Warning 'NOT POSTED: no orders, lifecycle, ownership, funding or realized PnL were settled; all trading locks remain.'
} finally { Pop-Location }
