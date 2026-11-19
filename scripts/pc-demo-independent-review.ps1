#Requires -Version 5.1
<#
  dev.3.130 independent OFFLINE financial candidate REVIEW ACK (NOT posting).
  No API keys, Binance requests, payment, settlement permission, or live trading.
  -Action verify-once consumes a one-use nonce in a SEPARATE existing root.
#>
[CmdletBinding()]
param(
  [ValidateSet('intent','verify-once')][string]$Action='intent',
  [Parameter(Mandatory=$true)][string]$RegistryRoot,
  [Parameter(Mandatory=$true)][string]$Manifest,
  [string]$Request='',
  [string]$Attestation='',
  [string]$ReviewerPublicKey='',
  [string]$PinnedPublicKeySHA256='',
  [string]$ReceiptRoot=''
)
$ErrorActionPreference='Stop'
$project=Split-Path -Parent $PSScriptRoot
$release=(Get-Content -LiteralPath (Join-Path $project 'VERSION') -Raw).Trim()
if ($release -cne 'v0.4.11-dev.3.138') {throw "Requires dev.3.138; found $release"}
foreach ($p in @($RegistryRoot,$Manifest)) {if (-not (Test-Path -LiteralPath $p)) {throw "Missing required offline input: $p"}}
$args2=@('run','./cmd/demo-close-independent-review','-action',$Action,
  '-registry',(Resolve-Path -LiteralPath $RegistryRoot).Path,
  '-manifest',(Resolve-Path -LiteralPath $Manifest).Path)
if ($Action -eq 'verify-once') {
  foreach ($p in @($Request,$Attestation,$ReviewerPublicKey,$ReceiptRoot)) {
    if ([string]::IsNullOrWhiteSpace($p) -or -not (Test-Path -LiteralPath $p)) {throw "Missing signed review input/root: $p"}
  }
  if ($PinnedPublicKeySHA256 -cnotmatch '^[0-9a-f]{64}$') {throw 'Independently pinned key SHA256 required'}
  $args2+=@('-request',(Resolve-Path -LiteralPath $Request).Path,
    '-attestation',(Resolve-Path -LiteralPath $Attestation).Path,
    '-reviewer-pubkey',(Resolve-Path -LiteralPath $ReviewerPublicKey).Path,
    '-pin-sha256',$PinnedPublicKeySHA256,
    '-receipt-root',(Resolve-Path -LiteralPath $ReceiptRoot).Path)
} elseif ($Request -ne '' -or $Attestation -ne '' -or $ReviewerPublicKey -ne '' -or $PinnedPublicKeySHA256 -ne '' -or $ReceiptRoot -ne '') {
  throw 'Intent creation cannot accept signature or receipt inputs'
}
Push-Location -LiteralPath $project
try {& go @args2; if ($LASTEXITCODE -ne 0) {throw 'Independent offline review BLOCKED (no financial posting)'}}
finally {Pop-Location}
