#Requires -Version 5.1
<# Offline NON-POSTING evidence reservation only; never touches API keys/Binance. #>
[CmdletBinding()]
param(
  [ValidateSet('inspect','reserve','consistency')][string]$Action='inspect',
  [Parameter(Mandatory=$true)][string]$RegistryRoot,
  [string]$Manifest='',
  [string]$Ack=''
)
$ErrorActionPreference='Stop'
$project=Split-Path -Parent $PSScriptRoot
$release=(Get-Content -LiteralPath (Join-Path $project 'VERSION') -Raw).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $release" }
$registry=Get-Item -LiteralPath $RegistryRoot -Force -ErrorAction Stop
if (-not $registry.PSIsContainer -or ($registry.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
 throw 'RegistryRoot must be an existing real offline directory'
}
$args2=@('run','./cmd/demo-close-global-reservation','-registry',$registry.FullName,'-action',$Action)
if ($Action -eq 'reserve') {
 if ($Ack -cne 'REVIEW_ONLY_NOT_SETTLEMENT' -or -not (Test-Path -LiteralPath $Manifest -PathType Leaf)) {
   throw 'Reserve requires -Manifest and -Ack REVIEW_ONLY_NOT_SETTLEMENT'
 }
 $args2+=@('-manifest',(Resolve-Path -LiteralPath $Manifest).Path,'-ack',$Ack)
} elseif ($Action -eq 'consistency') {
 if ($Ack -ne '' -or -not (Test-Path -LiteralPath $Manifest -PathType Leaf)) {
   throw 'Read-only consistency requires -Manifest and no -Ack'
 }
 $args2+=@('-manifest',(Resolve-Path -LiteralPath $Manifest).Path)
} elseif ($Manifest -ne '' -or $Ack -ne '') { throw 'Inspect cannot accept Manifest/Ack' }
Push-Location -LiteralPath $project
try {
 & go @args2
 if ($LASTEXITCODE -ne 0) { throw 'OFFLINE registry inspect/reserve/consistency BLOCKED; original data untouched' }
} finally { Pop-Location }
