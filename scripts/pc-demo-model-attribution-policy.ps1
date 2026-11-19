#Requires -Version 5.1
<#
Read-only OFFLINE bounded-model cost grouping for v0.4.11-dev.3.138.
Requires a stopped server, incident manifest (2..64) with independent data
snapshots. Does not read API keys, contact Binance, allocate funding or POST.
#>
[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$Manifest)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$release=(Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw -Encoding UTF8).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $release" }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required.' }
$manifestFile=Get-Item -LiteralPath $Manifest -Force -ErrorAction Stop
if ($manifestFile.PSIsContainer -or ($manifestFile.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) { throw 'Manifest must be a regular JSON file' }
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
  $listeners=@(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)
  if ($listeners.Count -gt 0) { throw 'Port 8080 is in use; stop the server and recheck' }
} else {
  $listeners=@(& netstat.exe -ano -p TCP | Where-Object { $_ -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+\d+\s*$' })
  if ($listeners.Count -gt 0) { throw 'Port 8080 listening; offline audit blocked' }
}
Push-Location -LiteralPath $root
try {
  & go run ./cmd/demo-close-cross-event-audit -manifest $manifestFile.FullName -model-policy
  if ($LASTEXITCODE -ne 0) { throw 'Model attribution policy BLOCKED. No settlement permitted.' }
} finally { Pop-Location }
