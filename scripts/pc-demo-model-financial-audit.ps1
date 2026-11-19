#Requires -Version 5.1
<#
Read-only OFFLINE model fee/funding evidence audit for dev.3.128.
No Binance API or order call; requires a stopped trader and prior Package A
archive with its matching original data snapshot and stored proposal.
Output is stdout JSON; no modification of accounting or trading files.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [Parameter(Mandatory=$true)][string]$IncidentPath,
    [string]$DataDir='',
    [string]$AssertionPath=''
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$release=(Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw -Encoding UTF8).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $release" }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required.' }
if ([string]::IsNullOrWhiteSpace($DataDir)) { $DataDir=Join-Path $root 'data' }
if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { throw "Missing original data directory: $DataDir" }
if (-not (Test-Path -LiteralPath $IncidentPath -PathType Leaf)) { throw "Missing archived incident: $IncidentPath" }
if ($AssertionPath -and -not (Test-Path -LiteralPath $AssertionPath -PathType Leaf)) { throw "Missing claim: $AssertionPath" }
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
    $listeners=@(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)
    if ($listeners.Count -gt 0) { throw 'Port 8080 is in use; stop server and verify the process before offline inspection.' }
} else {
    $listeners=@(& netstat.exe -ano -p TCP | Where-Object { $_ -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+\d+\s*$' })
    if ($listeners.Count -gt 0) { throw 'Port 8080 is listening; offline inspection denied.' }
}
$argsForGo=@('run','./cmd/demo-close-financial-review', '-model-evidence','-data',$DataDir,'-id',$ClientOrderID,'-incident',$IncidentPath)
if ($AssertionPath) { $argsForGo+=@('-model-assertion',$AssertionPath) }
Push-Location $root
try {
    $old=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & go @argsForGo; $code=$LASTEXITCODE }
    finally { $ErrorActionPreference=$old }
    if ($code -ne 0) { throw "Model financial evidence review BLOCKED (exit=$code). No settlement authorized." }
} finally { Pop-Location }
