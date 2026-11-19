#Requires -Version 5.1
# Offline Demo FORENSIC reservation only. NEVER changes ownership/Lifecycle or submits an order.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ClientOrderID,
    [ValidateSet('inspect','reserve')][string]$Action='inspect'
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {throw 'Unexpected release version'}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go compiler required'}
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {throw 'Stop trader server first'}
Push-Location $root
try {
    & go test ./cmd/demo-close-posting-intent
    if ($LASTEXITCODE -ne 0) {throw 'Intent command compilation failed'}
    $arguments=@('run','./cmd/demo-close-posting-intent','-data',(Join-Path $root 'data'),'-client',$ClientOrderID,'-action',$Action)
    if ($Action -eq 'reserve') {
        Write-Host 'This writes a NON-POSTED forensic intent only; it never settles a position.' -ForegroundColor Yellow
        $arguments+=@('-confirm','RESERVE_ONLY_NOT_SETTLEMENT')
    }
    $old=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & go @arguments; $exit=$LASTEXITCODE }
    finally { $ErrorActionPreference=$old }
    if ($exit -ne 0) {throw 'Posting intent blocked; preserve the original data.'}
} finally {Pop-Location}
