#Requires -Version 5.1
<# dev.3.132 offline simulated three-ledger commit only. NO real financial posting. #>
[CmdletBinding()]
param(
    [ValidateSet('inspect','advance','recover','resume-next')][string]$Action='inspect',
    [Parameter(Mandatory=$true)][string]$Root,
    [Parameter(Mandatory=$true)][string]$RegistryRoot,
    [Parameter(Mandatory=$true)][string]$Manifest,
    [ValidateSet('','INTENT','LIFECYCLE','OWNERSHIP','FINANCIAL','COMMIT')][string]$Step='',
    [string]$Ack='',
    [int]$ExpectedSteps=-1,
    [string]$ExpectedSHA256=''
)
$ErrorActionPreference='Stop'
$project=Split-Path -Parent $PSScriptRoot
$release=(Get-Content -LiteralPath (Join-Path $project 'VERSION') -Raw).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138, found $release" }
foreach ($p in @($Root,$RegistryRoot)) {
    $fi=Get-Item -LiteralPath $p -Force -ErrorAction Stop
    if (-not $fi.PSIsContainer -or ($fi.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
        throw "Unsafe offline root: $p"
    }
}
if (-not (Test-Path -LiteralPath $Manifest -PathType Leaf)) {throw 'Existing event manifest required'}
if ($Action -eq 'inspect' -or $Action -eq 'recover') {
    if ($Step -ne '' -or $Ack -ne '' -or $ExpectedSteps -ne -1 -or $ExpectedSHA256 -ne '') {throw 'read-only action must not set recovery pins, -Step or -Ack'}
} elseif ($Action -eq 'resume-next') {
    if ($Step -ne '' -or $Ack -cne 'SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT' -or $ExpectedSteps -lt 0 -or $ExpectedSHA256 -notmatch '^[a-f0-9]{64}$') {
        throw 'resume-next requires exact -ExpectedSteps/-ExpectedSHA256 from recover and simulation-only -Ack'
    }
} elseif ($Step -eq '' -or $Ack -cne 'SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT' -or $ExpectedSteps -ne -1 -or $ExpectedSHA256 -ne '') {
    throw 'advance requires -Step and -Ack SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT'
}
$args2=@('run','./cmd/demo-close-mock-financial-commit', '-root',(Resolve-Path -LiteralPath $Root).Path,
    '-registry',(Resolve-Path -LiteralPath $RegistryRoot).Path,
    '-manifest',(Resolve-Path -LiteralPath $Manifest).Path,'-action',$Action)
if ($Action -eq 'advance') {$args2+=@('-step',$Step,'-ack',$Ack)}
if ($Action -eq 'resume-next') {$args2+=@('-ack',$Ack,'-expected-steps',$ExpectedSteps,'-expected-sha256',$ExpectedSHA256)}
Push-Location -LiteralPath $project
try {
    & go @args2
    if ($LASTEXITCODE -ne 0) {throw 'OFFLINE simulation BLOCKED; real ledger never modified'}
} finally {Pop-Location}
