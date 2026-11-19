#Requires -Version 5.1
# Binance Futures Demo public market CHECK ONLY. The old unprotected
# DemoOneOrder flow is retired; use product protection/lifecycle work instead.
[CmdletBinding()]
param(
    [switch]$DemoOneOrder,
    [switch]$AcceptNoTPSL,
    [ValidateRange(30,600)][int]$Seconds = 180
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
# Explicit release guard; fail closed rather than using a stale PC test.
$expected = 'v0.4.11-dev.3.138'
$actual = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
if ($actual -ne $expected) { throw "Wrong release ($actual). Refusing test." }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go compiler is required.' }
if ($DemoOneOrder -or $AcceptNoTPSL) { throw 'DemoOneOrder retired: this script cannot submit unprotected Demo orders. Market check only.' }
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {
    throw 'Local trading server still listens on port 8080. Run stop.ps1 first, then inspect the server process.'
}
$resultsDir = Join-Path $root 'pc-results'
New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$mode = 'market-readonly'
$logPath = Join-Path $resultsDir ("tonight-" + $mode + '-' + $stamp + '.log')
$resultPath = Join-Path $resultsDir ("tonight-" + $mode + '-' + $stamp + '.json')
Push-Location $root
try {
    Write-Host '[1/2] Offline regression tests (NO ORDERS)' -ForegroundColor Cyan
    & go test -count=1 ./cmd/demo-auto-pc ./internal/exchange ./internal/trading
    if ($LASTEXITCODE -ne 0) { throw 'Offline Go tests failed; NO ORDERS.' }
    Write-Host '[2/2] Demo check. Market mode never reads .enc or submits an order.' -ForegroundColor Cyan
    $argsList = @('run','./cmd/demo-auto-pc','-root',$root,'-mode','market')
    # On Windows PowerShell 5.1, native stderr with 2>&1 can become a
    # terminating NativeCommandError under ErrorActionPreference=Stop.
    # Always collect the failed program output and write a result JSON.
    $nativePref = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & go @argsList 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $nativePref
    }
    ($output | Out-String) | Set-Content -LiteralPath $logPath -Encoding UTF8
    $output | ForEach-Object { Write-Host $_ }
    $classification = if ($code -eq 0) { 'MARKET_PASS_NO_ORDERS' } else { 'MARKET_FAIL' }
    [ordered]@{
        at_utc = (Get-Date).ToUniversalTime().ToString('o')
        release = $actual
        mode = $mode
        classification = $classification
        exit_code = $code
        note = 'Public Demo market-only check; never submits orders. Not proof of account health or TP/SL.'
        log = $logPath
    } | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $resultPath -Encoding UTF8
    Write-Host ("Result file: " + $resultPath)
    if ($code -ne 0) {
        throw 'Demo session was STOPPED or not ready. Inspect result/log and verify signed Demo positions before retrying.'
    }
    Write-Host 'MARKET PASS: NO ORDERS.' -ForegroundColor Green

} finally {
    Pop-Location
}
