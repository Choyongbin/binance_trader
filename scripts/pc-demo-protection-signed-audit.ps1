#Requires -Version 5.1
# Read-only Binance Futures Demo signed GET audit; NO ORDER/ALGO POST/DELETE.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidatePattern('^bt_[0-9a-f]{28}$')][string]$ClientOrderID,
    [ValidateRange(1,120)][int]$WindowHours = 24
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {
    throw 'Requires verified dev.3.128; no signed requests issued.'
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go 1.23+ is required.' }
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue) {
    throw 'Local trader server must be stopped; no signed requests issued.'
}
$enc = @(Get-ChildItem -LiteralPath $root -Filter '*.enc' -File)
if ($enc.Count -ne 1) { throw 'Exactly one locally stored Demo *.enc is required (2 lines: API key / secret).' }
$data = Join-Path $root 'data'
if (-not (Test-Path -LiteralPath $data -PathType Container)) { throw 'Missing existing data directory; no signed requests issued.' }
$dir = Join-Path $root 'pc-results'
if (-not (Test-Path -LiteralPath $dir)) { New-Item -Path $dir -ItemType Directory | Out-Null }
$name = 'signed-protection-review-' + (Get-Date -Format 'yyyyMMdd_HHmmss_fffffff') + '.json'
$out = Join-Path $dir $name
Push-Location $root
try {
    & go test ./cmd/demo-signed-inspect -run '^TestProtectionSigned' -count=1
    if ($LASTEXITCODE -ne 0) { throw 'Diagnostic Go tests failed; no signed GET.' }
    $savedPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & go run ./cmd/demo-signed-inspect -root $root -out $out -protection-client $ClientOrderID -protection-window-hours $WindowHours
        $status = $LASTEXITCODE
    } finally { $ErrorActionPreference = $savedPreference }
    Write-Host ('Read-only report path (including BLOCKED reports): ' + $out)
    if ($status -ne 0) { throw 'Read-only protection review BLOCKED. Preserve the report, do not send orders or repeat automatically.' }
    Write-Host 'Bounded signed review only. Trading/TP/SL submit/cancel authority: FALSE.' -ForegroundColor Yellow
} finally { Pop-Location }
