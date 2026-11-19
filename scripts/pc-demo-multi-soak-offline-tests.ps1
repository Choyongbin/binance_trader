#Requires -Version 5.1
<# v0.4.11-dev.3.138 — purely offline synthetic tests; no Binance credentials, orders or network. #>
[CmdletBinding()]
param([string]$Root = (Split-Path -Parent $PSScriptRoot))
$ErrorActionPreference = 'Stop'
$release = (Get-Content -LiteralPath (Join-Path $Root 'VERSION') -Raw -Encoding UTF8).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $release" }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go compiler is required' }
Push-Location -LiteralPath $Root
try {
  & go test -count=1 -v ./internal/trading -run '^TestOfflineMultiModelSoak'
  if ($LASTEXITCODE -ne 0) { throw 'Offline soak tests FAILED' }
  & go test -count=1 ./internal/version
  if ($LASTEXITCODE -ne 0) { throw 'Version test FAILED' }
  Write-Host 'OFFLINE SOAK PASS (synthetic time only; trading remains LOCKED)' -ForegroundColor Green
} finally { Pop-Location }
